package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/semanticbench"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	scalingGraphBatchSize = 64
	scalingIndexLabel     = "Document"
	scalingOtherLabel     = "Excluded"
	scalingDimensions     = 8
)

func (r *DriverRuntime) executeSemanticScaling(ctx context.Context, target map[string]any) (semanticbench.Report, error) {
	plan, err := spec.ParseSemanticScalingTarget(target)
	if err != nil {
		return semanticbench.Report{}, err
	}
	resources := r.Resources
	if resources == nil || resources.Semantic == nil || resources.RunID == "" {
		return semanticbench.Report{}, errors.New("semantic scaling requires a provisioned benchmark scenario")
	}
	report := semanticbench.NewReport(resources.RunID, plan)
	finish := func(reason string) (semanticbench.Report, error) {
		report.Reason = reason
		report.Finish()
		return report, nil // Inconclusive measurement, not a performance failure.
	}
	ctx, cancel := context.WithTimeout(ctx, plan.Timeout.Duration)
	defer cancel()
	report.Environment, err = r.scalingEnvironment(ctx)
	if err != nil {
		return finish("environment-identity-unavailable")
	}

	setup, stopSetup := context.WithTimeout(ctx, plan.SetupTimeout.Duration)
	admin, err := mycel.DialAdmin(setup, mycel.Config{Addr: resources.AdminAddr, Username: resources.AdminUsername, Password: resources.AdminPassword, ClientName: "mycel-lab-scaling-admin"})
	if err != nil {
		stopSetup()
		return finish("admin-connection-unavailable")
	}
	defer admin.Close()
	maxClients := semanticbench.Concurrencies()[1]
	clients := make([]*mycel.Client, 0, maxClients)
	if len(resources.Assignments) != 1 {
		stopSetup()
		return finish("search-actor-unavailable")
	}
	assignment := resources.Assignments[0]
	if assignment.ActorID != resources.Semantic.TargetActorID || assignment.DaemonAddr == "" || assignment.Username == "" || assignment.Password == "" {
		stopSetup()
		return finish("search-actor-unavailable")
	}
	for i := 0; i < maxClients; i++ {
		// Each connection logs in separately so rotating a shared refresh token
		// can't invalidate another worker. All use the same scoped principal.
		client, err := mycel.Dial(setup, mycel.Config{Addr: assignment.DaemonAddr, Username: assignment.Username, Password: assignment.Password, ClientName: "mycel-lab-scaling-client"})
		if err != nil {
			stopSetup()
			return finish("search-connection-unavailable")
		}
		defer client.Close()
		clients = append(clients, client)
	}
	stopSetup()

	var revision int64
	var seeded, fixedIndex int
	for i := range report.Points {
		p := &report.Points[i]
		if ctx.Err() != nil {
			return finish("experiment-budget-expired")
		}
		started := time.Now()
		pointCtx, stopPoint := context.WithTimeout(ctx, plan.SetupTimeout.Duration)
		wantIndexed := p.Size
		if plan.Sweep == spec.ScaleGraph && i > 0 {
			wantIndexed = fixedIndex
		}
		counts, generation, reason := prepareScalingPoint(pointCtx, admin, clients[0], assignment, resources, seeded, p.Size, wantIndexed, &revision)
		if pointCtx.Err() != nil && reason != "" {
			reason = "point-setup-budget-expired"
		}
		stopPoint()
		setupMS := semanticbench.Milliseconds(time.Since(started))
		p.SetupMS, p.Counts, p.Reason = &setupMS, counts, reason
		if reason != "" {
			// No later point is valid after a failed mutation (which may have
			// committed) or unverified setup. Keep its plan and prior samples.
			break
		}
		// First-point setup creates the deferred rule. Build the requests only
		// after its ID and selected binding have been confirmed by the index read.
		requests, err := scalingQueries(resources.Semantic)
		if err != nil {
			p.Reason = "unbound-search-route"
			break
		}
		seeded, fixedIndex = p.Size, wantIndexed
		for j, concurrency := range semanticbench.Concurrencies() {
			workloadCtx, stopWorkload := context.WithTimeout(ctx, plan.WorkloadTimeout.Duration)
			// Refresh by logging in outside the timed window, without replacing
			// established connections. Default daemon auth TTL is 15 minutes.
			if !loginScalingClients(workloadCtx, clients[:concurrency], assignment) {
				p.Workloads[j] = semanticbench.Summarize(concurrency, plan.Samples, 0, nil, "workload-login-unavailable")
				stopWorkload()
				continue
			}
			query := func(ctx context.Context, worker, axis int) semanticbench.Sample {
				return callScalingSearch(ctx, clients[worker], requests[axis], axis, generation, scalingCandidateCount(wantIndexed, axis))
			}
			p.Workloads[j] = measureScalingWorkload(workloadCtx, concurrency, plan.Samples, plan.RequestTimeout.Duration, query)
			stopWorkload()
		}
	}
	return finish("")
}

func loginScalingClients(ctx context.Context, clients []*mycel.Client, assignment provision.ActorAssignment) bool {
	for _, client := range clients {
		if _, err := client.Login(ctx, assignment.Username, assignment.Password); err != nil {
			return false
		}
	}
	return true
}

func (r *DriverRuntime) scalingEnvironment(ctx context.Context) (semanticbench.Environment, error) {
	cluster := r.Scenario.Cluster
	profile := r.Scenario.ProviderProfile
	if profile == nil || profile.Profile.Capabilities.Embedding == nil {
		return semanticbench.Environment{}, errors.New("benchmark provider fixture unavailable")
	}
	identity := semanticbench.Environment{
		Image: r.Environment.Metadata["imageOverride"], Driver: r.Environment.Driver,
		DaemonCount: cluster.Nodes, RaftPartitions: cluster.Raft.PartitionCount, RaftReplicaFactor: cluster.Raft.ReplicaFactor,
		ClientPlatform: runtime.GOOS + "/" + runtime.GOARCH, ClientCPUs: runtime.NumCPU(), VectorBackend: "mycel-file/exact",
		Provider: profile.Profile.Metadata.Name, ProviderSHA256: profile.SourceSHA256,
		VectorDimension: profile.Profile.Capabilities.Embedding.Dimensions,
	}
	if !env.IsDigestPinnedImageReference(identity.Image) || identity.Image != cluster.Image || identity.Driver != "compose" {
		return identity, errors.New("benchmark requires a digest-pinned Compose image")
	}
	for _, path := range filepath.SplitList(r.Environment.Metadata["composeFiles"]) {
		raw, err := os.ReadFile(path)
		if err != nil {
			return identity, err
		}
		sum := sha256.Sum256(raw)
		identity.ComposeSHA256 = append(identity.ComposeSHA256, hex.EncodeToString(sum[:]))
	}
	if len(identity.ComposeSHA256) == 0 {
		return identity, errors.New("compose inputs are unavailable")
	}
	infoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	const format = `{"cpus":{{json .NCPU}},"memory":{{json .MemTotal}},"arch":{{json .Architecture}},"version":{{json .ServerVersion}},"id":{{json .ID}}}`
	result, err := (executil.LocalRunner{}).Run(infoCtx, "docker", "info", "--format", format)
	var info struct {
		CPUs              int
		Memory            int64
		Arch, Version, ID string
	}
	if err != nil || json.Unmarshal([]byte(result.Stdout), &info) != nil || info.CPUs < 1 || info.Memory <= 0 || len(info.ID) == 0 || len(info.ID) > 200 || !safeMachineField(info.Arch) || !safeMachineField(info.Version) {
		return identity, errors.New("docker resource identity unavailable")
	}
	identity.DockerCPUs, identity.DockerMemory, identity.DockerArch, identity.DockerVersion = info.CPUs, info.Memory, info.Arch, info.Version
	engine := sha256.Sum256([]byte(info.ID))
	identity.DockerEngineSHA256 = hex.EncodeToString(engine[:])
	return identity, nil
}

var machineField = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.+-]{0,63}$`)

func safeMachineField(s string) bool { return machineField.MatchString(s) }

func prepareScalingPoint(ctx context.Context, admin *mycel.AdminClient, client *mycel.Client, assignment provision.ActorAssignment, resources *provision.ScenarioResources, seeded, size, indexed int, revision *int64) (*semanticbench.Counts, uint64, string) {
	if err := seedScalingGraph(ctx, client, assignment, seeded, size, indexed, revision); err != nil {
		return nil, 0, "graph-seeding-unconfirmed"
	}
	semantic := resources.Semantic
	if semantic.SemanticRuleID == "" {
		// Create a deferred rule after the first seed. Normal explicit dirty
		// work covers later points; the initial pinned backfill below covers
		// graph commits predating rule creation.
		_, _, err := executeSemanticRuleMutation(ctx, func(probeCtx context.Context) error {
			return provision.ValidateSemanticRuleForCreate(probeCtx, admin, resources)
		}, func(mutationCtx context.Context) error {
			return provision.CreateSemanticRule(mutationCtx, admin, resources)
		})
		if err != nil {
			return nil, 0, "semantic-rule-create-unconfirmed"
		}
		if _, err := executeSemanticEvent(ctx, resources, "semantic-backfill", map[string]any{"limit": indexed, "expectedSelected": indexed, "minimumGenerated": indexed, "force": true}); err != nil {
			return nil, 0, "initial-backfill-unconfirmed"
		}
	}
	proof, err := awaitScopedSemanticMaintenance(ctx, admin, semantic, semantic.MaintenancePollInterval, nil)
	if err != nil || !proof.Converged || !proof.FreshnessProved || len(proof.Observations) == 0 {
		return nil, 0, "index-not-fresh"
	}
	ready := proof.Observations[len(proof.Observations)-1].Reliability
	if ready == nil || ready.FreshThroughGraphRevision != uint64(*revision) {
		return nil, 0, "index-frontier-does-not-match-seed"
	}
	counts, err := readScalingCounts(ctx, client, assignment, semantic, size)
	if err != nil {
		return nil, 0, "counts-unverified"
	}
	if counts.Nodes != size || counts.Edges != 0 || counts.Records != indexed || counts.Vectors != indexed {
		return &counts, 0, "unexpected-graph-or-index-counts"
	}
	return &counts, ready.SemanticGeneration, ""
}

func scalingNodeID(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", i+1) }

func scalingNode(i, indexed int) *clientv1.NodeCreate {
	label, text := scalingIndexLabel, "axis-4 axis-4"
	anchors := spec.ScalingAxes * spec.ScalingTopK
	switch {
	case i >= indexed:
		label = scalingOtherLabel
	case i < anchors:
		axis := i / spec.ScalingTopK
		text = fmt.Sprintf("axis-%d axis-%d", axis, axis)
	default:
		// Score ~0.707 on one axis and zero on the other three. Matching
		// candidates grow with the index but stay below v0.26's 10k
		// authorized-candidate window, even at the 30k size point.
		text = fmt.Sprintf("axis-4 axis-%d", (i-anchors)%spec.ScalingAxes)
	}
	return &clientv1.NodeCreate{NodeId: proto.String(scalingNodeID(i)), Labels: []string{label}, Payload: &structpb.Struct{Fields: map[string]*structpb.Value{"text": structpb.NewStringValue(text)}}}
}

func scalingCandidateCount(indexed, axis int) int {
	filler := indexed - spec.ScalingAxes*spec.ScalingTopK
	return spec.ScalingTopK + (filler+spec.ScalingAxes-1-axis)/spec.ScalingAxes
}

func seedScalingGraph(ctx context.Context, client *mycel.Client, a provision.ActorAssignment, from, to, indexed int, revision *int64) error {
	session, err := client.OpenSession(ctx, a.SpaceID, a.DomainID)
	if err != nil {
		return err
	}
	defer closeScalingSession(ctx, client, session)
	for start := from; start < to; start += scalingGraphBatchSize {
		operations := make([]*clientv1.GraphOperation, 0, scalingGraphBatchSize)
		for i := start; i < min(start+scalingGraphBatchSize, to); i++ {
			operations = append(operations, &clientv1.GraphOperation{Operation: &clientv1.GraphOperation_CreateNode{CreateNode: scalingNode(i, indexed)}})
		}
		tx, err := client.BeginReadWriteTransaction(ctx, session)
		if err != nil {
			return err
		}
		call, cancel := client.AuthCallContext(ctx)
		applied, err := client.Graph.ApplyGraphOperations(call, &clientv1.ApplyGraphOperationsRequest{TransactionId: tx, Operations: operations})
		cancel()
		if err != nil || len(applied.GetResults()) != len(operations) {
			return errors.New("fixture apply unconfirmed")
		}
		commit, err := client.CommitTransactionResult(ctx, tx)
		if err != nil || commit.GetCommittedRevision() <= *revision {
			return errors.New("fixture commit unconfirmed")
		}
		*revision = commit.GetCommittedRevision()
	}
	return nil
}

func closeScalingSession(ctx context.Context, client *mycel.Client, session string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	_ = client.CloseSession(ctx, session)
}

func readScalingCounts(ctx context.Context, client *mycel.Client, a provision.ActorAssignment, semantic *provision.SemanticResources, maxNodes int) (semanticbench.Counts, error) {
	var counts semanticbench.Counts
	session, err := client.OpenSession(ctx, a.SpaceID, a.DomainID)
	if err != nil {
		return counts, err
	}
	defer closeScalingSession(ctx, client, session)
	tx, err := client.BeginReadOnlyTransaction(ctx, session)
	if err != nil {
		return counts, err
	}
	seen := make(map[string]bool, maxNodes)
	for page := ""; ; {
		call, cancel := client.AuthCallContext(ctx)
		r, err := client.Graph.ListNodes(call, &clientv1.ListNodesRequest{TransactionId: tx, PageSize: 500, PageToken: page})
		cancel()
		if err != nil || r == nil {
			return counts, errors.New("list graph nodes failed")
		}
		for _, node := range r.GetNodes() {
			id := node.GetNodeId()
			if id == "" || node.GetDomainId() != a.DomainID || seen[id] {
				return counts, errors.New("invalid graph node enumeration")
			}
			seen[id] = true
		}
		next := r.GetNextPageToken()
		if next == "" {
			break
		}
		if len(r.GetNodes()) == 0 || len(seen) >= maxNodes {
			return counts, errors.New("graph node enumeration exceeded planned bounds")
		}
		page = next
	}
	counts.Nodes = len(seen)
	call, cancel := client.AuthCallContext(ctx)
	edges, err := client.Graph.ListEdges(call, &clientv1.ListEdgesRequest{TransactionId: tx, PageSize: 1})
	cancel()
	if err != nil || edges == nil || len(edges.GetEdges()) != 0 || edges.GetNextPageToken() != "" {
		return counts, errors.New("expected an edge-free graph fixture")
	}
	if err := client.CloseTransaction(ctx, tx); err != nil {
		return counts, err
	}
	call, cancel = client.AuthCallContext(ctx)
	rules, err := client.Semantic.ListSemanticRules(call, &clientv1.ListSemanticRulesRequest{SpaceId: a.SpaceID, DomainId: a.DomainID, PageSize: 2})
	cancel()
	if err != nil || len(rules.GetRules()) != 1 || rules.GetNextPageToken() != "" {
		return counts, errors.New("expected exactly one semantic rule")
	}
	rule := rules.GetRules()[0]
	if rule.GetSemanticRuleId() != semantic.SemanticRuleID || rule.GetDomainId() != a.DomainID || !rule.GetEnabled() || len(rule.GetBindings()) != 1 {
		return counts, errors.New("unexpected semantic rule identity")
	}
	binding := rule.GetBindings()[0]
	index := binding.GetSearchIndex()
	if binding.GetKey() != semantic.EmbeddingBindingKey || binding.GetVectorStoreId() != semantic.VectorStoreID || !binding.GetEnabled() || index.GetState() != clientv1.SearchIndexState_SEARCH_INDEX_STATE_READY || index.GetLiveRecordCount() <= 0 {
		return counts, errors.New("semantic index not ready for the selected binding")
	}
	// This fixture uses a single self-projection binding, with one vector
	// per live record in the exact mycel-file index. No chunks or tombstones.
	counts.Records, counts.Vectors = int(index.GetLiveRecordCount()), int(index.GetLiveRecordCount())
	return counts, nil
}

func scalingQueries(semantic *provision.SemanticResources) ([spec.ScalingAxes]*clientv1.SemanticSearchRequest, error) {
	var requests [spec.ScalingAxes]*clientv1.SemanticSearchRequest
	if semantic == nil || strings.TrimSpace(semantic.SpaceID) == "" || strings.TrimSpace(semantic.DomainID) == "" || strings.TrimSpace(semantic.SemanticRuleID) == "" || strings.TrimSpace(semantic.EmbeddingBindingKey) == "" {
		return requests, errors.New("semantic scaling requires an exact scope, created rule ID, and binding key")
	}
	for axis := range requests {
		requests[axis] = &clientv1.SemanticSearchRequest{SpaceId: semantic.SpaceID, DomainId: semantic.DomainID, SemanticRuleId: proto.String(semantic.SemanticRuleID), EmbeddingBindingKey: proto.String(semantic.EmbeddingBindingKey), Query: fmt.Sprintf("axis-%d", axis), Limit: spec.ScalingTopK, MinScore: proto.Float64(spec.ScalingMinScore)}
	}
	return requests, nil
}

func callScalingSearch(ctx context.Context, client *mycel.Client, req *clientv1.SemanticSearchRequest, axis int, generation uint64, candidates int) semanticbench.Sample {
	call, cancel := client.AuthCallContext(ctx)
	started := time.Now()
	response, err := client.Semantic.SemanticSearch(call, req)
	sample := semanticbench.Sample{Elapsed: time.Since(started)}
	cancel()
	switch {
	case errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded:
		sample.Outcome, sample.Reason = semanticbench.Timeout, "rpc-DeadlineExceeded"
	case err != nil:
		sample.Outcome, sample.Reason = semanticbench.RPCError, "rpc-"+status.Code(err).String()
	default:
		if reason := scalingResponseMismatch(response, req, axis, generation, candidates); reason != "" {
			sample.Outcome, sample.Reason = semanticbench.UnexpectedResponse, reason
		}
	}
	return sample
}

func scalingResponseMismatch(response *clientv1.SemanticSearchResponse, req *clientv1.SemanticSearchRequest, axis int, generation uint64, candidates int) string {
	switch {
	case response == nil:
		return "missing-search-response"
	case len(response.GetWarnings()) != 0:
		return "search-response-warning"
	case len(response.GetResults()) != spec.ScalingTopK:
		return "unexpected-result-count"
	case int(response.GetExaminedSemanticCandidates()) != candidates || int(response.GetEligibleSemanticCandidates()) != candidates:
		return "unexpected-candidate-count"
	case (response.GetNextPageToken() != "") != (candidates > spec.ScalingTopK):
		return "unexpected-page-continuation"
	}
	for i, hit := range response.GetResults() {
		want := scalingNodeID(axis*spec.ScalingTopK + i)
		p := hit.GetProvenance()
		switch {
		case hit.GetNodeId() != want || hit.GetNode().GetNodeId() != want:
			return "unexpected-result-identity-or-order"
		case hit.GetNode().GetDomainId() != req.GetDomainId() || hit.GetSemanticRuleId() != req.GetSemanticRuleId() || hit.GetEmbeddingBindingKey() != req.GetEmbeddingBindingKey():
			return "unexpected-result-scope"
		case hit.GetRecordId() == "" || p.GetDimensions() != scalingDimensions || p.GetSemanticGeneration() != generation:
			return "unexpected-result-provenance"
		case math.IsNaN(hit.GetScore()) || math.Abs(hit.GetScore()-1) > 1e-9:
			return "unexpected-result-score"
		case hit.GetNode().GetPayload().GetFields()["text"].GetStringValue() != req.GetQuery()+" "+req.GetQuery():
			return "unexpected-result-payload"
		}
	}
	return ""
}

func measureScalingWorkload(ctx context.Context, clients, goal int, requestTimeout time.Duration, query func(context.Context, int, int) semanticbench.Sample) semanticbench.Workload {
	// Warm up each connection with the entire mix before opening the timed
	// window. A warm-up error isn't a timed sample or a successful benchmark.
	warm := semanticbench.SummarizeWarmup(runScalingQueries(ctx, clients, semanticbench.WarmupsPerClient, requestTimeout, query))
	warmupReason := ""
	if ctx.Err() != nil {
		warmupReason = "warmup-budget-expired"
	} else if len(warm.Failures) != 0 {
		warmupReason = "warmup-response-unverified"
	}
	if warmupReason != "" {
		w := semanticbench.Summarize(clients, goal, 0, nil, warmupReason)
		w.Warmup = warm
		return w
	}
	started := time.Now()
	samples := runScalingQueries(ctx, clients, goal/clients, requestTimeout, query)
	var reason string
	if ctx.Err() != nil {
		reason = "measurement-budget-expired"
	}
	w := semanticbench.Summarize(clients, goal, time.Since(started), samples, reason)
	w.Warmup = warm
	return w
}

func runScalingQueries(ctx context.Context, clients, perClient int, requestTimeout time.Duration, query func(context.Context, int, int) semanticbench.Sample) []semanticbench.Sample {
	batches := make([][]semanticbench.Sample, clients)
	var workers sync.WaitGroup
	ready := make(chan struct{})
	for client := range batches {
		workers.Go(func() {
			<-ready
			samples := make([]semanticbench.Sample, 0, perClient)
			for i := 0; i < perClient && ctx.Err() == nil; i++ {
				call, cancel := context.WithTimeout(ctx, requestTimeout)
				samples = append(samples, query(call, client, (client+i)%spec.ScalingAxes))
				cancel()
			}
			batches[client] = samples
		})
	}
	close(ready)
	workers.Wait()
	out := make([]semanticbench.Sample, 0, clients*perClient)
	for _, batch := range batches {
		out = append(out, batch...)
	}
	return out
}
