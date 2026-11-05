package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (p *maintenanceHealthProbe) GetSemanticRule(ctx context.Context, _ *adminv1.GetSemanticRuleRequest) (*adminv1.GetSemanticRuleResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.ruleCalls++
	n := p.ruleCalls
	p.mu.Unlock()
	return p.rule(ctx, n)
}

func disabledQuiescenceFixture(t *testing.T) (context.Context, *mycel.AdminClient, *provision.SemanticResources, *maintenanceHealthProbe, *adminv1.GetSemanticReliabilityStatusResponse, map[string]any) {
	t.Helper()
	ctx, admin, resources, p, proof := healthProbeFixture(t)
	resources.EvidenceValues["before-disable-generation"] = "1"
	resources.EvidenceValues["before-disable-fresh"] = "7"
	resources.EvidenceValues["disabled-node-revision"] = "8"
	proof.LatestObservedGraphRevision = 8
	proof.AnalyzedThroughGraphRevision, proof.FreshThroughGraphRevision = 7, 7
	original := p.rule
	p.rule = func(ctx context.Context, n int) (*adminv1.GetSemanticRuleResponse, error) {
		r, err := original(ctx, n)
		r.Rule.Enabled = false
		return r, err
	}
	target := map[string]any{"completion": "disabled-quiescent", "expectGenerationFrom": "before-disable-generation", "expectFreshRevisionFrom": "before-disable-fresh", "expectRevisionFrom": "disabled-node-revision"}
	return ctx, admin, resources, p, proof, target
}

// Authenticated synthetic public RPCs reproduce the retained 8/7/7 boundary;
// this is a harness regression, not daemon/runtime acceptance.
func TestSemanticDisabledQuiescencePreservesPausedFrontierReadOnly(t *testing.T) {
	ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
	result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
	if err != nil || !result.Converged || result.FreshnessProved || result.Completion != "disabled-quiescent" || len(result.Observations) != 1 {
		t.Fatalf("disabled quiescence must preserve—not advance—freshness: %+v %v", result, err)
	}
	o := result.Observations[0]
	if !o.ReadOnly || !o.DisabledRuleBefore.Observed || o.DisabledRuleBefore.Enabled || !o.DisabledRuleAfter.Observed || o.DisabledRuleAfter.Enabled || o.Reliability.FreshThroughGraphRevision != 7 || o.Reliability.LatestObservedGraphRevision != 8 {
		t.Fatal("paused certificate was fabricated or rule state was not bracketed")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.analyzeCalls != 0 || p.processCalls != 0 || p.ruleCalls != 2 {
		t.Fatal("disabled quiescence must use only bracketed public reads")
	}
}

func TestSemanticDisabledQuiescenceWaitsForAcknowledgedCommitReadOnly(t *testing.T) {
	ctx, admin, resources, p, proof, target := disabledQuiescenceFixture(t)
	p.reliability = func(_ context.Context, n int) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
		r := proto.Clone(proof).(*adminv1.GetSemanticReliabilityStatusResponse)
		if n == 1 {
			r.LatestObservedGraphRevision = 7
		}
		return r, nil
	}
	result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
	if err != nil || !result.Converged || result.FreshnessProved || len(result.Observations) != 2 {
		t.Fatal("unobserved acknowledged commit passed or did not converge read-only")
	}
	if result.Observations[0].Reliability.LatestObservedGraphRevision != 7 || result.Observations[1].Reliability.LatestObservedGraphRevision != 8 {
		t.Fatal("captured commit was not positively observed")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.analyzeCalls+p.processCalls != 0 {
		t.Fatal("incomplete read authorized maintenance")
	}
}

func TestSemanticDisabledQuiescenceCannotRelaxDefaultIndexedFreshness(t *testing.T) {
	for _, target := range []map[string]any{nil, {"completion": "indexed"}} {
		ctx, admin, resources, p, _, _ := disabledQuiescenceFixture(t)
		original := p.rule
		p.rule = func(ctx context.Context, n int) (*adminv1.GetSemanticRuleResponse, error) {
			r, err := original(ctx, n)
			r.Rule.Enabled = true
			return r, err
		}
		resources.MaintenanceTimeout = 100 * time.Millisecond
		result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
		if !errors.Is(err, context.DeadlineExceeded) || result.Converged || result.FreshnessProved || result.Completion != "indexed" {
			t.Fatal("indexed completion accepted the disabled 8/7/7 lag")
		}
	}
}

func TestSemanticDisabledQuiescenceRejectsMissingOrInvalidCapturesBeforeRPC(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "private invalid capture", "18446744073709551616"} {
		ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
		resources.EvidenceValues["before-disable-fresh"] = value
		result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
		if err == nil || result.Converged || len(result.Observations) != 0 || strings.Contains(err.Error(), "private") {
			t.Fatal("invalid paused capture passed, leaked or reached RPCs")
		}
		p.mu.Lock()
		if p.ruleCalls+p.analyzeCalls+p.processCalls+p.statusCalls+p.reliabilityCalls != 0 {
			t.Error("invalid capture reached RPCs")
		}
		p.mu.Unlock()
	}
	ctx, admin, resources, _, _, target := disabledQuiescenceFixture(t)
	resources.EvidenceValues["disabled-node-revision"] = "7"
	if r, err := awaitSemanticMaintenance(ctx, admin, resources, target); err == nil || r.Converged {
		t.Fatal("quiescence accepted no acknowledged post-disable revision")
	}
}

func TestSemanticDisabledQuiescenceRejectsUnknownModesAndMissingReferences(t *testing.T) {
	for _, target := range []map[string]any{{"completion": "disabled"}, {"completion": false}, {"completion": "disabled-quiescent"}, {"completion": "indexed", "expectFreshRevisionFrom": "before-disable-fresh"}, {"requireFreshAtLatest": false}} {
		ctx, admin, resources, p, _, _ := disabledQuiescenceFixture(t)
		if result, err := awaitSemanticMaintenance(ctx, admin, resources, target); err == nil || result.Converged {
			t.Fatal("malformed completion mode silently weakened freshness")
		}
		p.mu.Lock()
		if p.ruleCalls+p.statusCalls+p.analyzeCalls != 0 {
			t.Error("invalid target reached RPCs")
		}
		p.mu.Unlock()
	}
}

func TestSemanticDisabledQuiescenceRequiresExactPublicDisabledRule(t *testing.T) {
	cases := map[string]func(*adminv1.GetSemanticRuleResponse){
		"missing-rule":     func(r *adminv1.GetSemanticRuleResponse) { r.Rule = nil },
		"enabled":          func(r *adminv1.GetSemanticRuleResponse) { r.Rule.Enabled = true },
		"space":            func(r *adminv1.GetSemanticRuleResponse) { r.Rule.SpaceId = "private foreign space" },
		"domain":           func(r *adminv1.GetSemanticRuleResponse) { r.Rule.DomainId = "private foreign domain" },
		"rule":             func(r *adminv1.GetSemanticRuleResponse) { r.Rule.SemanticRuleId = "private foreign rule" },
		"binding":          func(r *adminv1.GetSemanticRuleResponse) { r.Rule.Embeddings = nil },
		"disabled-binding": func(r *adminv1.GetSemanticRuleResponse) { r.Rule.Embeddings[0].Enabled = false },
		"duplicate-binding": func(r *adminv1.GetSemanticRuleResponse) {
			r.Rule.Embeddings = append(r.Rule.Embeddings, proto.Clone(r.Rule.Embeddings[0]).(*adminv1.SemanticEmbeddingBinding))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
			original := p.rule
			p.rule = func(ctx context.Context, n int) (*adminv1.GetSemanticRuleResponse, error) {
				r, err := original(ctx, n)
				mutate(r)
				return r, err
			}
			result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
			raw, _ := json.Marshal(result)
			if err == nil || result.Converged || len(result.Observations) != 1 || strings.Contains(string(raw), "private") || strings.Contains(err.Error(), "private") {
				t.Fatal("unsupported rule proof passed/leaked")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.ruleCalls != 1 || p.analyzeCalls+p.processCalls+p.statusCalls+p.reliabilityCalls != 0 {
				t.Fatal("invalid disabled proof continued or mutated")
			}
		})
	}
}

func TestSemanticDisabledQuiescenceRejectsReenableBetweenReads(t *testing.T) {
	ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
	original := p.rule
	p.rule = func(ctx context.Context, n int) (*adminv1.GetSemanticRuleResponse, error) {
		r, err := original(ctx, n)
		if n == 2 {
			r.Rule.Enabled = true
		}
		return r, err
	}
	result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
	if err == nil || result.Converged || result.FreshnessProved || !result.Observations[0].DisabledRuleAfter.Enabled {
		t.Fatal("re-enable during the bracket became disabled quiescence")
	}
}

func TestSemanticDisabledQuiescencePreservesAllScopedFences(t *testing.T) {
	cases := map[string]func(*adminv1.GetSemanticReliabilityStatusResponse){
		"scope":             func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.SpaceId = "private foreign scope" },
		"generation":        func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.SemanticGeneration++ },
		"latest":            func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.LatestObservedGraphRevision++ },
		"analyzed-advanced": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.AnalyzedThroughGraphRevision++ },
		"fresh-advanced": func(r *adminv1.GetSemanticReliabilityStatusResponse) {
			r.AnalyzedThroughGraphRevision++
			r.FreshThroughGraphRevision++
		},
		"fresh-regressed": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.FreshThroughGraphRevision-- },
		"gaps": func(r *adminv1.GetSemanticReliabilityStatusResponse) {
			r.FreshThroughGraphRevision = 6
			r.UnresolvedGapCount = 1
			r.OldestUnresolvedGraphRevision = 7
			r.RebuildRequired = true
		},
		"degraded":        func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.Degraded = true },
		"capacity":        func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.CapacityState = "exhausted" },
		"missing-limits":  func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.ConfiguredFrontierLimit = 0 },
		"missing-rebuild": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.Rebuild = nil },
		"required-rebuild": func(r *adminv1.GetSemanticReliabilityStatusResponse) {
			r.Rebuild.State = "required"
			r.RebuildRequired = true
		},
		"running-rebuild": func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.Rebuild.State = "running" },
		"failed-rebuild":  func(r *adminv1.GetSemanticReliabilityStatusResponse) { r.Rebuild.State = "failed" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, admin, resources, p, proof, target := disabledQuiescenceFixture(t)
			mutate(proof)
			result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
			raw, _ := json.Marshal(result)
			if err == nil || result.Converged || len(result.Observations) != 1 || strings.Contains(string(raw), "private") {
				t.Fatal("disabled mode bypassed a current scope fence")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.analyzeCalls+p.processCalls != 0 {
				t.Fatal("disabled rejection invoked maintenance")
			}
		})
	}
}

func TestSemanticDisabledQuiescenceStillRequiresClearNodeHealthAndEmptyQueues(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
		resources.MaintenanceTimeout = 100 * time.Millisecond
		p.node = func(n int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
			if persistent || n == 1 {
				return transientMaintenanceNode(), nil
			}
			return clearMaintenanceNode(), nil
		}
		result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
		if persistent {
			if !errors.Is(err, context.DeadlineExceeded) || result.Converged {
				t.Fatal("persistent node degradation passed")
			}
		} else if err != nil || !result.Converged || len(result.Observations) != 2 {
			t.Fatal("known transient did not clear within finite read-only wait")
		}
		if result.FreshnessProved {
			t.Fatal("paused scope claimed full freshness")
		}
		p.mu.Lock()
		if p.analyzeCalls+p.processCalls != 0 {
			t.Error("paused wait ran maintenance")
		}
		p.mu.Unlock()
	}
	for _, permanent := range []bool{false, true} {
		ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
		p.node = func(n int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
			r := clearMaintenanceNode()
			if permanent {
				r.QueueDepthFailedPermanent = 1
			} else if n == 1 {
				r.QueueDepthPending = 1
			}
			return r, nil
		}
		result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
		if permanent {
			if err == nil || result.Converged {
				t.Fatal("permanent queued work passed")
			}
		} else if err != nil || !result.Converged || len(result.Observations) != 2 {
			t.Fatal("pending queue was ignored or did not drain read-only")
		}
	}
	ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
	p.node = func(int) (*adminv1.GetSemanticMaintenanceStatusResponse, error) {
		r := transientMaintenanceNode()
		r.DegradedReason = "private unknown error"
		return r, nil
	}
	if r, err := awaitSemanticMaintenance(ctx, admin, resources, target); err == nil || r.Converged || len(r.Observations) != 1 {
		t.Fatal("unknown degraded health was ignored")
	}
}

func TestSemanticDisabledQuiescenceRuleRPCsAreBoundedAndNeverRetried(t *testing.T) {
	for _, stage := range []int{1, 2} {
		ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
		resources.MaintenanceTimeout = 100 * time.Millisecond
		original := p.rule
		p.rule = func(ctx context.Context, n int) (*adminv1.GetSemanticRuleResponse, error) {
			if n == stage {
				<-ctx.Done()
				return nil, status.Error(codes.DeadlineExceeded, "private details")
			}
			return original(ctx, n)
		}
		result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
		if !errors.Is(err, context.DeadlineExceeded) || result.Converged || len(result.Observations) != 1 {
			t.Fatal("rule read escaped operation deadline")
		}
		p.mu.Lock()
		if p.ruleCalls != stage || p.analyzeCalls+p.processCalls != 0 {
			t.Error("failed rule RPC was retried or caused mutation")
		}
		p.mu.Unlock()
	}
	for _, code := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated, codes.Unavailable} {
		ctx, admin, resources, p, _, target := disabledQuiescenceFixture(t)
		p.rule = func(context.Context, int) (*adminv1.GetSemanticRuleResponse, error) {
			return nil, status.Error(code, "private failure")
		}
		result, err := awaitSemanticMaintenance(ctx, admin, resources, target)
		raw, _ := json.Marshal(result)
		if err == nil || result.Converged || result.Observations[0].DisabledRuleBefore.RPCCode != code.String() || strings.Contains(string(raw), "private") || strings.Contains(err.Error(), "private") {
			t.Fatal("failed rule proof passed or leaked")
		}
		p.mu.Lock()
		if p.ruleCalls != 1 {
			t.Error("failed rule read retried")
		}
		p.mu.Unlock()
	}
}

func TestSemanticDisabledQuiescenceReenableReturnsToStrictFreshness(t *testing.T) {
	ctx, admin, resources, p, proof, target := disabledQuiescenceFixture(t)
	enabled := true
	original := p.rule
	p.rule = func(ctx context.Context, n int) (*adminv1.GetSemanticRuleResponse, error) {
		r, err := original(ctx, n)
		r.Rule.Enabled = enabled
		return r, err
	}
	// Healthy pre-disable frontier is captured by the existing reliability oracle.
	proof.LatestObservedGraphRevision = 7
	before, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
	if err != nil || !before.FreshnessProved {
		t.Fatal("healthy enabled baseline failed")
	}
	evidence, err := assertSemanticReliability(ctx, admin, resources, map[string]any{"requireFreshAtLatest": true})
	if err != nil {
		t.Fatal(err)
	}
	captureSemanticReliability(&provision.ScenarioResources{Semantic: resources}, map[string]any{"captureGenerationAs": "before-disable-generation", "captureFreshRevisionAs": "before-disable-fresh"}, evidence)
	proof.LatestObservedGraphRevision = 8
	enabled = false
	paused, err := awaitSemanticMaintenance(ctx, admin, resources, target)
	if err != nil || paused.FreshnessProved || !paused.Converged {
		t.Fatal("valid disable boundary failed")
	}
	// A re-enabled and normally caught-up rule must use ordinary indexed completion.
	enabled = true
	proof.AnalyzedThroughGraphRevision, proof.FreshThroughGraphRevision = 8, 8
	indexed, err := awaitSemanticMaintenance(ctx, admin, resources, nil)
	if err != nil || !indexed.Converged || !indexed.FreshnessProved || indexed.Completion != "indexed" {
		t.Fatal("re-enable did not restore strict indexed completion")
	}
	if r, err := awaitSemanticMaintenance(ctx, admin, resources, target); err == nil || r.Converged {
		t.Fatal("disabled completion could be reused after re-enable")
	}
}
