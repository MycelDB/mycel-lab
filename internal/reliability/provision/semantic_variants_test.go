package provision

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func variantScopeFixture() *ScenarioResources {
	root := &SemanticResources{SpaceID: "same-space", DomainID: "primary-domain", TargetActorID: "same-actor", TargetPrincipalID: "same-principal", EvidenceValues: map[string]string{"primary": "1"}}
	peer := &SemanticResources{SpaceID: root.SpaceID, DomainID: "secondary-domain", TargetActorID: root.TargetActorID, TargetPrincipalID: root.TargetPrincipalID, SemanticRuleKey: "peer-rule", EvidenceValues: map[string]string{"peer": "1"}}
	root.DomainPeers = map[string]*SemanticResources{"secondary": peer}
	return &ScenarioResources{Semantic: root, Assignments: []ActorAssignment{{ActorID: root.TargetActorID, PrincipalID: root.TargetPrincipalID, SpaceID: root.SpaceID, DomainID: root.DomainID, DaemonAddr: "same-endpoint", AccessToken: "same-token", RefreshToken: "same-refresh", Password: "same-private-password"}}}
}
func TestSemanticVariantRejectsAliasedNamesBeforeAnyMutation(t *testing.T) {
	resources := variantScopeFixture()
	resources.Semantic.SpaceID = "00000000-0000-4000-8000-000000000001"
	resources.Semantic.DomainID = "00000000-0000-4000-8000-000000000002"
	resources.Assignments[0].SpaceID, resources.Assignments[0].DomainID = resources.Semantic.SpaceID, resources.Semantic.DomainID
	resources.semanticPlan = &semanticProvisionPlan{}
	_, err := CreateSemanticVariant(context.Background(), nil, resources, "secondary", resources.Semantic.DomainID, true, 8, "space-a")
	if err == nil || !strings.Contains(err.Error(), "overwrite") || len(resources.Semantic.Routes) != 0 {
		t.Fatal("peer and route reused the same inference resource namespace")
	}
}
func TestSemanticDomainViewVariesOnlyDomain(t *testing.T) {
	root := variantScopeFixture()
	view, err := SemanticScopeView(root, "secondary")
	if err != nil {
		t.Fatal(err)
	}
	before, after := root.Assignments[0], view.Assignments[0]
	if after.DomainID == before.DomainID || view.Semantic == root.Semantic {
		t.Fatal("domain did not change")
	}
	after.DomainID = before.DomainID
	if after != before || view.Semantic.SpaceID != root.Semantic.SpaceID || view.Semantic.TargetPrincipalID != root.Semantic.TargetPrincipalID {
		t.Fatal("isolation changed credentials/principal/space/daemon")
	}
	view.Assignments[0].AccessToken = "changed"
	if root.Assignments[0].AccessToken != "same-token" {
		t.Fatal("scope view mutated original actor")
	}
}
func TestSemanticDomainViewRejectsCrossSpacePrincipalAndMissingDomain(t *testing.T) {
	for name, mutate := range map[string]func(*ScenarioResources){
		"same domain":         func(r *ScenarioResources) { r.Semantic.DomainPeers["secondary"].DomainID = r.Semantic.DomainID },
		"different space":     func(r *ScenarioResources) { r.Semantic.DomainPeers["secondary"].SpaceID = "elsewhere" },
		"different principal": func(r *ScenarioResources) { r.Semantic.DomainPeers["secondary"].TargetPrincipalID = "other" },
		"different actor":     func(r *ScenarioResources) { r.Semantic.DomainPeers["secondary"].TargetActorID = "other" },
		"missing actor":       func(r *ScenarioResources) { r.Assignments = nil },
		"actor wrong domain":  func(r *ScenarioResources) { r.Assignments[0].DomainID = "wrong" },
	} {
		t.Run(name, func(t *testing.T) {
			r := variantScopeFixture()
			mutate(r)
			if _, err := SemanticScopeView(r, "secondary"); err == nil {
				t.Fatal("invalid isolation accepted")
			}
		})
	}
}
func TestSemanticOwnedGraphCloneIsolatesNestedCleanupEvidence(t *testing.T) {
	source := variantScopeFixture()
	source.Semantic.Routes = map[string]*SemanticResources{"new-model": {EvidenceValues: map[string]string{"model": "old"}, Cleanup: []SemanticOperation{{Status: "planned"}}}}
	clone := CloneScenarioResources(*source)
	clone.Semantic.DomainPeers["secondary"].EvidenceValues["peer"] = "changed"
	clone.Semantic.Routes["new-model"].Cleanup[0].Status = "changed"
	clone.Semantic.Routes["new-model"].EvidenceValues["model"] = "changed"
	if source.Semantic.DomainPeers["secondary"].EvidenceValues["peer"] != "1" || source.Semantic.Routes["new-model"].Cleanup[0].Status != "planned" || source.Semantic.Routes["new-model"].EvidenceValues["model"] != "old" {
		t.Fatal("late nested cleanup writes escaped the isolated snapshot")
	}
}

type variantCleanupProbe struct {
	commonv1.UnimplementedAuthServiceServer
	adminv1.UnimplementedAdminSemanticServiceServer
	adminv1.UnimplementedAdminIntelligenceAccessProfileServiceServer
	adminv1.UnimplementedAdminInferenceCatalogServiceServer
	mu          sync.Mutex
	rootDeleted bool
	calls       []string
}

func (*variantCleanupProbe) Login(context.Context, *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	return &commonv1.LoginResponse{AccessToken: "cleanup-fixture-token", AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}
func (p *variantCleanupProbe) DeleteSemanticRule(context.Context, *adminv1.DeleteSemanticRuleRequest) (*adminv1.DeleteSemanticRuleResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rootDeleted = true
	p.calls = append(p.calls, "owning-rule")
	return &adminv1.DeleteSemanticRuleResponse{}, nil
}
func (p *variantCleanupProbe) DeleteIntelligenceProfile(context.Context, *adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest) (*adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.rootDeleted {
		return nil, status.Error(codes.FailedPrecondition, "active owning rule")
	}
	p.calls = append(p.calls, "child-profile")
	return &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileResponse{}, nil
}
func (p *variantCleanupProbe) DeleteModel(context.Context, *adminv1.AdminInferenceCatalogServiceDeleteModelRequest) (*adminv1.AdminInferenceCatalogServiceDeleteModelResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, "child-model")
	return &adminv1.AdminInferenceCatalogServiceDeleteModelResponse{}, nil
}
func TestSemanticVariantCleanupDeletesOwningRuleBeforeSelectedRoute(t *testing.T) {
	probe := &variantCleanupProbe{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(server, probe)
	adminv1.RegisterAdminSemanticServiceServer(server, probe)
	adminv1.RegisterAdminIntelligenceAccessProfileServiceServer(server, probe)
	adminv1.RegisterAdminInferenceCatalogServiceServer(server, probe)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: listener.Addr().String(), Username: "fixture-admin", Password: "fixture-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	root := &SemanticResources{SpaceID: "space", SemanticRuleID: "rule", Routes: map[string]*SemanticResources{"model-v2": {SpaceID: "space", IntelligenceProfileID: "profile", ModelID: "model"}}}
	if failures := cleanupSemanticResources(ctx, admin, root); len(failures) != 0 {
		t.Fatalf("owned route cleanup failed: %v", failures)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if len(probe.calls) != 3 || probe.calls[0] != "owning-rule" || probe.calls[1] != "child-profile" || probe.calls[2] != "child-model" {
		t.Fatalf("unsafe cleanup ordering: %v", probe.calls)
	}
}
