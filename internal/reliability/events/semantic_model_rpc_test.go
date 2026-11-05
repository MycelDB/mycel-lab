package events

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type compatibilityCatalogProbe struct {
	commonv1.UnimplementedAuthServiceServer
	adminv1.UnimplementedAdminInferenceCatalogServiceServer
	mu          sync.Mutex
	model       *adminv1.InferenceModel
	applies     int
	unavailable bool
}

func (*compatibilityCatalogProbe) Login(context.Context, *commonv1.LoginRequest) (*commonv1.LoginResponse, error) {
	return &commonv1.LoginResponse{AccessToken: "integration-work-token", AccessTokenExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}
func (p *compatibilityCatalogProbe) ListModels(ctx context.Context, request *adminv1.AdminInferenceCatalogServiceListModelsRequest) (*adminv1.AdminInferenceCatalogServiceListModelsResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok || request.GetPageSize() != 128 || request.GetKind() != "embedding" {
		return nil, status.Error(codes.InvalidArgument, "missing finite scoped read")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return &adminv1.AdminInferenceCatalogServiceListModelsResponse{Models: []*adminv1.InferenceModel{proto.Clone(p.model).(*adminv1.InferenceModel)}}, nil
}
func (p *compatibilityCatalogProbe) ApplyInferencePackage(ctx context.Context, request *adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest) (*adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse, error) {
	if err := semanticWorkProbeAuthenticated(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applies++
	if p.unavailable {
		return nil, status.Error(codes.Unavailable, "sensitive raw provider details")
	}
	if len(request.GetModels()) != 1 || len(request.GetModelEndpoints()) != 0 || len(request.GetVectorStores()) != 0 || len(request.GetModelEndpointCapabilities()) != 0 || request.GetName() != "owned-route-package" || request.GetSource() != "mycel-lab" {
		return nil, status.Error(codes.InvalidArgument, "mutation escaped owned model scope")
	}
	model := request.GetModels()[0]
	if model.GetModelId() != p.model.GetModelId() || model.GetKey() != p.model.GetKey() || model.GetModelName() != p.model.GetModelName() {
		return nil, status.Error(codes.InvalidArgument, "immutable configured identity changed")
	}
	p.model = proto.Clone(model).(*adminv1.InferenceModel)
	return &adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse{Models: []*adminv1.InferenceModel{proto.Clone(p.model).(*adminv1.InferenceModel)}}, nil
}
func catalogCompatibilityClient(t *testing.T, probe *compatibilityCatalogProbe) (context.Context, *mycel.AdminClient) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	commonv1.RegisterAuthServiceServer(server, probe)
	adminv1.RegisterAdminInferenceCatalogServiceServer(server, probe)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: listener.Addr().String(), Username: "fixture-admin", Password: "fixture-password"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	return ctx, admin
}
func TestConfiguredModelMigrationUsesAuthenticatedBoundedPublicCatalogRPC(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "ambiguous-failure"}[unavailable], func(t *testing.T) {
			root := compatibilityResources(10)
			route := compatibilityResources(30)
			route.DomainID = root.DomainID
			route.PackageName = "owned-route-package"
			route.ModelKey = "selected-model"
			root.Routes = map[string]*provision.SemanticResources{"model-v2": route}
			probe := &compatibilityCatalogProbe{model: &adminv1.InferenceModel{ModelId: route.ModelID, Key: route.ModelKey, Kind: "embedding", ModelName: "fixture-provider-model", Dimensions: 8, VectorSpaceKey: "space-a", Enabled: true}, unavailable: unavailable}
			ctx, admin := catalogCompatibilityClient(t, probe)
			evidence, err := executeSemanticModelConfigUpdate(ctx, admin, root, map[string]any{"route": "model-v2", "dimensions": 16})
			probe.mu.Lock()
			calls := probe.applies
			probe.mu.Unlock()
			if calls != 1 {
				t.Fatal("catalog mutation was repeated")
			}
			if unavailable {
				if err == nil || strings.Contains(err.Error(), "sensitive raw") {
					t.Fatal("ambiguous failure passed or leaked diagnostics")
				}
			} else if err != nil || evidence.ModelID != route.ModelID || evidence.Dimensions != 16 || evidence.VectorSpaceKey != "space-a" {
				t.Fatalf("shape/identity mutation failed: %+v %v", evidence, err)
			}
		})
	}
}

type compatibilityModelPages struct {
	adminv1.AdminInferenceCatalogServiceClient
	responses []*adminv1.AdminInferenceCatalogServiceListModelsResponse
	calls     int
}

func (p *compatibilityModelPages) ListModels(context.Context, *adminv1.AdminInferenceCatalogServiceListModelsRequest, ...grpc.CallOption) (*adminv1.AdminInferenceCatalogServiceListModelsResponse, error) {
	if p.calls >= len(p.responses) {
		return nil, status.Error(codes.Unavailable, "unplanned listing")
	}
	response := p.responses[p.calls]
	p.calls++
	return response, nil
}
func TestConfiguredModelListingRequiresCompleteUnambiguousBoundedEvidence(t *testing.T) {
	route := compatibilityResources(10)
	route.ModelKey = "selected-model"
	model := &adminv1.InferenceModel{ModelId: route.ModelID, Key: route.ModelKey}
	first := &adminv1.AdminInferenceCatalogServiceListModelsResponse{Models: []*adminv1.InferenceModel{model}, NextPageToken: "page-2"}
	cases := []struct {
		name      string
		responses []*adminv1.AdminInferenceCatalogServiceListModelsResponse
		valid     bool
	}{
		{"complete", []*adminv1.AdminInferenceCatalogServiceListModelsResponse{first, {}}, true},
		{"duplicate-on-later-page", []*adminv1.AdminInferenceCatalogServiceListModelsResponse{first, {Models: []*adminv1.InferenceModel{model}}}, false},
		{"repeated-continuation", []*adminv1.AdminInferenceCatalogServiceListModelsResponse{first, {NextPageToken: "page-2"}}, false},
		{"missing-page", []*adminv1.AdminInferenceCatalogServiceListModelsResponse{first}, false},
	}
	endless := []*adminv1.AdminInferenceCatalogServiceListModelsResponse{first}
	for i := 1; i < 16; i++ {
		endless = append(endless, &adminv1.AdminInferenceCatalogServiceListModelsResponse{NextPageToken: fmt.Sprintf("later-%d", i)})
	}
	cases = append(cases, struct {
		name      string
		responses []*adminv1.AdminInferenceCatalogServiceListModelsResponse
		valid     bool
	}{"page-limit", endless, false})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &compatibilityModelPages{responses: tc.responses}
			_, err := readConfiguredModel(context.Background(), p, route)
			if (err == nil) != tc.valid || p.calls > 16 {
				t.Fatalf("partial/ambiguous model evidence accepted: calls=%d error=%v", p.calls, err)
			}
			if tc.valid && p.calls != 2 {
				t.Fatal("model mutation allowed before complete listing")
			}
		})
	}
}

func TestMigrationGenerationFenceRunsBeforeExpectedRebuildEarlyReturn(t *testing.T) {
	ctx, admin := catalogCompatibilityClient(t, &compatibilityCatalogProbe{})
	p, resources, _ := semanticWorkFixture()
	resources.EvidenceValues["before"] = "1"
	p.reliability.RebuildRequired = true
	p.reliability.Rebuild = &adminv1.SemanticRebuildStatus{State: "required"}
	admin.SemanticMaintenance = p
	target := map[string]any{"expectRebuildRequired": true, "requireGenerationAfter": "before"}
	if _, err := assertSemanticReliability(ctx, admin, resources, target); err == nil {
		t.Fatal("required-rebuild branch ignored generation fence")
	}
	p.reliability.SemanticGeneration = 2
	if _, err := assertSemanticReliability(ctx, admin, resources, target); err != nil {
		t.Fatal(err)
	}
	target["minimumSemanticGeneration"] = 3
	if _, err := assertSemanticReliability(ctx, admin, resources, target); err == nil {
		t.Fatal("required-rebuild branch ignored explicit minimum generation")
	}
}
