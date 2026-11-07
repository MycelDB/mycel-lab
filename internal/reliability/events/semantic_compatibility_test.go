package events

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func compatibilityID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }
func compatibilityResources(n int) *provision.SemanticResources {
	return &provision.SemanticResources{SpaceID: compatibilityID(1), DomainID: compatibilityID(n), TargetActorID: "controller-0", TargetPrincipalID: compatibilityID(2), SemanticRuleID: compatibilityID(n + 1), EmbeddingBindingKey: "search", IntelligenceProfileID: compatibilityID(n + 2), ModelID: compatibilityID(n + 3), EndpointID: compatibilityID(n + 4), CapabilityID: compatibilityID(n + 5), VectorStoreID: compatibilityID(n + 6), EvidenceValues: map[string]string{"commit": "1"}, MaintenanceTimeout: time.Second}
}
func TestSemanticDomainIsolationUsesBroadSamePrincipalSameSpaceQueries(t *testing.T) {
	for _, corruption := range []string{"", "foreign-hydration", "foreign-route", "foreign-query-result"} {
		t.Run("case-"+corruption, func(t *testing.T) {
			a, b := compatibilityResources(10), compatibilityResources(30)
			a.DomainPeers = map[string]*provision.SemanticResources{"secondary": b}
			node := compatibilityID(4670)
			resources := semanticSearchProbeFixture(t, func(ctx context.Context, request *clientv1.SemanticSearchRequest) (*clientv1.SemanticSearchResponse, error) {
				md, _ := metadata.FromIncomingContext(ctx)
				if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer local-search-probe-token" {
					t.Error("domain queries changed endpoint-issued credentials")
				}
				if request.GetSpaceId() != a.SpaceID || request.GetSemanticRuleId() != "" || request.GetEmbeddingBindingKey() != "" {
					t.Error("domain-only assertion was masked by rule/binding or different-space filtering")
				}
				scope, own := a, "axis-0"
				if request.GetDomainId() == b.DomainID {
					scope, own = b, "axis-2"
				} else if request.GetDomainId() != a.DomainID {
					t.Error("unknown domain")
				}
				if request.GetQuery() != own && corruption != "foreign-query-result" {
					return &clientv1.SemanticSearchResponse{}, nil
				}
				payloadText := own
				if corruption == "foreign-hydration" {
					payloadText = "foreign-content"
				}
				payload, _ := structpb.NewStruct(map[string]any{"text": payloadText})
				selected := scope
				if corruption == "foreign-route" {
					selected = b
				}
				record := compatibilityID(100)
				if scope == b {
					record = compatibilityID(101)
				}
				return &clientv1.SemanticSearchResponse{Results: []*clientv1.SemanticSearchResult{{NodeId: node, RecordId: record, SemanticRuleId: scope.SemanticRuleID, EmbeddingBindingKey: "search", Score: 1, Node: &clientv1.Node{NodeId: node, Payload: payload}, Provenance: &clientv1.SemanticResultProvenance{SourceGraphRevision: 1, SemanticGeneration: 1, SemanticConfigurationHash: strings.Repeat("a", 64), OpaqueSourceVersion: "opaque-fixture", Dimensions: 8, VectorSpaceKey: "fixture/8", ConfiguredIntelligenceProfileId: selected.IntelligenceProfileID, ConfiguredModelEndpointId: selected.EndpointID, ConfiguredModelId: selected.ModelID, ConfiguredCapabilityId: selected.CapabilityID, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano)}}}}, nil
			})
			resources.Semantic = a
			resources.Assignments[0].SpaceID, resources.Assignments[0].DomainID, resources.Assignments[0].PrincipalID = a.SpaceID, a.DomainID, a.TargetPrincipalID
			target := map[string]any{"peer": "secondary", "nodeId": node, "primaryQuery": "axis-0", "peerQuery": "axis-2", "expectPrimaryRevisionFrom": "commit", "expectPeerRevisionFrom": "commit", "timeout": "1s"}
			_, err := executeSemanticDomainIsolation(context.Background(), resources, target)
			if (err != nil) != (corruption != "") {
				t.Fatalf("domain isolation corruption %q: %v", corruption, err)
			}
		})
	}
}
func TestConfiguredModelUpdateChangesExactlyOneCompatibilityField(t *testing.T) {
	original := &adminv1.InferenceModel{ModelId: compatibilityID(10), Key: "same-key", Kind: "embedding", ModelName: "same-provider-model", Dimensions: 8, VectorSpaceKey: "space-a", ConnectorTypes: []string{"fake"}, Enabled: true}
	for _, target := range []map[string]any{{"dimensions": 16}, {"vectorSpaceKey": "space-b"}} {
		changed, err := prepareModelCompatibilityUpdate(original, target)
		if err != nil {
			t.Fatal(err)
		}
		if original.Dimensions != 8 || original.VectorSpaceKey != "space-a" {
			t.Fatal("pre-mutation snapshot was modified")
		}
		restored := proto.Clone(changed).(*adminv1.InferenceModel)
		restored.Dimensions, restored.VectorSpaceKey = original.Dimensions, original.VectorSpaceKey
		if !proto.Equal(restored, original) {
			t.Fatal("configured shape update altered unrelated identity or policy")
		}
	}
	for _, target := range []map[string]any{{}, {"dimensions": 8}, {"dimensions": 0}, {"dimensions": int64(4294967312)}, {"dimensions": 65537}, {"dimensions": 16, "vectorSpaceKey": "space-b"}, {"vectorSpaceKey": "space-a"}} {
		if _, err := prepareModelCompatibilityUpdate(original, target); err == nil {
			t.Fatal("ambiguous/no-op compatibility change passed")
		}
	}
}
func TestConfiguredSearchRejectsOldRouteDimensionAndVectorSpace(t *testing.T) {
	root := compatibilityResources(10)
	route := compatibilityResources(30)
	route.DomainID = root.DomainID
	root.Routes = map[string]*provision.SemanticResources{"model-v2": route}
	correct := SemanticSearchResultItem{SemanticRuleID: root.SemanticRuleID, EmbeddingBindingKey: "search", Dimensions: 16, VectorSpaceKey: "space-b", ConfiguredProfileID: route.IntelligenceProfileID, ConfiguredModelID: route.ModelID, ConfiguredModelEndpointID: route.EndpointID, ConfiguredCapabilityID: route.CapabilityID}
	target := map[string]any{"requireConfiguredRoute": true, "configuredRoute": "model-v2", "expectedDimensions": 16, "expectedVectorSpaceKey": "space-b"}
	if err := validateSemanticSearchShape(correct, root, target); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*SemanticSearchResultItem){func(i *SemanticSearchResultItem) { i.Dimensions = 8 }, func(i *SemanticSearchResultItem) { i.VectorSpaceKey = "space-a" }, func(i *SemanticSearchResultItem) { i.ConfiguredModelID = root.ModelID }, func(i *SemanticSearchResultItem) { i.ConfiguredProfileID = root.IntelligenceProfileID }, func(i *SemanticSearchResultItem) { i.SemanticRuleID = route.SemanticRuleID }} {
		item := correct
		mutate(&item)
		if validateSemanticSearchShape(item, root, target) == nil {
			t.Fatal("stale/mixed configured provenance passed")
		}
	}
}
func TestMigrationGenerationFenceRejectsMissingZeroSameAndOlderCaptures(t *testing.T) {
	for _, before := range []string{"", "0", "not-a-number", "2", "3"} {
		root := &provision.SemanticResources{EvidenceValues: map[string]string{"before": before}}
		if requireSemanticGenerationAfter(root, "before", 2) == nil {
			t.Fatal("incompatible migration did not require new generation")
		}
	}
	root := &provision.SemanticResources{EvidenceValues: map[string]string{"before": "1"}}
	if err := requireSemanticGenerationAfter(root, "before", 2); err != nil {
		t.Fatal(err)
	}
}
