package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/protobuf/proto"
)

type SemanticVariantEvidence struct {
	SchemaVersion  int    `json:"schemaVersion"`
	Name           string `json:"name"`
	SpaceID        string `json:"spaceId"`
	DomainID       string `json:"domainId"`
	PrincipalID    string `json:"principalId"`
	ModelID        string `json:"modelId,omitempty"`
	ProfileID      string `json:"profileId,omitempty"`
	Dimensions     int32  `json:"dimensions,omitempty"`
	VectorSpaceKey string `json:"vectorSpaceKey,omitempty"`
}

func executeSemanticDomainPeerCreate(ctx context.Context, admin *mycel.AdminClient, resources *provision.ScenarioResources, target map[string]any) (SemanticVariantEvidence, error) {
	name := targetString(target, "name")
	root := resources.Semantic
	result := SemanticVariantEvidence{SchemaVersion: 1, Name: name, SpaceID: root.SpaceID, PrincipalID: root.TargetPrincipalID}
	if root.LiveProvider || len(root.DomainPeers)+len(root.Routes) >= 4 || root.DomainPeers[name] != nil || root.Routes[name] != nil {
		return result, errors.New("domain peer must be a new finite credential-free fixture")
	}
	actor, assignment, err := dialSemanticActor(ctx, resources)
	if err != nil {
		return result, errors.New("domain peer actor authentication failed")
	}
	defer actor.Close()
	if assignment.SpaceID != root.SpaceID || assignment.PrincipalID != root.TargetPrincipalID {
		return result, errors.New("domain peer actor must retain original identity and space")
	}
	sum := sha256.Sum256([]byte(resources.RunID + "/domain/" + name))
	key := "domain-" + hex.EncodeToString(sum[:8])
	domain, err := actor.CreateDomain(ctx, root.SpaceID, key, key, "synthetic same-space isolation fixture", mycel.NormalDomainPolicy())
	if err != nil {
		return result, errors.New("domain creation failed; mutation not resubmitted")
	}
	if domain == nil || domain.GetSpaceId() != root.SpaceID || !spec.IsCanonicalSemanticUUID(domain.GetDomainId()) || domain.GetDomainId() == root.DomainID || domain.GetDefault() || domain.GetSystem() || domain.GetReadOnly() {
		return result, errors.New("domain creation did not establish a distinct writable same-space domain")
	}
	result.DomainID = domain.GetDomainId()
	resources.Domains = append(resources.Domains, provision.Domain{ActorID: assignment.ActorID, GroupName: assignment.GroupName, SpaceID: root.SpaceID, Key: key, Name: key, DomainID: domain.GetDomainId()})
	peer, err := provision.CreateSemanticVariant(ctx, admin, resources, name, domain.GetDomainId(), false, 0, "")
	if peer != nil {
		result.ModelID, result.ProfileID = peer.ModelID, peer.IntelligenceProfileID
	}
	return result, err
}

func executeSemanticRouteCreate(ctx context.Context, admin *mycel.AdminClient, resources *provision.ScenarioResources, target map[string]any) (SemanticVariantEvidence, error) {
	root := resources.Semantic
	result := SemanticVariantEvidence{SchemaVersion: 1, Name: targetString(target, "name"), SpaceID: root.SpaceID, DomainID: root.DomainID, PrincipalID: root.TargetPrincipalID, Dimensions: int32(intTarget(target, "dimensions", 0)), VectorSpaceKey: targetString(target, "vectorSpaceKey")}
	route, err := provision.CreateSemanticVariant(ctx, admin, resources, result.Name, root.DomainID, true, int(result.Dimensions), result.VectorSpaceKey)
	if route != nil {
		result.ModelID, result.ProfileID = route.ModelID, route.IntelligenceProfileID
	}
	return result, err
}

func selectedSemanticBinding(rule *adminv1.SemanticGenerationRule, key string) (*adminv1.SemanticEmbeddingBinding, error) {
	var selected *adminv1.SemanticEmbeddingBinding
	if rule == nil {
		return nil, errors.New("semantic rule is absent")
	}
	for _, binding := range rule.GetEmbeddings() {
		if binding.GetKey() == key {
			if selected != nil {
				return nil, errors.New("semantic binding is ambiguous")
			}
			selected = binding
		}
	}
	if selected == nil {
		return nil, errors.New("semantic binding is missing")
	}
	return selected, nil
}
func configuredSemanticRoute(root *provision.SemanticResources, name string) (*provision.SemanticResources, error) {
	if root == nil {
		return nil, errors.New("semantic route resources are absent")
	}
	route := root
	if name != "" {
		route = root.Routes[name]
	}
	if route == nil || route.SpaceID != root.SpaceID || route.DomainID != root.DomainID || route.TargetActorID != root.TargetActorID || route.TargetPrincipalID != root.TargetPrincipalID {
		return nil, errors.New("configured route is missing or out of scope")
	}
	for _, id := range []string{route.IntelligenceProfileID, route.ModelID, route.EndpointID, route.CapabilityID, route.VectorStoreID} {
		if !spec.IsCanonicalSemanticUUID(id) {
			return nil, errors.New("configured route has incomplete durable identity")
		}
	}
	return route, nil
}
func executeSemanticRouteBind(ctx context.Context, admin *mycel.AdminClient, root *provision.SemanticResources, target map[string]any) (map[string]any, error) {
	if root.LiveProvider {
		return nil, errors.New("route migration requires a credential-free fixture")
	}
	route, err := configuredSemanticRoute(root, targetString(target, "route"))
	if err != nil {
		return nil, err
	}
	if route == root || route.ModelID == root.ModelID {
		return nil, errors.New("model migration must select a distinct configured model identity")
	}
	authenticated, cancel := admin.AuthCallContext(ctx)
	defer cancel()
	current, err := admin.Semantic.GetSemanticRule(authenticated, &adminv1.GetSemanticRuleRequest{SpaceId: root.SpaceID, SemanticRuleId: root.SemanticRuleID})
	if err != nil || current.GetRule() == nil {
		return nil, errors.New("read semantic rule before migration failed")
	}
	rule := proto.Clone(current.GetRule()).(*adminv1.SemanticGenerationRule)
	if rule.GetSpaceId() != root.SpaceID || rule.GetDomainId() != root.DomainID || rule.GetSemanticRuleId() != root.SemanticRuleID {
		return nil, errors.New("migration rule is out of scope")
	}
	binding, err := selectedSemanticBinding(rule, root.EmbeddingBindingKey)
	if err != nil {
		return nil, err
	}
	binding.IntelligenceProfileId, binding.VectorStoreId = route.IntelligenceProfileID, route.VectorStoreID
	binding.IntelligenceProfile, binding.VectorStore = "", ""
	updated, err := admin.Semantic.UpdateSemanticRule(authenticated, &adminv1.UpdateSemanticRuleRequest{SpaceId: root.SpaceID, SemanticRuleId: root.SemanticRuleID, Rule: rule})
	if err != nil {
		return nil, errors.New("semantic route mutation failed; not resubmitted")
	}
	selected, err := selectedSemanticBinding(updated.GetRule(), root.EmbeddingBindingKey)
	if err != nil || updated.GetRule().GetSpaceId() != root.SpaceID || updated.GetRule().GetDomainId() != root.DomainID || updated.GetRule().GetSemanticRuleId() != root.SemanticRuleID || selected.GetIntelligenceProfileId() != route.IntelligenceProfileID || selected.GetVectorStoreId() != route.VectorStoreID {
		return nil, errors.New("semantic route mutation response does not match requested identity")
	}
	return map[string]any{"semanticRuleId": root.SemanticRuleID, "modelId": route.ModelID, "profileId": route.IntelligenceProfileID, "route": targetString(target, "route")}, nil
}

func readConfiguredModel(ctx context.Context, client adminv1.AdminInferenceCatalogServiceClient, route *provision.SemanticResources) (*adminv1.InferenceModel, error) {
	token := ""
	seen := map[string]bool{}
	var selected *adminv1.InferenceModel
	for page := 0; page < 16; page++ {
		response, err := client.ListModels(ctx, &adminv1.AdminInferenceCatalogServiceListModelsRequest{PageSize: 128, PageToken: token, Kind: "embedding"})
		if err != nil || response == nil || len(response.GetModels()) > 128 {
			return nil, errors.New("configured model listing is unavailable or unbounded")
		}
		for _, model := range response.GetModels() {
			if model.GetModelId() == route.ModelID {
				if selected != nil || model.GetKey() != route.ModelKey {
					return nil, errors.New("configured model identity is ambiguous")
				}
				selected = proto.Clone(model).(*adminv1.InferenceModel)
			}
		}
		token = response.GetNextPageToken()
		if token == "" {
			if selected == nil {
				return nil, errors.New("configured model is missing")
			}
			return selected, nil
		}
		if seen[token] {
			return nil, errors.New("model listing repeated a continuation")
		}
		seen[token] = true
	}
	return nil, errors.New("model listing exceeded finite page bound")
}

func prepareModelCompatibilityUpdate(model *adminv1.InferenceModel, target map[string]any) (*adminv1.InferenceModel, error) {
	if model == nil || !spec.IsCanonicalSemanticUUID(model.GetModelId()) || model.GetDimensions() <= 0 || model.GetVectorSpaceKey() == "" {
		return nil, errors.New("configured model shape is unsupported")
	}
	changed := proto.Clone(model).(*adminv1.InferenceModel)
	_, dimension := target["dimensions"]
	_, space := target["vectorSpaceKey"]
	if dimension == space {
		return nil, errors.New("model compatibility changes must independently change dimension or vector space")
	}
	if dimension {
		dimensions := intTarget(target, "dimensions", 0)
		if dimensions < 1 || dimensions > 65536 || dimensions == int(model.Dimensions) {
			return nil, errors.New("dimension update must be positive, bounded, and different")
		}
		changed.Dimensions = int32(dimensions)
	}
	if space {
		changed.VectorSpaceKey = targetString(target, "vectorSpaceKey")
		if changed.VectorSpaceKey == "" || changed.VectorSpaceKey == model.VectorSpaceKey {
			return nil, errors.New("vector-space update must be explicit and different")
		}
	}
	return changed, nil
}
func executeSemanticModelConfigUpdate(ctx context.Context, admin *mycel.AdminClient, root *provision.SemanticResources, target map[string]any) (SemanticVariantEvidence, error) {
	result := SemanticVariantEvidence{SchemaVersion: 1, Name: targetString(target, "route"), SpaceID: root.SpaceID, DomainID: root.DomainID, PrincipalID: root.TargetPrincipalID}
	if root.LiveProvider {
		return result, errors.New("model compatibility mutation requires a credential-free fixture")
	}
	route, err := configuredSemanticRoute(root, result.Name)
	if err != nil {
		return result, err
	}
	authenticated, cancel := admin.AuthCallContext(ctx)
	defer cancel()
	current, err := readConfiguredModel(authenticated, admin.InferenceCatalog, route)
	if err != nil {
		return result, err
	}
	changed, err := prepareModelCompatibilityUpdate(current, target)
	if err != nil {
		return result, err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(changed)
	if err != nil {
		return result, errors.New("model compatibility request is invalid")
	}
	checksum := sha256.Sum256(raw)
	digest := hex.EncodeToString(checksum[:])
	response, err := admin.InferenceCatalog.ApplyInferencePackage(authenticated, &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{Name: route.PackageName, Version: "compat-" + digest[:16], Source: "mycel-lab", Checksum: digest, Models: []*adminv1.InferenceModel{changed}})
	if err != nil {
		return result, errors.New("model compatibility mutation failed; not resubmitted")
	}
	if response == nil || len(response.GetModels()) != 1 {
		return result, errors.New("model compatibility response is incomplete")
	}
	stored := response.GetModels()[0]
	if stored.GetModelId() != current.GetModelId() || stored.GetKey() != current.GetKey() || stored.GetModelName() != current.GetModelName() || stored.GetDimensions() != changed.GetDimensions() || stored.GetVectorSpaceKey() != changed.GetVectorSpaceKey() {
		return result, errors.New("model compatibility mutation changed unexpected identity or shape")
	}
	result.ModelID, result.ProfileID, result.Dimensions, result.VectorSpaceKey = stored.GetModelId(), route.IntelligenceProfileID, stored.GetDimensions(), stored.GetVectorSpaceKey()
	return result, nil
}

func validateSemanticSearchShape(item SemanticSearchResultItem, resources *provision.SemanticResources, target map[string]any) error {
	if expected := intTarget(target, "expectedDimensions", 0); expected > 0 && item.Dimensions != int32(expected) {
		return errors.New("semantic result retained an incompatible vector dimension")
	}
	if expected := targetString(target, "expectedVectorSpaceKey"); expected != "" && item.VectorSpaceKey != expected {
		return errors.New("semantic result retained an incompatible vector-space identity")
	}
	if boolTarget(target, "requireConfiguredRoute", false) {
		route, err := configuredSemanticRoute(resources, targetString(target, "configuredRoute"))
		if err != nil {
			return err
		}
		if item.SemanticRuleID != resources.SemanticRuleID || item.EmbeddingBindingKey != resources.EmbeddingBindingKey || item.ConfiguredProfileID != route.IntelligenceProfileID || item.ConfiguredModelEndpointID != route.EndpointID || item.ConfiguredModelID != route.ModelID || item.ConfiguredCapabilityID != route.CapabilityID {
			return errors.New("semantic result configured provenance crossed its domain or selected model route")
		}
	}
	return nil
}

func requireSemanticGenerationAfter(resources *provision.SemanticResources, capture string, generation uint64) error {
	if capture == "" {
		return nil
	}
	before, err := strconv.ParseUint(resources.EvidenceValues[capture], 10, 64)
	if err != nil || before == 0 || generation <= before {
		return errors.New("incompatible configuration did not advance the captured semantic generation")
	}
	return nil
}
