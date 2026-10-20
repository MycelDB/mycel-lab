package provision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func provisionSemanticResources(ctx context.Context, admin *mycel.AdminClient, resources *ScenarioResources) error {
	if resources == nil || resources.Semantic == nil || resources.semanticPlan == nil {
		return nil
	}
	semanticResources := resources.Semantic
	plan := resources.semanticPlan
	if semanticResources.SpaceID == "" || semanticResources.DomainID == "" {
		return errors.New("semantic target assignment is missing space or domain identity")
	}
	metadata := map[string]any{}
	if plan.Capability.FixtureStrategy != "" {
		metadata["mycel_fixture"] = map[string]any{"strategy": plan.Capability.FixtureStrategy}
	}
	endpointMetadata, err := structpb.NewStruct(metadata)
	if err != nil {
		return fmt.Errorf("encode endpoint metadata: %w", err)
	}
	modelMetadata, err := structpb.NewStruct(map[string]any{"fixtureStrategy": plan.Capability.FixtureStrategy})
	if err != nil {
		return fmt.Errorf("encode model metadata: %w", err)
	}
	endpointURL := strings.TrimSpace(plan.Capability.BaseURL)
	modelName := strings.TrimSpace(plan.Capability.ProviderModel)
	if modelName == "" {
		modelName = semanticResources.ModelKey
	}
	applyResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse, error) {
		return admin.InferenceCatalog.ApplyInferencePackage(callCtx, &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{
			Name: semanticResources.PackageName, Version: semanticResources.PackageVersion, Source: "mycel-lab", Checksum: semanticResources.PackageName,
			ModelEndpoints: []*adminv1.ModelEndpoint{{
				Key: semanticResources.EndpointKey, Name: semanticResources.EndpointKey, ConnectorType: plan.Capability.ConnectorType,
				EndpointUrl: endpointURL, NetworkClass: "local", PrivacyClass: "local_only", AuthModes: []string{"none"}, Operations: []string{"embeddings"}, Enabled: true, Metadata: endpointMetadata,
			}},
			Models: []*adminv1.InferenceModel{{
				Key: semanticResources.ModelKey, Kind: "embedding", ModelName: modelName, ConnectorTypes: []string{plan.Capability.ConnectorType},
				Dimensions: int32(plan.Capability.Dimensions), VectorSpaceKey: plan.Capability.VectorSpaceKey, Enabled: true, Metadata: modelMetadata,
			}},
			VectorStores: []*adminv1.VectorStore{{Key: semanticResources.VectorStoreKey, Name: semanticResources.VectorStoreKey, Type: "mycel-file", PrivacyClass: "local_only", Enabled: true}},
			ModelEndpointCapabilities: []*adminv1.ModelEndpointCapabilityDefinition{{
				ModelEndpoint: semanticResources.EndpointKey, Model: semanticResources.ModelKey, Operation: "embeddings", Enabled: proto.Bool(true), ModelNameOverride: modelName,
			}},
		})
	})
	if err != nil {
		return semanticOperationFailed(semanticResources, "inference-package", err)
	}
	if applyResponse.GetPackage() == nil || len(applyResponse.GetModelEndpoints()) != 1 || len(applyResponse.GetModels()) != 1 || len(applyResponse.GetVectorStores()) != 1 || len(applyResponse.GetModelEndpointCapabilities()) != 1 {
		return semanticOperationFailed(semanticResources, "inference-package", errors.New("catalog package response is incomplete"))
	}
	semanticResources.PackageID = applyResponse.GetPackage().GetInferencePackageId()
	semanticResources.EndpointID = applyResponse.GetModelEndpoints()[0].GetModelEndpointId()
	semanticResources.ModelID = applyResponse.GetModels()[0].GetModelId()
	semanticResources.VectorStoreID = applyResponse.GetVectorStores()[0].GetVectorStoreId()
	semanticResources.CapabilityID = applyResponse.GetModelEndpointCapabilities()[0].GetModelEndpointCapabilityId()
	semanticOperationCompleted(semanticResources, "inference-package")

	profileResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse, error) {
		return admin.InferenceProfiles.CreateIntelligenceProfile(callCtx, &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest{
			SpaceId: semanticResources.SpaceID, Key: semanticResources.IntelligenceProfileKey, DisplayName: semanticResources.IntelligenceProfileKey,
			Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_EMBEDDINGS, Purpose: "semantic_search", DomainIds: []string{semanticResources.DomainID},
			CapabilityRefs: []string{semanticResources.CapabilityID}, EndpointRefs: []string{semanticResources.EndpointID}, ModelRefs: []string{semanticResources.ModelID},
			PrivacyRequirement: &commonv1.InferencePrivacyRequirement{AllowedPrivacyClasses: []commonv1.InferencePrivacyClass{commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_LOCAL_ONLY}, RequireLocalEndpoint: true, DisallowThirdParty: true}, Enabled: true,
		})
	})
	if err != nil {
		return semanticOperationFailed(semanticResources, "intelligence-profile", err)
	}
	if profileResponse.GetIntelligenceProfile() == nil {
		return semanticOperationFailed(semanticResources, "intelligence-profile", errors.New("profile response is incomplete"))
	}
	semanticResources.IntelligenceProfileID = profileResponse.GetIntelligenceProfile().GetIntelligenceProfileId()
	semanticOperationCompleted(semanticResources, "intelligence-profile")

	credentialResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialResponse, error) {
		authType := "none"
		if plan.Secret != "" {
			authType = "api_key"
		}
		return admin.InferenceCredentials.CreateCredential(callCtx, &adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialRequest{
			Key: semanticResources.CredentialKey, DisplayName: semanticResources.CredentialKey, ModelEndpointId: semanticResources.EndpointID,
			OwnerType: "space", OwnerId: semanticResources.SpaceID, AuthType: authType, IsDefault: true, SecretValue: plan.Secret,
		})
	})
	if err != nil {
		return semanticOperationFailed(semanticResources, "credential", err)
	}
	if credentialResponse.GetCredential() == nil {
		return semanticOperationFailed(semanticResources, "credential", errors.New("credential response is incomplete"))
	}
	semanticResources.CredentialID = credentialResponse.GetCredential().GetCredentialId()
	semanticOperationCompleted(semanticResources, "credential")

	grantResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessGrantServiceCreateCredentialGrantResponse, error) {
		return admin.InferenceGrants.CreateCredentialGrant(callCtx, &adminv1.AdminIntelligenceAccessGrantServiceCreateCredentialGrantRequest{
			SpaceId: semanticResources.SpaceID, CredentialId: semanticResources.CredentialID,
			Scope:      &commonv1.IntelligenceAccessScope{SpaceId: semanticResources.SpaceID, DomainId: semanticResources.DomainID, IncludeDescendants: true},
			Operations: []string{"embeddings"}, ModelEndpointId: semanticResources.EndpointID, ModelId: semanticResources.ModelID,
			IsDefault: true, AllowBackgroundUse: true,
		})
	})
	if err != nil {
		return semanticOperationFailed(semanticResources, "credential-grant", err)
	}
	if grantResponse.GetCredentialGrant() == nil {
		return semanticOperationFailed(semanticResources, "credential-grant", errors.New("grant response is incomplete"))
	}
	semanticResources.CredentialGrantID = grantResponse.GetCredentialGrant().GetCredentialGrantId()
	semanticOperationCompleted(semanticResources, "credential-grant")

	policyResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessPolicyServiceCreateAccessPolicyResponse, error) {
		return admin.InferencePolicies.CreateAccessPolicy(callCtx, &adminv1.AdminIntelligenceAccessPolicyServiceCreateAccessPolicyRequest{
			SpaceId: semanticResources.SpaceID,
			Scope:   &commonv1.IntelligenceAccessScope{SpaceId: semanticResources.SpaceID, DomainId: semanticResources.DomainID, IncludeDescendants: true},
			Effect:  "allow", Operations: []string{"embeddings"}, AllowedPrivacyClasses: []string{"local_only"}, RequireLocalEndpoint: true,
			DisallowThirdParty: true, Reason: "mycel-lab semantic run " + resources.RunID,
		})
	})
	if err != nil {
		return semanticOperationFailed(semanticResources, "access-policy", err)
	}
	if policyResponse.GetAccessPolicy() == nil {
		return semanticOperationFailed(semanticResources, "access-policy", errors.New("policy response is incomplete"))
	}
	semanticResources.AccessPolicyID = policyResponse.GetAccessPolicy().GetAccessPolicyId()
	semanticOperationCompleted(semanticResources, "access-policy")

	if plan.Spec.Rule.DeferProvisioning {
		setSemanticOperationStatus(semanticResources, "semantic-rule", "deferred", "")
		return nil
	}
	return CreateSemanticRule(ctx, admin, resources)
}

// CreateSemanticRule creates a deferred run-scoped semantic rule. Repeated calls
// in the same run are no-ops once the rule identity is known.
func CreateSemanticRule(ctx context.Context, admin *mycel.AdminClient, resources *ScenarioResources) error {
	if resources == nil || resources.Semantic == nil || resources.semanticPlan == nil {
		return errors.New("semantic resources are incomplete")
	}
	semanticResources := resources.Semantic
	if semanticResources.SemanticRuleID != "" {
		return nil
	}
	rule := resources.semanticPlan.Spec.Rule
	ruleResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.CreateSemanticRuleResponse, error) {
		return admin.Semantic.CreateSemanticRule(callCtx, &adminv1.CreateSemanticRuleRequest{Rule: &adminv1.SemanticGenerationRule{
			SpaceId: semanticResources.SpaceID, DomainId: semanticResources.DomainID, Key: rule.Key, DisplayName: rule.DisplayName, Description: rule.Description, Enabled: true,
			Trigger:     &adminv1.SemanticTriggerPolicy{Events: []string{"node.created", "node.updated", "node.deleted"}, Debounce: rule.DirtyCooldown.String()},
			Selector:    &adminv1.SemanticTargetSelector{Mode: "node_type", Labels: append([]string(nil), rule.TargetLabels...), MaxResults: 4096},
			Source:      &adminv1.SemanticSourceAssemblyPolicy{Mode: rule.SourceMode, IncludeProperties: append([]string(nil), rule.IncludeProperties...), ExcludeProperties: append([]string(nil), rule.ExcludeProperties...), MinimumTextLength: int32(rule.MinimumTextLength)},
			Embeddings:  []*adminv1.SemanticEmbeddingBinding{{Key: rule.BindingKey, Purpose: rule.Purpose, IntelligenceProfileId: semanticResources.IntelligenceProfileID, VectorStoreId: semanticResources.VectorStoreID, Enabled: true}},
			Maintenance: &adminv1.SemanticMaintenancePolicy{DirtyCooldown: rule.DirtyCooldown.String(), MaxBatchSize: int32(rule.MaxBatchSize), WorkerConcurrency: int32(rule.WorkerConcurrency)},
			Storage:     &adminv1.SemanticStoragePolicy{Searchable: true, PhysicalIndex: rule.PhysicalIndex},
		}})
	})
	if err != nil {
		listed, listErr := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.ListSemanticRulesResponse, error) {
			return admin.Semantic.ListSemanticRules(callCtx, &adminv1.ListSemanticRulesRequest{SpaceId: semanticResources.SpaceID, DomainId: semanticResources.DomainID, PageSize: 500, IncludeDisabled: true})
		})
		if listErr == nil {
			for _, existing := range listed.GetRules() {
				if existing.GetKey() == rule.Key {
					semanticResources.SemanticRuleID = existing.GetSemanticRuleId()
					err = nil
					break
				}
			}
		}
		if err != nil {
			return semanticOperationFailed(semanticResources, "semantic-rule", err)
		}
	}
	if semanticResources.SemanticRuleID == "" && ruleResponse.GetRule() == nil {
		return semanticOperationFailed(semanticResources, "semantic-rule", errors.New("semantic rule response is incomplete"))
	}
	if semanticResources.SemanticRuleID == "" {
		semanticResources.SemanticRuleID = ruleResponse.GetRule().GetSemanticRuleId()
	}
	if _, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.GetSemanticReliabilityStatusResponse, error) {
		return admin.SemanticMaintenance.GetSemanticReliabilityStatus(callCtx, &adminv1.GetSemanticReliabilityStatusRequest{SpaceId: semanticResources.SpaceID, SemanticRuleId: semanticResources.SemanticRuleID, EmbeddingBindingKey: semanticResources.EmbeddingBindingKey})
	}); err != nil {
		return semanticOperationFailed(semanticResources, "semantic-reliability-capability", fmt.Errorf("required SGT5 semantic reliability capability unavailable: %w", err))
	}
	semanticOperationCompleted(semanticResources, "semantic-rule")
	semanticOperationCompleted(semanticResources, "semantic-reliability-capability")
	return nil
}

func withAdminAuth[T any](ctx context.Context, admin *mycel.AdminClient, call func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := admin.AuthCallContext(ctx)
	defer cancel()
	return call(callCtx)
}

func semanticOperationCompleted(resources *SemanticResources, resource string) {
	setSemanticOperationStatus(resources, resource, "completed", "")
}

func setSemanticOperationStatus(resources *SemanticResources, resource, state, message string) {
	for index := range resources.Operations {
		if resources.Operations[index].Resource == resource {
			resources.Operations[index].Status = state
			resources.Operations[index].Error = message
			return
		}
	}
}

func semanticOperationFailed(resources *SemanticResources, resource string, err error) error {
	for index := range resources.Operations {
		if resources.Operations[index].Resource == resource {
			resources.Operations[index].Status = "failed"
			resources.Operations[index].Error = err.Error()
			break
		}
	}
	return err
}

func cleanupSemanticResources(ctx context.Context, admin *mycel.AdminClient, resources *SemanticResources) []string {
	if resources == nil {
		return nil
	}
	var failures []string
	steps := []struct {
		resource string
		id       string
		call     func(context.Context) error
	}{
		{"semantic-rule", resources.SemanticRuleID, func(callCtx context.Context) error {
			_, err := admin.Semantic.DeleteSemanticRule(callCtx, &adminv1.DeleteSemanticRuleRequest{SpaceId: resources.SpaceID, SemanticRuleId: resources.SemanticRuleID, PurgeVectors: true})
			return err
		}},
		{"access-policy", resources.AccessPolicyID, func(callCtx context.Context) error {
			_, err := admin.InferencePolicies.DeleteAccessPolicy(callCtx, &adminv1.AdminIntelligenceAccessPolicyServiceDeleteAccessPolicyRequest{SpaceId: resources.SpaceID, AccessPolicyId: resources.AccessPolicyID})
			return err
		}},
		{"credential-grant", resources.CredentialGrantID, func(callCtx context.Context) error {
			_, err := admin.InferenceGrants.DeleteCredentialGrant(callCtx, &adminv1.AdminIntelligenceAccessGrantServiceDeleteCredentialGrantRequest{SpaceId: resources.SpaceID, CredentialGrantId: resources.CredentialGrantID})
			return err
		}},
		{"credential", resources.CredentialID, func(callCtx context.Context) error {
			_, err := admin.InferenceCredentials.DeleteCredential(callCtx, &adminv1.AdminIntelligenceAccessCredentialServiceDeleteCredentialRequest{CredentialId: resources.CredentialID, DeleteGrants: true, DeleteSecret: true})
			return err
		}},
		{"intelligence-profile", resources.IntelligenceProfileID, func(callCtx context.Context) error {
			_, err := admin.InferenceProfiles.DeleteIntelligenceProfile(callCtx, &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: resources.SpaceID, IntelligenceProfileId: resources.IntelligenceProfileID})
			return err
		}},
		{"capability", resources.CapabilityID, func(callCtx context.Context) error {
			_, err := admin.InferenceCatalog.DeleteModelEndpointCapability(callCtx, &adminv1.AdminInferenceCatalogServiceDeleteModelEndpointCapabilityRequest{ModelEndpointCapabilityId: resources.CapabilityID})
			return err
		}},
		{"vector-store", resources.VectorStoreID, func(callCtx context.Context) error {
			_, err := admin.InferenceCatalog.DeleteVectorStore(callCtx, &adminv1.AdminInferenceCatalogServiceDeleteVectorStoreRequest{VectorStoreId: resources.VectorStoreID})
			return err
		}},
		{"model", resources.ModelID, func(callCtx context.Context) error {
			_, err := admin.InferenceCatalog.DeleteModel(callCtx, &adminv1.AdminInferenceCatalogServiceDeleteModelRequest{ModelId: resources.ModelID})
			return err
		}},
		{"endpoint", resources.EndpointID, func(callCtx context.Context) error {
			_, err := admin.InferenceCatalog.DeleteModelEndpoint(callCtx, &adminv1.AdminInferenceCatalogServiceDeleteModelEndpointRequest{ModelEndpointId: resources.EndpointID})
			return err
		}},
	}
	for _, step := range steps {
		if strings.TrimSpace(step.id) == "" {
			setSemanticCleanupStatus(resources, step.resource, "not-created", "")
			continue
		}
		callCtx, cancel := admin.AuthCallContext(ctx)
		err := step.call(callCtx)
		cancel()
		if err != nil && status.Code(err) != codes.NotFound {
			message := fmt.Sprintf("delete %s %s: %v", step.resource, step.id, err)
			failures = append(failures, message)
			setSemanticCleanupStatus(resources, step.resource, "failed", err.Error())
			continue
		}
		setSemanticCleanupStatus(resources, step.resource, "completed", "")
	}
	return failures
}

func setSemanticCleanupStatus(resources *SemanticResources, resource, state, message string) {
	for index := range resources.Cleanup {
		if resources.Cleanup[index].Resource == resource {
			resources.Cleanup[index].Status = state
			resources.Cleanup[index].Error = message
			return
		}
	}
}

var _ = structpb.NewStruct
