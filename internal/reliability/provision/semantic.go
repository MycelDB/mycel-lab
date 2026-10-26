package provision

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func ValidateSemanticProviderBudgetStatusIdentity(status *adminv1.SemanticProviderBudgetStatus, resources *SemanticResources) error {
	if status == nil || resources == nil || resources.ExpectedProviderBudget == nil {
		return errors.New("semantic provider budget status identity is incomplete")
	}
	if status.GetSpaceId() != resources.SpaceID || status.GetSemanticRuleId() != resources.SemanticRuleID || status.GetEmbeddingBindingKey() != resources.EmbeddingBindingKey || status.GetCorrelationId() != resources.ExpectedProviderBudget.GetCorrelationId() || status.GetBudgetScopeId() == "" {
		return errors.New("semantic provider budget status identity differs from the requested scope")
	}
	if resources.ProviderBudgetScopeID != "" && status.GetBudgetScopeId() != resources.ProviderBudgetScopeID {
		return errors.New("semantic provider budget status scope differs from the retained scope")
	}
	return nil
}

func provisionSemanticResources(ctx context.Context, admin *mycel.AdminClient, resources *ScenarioResources, opts Options) error {
	if resources == nil || resources.Semantic == nil || resources.semanticPlan == nil {
		return nil
	}
	semanticResources := resources.Semantic
	plan := resources.semanticPlan
	if semanticResources.SpaceID == "" || semanticResources.DomainID == "" {
		return errors.New("semantic target assignment is missing space or domain identity")
	}
	if plan.ProviderProfile != nil && plan.ProviderProfile.Profile.Mode == provider.ModeLive {
		if err := discoverAndAuthorizeLiveSemanticProvider(ctx, admin, resources, opts); err != nil {
			failedResource := "provider-budget-capability"
			if semanticOperationStatus(semanticResources, "provider-budget-capability") == "completed" {
				failedResource = "credential"
			}
			return semanticOperationFailed(semanticResources, failedResource, err)
		}
	} else {
		setSemanticOperationStatus(semanticResources, "provider-budget-capability", "not-required", "")
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
	networkClass := strings.TrimSpace(plan.Capability.NetworkClass)
	privacyClass := strings.TrimSpace(plan.Capability.PrivacyClass)
	if networkClass == "" {
		networkClass = "local"
	}
	if privacyClass == "" {
		privacyClass = "local_only"
	}
	if modelName == "" {
		modelName = semanticResources.ModelKey
	}
	applyResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse, error) {
		return admin.InferenceCatalog.ApplyInferencePackage(callCtx, &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{
			Name: semanticResources.PackageName, Version: semanticResources.PackageVersion, Source: "mycel-lab", Checksum: semanticResources.PackageName,
			ModelEndpoints: []*adminv1.ModelEndpoint{{
				Key: semanticResources.EndpointKey, Name: semanticResources.EndpointKey, ConnectorType: plan.Capability.ConnectorType,
				EndpointUrl: endpointURL, NetworkClass: networkClass, PrivacyClass: privacyClass, AuthModes: []string{semanticEndpointAuthMode(plan.Secret)}, Operations: []string{"embeddings"}, Enabled: true, Metadata: endpointMetadata,
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
			PrivacyRequirement: semanticPrivacyRequirement(privacyClass), Enabled: true,
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

	if semanticResources.LiveProvider && strings.TrimSpace(semanticResources.TargetPrincipalID) == "" {
		return semanticOperationFailed(semanticResources, "credential-grant", errors.New("live semantic target principal is unavailable"))
	}
	grantResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessGrantServiceCreateCredentialGrantResponse, error) {
		request := &adminv1.AdminIntelligenceAccessGrantServiceCreateCredentialGrantRequest{
			SpaceId: semanticResources.SpaceID, CredentialId: semanticResources.CredentialID,
			Scope:      &commonv1.IntelligenceAccessScope{SpaceId: semanticResources.SpaceID, DomainId: semanticResources.DomainID, IncludeDescendants: true},
			Operations: []string{"embeddings"}, ModelEndpointId: semanticResources.EndpointID, ModelId: semanticResources.ModelID,
			IsDefault: true, AllowBackgroundUse: true,
		}
		if semanticResources.LiveProvider {
			request.GranteePrincipalIds = []string{semanticResources.TargetPrincipalID}
		}
		return admin.InferenceGrants.CreateCredentialGrant(callCtx, request)
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
			Effect:  "allow", Operations: []string{"embeddings"}, AllowedPrivacyClasses: []string{privacyClass}, RequireLocalEndpoint: privacyClass == "local_only",
			DisallowThirdParty: privacyClass != "third_party", Reason: "mycel-lab semantic run " + resources.RunID,
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
	providerBudget := semanticProviderBudgetPolicy(resources.semanticPlan)
	if semanticResources.LiveProvider {
		semanticResources.ExpectedProviderBudget = proto.Clone(providerBudget).(*adminv1.SemanticProviderBudgetPolicy)
	}
	ruleResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.CreateSemanticRuleResponse, error) {
		return admin.Semantic.CreateSemanticRule(callCtx, &adminv1.CreateSemanticRuleRequest{Rule: &adminv1.SemanticGenerationRule{
			SpaceId: semanticResources.SpaceID, DomainId: semanticResources.DomainID, Key: rule.Key, DisplayName: rule.DisplayName, Description: rule.Description, Enabled: true, OwnerPrincipalId: semanticResources.TargetPrincipalID,
			Trigger:     &adminv1.SemanticTriggerPolicy{Events: []string{"node.created", "node.updated", "node.deleted"}, Debounce: rule.DirtyCooldown.String()},
			Selector:    &adminv1.SemanticTargetSelector{Mode: "node_type", Labels: append([]string(nil), rule.TargetLabels...), MaxResults: 4096},
			Source:      &adminv1.SemanticSourceAssemblyPolicy{Mode: rule.SourceMode, IncludeProperties: append([]string(nil), rule.IncludeProperties...), ExcludeProperties: append([]string(nil), rule.ExcludeProperties...), MinimumTextLength: int32(rule.MinimumTextLength)},
			Embeddings:  []*adminv1.SemanticEmbeddingBinding{{Key: rule.BindingKey, Purpose: rule.Purpose, IntelligenceProfileId: semanticResources.IntelligenceProfileID, VectorStoreId: semanticResources.VectorStoreID, Enabled: true, ProviderBudget: providerBudget}},
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
	if resources.semanticPlan.ProviderProfile != nil && resources.semanticPlan.ProviderProfile.Profile.Mode == provider.ModeLive {
		statusResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.GetSemanticProviderBudgetStatusResponse, error) {
			return admin.SemanticMaintenance.GetSemanticProviderBudgetStatus(callCtx, &adminv1.GetSemanticProviderBudgetStatusRequest{SpaceId: semanticResources.SpaceID, Selector: &adminv1.GetSemanticProviderBudgetStatusRequest_CurrentBinding{CurrentBinding: &adminv1.SemanticProviderBudgetBindingSelector{SemanticRuleId: semanticResources.SemanticRuleID, EmbeddingBindingKey: semanticResources.EmbeddingBindingKey}}})
		})
		if err != nil {
			return semanticOperationFailed(semanticResources, "semantic-rule", fmt.Errorf("semantic provider budget status unavailable after rule creation: %w", err))
		}
		budgetStatus := statusResponse.GetStatus()
		if err := ValidateSemanticProviderBudgetStatusIdentity(budgetStatus, semanticResources); err != nil {
			return semanticOperationFailed(semanticResources, "semantic-rule", err)
		}
		if budgetStatus.GetState() != adminv1.SemanticProviderBudgetScopeState_SEMANTIC_PROVIDER_BUDGET_SCOPE_STATE_ACTIVE || budgetStatus.GetDenialReason() != adminv1.SemanticProviderBudgetDenialReason_SEMANTIC_PROVIDER_BUDGET_DENIAL_REASON_NONE {
			return semanticOperationFailed(semanticResources, "semantic-rule", errors.New("semantic provider budget scope is not active after rule creation"))
		}
		if !proto.Equal(budgetStatus.GetLimits(), semanticResources.ExpectedProviderBudget) || budgetStatus.GetModelEndpointId() != semanticResources.EndpointID || budgetStatus.GetModelId() != semanticResources.ModelID || budgetStatus.GetModelEndpointCapabilityId() != semanticResources.CapabilityID {
			return semanticOperationFailed(semanticResources, "semantic-rule", errors.New("semantic provider budget scope policy or pinned route does not match the requested configuration"))
		}
		semanticResources.ProviderBudgetScopeID = budgetStatus.GetBudgetScopeId()
	}
	semanticOperationCompleted(semanticResources, "semantic-rule")
	semanticOperationCompleted(semanticResources, "semantic-reliability-capability")
	return nil
}

func discoverAndAuthorizeLiveSemanticProvider(ctx context.Context, admin *mycel.AdminClient, resources *ScenarioResources, opts Options) error {
	plan := resources.semanticPlan
	capabilities, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.GetSemanticProviderBudgetCapabilitiesResponse, error) {
		return admin.SemanticMaintenance.GetSemanticProviderBudgetCapabilities(callCtx, &adminv1.GetSemanticProviderBudgetCapabilitiesRequest{SpaceId: resources.Semantic.SpaceID})
	})
	if err != nil {
		return fmt.Errorf("authenticated semantic provider budget discovery failed: %w", err)
	}
	if strings.TrimSpace(capabilities.GetContractVersion()) != "semantic-provider-budget/v1" {
		return fmt.Errorf("unsupported semantic provider budget contract %q", capabilities.GetContractVersion())
	}
	checks := []struct {
		name string
		ok   bool
	}{
		{"daemon_owned_scopes", capabilities.GetDaemonOwnedScopes()},
		{"raft_replicated", capabilities.GetRaftReplicated()},
		{"pre_provider_attempt_reservation", capabilities.GetPreProviderAttemptReservation()},
		{"durable_restart_recovery", capabilities.GetDurableRestartRecovery()},
		{"request_limit", capabilities.GetRequestLimit()},
		{"derived_attempt_limit", capabilities.GetDerivedAttemptLimit()},
		{"aggregate_retry_limit", capabilities.GetAggregateRetryLimit()},
		{"per_request_retry_limit", capabilities.GetPerRequestRetryLimit()},
		{"provider_concurrency_limit", capabilities.GetProviderConcurrencyLimit()},
		{"per_request_input_token_limit", capabilities.GetPerRequestInputTokenLimit()},
		{"aggregate_input_token_limit", capabilities.GetAggregateInputTokenLimit()},
		{"request_timeout", capabilities.GetRequestTimeout()},
		{"total_timeout", capabilities.GetTotalTimeout()},
		{"versioned_cost_limit", capabilities.GetVersionedCostLimit()},
		{"conservative_ambiguous_reservations", capabilities.GetConservativeAmbiguousReservations()},
		{"append_only_attempt_events", capabilities.GetAppendOnlyAttemptEvents()},
		{"overage_exhaustion", capabilities.GetOverageExhaustion()},
		{"bounded_attempt_event_pages", capabilities.GetMaximumAttemptEventPageSize() > 0},
		{"bounded_scope_pages", capabilities.GetMaximumScopePageSize() > 0},
	}
	batching := plan.ProviderProfile.Profile.Capabilities.Embedding.Batching
	checks = append(checks, struct {
		name string
		ok   bool
	}{name: "selected_batching_mode", ok: batching.Mode == "single" && batching.MaxItems == 1 && capabilities.GetSingleItemBatching() || batching.Mode == "batch" && capabilities.GetMultiItemBatching()})
	verified := make([]string, 0, len(checks))
	for _, check := range checks {
		if !check.ok {
			return fmt.Errorf("daemon semantic provider budget capability %s is not enforced", check.name)
		}
		verified = append(verified, check.name)
	}
	currencyOK := false
	for _, currency := range capabilities.GetSupportedCurrencies() {
		if currency == "USD" {
			currencyOK = true
			break
		}
	}
	if !currencyOK {
		return errors.New("daemon semantic provider budget capability does not support USD")
	}
	resources.Semantic.ProviderBudgetCapabilities = verified
	resources.Semantic.ProviderBudgetContractVersion = capabilities.GetContractVersion()
	resources.Semantic.ProviderBudgetMaxAttemptEventPage = capabilities.GetMaximumAttemptEventPageSize()
	resources.Semantic.ProviderBudgetMaxScopePage = capabilities.GetMaximumScopePageSize()
	resources.Semantic.EvidenceValues["providerBudgetDiscovery"] = "authenticated"
	setSemanticOperationStatus(resources.Semantic, "provider-budget-capability", "completed", "")
	if opts.PersistResources != nil {
		if err := opts.PersistResources(*resources); err != nil {
			return fmt.Errorf("persist authenticated semantic provider budget discovery: %w", err)
		}
	}

	manifest := provider.BuiltInHarnessManifest()
	for _, capability := range []provider.EnforcementCapability{
		provider.EnforceProviderRequests,
		provider.EnforceProviderConcurrency,
		provider.EnforceProviderRetries,
		provider.EnforceProviderRequestTimeout,
		provider.EnforceProviderTotalTimeout,
		provider.EnforceProviderInputTokens,
		provider.EnforceProviderEstimatedCost,
		provider.EnforceProviderBatchItems,
	} {
		manifest = manifest.With(capability, "authenticated-daemon:"+capabilities.GetContractVersion())
	}
	secrets, err := plan.ProviderProfile.CompleteLiveProviderValidation(manifest, opts.ProviderLookupEnv)
	if err != nil {
		return err
	}
	if len(secrets) != 1 {
		return errors.New("live semantic provider requires exactly one resolved credential")
	}
	plan.Secret = secrets[0]
	if opts.ProviderSecretObserver != nil {
		opts.ProviderSecretObserver(plan.Secret)
	}
	return nil
}

func semanticEndpointAuthMode(secret string) string {
	if secret != "" {
		return "api_key"
	}
	return "none"
}

func semanticPrivacyRequirement(privacyClass string) *commonv1.InferencePrivacyRequirement {
	if privacyClass == "third_party" {
		return &commonv1.InferencePrivacyRequirement{AllowedPrivacyClasses: []commonv1.InferencePrivacyClass{commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_THIRD_PARTY}}
	}
	if privacyClass == "private" || privacyClass == "customer_managed" {
		return &commonv1.InferencePrivacyRequirement{AllowedPrivacyClasses: []commonv1.InferencePrivacyClass{commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_PRIVATE}}
	}
	return &commonv1.InferencePrivacyRequirement{AllowedPrivacyClasses: []commonv1.InferencePrivacyClass{commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_LOCAL_ONLY}, RequireLocalEndpoint: true, DisallowThirdParty: true}
}

func semanticProviderBudgetPolicy(plan *semanticProvisionPlan) *adminv1.SemanticProviderBudgetPolicy {
	if plan == nil || plan.ProviderProfile == nil || plan.ProviderProfile.Profile.Mode != provider.ModeLive {
		return nil
	}
	budget := plan.ProviderProfile.Profile.Budgets
	capability := plan.ProviderProfile.Profile.Capabilities.Embedding
	maxRequests := budget.MaxRequests
	maxRetryAttempts := maxRequests * budget.MaxRetriesPerRequest
	maxRetriesPerRequest := budget.MaxRetriesPerRequest
	maxConcurrent := budget.MaxConcurrentRequests
	maxBatchItems := int64(capability.Batching.MaxItems)
	maxInputPerRequest := budget.MaxInputTokensPerRequest
	maxInput := budget.MaxInputTokens
	batchingMode := adminv1.SemanticProviderBatchingMode_SEMANTIC_PROVIDER_BATCHING_MODE_SINGLE
	if capability.Batching.Mode == "batch" {
		batchingMode = adminv1.SemanticProviderBatchingMode_SEMANTIC_PROVIDER_BATCHING_MODE_BATCH
	}
	policy := &adminv1.SemanticProviderBudgetPolicy{
		CorrelationId:              plan.ProviderProfile.Profile.Metadata.Name,
		MaxRequests:                &maxRequests,
		MaxRetryAttempts:           &maxRetryAttempts,
		MaxRetriesPerRequest:       &maxRetriesPerRequest,
		MaxConcurrentProviderCalls: &maxConcurrent,
		BatchingMode:               &batchingMode,
		MaxBatchItems:              &maxBatchItems,
		MaxInputTokensPerRequest:   &maxInputPerRequest,
		MaxInputTokens:             &maxInput,
		RequestTimeout:             durationpb.New(budget.RequestTimeout.Duration),
		TotalTimeout:               durationpb.New(budget.TotalTimeout.Duration),
	}
	if budget.Cost != nil {
		inputPrice := budget.Cost.InputMicrosPerMillionTokens
		maxCost := budget.Cost.MaxEstimatedCostMicros
		policy.Cost = &adminv1.SemanticProviderCostLimit{Currency: budget.Cost.Currency, PriceDefinitionVersion: budget.Cost.PriceDefinitionVersion, InputMicrosPerMillionTokens: &inputPrice, MaxEstimatedCostMicros: &maxCost}
	}
	return policy
}

func withAdminAuth[T any](ctx context.Context, admin *mycel.AdminClient, call func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := admin.AuthCallContext(ctx)
	defer cancel()
	return call(callCtx)
}

func semanticOperationCompleted(resources *SemanticResources, resource string) {
	setSemanticOperationStatus(resources, resource, "completed", "")
}

func semanticOperationStatus(resources *SemanticResources, resource string) string {
	for _, operation := range resources.Operations {
		if operation.Resource == resource {
			return operation.Status
		}
	}
	return ""
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
	recordedError := err.Error()
	returnedError := err
	if resources.LiveProvider {
		category := "operation_failed"
		if code := status.Code(err); code != codes.Unknown {
			category = strings.ToLower(code.String())
		} else if errors.Is(err, context.Canceled) {
			category = "canceled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			category = "deadline_exceeded"
		}
		recordedError = category
		returnedError = fmt.Errorf("live semantic provisioning failed (%s)", category)
	}
	for index := range resources.Operations {
		if resources.Operations[index].Resource == resource {
			resources.Operations[index].Status = "failed"
			resources.Operations[index].Error = recordedError
			break
		}
	}
	return returnedError
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
			if semanticCleanupRetainedByAudit(err) {
				setSemanticCleanupStatus(resources, step.resource, "retained-by-audit", "immutable audit references")
				continue
			}
			message := sanitizedSemanticCleanupFailure(step.resource, err)
			failures = append(failures, message)
			setSemanticCleanupStatus(resources, step.resource, "failed", message)
			continue
		}
		setSemanticCleanupStatus(resources, step.resource, "completed", "")
	}
	return failures
}

const (
	maxCleanupReferenceEvidence     = 20
	maxCleanupReferenceTokens       = maxCleanupReferenceEvidence + 1
	maxCleanupReferenceMessageBytes = 4096
)

type semanticCleanupReference struct {
	Type string
	ID   string
}

func semanticCleanupReferences(err error) ([]semanticCleanupReference, string, int) {
	if status.Code(err) != codes.FailedPrecondition {
		return nil, "not-failed-precondition", 0
	}
	message := status.Convert(err).Message()
	if len(message) > maxCleanupReferenceMessageBytes {
		return nil, "reference-evidence-too-large", 0
	}
	const marker = " is referenced by "
	index := strings.Index(message, marker)
	if index < 0 {
		return nil, "unrecognized-precondition", 0
	}
	referenceText := strings.TrimSpace(message[index+len(marker):])
	if referenceText == "" {
		return nil, "empty-reference-list", 0
	}
	observedReferenceCount := strings.Count(referenceText, ",") + 1
	tokens := strings.SplitN(referenceText, ",", maxCleanupReferenceTokens+1)
	parseTokens := tokens
	forcedRejection := ""
	if observedReferenceCount > maxCleanupReferenceTokens || (observedReferenceCount > maxCleanupReferenceEvidence && !semanticCleanupTruncationPattern.MatchString(strings.TrimSpace(tokens[observedReferenceCount-1]))) {
		forcedRejection = "reference-list-too-large"
		parseTokens = tokens[:maxCleanupReferenceEvidence]
	}
	references := make([]semanticCleanupReference, 0, min(len(parseTokens), maxCleanupReferenceEvidence))
	for _, token := range parseTokens {
		token = strings.TrimSpace(token)
		matches := semanticCleanupReferencePattern.FindStringSubmatch(token)
		if len(matches) != 3 {
			// The daemon truncates untyped reference lists with "...N more".
			// Reject those lists because the hidden references are not proven to
			// belong to immutable audit history.
			if forcedRejection != "" {
				return references, forcedRejection, observedReferenceCount
			}
			if semanticCleanupTruncationPattern.MatchString(token) {
				return references, "truncated-reference-list", observedReferenceCount
			}
			return references, "malformed-reference-list", observedReferenceCount
		}
		references = append(references, semanticCleanupReference{Type: matches[1], ID: strings.ToLower(matches[2])})
	}
	if len(references) == 0 {
		return nil, "empty-reference-list", 0
	}
	if forcedRejection != "" {
		return references, forcedRejection, observedReferenceCount
	}
	return references, "", len(references)
}

func semanticCleanupRetainedByAudit(err error) bool {
	references, rejectionCategory, _ := semanticCleanupReferences(err)
	if rejectionCategory != "" {
		return false
	}
	for _, reference := range references {
		if reference.Type != "usage_event" && reference.Type != "policy_decision" {
			return false
		}
	}
	return true
}

var (
	semanticCleanupReferencePattern  = regexp.MustCompile(`^([a-z][a-z_]*):([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`)
	semanticCleanupTruncationPattern = regexp.MustCompile(`^\.\.\.[1-9][0-9]* more$`)
)

func sanitizedSemanticCleanupFailure(resource string, err error) string {
	return fmt.Sprintf("delete %s: %s", resource, sanitizedProcedureInferenceProvisionError(err))
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
