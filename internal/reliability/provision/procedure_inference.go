package provision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func provisionProcedureInferenceResources(ctx context.Context, admin *mycel.AdminClient, resources *ScenarioResources) error {
	if resources == nil || resources.ProcedureInference == nil || resources.procedureInferencePlan == nil {
		return nil
	}
	out := resources.ProcedureInference
	plan := resources.procedureInferencePlan
	if out.SpaceID == "" || out.DomainID == "" {
		return errors.New("procedure inference target assignment is missing space or domain identity")
	}
	writerPrincipalID := procedureInferenceTargetPrincipalID(resources)
	if writerPrincipalID == "" {
		return errors.New("procedure inference target assignment is missing principal identity")
	}
	modelName := strings.TrimSpace(plan.Capability.ProviderModel)
	if modelName == "" {
		modelName = out.ModelKey
	}
	metadata, err := structpb.NewStruct(map[string]any{"mycel_lab_mode": plan.Capability.Mode})
	if err != nil {
		return err
	}
	applyResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse, error) {
		return admin.InferenceCatalog.ApplyInferencePackage(callCtx, &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{
			Name: out.PackageName, Version: out.PackageVersion, Source: "mycel-lab", Checksum: out.PackageName,
			ModelEndpoints: []*adminv1.ModelEndpoint{{
				Key: out.EndpointKey, Name: out.EndpointKey, ConnectorType: plan.Capability.ConnectorType,
				EndpointUrl: strings.TrimSpace(plan.Capability.BaseURL), NetworkClass: "local", PrivacyClass: "local_only",
				AuthModes: []string{"none"}, Operations: []string{"chat"}, Enabled: true, Metadata: metadata,
			}},
			Models: []*adminv1.InferenceModel{{
				Key: out.ModelKey, Kind: "generative", ModelName: modelName, ConnectorTypes: []string{plan.Capability.ConnectorType},
				InputModalities: []string{"text"}, OutputModalities: []string{"text"}, Enabled: true, Metadata: metadata,
			}},
			ModelEndpointCapabilities: []*adminv1.ModelEndpointCapabilityDefinition{{
				ModelEndpoint: out.EndpointKey, Model: out.ModelKey, Operation: "chat", Enabled: proto.Bool(true), ModelNameOverride: modelName,
			}},
		})
	})
	if err != nil {
		return procedureInferenceOperationFailed(out, "inference-package", err)
	}
	if applyResponse.GetPackage() == nil || len(applyResponse.GetModelEndpoints()) != 1 || len(applyResponse.GetModels()) != 1 || len(applyResponse.GetModelEndpointCapabilities()) != 1 {
		return procedureInferenceOperationFailed(out, "inference-package", errors.New("catalog package response is incomplete"))
	}
	out.PackageID = applyResponse.GetPackage().GetInferencePackageId()
	out.EndpointID = applyResponse.GetModelEndpoints()[0].GetModelEndpointId()
	out.ModelID = applyResponse.GetModels()[0].GetModelId()
	out.CapabilityID = applyResponse.GetModelEndpointCapabilities()[0].GetModelEndpointCapabilityId()
	procedureInferenceOperationCompleted(out, "inference-package")

	profileResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileResponse, error) {
		return admin.InferenceProfiles.CreateIntelligenceProfile(callCtx, &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest{
			SpaceId: out.SpaceID, Key: out.IntelligenceProfileKey, DisplayName: out.IntelligenceProfileKey,
			Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT, Purpose: "automation", DomainIds: []string{out.DomainID},
			CapabilityRefs: []string{out.CapabilityID}, EndpointRefs: []string{out.EndpointID}, ModelRefs: []string{out.ModelID},
			PrivacyRequirement: &commonv1.InferencePrivacyRequirement{AllowedPrivacyClasses: []commonv1.InferencePrivacyClass{commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_LOCAL_ONLY}, RequireLocalEndpoint: true, DisallowThirdParty: true}, Enabled: true,
		})
	})
	if err != nil {
		return procedureInferenceOperationFailed(out, "intelligence-profile", err)
	}
	if profileResponse.GetIntelligenceProfile() == nil {
		return procedureInferenceOperationFailed(out, "intelligence-profile", errors.New("profile response is incomplete"))
	}
	out.IntelligenceProfileID = profileResponse.GetIntelligenceProfile().GetIntelligenceProfileId()
	procedureInferenceOperationCompleted(out, "intelligence-profile")

	credentialResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialResponse, error) {
		authType := "none"
		if plan.Secret != "" {
			authType = "api_key"
		}
		return admin.InferenceCredentials.CreateCredential(callCtx, &adminv1.AdminIntelligenceAccessCredentialServiceCreateCredentialRequest{
			Key: out.CredentialKey, DisplayName: out.CredentialKey, ModelEndpointId: out.EndpointID,
			OwnerType: "space", OwnerId: out.SpaceID, AuthType: authType, IsDefault: true, SecretValue: plan.Secret,
		})
	})
	if err != nil {
		return procedureInferenceOperationFailed(out, "credential", err)
	}
	if credentialResponse.GetCredential() == nil {
		return procedureInferenceOperationFailed(out, "credential", errors.New("credential response is incomplete"))
	}
	out.CredentialID = credentialResponse.GetCredential().GetCredentialId()
	procedureInferenceOperationCompleted(out, "credential")

	grantResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessGrantServiceCreateCredentialGrantResponse, error) {
		return admin.InferenceGrants.CreateCredentialGrant(callCtx, procedureInferenceCredentialGrantRequest(out, writerPrincipalID))
	})
	if err != nil {
		return procedureInferenceOperationFailed(out, "credential-grant", err)
	}
	if grantResponse.GetCredentialGrant() == nil {
		return procedureInferenceOperationFailed(out, "credential-grant", errors.New("grant response is incomplete"))
	}
	out.CredentialGrantID = grantResponse.GetCredentialGrant().GetCredentialGrantId()
	procedureInferenceOperationCompleted(out, "credential-grant")

	policyResponse, err := withAdminAuth(ctx, admin, func(callCtx context.Context) (*adminv1.AdminIntelligenceAccessPolicyServiceCreateAccessPolicyResponse, error) {
		return admin.InferencePolicies.CreateAccessPolicy(callCtx, &adminv1.AdminIntelligenceAccessPolicyServiceCreateAccessPolicyRequest{
			SpaceId: out.SpaceID, Scope: &commonv1.IntelligenceAccessScope{SpaceId: out.SpaceID, DomainId: out.DomainID, IncludeDescendants: true},
			Effect: "allow", Operations: []string{"chat"}, AllowedPrivacyClasses: []string{"local_only"}, RequireLocalEndpoint: true,
			DisallowThirdParty: true, Reason: "mycel-lab procedure inference run " + resources.RunID,
		})
	})
	if err != nil {
		return procedureInferenceOperationFailed(out, "access-policy", err)
	}
	if policyResponse.GetAccessPolicy() == nil {
		return procedureInferenceOperationFailed(out, "access-policy", errors.New("policy response is incomplete"))
	}
	out.AccessPolicyID = policyResponse.GetAccessPolicy().GetAccessPolicyId()
	procedureInferenceOperationCompleted(out, "access-policy")
	return nil
}

func procedureInferenceCredentialGrantRequest(resources *ProcedureInferenceResources, writerPrincipalID string) *adminv1.AdminIntelligenceAccessGrantServiceCreateCredentialGrantRequest {
	if resources == nil {
		return nil
	}
	return &adminv1.AdminIntelligenceAccessGrantServiceCreateCredentialGrantRequest{
		SpaceId: resources.SpaceID, CredentialId: resources.CredentialID,
		Scope:      &commonv1.IntelligenceAccessScope{SpaceId: resources.SpaceID, DomainId: resources.DomainID, IncludeDescendants: true},
		Operations: []string{"chat"}, ModelEndpointId: resources.EndpointID, ModelId: resources.ModelID, IsDefault: true, AllowBackgroundUse: true,
		AllowOnBehalfOfPrincipalIds: []string{strings.TrimSpace(writerPrincipalID)},
	}
}

func procedureInferenceTargetPrincipalID(resources *ScenarioResources) string {
	if resources == nil || resources.ProcedureInference == nil {
		return ""
	}
	for _, assignment := range resources.Assignments {
		if assignment.ActorID == resources.ProcedureInference.TargetActorID {
			return strings.TrimSpace(assignment.PrincipalID)
		}
	}
	return ""
}

func cleanupProcedureInferenceResources(ctx context.Context, admin *mycel.AdminClient, resources *ProcedureInferenceResources) []string {
	if resources == nil {
		return nil
	}
	if strings.TrimSpace(resources.PackageID) == "" {
		setProcedureInferenceCleanupStatus(resources, "inference-package", "not-created", "", nil)
	} else {
		setProcedureInferenceCleanupStatus(resources, "inference-package", "retained-by-design", "", map[string]any{"retentionCategory": "catalog-package-no-delete-api"})
	}
	var failures []string
	steps := []struct {
		resource string
		id       string
		call     func(context.Context) error
	}{
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
			setProcedureInferenceCleanupStatus(resources, step.resource, "not-created", "", nil)
			continue
		}
		callCtx, cancel := admin.AuthCallContext(ctx)
		err := step.call(callCtx)
		cancel()
		if err != nil && status.Code(err) != codes.NotFound {
			if details, retained := procedureInferenceCleanupRetainedByAudit(resources, err); retained {
				setProcedureInferenceCleanupStatus(resources, step.resource, "retained-by-audit", "immutable audit closure", details)
				continue
			}
			message := fmt.Sprintf("delete %s: %s", step.resource, sanitizedProcedureInferenceProvisionError(err))
			failures = append(failures, message)
			setProcedureInferenceCleanupStatus(resources, step.resource, "failed", message, nil)
			continue
		}
		setProcedureInferenceCleanupStatus(resources, step.resource, "completed", "", nil)
	}
	return failures
}

func procedureInferenceCleanupRetainedByAudit(resources *ProcedureInferenceResources, err error) (map[string]any, bool) {
	references, ok := semanticCleanupReferences(err)
	if !ok {
		return nil, false
	}
	for _, reference := range references {
		if reference.Type == "usage_event" || reference.Type == "policy_decision" {
			continue
		}
		if !procedureInferenceRunOwnedRetainedReference(resources, reference) {
			return nil, false
		}
	}
	sort.Slice(references, func(i, j int) bool {
		if references[i].Type == references[j].Type {
			return references[i].ID < references[j].ID
		}
		return references[i].Type < references[j].Type
	})
	evidence := make([]map[string]string, 0, len(references))
	for _, reference := range references {
		digest := sha256.Sum256([]byte(reference.Type + ":" + reference.ID))
		evidence = append(evidence, map[string]string{"type": reference.Type, "idHash": hex.EncodeToString(digest[:])})
	}
	return map[string]any{
		"retentionCategory": "immutable-audit-closure",
		"referenceCount":    len(references),
		"references":        evidence,
	}, true
}

func procedureInferenceRunOwnedRetainedReference(resources *ProcedureInferenceResources, reference semanticCleanupReference) bool {
	if resources == nil {
		return false
	}
	var resource, id string
	switch reference.Type {
	case "credential_grant":
		resource, id = "credential-grant", resources.CredentialGrantID
	case "credential":
		resource, id = "credential", resources.CredentialID
	case "intelligence_profile":
		resource, id = "intelligence-profile", resources.IntelligenceProfileID
	case "capability":
		resource, id = "capability", resources.CapabilityID
	default:
		return false
	}
	return strings.EqualFold(strings.TrimSpace(id), reference.ID) && procedureInferenceCleanupState(resources, resource) == "retained-by-audit"
}

func procedureInferenceCleanupState(resources *ProcedureInferenceResources, resource string) string {
	if resources == nil {
		return ""
	}
	for _, operation := range resources.Cleanup {
		if operation.Resource == resource {
			return operation.Status
		}
	}
	return ""
}

func procedureInferenceOperationCompleted(resources *ProcedureInferenceResources, resource string) {
	setProcedureInferenceOperationStatus(resources, resource, "completed", "")
}

func procedureInferenceOperationFailed(resources *ProcedureInferenceResources, resource string, err error) error {
	setProcedureInferenceOperationStatus(resources, resource, "failed", sanitizedProcedureInferenceProvisionError(err))
	return err
}

func sanitizedProcedureInferenceProvisionError(err error) string {
	if err == nil {
		return ""
	}
	if code := status.Code(err); code != codes.Unknown {
		return "rpc_" + strings.ToLower(code.String())
	}
	return "operation_failed"
}

func setProcedureInferenceOperationStatus(resources *ProcedureInferenceResources, resource, state, message string) {
	for index := range resources.Operations {
		if resources.Operations[index].Resource == resource {
			resources.Operations[index].Status = state
			resources.Operations[index].Error = message
			return
		}
	}
}

func setProcedureInferenceCleanupStatus(resources *ProcedureInferenceResources, resource, state, message string, details map[string]any) {
	for index := range resources.Cleanup {
		if resources.Cleanup[index].Resource == resource {
			resources.Cleanup[index].Status = state
			resources.Cleanup[index].Error = message
			resources.Cleanup[index].Details = details
			return
		}
	}
}
