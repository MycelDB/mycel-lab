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

type procedureInferenceCleanupPass struct {
	Round int
	Name  string
}

type procedureInferenceCleanupProof struct {
	nodes map[string]procedureInferenceCleanupProofNode
}

type procedureInferenceCleanupProofNode struct {
	ID            string
	Details       map[string]any
	ImmutableRoot bool
	Dependencies  []procedureInferenceCleanupProofDependency
}

type procedureInferenceCleanupProofDependency struct {
	Resource string
	ID       string
}

func nextProcedureInferenceCleanupRound(resources *ProcedureInferenceResources) int {
	if resources == nil {
		return 0
	}
	resources.cleanupRound++
	return resources.cleanupRound
}

func procedureInferenceProof(resources *ProcedureInferenceResources) *procedureInferenceCleanupProof {
	if resources.cleanupProof == nil {
		resources.cleanupProof = &procedureInferenceCleanupProof{nodes: make(map[string]procedureInferenceCleanupProofNode)}
	}
	return resources.cleanupProof
}

func (proof *procedureInferenceCleanupProof) recordValidated(resources *ProcedureInferenceResources, resource string, references []semanticCleanupReference, details map[string]any) {
	if proof == nil || resources == nil {
		return
	}
	id := procedureInferenceCleanupResourceID(resources, resource)
	if id == "" {
		return
	}
	node := procedureInferenceCleanupProofNode{ID: strings.ToLower(id), Details: details}
	seen := make(map[string]struct{})
	for _, reference := range references {
		if reference.Type == "usage_event" || reference.Type == "policy_decision" {
			node.ImmutableRoot = true
			continue
		}
		dependencyResource, dependencyID := procedureInferenceRunOwnedReference(resources, reference)
		key := dependencyResource + ":" + strings.ToLower(dependencyID)
		if dependencyResource == "" || dependencyID == "" {
			return
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		node.Dependencies = append(node.Dependencies, procedureInferenceCleanupProofDependency{Resource: dependencyResource, ID: strings.ToLower(dependencyID)})
	}
	if proof.nodes == nil {
		proof.nodes = make(map[string]procedureInferenceCleanupProofNode)
	}
	proof.nodes[resource] = node
}

func (proof *procedureInferenceCleanupProof) node(resource, id string) (procedureInferenceCleanupProofNode, bool) {
	if proof == nil {
		return procedureInferenceCleanupProofNode{}, false
	}
	node, ok := proof.nodes[resource]
	if !ok || node.ID != strings.ToLower(strings.TrimSpace(id)) || !proof.rooted(resource, node.ID, make(map[string]bool)) {
		return procedureInferenceCleanupProofNode{}, false
	}
	return node, true
}

func (proof *procedureInferenceCleanupProof) rooted(resource, id string, visiting map[string]bool) bool {
	node, ok := proof.nodes[resource]
	if !ok || node.ID != strings.ToLower(strings.TrimSpace(id)) || visiting[resource] {
		return false
	}
	visiting[resource] = true
	defer delete(visiting, resource)
	rooted := node.ImmutableRoot
	for _, dependency := range node.Dependencies {
		if !proof.rooted(dependency.Resource, dependency.ID, visiting) {
			return false
		}
		rooted = true
	}
	return rooted
}

func (proof *procedureInferenceCleanupProof) contains(resource, id string) bool {
	_, ok := proof.node(resource, id)
	return ok
}

func (proof *procedureInferenceCleanupProof) clear() {
	if proof != nil {
		clear(proof.nodes)
	}
}

func cleanupProcedureInferenceResources(ctx context.Context, admin *mycel.AdminClient, resources *ProcedureInferenceResources, pass procedureInferenceCleanupPass) []string {
	if resources == nil {
		return nil
	}
	if strings.TrimSpace(resources.PackageID) == "" {
		setProcedureInferenceCleanupStatus(resources, "inference-package", "not-created", "", nil)
	} else {
		setProcedureInferenceCleanupStatus(resources, "inference-package", "retained-by-design", "", map[string]any{"retentionCategory": "catalog-package-no-delete-api"})
	}
	var failures []string
	proof := procedureInferenceProof(resources)
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
			recordProcedureInferenceCleanupAttempt(resources, step.resource, pass, "not-created", "not-created", "", nil)
			continue
		}
		callCtx, cancel := admin.AuthCallContext(ctx)
		err := step.call(callCtx)
		cancel()
		code := status.Code(err)
		if err != nil && code != codes.NotFound {
			details, retained := procedureInferenceCleanupRetainedByAudit(resources, step.resource, proof, err)
			if retained {
				setProcedureInferenceCleanupStatus(resources, step.resource, "retained-by-audit", "immutable audit closure", details)
				recordProcedureInferenceCleanupAttempt(resources, step.resource, pass, "failed-precondition", "retained-by-audit", "immutable audit closure", details)
				continue
			}
			if shouldInvalidateProcedureInferenceCleanupProof(code) {
				proof.clear()
			}
			message := fmt.Sprintf("delete %s: %s", step.resource, sanitizedProcedureInferenceProvisionError(err))
			failures = append(failures, message)
			setProcedureInferenceCleanupStatus(resources, step.resource, "failed", message, details)
			rpcOutcome := "rpc-error"
			if code == codes.FailedPrecondition {
				rpcOutcome = "failed-precondition"
			}
			recordProcedureInferenceCleanupAttempt(resources, step.resource, pass, rpcOutcome, "failed", message, details)
			continue
		}
		rpcOutcome := "success"
		if code == codes.NotFound {
			rpcOutcome = "not-found"
		}
		if node, retained := proof.node(step.resource, step.id); retained {
			setProcedureInferenceCleanupStatus(resources, step.resource, "retained-by-audit", "immutable audit closure", node.Details)
			recordProcedureInferenceCleanupAttempt(resources, step.resource, pass, rpcOutcome, "retained-by-audit", "", nil)
			continue
		}
		setProcedureInferenceCleanupStatus(resources, step.resource, "completed", "", nil)
		recordProcedureInferenceCleanupAttempt(resources, step.resource, pass, rpcOutcome, "completed", "", nil)
	}
	return failures
}

func shouldInvalidateProcedureInferenceCleanupProof(code codes.Code) bool {
	return code == codes.FailedPrecondition
}

func procedureInferenceCleanupRetainedByAudit(resources *ProcedureInferenceResources, targetResource string, proof *procedureInferenceCleanupProof, err error) (map[string]any, bool) {
	references, rejectionCategory, observedReferenceCount := semanticCleanupReferences(err)
	if rejectionCategory != "" {
		return procedureInferenceCleanupRejectionEvidence(rejectionCategory, observedReferenceCount, procedureInferenceSafeEvidenceReferences(references)), false
	}
	for _, reference := range references {
		if !procedureInferenceKnownCleanupReferenceType(reference.Type) {
			return procedureInferenceCleanupRejectionEvidence("unsupported-reference-type", len(references), procedureInferenceSafeEvidenceReferences(references)), false
		}
		if !procedureInferenceAllowedCleanupReference(targetResource, reference.Type) {
			return procedureInferenceCleanupRejectionEvidence("invalid-reference-edge", len(references), references), false
		}
	}
	for _, reference := range references {
		if reference.Type == "usage_event" || reference.Type == "policy_decision" {
			continue
		}
		owned, retained := procedureInferenceRunOwnedRetainedReference(resources, proof, reference)
		if !owned {
			return procedureInferenceCleanupRejectionEvidence("foreign-run-owned-reference", len(references), references), false
		}
		if !retained {
			return procedureInferenceCleanupRejectionEvidence("unrooted-run-owned-reference", len(references), references), false
		}
	}
	details := procedureInferenceCleanupReferenceEvidence("retentionCategory", "immutable-audit-closure", references)
	proof.recordValidated(resources, targetResource, references, details)
	if !proof.contains(targetResource, procedureInferenceCleanupResourceID(resources, targetResource)) {
		if proof != nil {
			delete(proof.nodes, targetResource)
		}
		return procedureInferenceCleanupRejectionEvidence("unrooted-run-owned-reference", len(references), references), false
	}
	return details, true
}

func procedureInferenceKnownCleanupReferenceType(referenceType string) bool {
	switch referenceType {
	case "usage_event", "policy_decision", "credential_grant", "credential", "intelligence_profile", "inference_policy", "capability":
		return true
	default:
		return false
	}
}

func procedureInferenceAllowedCleanupReference(targetResource, referenceType string) bool {
	if referenceType == "usage_event" || referenceType == "policy_decision" {
		return true
	}
	allowed := map[string]map[string]struct{}{
		"credential":           {"credential_grant": {}},
		"intelligence-profile": {"credential_grant": {}, "inference_policy": {}},
		"capability":           {"credential_grant": {}, "intelligence_profile": {}},
		"model":                {"credential_grant": {}, "intelligence_profile": {}, "capability": {}},
		"endpoint":             {"credential_grant": {}, "credential": {}, "intelligence_profile": {}, "capability": {}},
	}
	_, ok := allowed[targetResource][referenceType]
	return ok
}

func procedureInferenceSafeEvidenceReferences(references []semanticCleanupReference) []semanticCleanupReference {
	safe := make([]semanticCleanupReference, 0, len(references))
	for _, reference := range references {
		switch reference.Type {
		case "usage_event", "policy_decision", "credential_grant", "credential", "intelligence_profile", "inference_policy", "capability", "semantic_index":
			safe = append(safe, reference)
		}
	}
	return safe
}

func procedureInferenceCleanupRejectionEvidence(category string, observedReferenceCount int, references []semanticCleanupReference) map[string]any {
	details := map[string]any{"rejectionCategory": category, "observedReferenceCount": observedReferenceCount}
	if len(references) == 0 {
		return details
	}
	evidence := procedureInferenceCleanupReferenceEvidence("rejectionCategory", category, references)
	evidence["observedReferenceCount"] = observedReferenceCount
	return evidence
}

func procedureInferenceCleanupReferenceEvidence(categoryKey, category string, references []semanticCleanupReference) map[string]any {
	sorted := append([]semanticCleanupReference(nil), references...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Type == sorted[j].Type {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].Type < sorted[j].Type
	})
	evidence := make([]map[string]string, 0, len(sorted))
	for _, reference := range sorted {
		digest := sha256.Sum256([]byte(reference.Type + ":" + reference.ID))
		evidence = append(evidence, map[string]string{"type": reference.Type, "idHash": hex.EncodeToString(digest[:])})
	}
	return map[string]any{
		categoryKey:      category,
		"referenceCount": len(sorted),
		"references":     evidence,
	}
}

func procedureInferenceRunOwnedRetainedReference(resources *ProcedureInferenceResources, proof *procedureInferenceCleanupProof, reference semanticCleanupReference) (bool, bool) {
	resource, id := procedureInferenceRunOwnedReference(resources, reference)
	if resource == "" || !strings.EqualFold(strings.TrimSpace(id), reference.ID) {
		return false, false
	}
	return true, proof.contains(resource, id)
}

func procedureInferenceRunOwnedReference(resources *ProcedureInferenceResources, reference semanticCleanupReference) (string, string) {
	if resources == nil {
		return "", ""
	}
	switch reference.Type {
	case "credential_grant":
		return "credential-grant", resources.CredentialGrantID
	case "credential":
		return "credential", resources.CredentialID
	case "intelligence_profile":
		return "intelligence-profile", resources.IntelligenceProfileID
	case "inference_policy":
		return "access-policy", resources.AccessPolicyID
	case "capability":
		return "capability", resources.CapabilityID
	default:
		return "", ""
	}
}

func procedureInferenceCleanupResourceID(resources *ProcedureInferenceResources, resource string) string {
	if resources == nil {
		return ""
	}
	switch resource {
	case "access-policy":
		return strings.ToLower(strings.TrimSpace(resources.AccessPolicyID))
	case "credential-grant":
		return strings.ToLower(strings.TrimSpace(resources.CredentialGrantID))
	case "credential":
		return strings.ToLower(strings.TrimSpace(resources.CredentialID))
	case "intelligence-profile":
		return strings.ToLower(strings.TrimSpace(resources.IntelligenceProfileID))
	case "capability":
		return strings.ToLower(strings.TrimSpace(resources.CapabilityID))
	case "model":
		return strings.ToLower(strings.TrimSpace(resources.ModelID))
	case "endpoint":
		return strings.ToLower(strings.TrimSpace(resources.EndpointID))
	default:
		return ""
	}
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

func recordProcedureInferenceCleanupAttempt(resources *ProcedureInferenceResources, resource string, pass procedureInferenceCleanupPass, rpcOutcome, state, message string, details map[string]any) {
	if resources == nil {
		return
	}
	for index := range resources.Cleanup {
		if resources.Cleanup[index].Resource == resource {
			resources.Cleanup[index].Attempts = append(resources.Cleanup[index].Attempts, CleanupOperationAttempt{
				Round: pass.Round, Pass: pass.Name, RPCOutcome: rpcOutcome, Status: state, Error: message, Details: details,
			})
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
