package provision

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultAdminUsername = "admin"
	DefaultAdminPassword = "admin-password"
)

type ScenarioResources struct {
	ScenarioName           string                       `json:"scenarioName"`
	RunID                  string                       `json:"runId"`
	DryRun                 bool                         `json:"dryRun"`
	AdminAddr              string                       `json:"adminAddr,omitempty"`
	DaemonAddrs            []string                     `json:"daemonAddrs,omitempty"`
	Principals             []Principal                  `json:"principals"`
	Spaces                 []Space                      `json:"spaces"`
	Domains                []Domain                     `json:"domains"`
	Assignments            []ActorAssignment            `json:"assignments"`
	Semantic               *SemanticResources           `json:"semantic,omitempty"`
	ProcedureInference     *ProcedureInferenceResources `json:"procedureInference,omitempty"`
	Cleanup                CleanupStatus                `json:"cleanup,omitempty"`
	semanticPlan           *semanticProvisionPlan
	procedureInferencePlan *procedureInferenceProvisionPlan
	CreatedAt              time.Time `json:"createdAt"`
}

type Principal struct {
	ActorID     string `json:"actorId"`
	GroupName   string `json:"groupName"`
	Username    string `json:"username"`
	PrincipalID string `json:"principalId,omitempty"`
	Role        string `json:"role"`
	Password    string `json:"-"`
}

type Space struct {
	ActorID       string `json:"actorId"`
	GroupName     string `json:"groupName"`
	Name          string `json:"name"`
	SpaceID       string `json:"spaceId,omitempty"`
	OwnerUsername string `json:"ownerUsername"`
}

type Domain struct {
	ActorID   string `json:"actorId"`
	GroupName string `json:"groupName"`
	SpaceName string `json:"spaceName"`
	SpaceID   string `json:"spaceId,omitempty"`
	Key       string `json:"key"`
	Name      string `json:"name"`
	DomainID  string `json:"domainId,omitempty"`
}

type ActorAssignment struct {
	ActorID       string    `json:"actorId"`
	GroupName     string    `json:"groupName"`
	Index         int       `json:"index"`
	BehaviorType  string    `json:"behaviorType"`
	PrincipalID   string    `json:"principalId,omitempty"`
	Username      string    `json:"username,omitempty"`
	Password      string    `json:"-"`
	AccessToken   string    `json:"-"`
	RefreshToken  string    `json:"-"`
	TokenExpires  time.Time `json:"-"`
	SpaceID       string    `json:"spaceId,omitempty"`
	DomainID      string    `json:"domainId,omitempty"`
	DaemonAddr    string    `json:"daemonAddr,omitempty"`
	TargetGroup   string    `json:"targetGroup,omitempty"`
	TargetActorID string    `json:"targetActorId,omitempty"`
}

type CleanupStatus struct {
	Attempted       bool     `json:"attempted"`
	Completed       bool     `json:"completed"`
	Errors          []string `json:"-"`
	ErrorCategories []string `json:"errorCategories,omitempty"`
}

type ProcedureInferenceResources struct {
	TargetActorID          string              `json:"targetActorId"`
	AuditActorID           string              `json:"auditActorId"`
	SpaceID                string              `json:"spaceId,omitempty"`
	DomainID               string              `json:"domainId,omitempty"`
	PackageName            string              `json:"packageName"`
	PackageVersion         string              `json:"packageVersion"`
	PackageID              string              `json:"packageId,omitempty"`
	EndpointKey            string              `json:"endpointKey"`
	EndpointID             string              `json:"endpointId,omitempty"`
	ModelKey               string              `json:"modelKey"`
	ModelID                string              `json:"modelId,omitempty"`
	CapabilityID           string              `json:"capabilityId,omitempty"`
	IntelligenceProfileKey string              `json:"intelligenceProfileKey"`
	IntelligenceProfileID  string              `json:"intelligenceProfileId,omitempty"`
	CredentialKey          string              `json:"credentialKey"`
	CredentialID           string              `json:"credentialId,omitempty"`
	CredentialGrantID      string              `json:"credentialGrantId,omitempty"`
	AccessPolicyID         string              `json:"accessPolicyId,omitempty"`
	ProcedureID            string              `json:"procedureId"`
	BindingID              string              `json:"bindingId"`
	TargetLabels           []string            `json:"targetLabels"`
	Operations             []SemanticOperation `json:"operations"`
	Cleanup                []SemanticOperation `json:"cleanup"`
	cleanupProof           *procedureInferenceCleanupProof
	cleanupRound           int
}

type SemanticResources struct {
	TargetActorID                     string                                `json:"targetActorId"`
	TargetPrincipalID                 string                                `json:"-"`
	LiveProvider                      bool                                  `json:"-"`
	ExpectedProviderBudget            *adminv1.SemanticProviderBudgetPolicy `json:"-"`
	SpaceID                           string                                `json:"spaceId,omitempty"`
	DomainID                          string                                `json:"domainId,omitempty"`
	PackageName                       string                                `json:"packageName"`
	PackageVersion                    string                                `json:"packageVersion"`
	PackageID                         string                                `json:"packageId,omitempty"`
	EndpointKey                       string                                `json:"endpointKey"`
	EndpointID                        string                                `json:"endpointId,omitempty"`
	ModelKey                          string                                `json:"modelKey"`
	ModelID                           string                                `json:"modelId,omitempty"`
	CapabilityID                      string                                `json:"capabilityId,omitempty"`
	VectorStoreKey                    string                                `json:"vectorStoreKey"`
	VectorStoreID                     string                                `json:"vectorStoreId,omitempty"`
	IntelligenceProfileKey            string                                `json:"intelligenceProfileKey"`
	IntelligenceProfileID             string                                `json:"intelligenceProfileId,omitempty"`
	CredentialKey                     string                                `json:"credentialKey"`
	CredentialID                      string                                `json:"credentialId,omitempty"`
	CredentialGrantID                 string                                `json:"credentialGrantId,omitempty"`
	AccessPolicyID                    string                                `json:"accessPolicyId,omitempty"`
	SemanticRuleKey                   string                                `json:"semanticRuleKey"`
	SemanticRuleID                    string                                `json:"semanticRuleId,omitempty"`
	EmbeddingBindingKey               string                                `json:"embeddingBindingKey"`
	Operations                        []SemanticOperation                   `json:"operations"`
	Cleanup                           []SemanticOperation                   `json:"cleanup"`
	MaintenanceTimeout                time.Duration                         `json:"maintenanceTimeout"`
	MaintenancePollInterval           time.Duration                         `json:"maintenancePollInterval"`
	MaintenanceAnalyzeLimit           int                                   `json:"maintenanceAnalyzeLimit"`
	MaintenanceProcessLimit           int                                   `json:"maintenanceProcessLimit"`
	EvidenceValues                    map[string]string                     `json:"evidenceValues,omitempty"`
	ProviderBudgetCapabilities        []string                              `json:"providerBudgetCapabilities,omitempty"`
	ProviderBudgetContractVersion     string                                `json:"providerBudgetContractVersion,omitempty"`
	ProviderBudgetMaxAttemptEventPage int32                                 `json:"providerBudgetMaxAttemptEventPage,omitempty"`
	ProviderBudgetMaxScopePage        int32                                 `json:"providerBudgetMaxScopePage,omitempty"`
	ProviderBudgetScopeID             string                                `json:"providerBudgetScopeId,omitempty"`
	cleanupProof                      *procedureInferenceCleanupProof
}

type SemanticOperation struct {
	Sequence int                       `json:"sequence"`
	Action   string                    `json:"action"`
	Resource string                    `json:"resource"`
	Status   string                    `json:"status"`
	Details  map[string]any            `json:"details,omitempty"`
	Error    string                    `json:"error,omitempty"`
	Attempts []CleanupOperationAttempt `json:"attempts,omitempty"`
}

type CleanupOperationAttempt struct {
	Round      int            `json:"round"`
	Pass       string         `json:"pass"`
	RPCOutcome string         `json:"rpcOutcome"`
	Status     string         `json:"status"`
	Details    map[string]any `json:"details,omitempty"`
	Error      string         `json:"error,omitempty"`
}

type procedureInferenceProvisionPlan struct {
	Spec       spec.ProcedureInferenceScenarioSpec
	Capability providerCapability
	Secret     string
}

type semanticProvisionPlan struct {
	Spec            spec.SemanticScenarioSpec
	Capability      providerCapability
	ProviderProfile *provider.ResolvedProfile
	Secret          string
}

type providerCapability struct {
	ConnectorType   string
	BaseURL         string
	NetworkClass    string
	PrivacyClass    string
	Dimensions      int
	VectorSpaceKey  string
	FixtureStrategy string
	ProviderModel   string
	Mode            string
}

type Options struct {
	RunID                  string
	DaemonAddrs            []string
	AdminUsername          string
	AdminPassword          string
	DryRun                 bool
	ProviderLookupEnv      func(string) (string, bool)
	ProviderSecretObserver func(string)
	PersistResources       func(ScenarioResources) error
}

func PlanScenario(scenario spec.ResolvedScenario, opts Options) (ScenarioResources, error) {
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = sanitizeName(scenario.Metadata.Name)
	}
	resources := ScenarioResources{ScenarioName: scenario.Metadata.Name, RunID: runID, DryRun: opts.DryRun, CreatedAt: time.Now().UTC(), DaemonAddrs: append([]string(nil), opts.DaemonAddrs...)}
	if len(resources.DaemonAddrs) > 0 {
		resources.AdminAddr = resources.DaemonAddrs[0]
	}
	shortRun := shortRunID(runID)
	writerAssignments := map[string][]ActorAssignment{}
	for _, group := range scenario.ActorGroups {
		behaviorType := behaviorType(group)
		switch behaviorType {
		case "graph-transaction", "semantic-control":
			for i := 0; i < group.Count; i++ {
				actorID := fmt.Sprintf("%s-%d", group.Name, i)
				groupResourceName := spec.NormalizeActorGroupResourceName(group.Name)
				username := fmt.Sprintf("mlab-%s-%s-%d", shortRun, groupResourceName, i)
				password := fmt.Sprintf("mlab-%s-%s-%d-password", shortRun, groupResourceName, i)
				spaceName := scopedActorResourceName(shortRun, group.Name, stringFromNested(group.Profile.Behavior, "dataScope", "space", "prefix", "space"), i)
				domainKey := scopedActorResourceName(shortRun, group.Name, stringFromNested(group.Profile.Behavior, "dataScope", "domain", "prefix", "domain"), i)
				domainName := domainKey
				addr := daemonAddr(opts.DaemonAddrs, len(resources.Assignments))
				resources.Principals = append(resources.Principals, Principal{ActorID: actorID, GroupName: group.Name, Username: username, Password: password, Role: "writer"})
				resources.Spaces = append(resources.Spaces, Space{ActorID: actorID, GroupName: group.Name, Name: spaceName, OwnerUsername: username})
				resources.Domains = append(resources.Domains, Domain{ActorID: actorID, GroupName: group.Name, SpaceName: spaceName, Key: domainKey, Name: domainName})
				assignment := ActorAssignment{ActorID: actorID, GroupName: group.Name, Index: i, BehaviorType: behaviorType, Username: username, Password: password, DaemonAddr: addr}
				resources.Assignments = append(resources.Assignments, assignment)
				writerAssignments[group.Name] = append(writerAssignments[group.Name], assignment)
			}
		case "gql-read", "procedure-inference-audit":
			targetGroups := targetGroups(group)
			var targets []ActorAssignment
			for _, name := range targetGroups {
				targets = append(targets, writerAssignments[name]...)
			}
			if len(targets) == 0 {
				continue
			}
			for i := 0; i < group.Count; i++ {
				actorID := fmt.Sprintf("%s-%d", group.Name, i)
				groupResourceName := spec.NormalizeActorGroupResourceName(group.Name)
				username := fmt.Sprintf("mlab-%s-%s-%d", shortRun, groupResourceName, i)
				password := fmt.Sprintf("mlab-%s-%s-%d-password", shortRun, groupResourceName, i)
				target := targets[i%len(targets)]
				addr := daemonAddr(opts.DaemonAddrs, len(resources.Assignments))
				resources.Principals = append(resources.Principals, Principal{ActorID: actorID, GroupName: group.Name, Username: username, Password: password, Role: "reader"})
				resources.Assignments = append(resources.Assignments, ActorAssignment{ActorID: actorID, GroupName: group.Name, Index: i, BehaviorType: behaviorType, Username: username, Password: password, SpaceID: target.SpaceID, DomainID: target.DomainID, DaemonAddr: addr, TargetGroup: target.GroupName, TargetActorID: target.ActorID})
			}
		}
	}
	if scenario.Semantic != nil {
		semanticResources, plan, err := planSemanticResources(scenario, resources, shortRun)
		if err != nil {
			return ScenarioResources{}, err
		}
		resources.Semantic = semanticResources
		resources.semanticPlan = plan
	}
	if scenario.ProcedureInference != nil {
		procedureResources, plan, err := planProcedureInferenceResources(scenario, resources, shortRun)
		if err != nil {
			return ScenarioResources{}, err
		}
		resources.ProcedureInference = procedureResources
		resources.procedureInferencePlan = plan
	}
	return resources, nil
}

func planProcedureInferenceResources(scenario spec.ResolvedScenario, resources ScenarioResources, shortRun string) (*ProcedureInferenceResources, *procedureInferenceProvisionPlan, error) {
	if scenario.ProcedureInference == nil || scenario.ProviderProfile == nil || scenario.ProviderProfile.Profile.Capabilities.Generation == nil {
		return nil, nil, errors.New("procedure inference provisioning requires a resolved generation provider profile")
	}
	var target, audit ActorAssignment
	for _, assignment := range resources.Assignments {
		switch assignment.GroupName {
		case scenario.ProcedureInference.TargetActorGroup:
			if target.ActorID == "" {
				target = assignment
			}
		case scenario.ProcedureInference.AuditActorGroup:
			if audit.ActorID == "" {
				audit = assignment
			}
		}
	}
	if target.ActorID == "" || audit.ActorID == "" {
		return nil, nil, errors.New("procedure inference target and audit actor assignments are required")
	}
	capability := scenario.ProviderProfile.Profile.Capabilities.Generation
	prefix := "mlab-" + shortRun
	out := &ProcedureInferenceResources{
		TargetActorID: target.ActorID, AuditActorID: audit.ActorID,
		PackageName: prefix + "-procedure-inference", PackageVersion: "1",
		EndpointKey: prefix + "-procedure-endpoint", ModelKey: prefix + "-procedure-model",
		IntelligenceProfileKey: prefix + "-procedure-profile", CredentialKey: prefix + "-procedure-credential",
		ProcedureID: scenario.ProcedureInference.ProcedureID, BindingID: scenario.ProcedureInference.BindingID,
		TargetLabels: append([]string(nil), scenario.ProcedureInference.TargetLabels...),
	}
	for index, item := range []struct{ action, resource string }{
		{"apply", "inference-package"}, {"create", "intelligence-profile"}, {"create", "credential"},
		{"create", "credential-grant"}, {"create", "access-policy"},
	} {
		out.Operations = append(out.Operations, SemanticOperation{Sequence: index + 1, Action: item.action, Resource: item.resource, Status: "planned"})
	}
	for index, item := range []struct{ action, resource string }{
		{"delete", "access-policy"}, {"delete", "credential-grant"}, {"delete", "credential"},
		{"delete", "intelligence-profile"}, {"delete", "capability"}, {"delete", "model"}, {"delete", "endpoint"},
		{"retain", "inference-package"},
	} {
		out.Cleanup = append(out.Cleanup, SemanticOperation{Sequence: index + 1, Action: item.action, Resource: item.resource, Status: "planned"})
	}
	secret := ""
	if values := scenario.ProviderProfile.RuntimeSecretValues(); len(values) > 0 {
		secret = values[0]
	}
	return out, &procedureInferenceProvisionPlan{Spec: *scenario.ProcedureInference, Secret: secret, Capability: providerCapability{
		ConnectorType: capability.ConnectorType, BaseURL: capability.BaseURL, ProviderModel: capability.ProviderModelName, Mode: string(scenario.ProviderProfile.Profile.Mode),
	}}, nil
}

func semanticOperationPlan() []struct{ action, resource string } {
	return []struct{ action, resource string }{
		{"discover", "provider-budget-capability"},
		{"apply", "inference-package"},
		{"create", "intelligence-profile"},
		{"create", "credential"},
		{"create", "credential-grant"},
		{"create", "access-policy"},
		{"create", "semantic-rule"},
	}
}

func planSemanticResources(scenario spec.ResolvedScenario, resources ScenarioResources, shortRun string) (*SemanticResources, *semanticProvisionPlan, error) {
	if scenario.Semantic == nil || scenario.ProviderProfile == nil || scenario.ProviderProfile.Profile.Capabilities.Embedding == nil {
		return nil, nil, errors.New("semantic provisioning requires a resolved embedding provider profile")
	}
	var target ActorAssignment
	for _, assignment := range resources.Assignments {
		if assignment.GroupName == scenario.Semantic.TargetActorGroup {
			target = assignment
			break
		}
	}
	if target.ActorID == "" {
		return nil, nil, fmt.Errorf("semantic target actor group %q has no assignment", scenario.Semantic.TargetActorGroup)
	}
	capability := scenario.ProviderProfile.Profile.Capabilities.Embedding
	prefix := "mlab-" + shortRun
	semanticResources := &SemanticResources{
		TargetActorID:           target.ActorID,
		TargetPrincipalID:       target.PrincipalID,
		LiveProvider:            scenario.ProviderProfile.Profile.Mode == provider.ModeLive,
		PackageName:             prefix + "-semantic",
		PackageVersion:          "1",
		EndpointKey:             prefix + "-endpoint",
		ModelKey:                prefix + "-model",
		VectorStoreKey:          prefix + "-vectors",
		IntelligenceProfileKey:  prefix + "-profile",
		CredentialKey:           prefix + "-credential",
		SemanticRuleKey:         scenario.Semantic.Rule.Key,
		EmbeddingBindingKey:     scenario.Semantic.Rule.BindingKey,
		MaintenanceTimeout:      scenario.Semantic.Maintenance.Timeout.Duration,
		MaintenancePollInterval: scenario.Semantic.Maintenance.PollInterval.Duration,
		MaintenanceAnalyzeLimit: scenario.Semantic.Maintenance.AnalyzeLimit,
		MaintenanceProcessLimit: scenario.Semantic.Maintenance.ProcessLimit,
		EvidenceValues:          map[string]string{},
	}
	operationNames := semanticOperationPlan()
	for index, operation := range operationNames {
		semanticResources.Operations = append(semanticResources.Operations, SemanticOperation{Sequence: index + 1, Action: operation.action, Resource: operation.resource, Status: "planned"})
	}
	cleanupResources := []string{"semantic-rule", "access-policy", "credential-grant", "credential", "intelligence-profile", "capability", "vector-store", "model", "endpoint"}
	for index, resource := range cleanupResources {
		semanticResources.Cleanup = append(semanticResources.Cleanup, SemanticOperation{Sequence: index + 1, Action: "delete", Resource: resource, Status: "planned"})
	}
	secret := ""
	if values := scenario.ProviderProfile.RuntimeSecretValues(); len(values) > 0 {
		secret = values[0]
	}
	return semanticResources, &semanticProvisionPlan{Spec: *scenario.Semantic, ProviderProfile: scenario.ProviderProfile, Secret: secret, Capability: providerCapability{
		ConnectorType: capability.ConnectorType, BaseURL: capability.BaseURL, NetworkClass: capability.NetworkClass, PrivacyClass: capability.PrivacyClass, Dimensions: capability.Dimensions,
		VectorSpaceKey: capability.VectorSpaceKey, FixtureStrategy: capability.FixtureStrategy,
		ProviderModel: capability.ProviderModelName, Mode: string(scenario.ProviderProfile.Profile.Mode),
	}}, nil
}

func ProvisionScenario(ctx context.Context, resources *ScenarioResources, opts Options) error {
	if resources == nil || opts.DryRun || resources.DryRun {
		return nil
	}
	addr := resources.AdminAddr
	if addr == "" && len(opts.DaemonAddrs) > 0 {
		addr = opts.DaemonAddrs[0]
	}
	if addr == "" {
		return fmt.Errorf("daemon address is required for provisioning")
	}
	admin, err := dialAdminWithRetry(ctx, addr, opts)
	if err != nil {
		return fmt.Errorf("dial admin %s: %w", addr, err)
	}
	defer admin.Close()
	principalByActor := map[string]Principal{}
	for i := range resources.Principals {
		p := &resources.Principals[i]
		created, err := admin.EnsureUser(ctx, p.Username, p.Password)
		if err != nil {
			return fmt.Errorf("ensure user %s: %w", p.Username, err)
		}
		p.PrincipalID = created.UserID
		principalByActor[p.ActorID] = *p
	}
	spaceByName := map[string]Space{}
	domainByActor := map[string]Domain{}
	for i := range resources.Spaces {
		sp := &resources.Spaces[i]
		domain := domainForSpace(resources.Domains, sp.Name)
		createdSpace, createdDomain, err := admin.EnsureSpace(ctx, sp.Name, sp.OwnerUsername, domain.Key, domain.Name)
		if err != nil {
			return fmt.Errorf("ensure space %s: %w", sp.Name, err)
		}
		sp.SpaceID = createdSpace.SpaceID
		spaceByName[sp.Name] = *sp
		for j := range resources.Domains {
			if resources.Domains[j].SpaceName == sp.Name {
				resources.Domains[j].SpaceID = createdSpace.SpaceID
				resources.Domains[j].DomainID = createdDomain.DomainID
				domainByActor[resources.Domains[j].ActorID] = resources.Domains[j]
			}
		}
		if p, ok := principalByActor[sp.ActorID]; ok {
			if _, err := admin.SetPrincipalRolesForScope(ctx, p.PrincipalID, "space", createdSpace.SpaceID, "", []string{"space.editor"}, "mycel-lab writer provisioning"); err != nil {
				return fmt.Errorf("grant writer access for %s on space %s: %w", p.Username, createdSpace.SpaceID, err)
			}
		}
	}
	for i := range resources.Assignments {
		a := &resources.Assignments[i]
		if p, ok := principalByActor[a.ActorID]; ok {
			a.PrincipalID = p.PrincipalID
		}
		if a.SpaceID == "" {
			if sp, ok := spaceByName[spaceNameForActor(resources.Spaces, a.ActorID)]; ok {
				a.SpaceID = sp.SpaceID
			}
		}
		if a.DomainID == "" {
			if d, ok := domainByActor[a.ActorID]; ok {
				a.DomainID = d.DomainID
			}
		}
	}
	targets := map[string]ActorAssignment{}
	for _, assignment := range resources.Assignments {
		targets[assignment.ActorID] = assignment
	}
	for i := range resources.Assignments {
		a := &resources.Assignments[i]
		if (a.BehaviorType == "gql-read" || a.BehaviorType == "procedure-inference-audit") && a.TargetActorID != "" {
			if target, ok := targets[a.TargetActorID]; ok {
				a.SpaceID = target.SpaceID
				a.DomainID = target.DomainID
			}
		}
		if a.BehaviorType == "gql-read" && a.SpaceID != "" && a.PrincipalID != "" {
			if _, err := admin.SetPrincipalRolesForScope(ctx, a.PrincipalID, "space", a.SpaceID, "", []string{"space.viewer"}, "mycel-lab reader provisioning"); err != nil {
				return fmt.Errorf("grant reader access for %s on space %s: %w", a.Username, a.SpaceID, err)
			}
		}
		if a.BehaviorType == "procedure-inference-audit" && a.SpaceID != "" && a.PrincipalID != "" {
			if _, err := admin.SetPrincipalRolesForScope(ctx, a.PrincipalID, "space", a.SpaceID, "", []string{"space.viewer", "automation.admin"}, "mycel-lab procedure inference audit provisioning"); err != nil {
				return fmt.Errorf("grant procedure inference audit access for %s on space %s: %w", a.Username, a.SpaceID, err)
			}
		}
	}
	if resources.ProcedureInference != nil {
		for _, assignment := range resources.Assignments {
			if assignment.ActorID == resources.ProcedureInference.TargetActorID {
				resources.ProcedureInference.SpaceID = assignment.SpaceID
				resources.ProcedureInference.DomainID = assignment.DomainID
				break
			}
		}
		if err := provisionProcedureInferenceResources(ctx, admin, resources); err != nil {
			rollbackCtx, rollbackCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			rollbackRound := nextProcedureInferenceCleanupRound(resources.ProcedureInference)
			rollbackFailures := cleanupProcedureInferenceResources(rollbackCtx, admin, resources.ProcedureInference, procedureInferenceCleanupPass{Round: rollbackRound, Name: "provision-rollback"})
			rollbackCancel()
			if len(rollbackFailures) > 0 {
				return errors.Join(fmt.Errorf("provision procedure inference resources: %w", err), fmt.Errorf("procedure inference rollback: %s", strings.Join(rollbackFailures, "; ")))
			}
			return fmt.Errorf("provision procedure inference resources: %w", err)
		}
	}
	if resources.Semantic != nil {
		refreshSemanticTargetAssignment(resources)
		if strings.TrimSpace(resources.Semantic.TargetPrincipalID) == "" {
			return fmt.Errorf("semantic target principal is unavailable after principal provisioning")
		}
		if err := provisionSemanticResources(ctx, admin, resources, opts); err != nil {
			rollbackCtx, rollbackCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			rollbackFailures := cleanupSemanticResources(rollbackCtx, admin, resources.Semantic)
			rollbackCancel()
			if len(rollbackFailures) > 0 {
				return errors.Join(fmt.Errorf("provision semantic resources: %w", err), fmt.Errorf("semantic rollback: %s", strings.Join(rollbackFailures, "; ")))
			}
			return fmt.Errorf("provision semantic resources: %w", err)
		}
	}
	for i := range resources.Assignments {
		a := &resources.Assignments[i]
		if a.PrincipalID == "" {
			continue
		}
		sessionAdmin := admin
		if a.DaemonAddr != "" && a.DaemonAddr != addr {
			var err error
			sessionAdmin, err = dialAdminWithRetry(ctx, a.DaemonAddr, opts)
			if err != nil {
				return fmt.Errorf("dial actor daemon %s for %s session: %w", a.DaemonAddr, a.ActorID, err)
			}
		}
		session, err := sessionAdmin.CreateUserSession(ctx, a.PrincipalID)
		if sessionAdmin != admin {
			_ = sessionAdmin.Close()
		}
		if err != nil {
			return fmt.Errorf("create user session for %s: %w", a.Username, err)
		}
		a.AccessToken = session.AccessToken
		a.RefreshToken = session.RefreshToken
		a.TokenExpires = session.AccessTokenExpireTime
	}
	return nil
}

func refreshSemanticTargetAssignment(resources *ScenarioResources) {
	if resources == nil || resources.Semantic == nil {
		return
	}
	for _, assignment := range resources.Assignments {
		if assignment.ActorID == resources.Semantic.TargetActorID {
			resources.Semantic.SpaceID = assignment.SpaceID
			resources.Semantic.DomainID = assignment.DomainID
			resources.Semantic.TargetPrincipalID = assignment.PrincipalID
			return
		}
	}
}

func CleanupScenario(ctx context.Context, resources *ScenarioResources, opts Options) error {
	if resources == nil || resources.DryRun || opts.DryRun {
		return nil
	}
	resources.Cleanup.Attempted = true
	resources.Cleanup.Completed = false
	resources.Cleanup.Errors = nil
	resources.Cleanup.ErrorCategories = nil
	addr := firstNonEmpty(resources.AdminAddr, firstAddr(opts.DaemonAddrs))
	if addr == "" {
		resources.Cleanup.Errors = append(resources.Cleanup.Errors, "daemon address is required for cleanup")
		resources.Cleanup.ErrorCategories = []string{"daemon_address_missing"}
		return fmt.Errorf("daemon address is required for cleanup")
	}
	admin, err := dialAdminWithRetry(ctx, addr, opts)
	if err != nil {
		resources.Cleanup.Errors = append(resources.Cleanup.Errors, err.Error())
		resources.Cleanup.ErrorCategories = []string{"daemon_connect_failed"}
		return err
	}
	defer admin.Close()
	procedureRound := nextProcedureInferenceCleanupRound(resources.ProcedureInference)
	procedurePass := 0
	errs := cleanupAcrossSpaceBarrier(
		func() []string {
			procedurePass++
			passName := "pre-space-barrier"
			if procedurePass > 1 {
				passName = "post-space-barrier"
			}
			failures := cleanupProcedureInferenceResources(ctx, admin, resources.ProcedureInference, procedureInferenceCleanupPass{Round: procedureRound, Name: passName})
			return append(failures, cleanupSemanticResources(ctx, admin, resources.Semantic)...)
		},
		func() []string {
			var failures []string
			seenSpaces := map[string]bool{}
			spaces := append([]Space(nil), resources.Spaces...)
			sort.Slice(spaces, func(i, j int) bool { return spaces[i].Name > spaces[j].Name })
			for _, sp := range spaces {
				if sp.SpaceID == "" || seenSpaces[sp.SpaceID] {
					continue
				}
				seenSpaces[sp.SpaceID] = true
				callCtx, cancel := admin.AuthCallContext(ctx)
				_, err := admin.Spaces.DeleteSpace(callCtx, &adminv1.DeleteSpaceRequest{SpaceId: sp.SpaceID})
				cancel()
				if err != nil && status.Code(err) != codes.NotFound {
					failures = append(failures, fmt.Sprintf("delete space %s: %v", sp.SpaceID, err))
				}
			}
			return failures
		},
	)
	for _, p := range resources.Principals {
		if p.PrincipalID == "" {
			continue
		}
		callCtx, cancel := admin.AuthCallContext(ctx)
		_, err := admin.Principals.DeletePrincipal(callCtx, &adminv1.DeletePrincipalRequest{PrincipalId: p.PrincipalID, RevokeSessions: true})
		cancel()
		if err != nil && status.Code(err) != codes.NotFound {
			errs = append(errs, fmt.Sprintf("delete principal %s: %v", p.PrincipalID, err))
		}
	}
	resources.Cleanup.Errors = errs
	resources.Cleanup.Completed = len(errs) == 0
	if len(errs) > 0 {
		resources.Cleanup.ErrorCategories = []string{"resource_cleanup_failed"}
		return fmt.Errorf("cleanup scenario resources: %s", strings.Join(errs, "; "))
	}
	return nil
}

func cleanupAcrossSpaceBarrier(cleanupSemantic, deleteSpaces func() []string) []string {
	// The first pass removes resources that are not retained by inference audit
	// history. Policy decisions and usage events are immutable while their space
	// exists, so only failures from the retry after space deletion are final.
	_ = cleanupSemantic()
	failures := deleteSpaces()
	return append(failures, cleanupSemantic()...)
}

func dialAdminWithRetry(ctx context.Context, addr string, opts Options) (*mycel.AdminClient, error) {
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for {
		admin, err := mycel.DialAdmin(ctx, mycel.Config{Addr: addr, Username: firstNonEmpty(opts.AdminUsername, DefaultAdminUsername), Password: firstNonEmpty(opts.AdminPassword, DefaultAdminPassword), ClientName: "mycel-lab"})
		if err == nil {
			return admin, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func AssignmentMap(resources ScenarioResources) map[string]ActorAssignment {
	out := map[string]ActorAssignment{}
	for _, assignment := range resources.Assignments {
		out[assignment.ActorID] = assignment
	}
	return out
}

func behaviorType(group spec.ResolvedActorGroup) string {
	value, _ := group.Profile.Behavior["type"].(string)
	return strings.TrimSpace(value)
}

func targetGroups(group spec.ResolvedActorGroup) []string {
	target, _ := group.Target["actorGroups"].([]any)
	var out []string
	for _, value := range target {
		if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func stringFromNested(root map[string]any, a, b, c, fallback string) string {
	am, _ := root[a].(map[string]any)
	bm, _ := am[b].(map[string]any)
	if s, _ := bm[c].(string); strings.TrimSpace(s) != "" {
		return strings.TrimSpace(s)
	}
	return fallback
}

func domainForSpace(domains []Domain, spaceName string) Domain {
	for _, domain := range domains {
		if domain.SpaceName == spaceName {
			return domain
		}
	}
	return Domain{Key: "default", Name: "default"}
}

func spaceNameForActor(spaces []Space, actorID string) string {
	for _, space := range spaces {
		if space.ActorID == actorID {
			return space.Name
		}
	}
	return ""
}

func daemonAddr(addrs []string, index int) string {
	if len(addrs) == 0 {
		return ""
	}
	return addrs[index%len(addrs)]
}

func scopedActorResourceName(run, group, prefix string, index int) string {
	return fmt.Sprintf("mlab-%s-%s-%s-%d", sanitizeName(run), spec.NormalizeActorGroupResourceName(group), sanitizeName(prefix), index)
}

func shortRunID(runID string) string {
	runID = sanitizeName(runID)
	if len(runID) > 18 {
		return runID[len(runID)-18:]
	}
	return runID
}

func sanitizeName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "run"
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstAddr(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
