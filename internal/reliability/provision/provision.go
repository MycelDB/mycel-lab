package provision

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

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
	ScenarioName string            `json:"scenarioName"`
	RunID        string            `json:"runId"`
	DryRun       bool              `json:"dryRun"`
	AdminAddr    string            `json:"adminAddr,omitempty"`
	Principals   []Principal       `json:"principals"`
	Spaces       []Space           `json:"spaces"`
	Domains      []Domain          `json:"domains"`
	Assignments  []ActorAssignment `json:"assignments"`
	Cleanup      CleanupStatus     `json:"cleanup,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
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
	Attempted bool     `json:"attempted"`
	Completed bool     `json:"completed"`
	Errors    []string `json:"errors,omitempty"`
}

type Options struct {
	RunID         string
	DaemonAddrs   []string
	AdminUsername string
	AdminPassword string
	DryRun        bool
}

func PlanScenario(scenario spec.ResolvedScenario, opts Options) (ScenarioResources, error) {
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID = sanitizeName(scenario.Metadata.Name)
	}
	resources := ScenarioResources{ScenarioName: scenario.Metadata.Name, RunID: runID, DryRun: opts.DryRun, CreatedAt: time.Now().UTC()}
	if len(opts.DaemonAddrs) > 0 {
		resources.AdminAddr = opts.DaemonAddrs[0]
	}
	shortRun := shortRunID(runID)
	writerAssignments := map[string][]ActorAssignment{}
	for _, group := range scenario.ActorGroups {
		behaviorType := behaviorType(group)
		switch behaviorType {
		case "graph-transaction":
			for i := 0; i < group.Count; i++ {
				actorID := fmt.Sprintf("%s-%d", group.Name, i)
				username := fmt.Sprintf("mlab-%s-%s-%d", shortRun, sanitizeName(group.Name), i)
				password := fmt.Sprintf("mlab-%s-%s-%d-password", shortRun, sanitizeName(group.Name), i)
				spaceName := scopedName(shortRun, stringFromNested(group.Profile.Behavior, "dataScope", "space", "prefix", "space"), i)
				domainKey := scopedName(shortRun, stringFromNested(group.Profile.Behavior, "dataScope", "domain", "prefix", "domain"), i)
				domainName := domainKey
				addr := daemonAddr(opts.DaemonAddrs, len(resources.Assignments))
				resources.Principals = append(resources.Principals, Principal{ActorID: actorID, GroupName: group.Name, Username: username, Password: password, Role: "writer"})
				resources.Spaces = append(resources.Spaces, Space{ActorID: actorID, GroupName: group.Name, Name: spaceName, OwnerUsername: username})
				resources.Domains = append(resources.Domains, Domain{ActorID: actorID, GroupName: group.Name, SpaceName: spaceName, Key: domainKey, Name: domainName})
				assignment := ActorAssignment{ActorID: actorID, GroupName: group.Name, Index: i, BehaviorType: behaviorType, Username: username, Password: password, DaemonAddr: addr}
				resources.Assignments = append(resources.Assignments, assignment)
				writerAssignments[group.Name] = append(writerAssignments[group.Name], assignment)
			}
		case "gql-read":
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
				username := fmt.Sprintf("mlab-%s-%s-%d", shortRun, sanitizeName(group.Name), i)
				password := fmt.Sprintf("mlab-%s-%s-%d-password", shortRun, sanitizeName(group.Name), i)
				target := targets[i%len(targets)]
				addr := daemonAddr(opts.DaemonAddrs, len(resources.Assignments))
				resources.Principals = append(resources.Principals, Principal{ActorID: actorID, GroupName: group.Name, Username: username, Password: password, Role: "reader"})
				resources.Assignments = append(resources.Assignments, ActorAssignment{ActorID: actorID, GroupName: group.Name, Index: i, BehaviorType: behaviorType, Username: username, Password: password, SpaceID: target.SpaceID, DomainID: target.DomainID, DaemonAddr: addr, TargetGroup: target.GroupName, TargetActorID: target.ActorID})
			}
		}
	}
	return resources, nil
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
		if a.BehaviorType == "gql-read" && a.TargetActorID != "" {
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

func CleanupScenario(ctx context.Context, resources *ScenarioResources, opts Options) error {
	if resources == nil || resources.DryRun || opts.DryRun {
		return nil
	}
	resources.Cleanup.Attempted = true
	addr := firstNonEmpty(resources.AdminAddr, firstAddr(opts.DaemonAddrs))
	if addr == "" {
		resources.Cleanup.Errors = append(resources.Cleanup.Errors, "daemon address is required for cleanup")
		return fmt.Errorf("daemon address is required for cleanup")
	}
	admin, err := dialAdminWithRetry(ctx, addr, opts)
	if err != nil {
		resources.Cleanup.Errors = append(resources.Cleanup.Errors, err.Error())
		return err
	}
	defer admin.Close()
	var errs []string
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
			errs = append(errs, fmt.Sprintf("delete space %s: %v", sp.SpaceID, err))
		}
	}
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
		return fmt.Errorf("cleanup scenario resources: %s", strings.Join(errs, "; "))
	}
	return nil
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

func scopedName(run, prefix string, index int) string {
	return fmt.Sprintf("mlab-%s-%s-%d", sanitizeName(run), sanitizeName(prefix), index)
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
