package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

const defaultUserBackupRestorePassword = "mycel-lab-restore-password"

type UserBackupOperationResult struct {
	Operation      string                   `json:"operation"`
	NodeName       string                   `json:"nodeName,omitempty"`
	ActorID        string                   `json:"actorId,omitempty"`
	Username       string                   `json:"username,omitempty"`
	TargetUsername string                   `json:"targetUsername,omitempty"`
	File           string                   `json:"file,omitempty"`
	HostFile       string                   `json:"hostFile,omitempty"`
	Stdout         string                   `json:"stdout,omitempty"`
	Stderr         string                   `json:"stderr,omitempty"`
	Import         *userBackupImportSummary `json:"import,omitempty"`
	Fixture        *userBackupFixtureResult `json:"fixture,omitempty"`
	Verification   *userBackupVerifyResult  `json:"verification,omitempty"`
	Safety         *userBackupSafetyResult  `json:"safety,omitempty"`
}

type userBackupImportSummary struct {
	PrincipalID    string            `json:"principal_id"`
	UserID         string            `json:"user_id,omitempty"`
	Username       string            `json:"username"`
	SpacesCreated  int               `json:"spaces_created"`
	DomainsCreated int               `json:"domains_created"`
	NodesImported  int64             `json:"nodes_imported"`
	EdgesImported  int64             `json:"edges_imported"`
	BlobsImported  int64             `json:"blobs_imported"`
	SpaceIDMap     map[string]string `json:"space_id_map"`
	DomainIDMap    map[string]string `json:"domain_id_map"`
}

type userBackupFixtureResult struct {
	SessionID     string `json:"sessionId,omitempty"`
	TransactionID string `json:"transactionId,omitempty"`
	NodesCreated  int    `json:"nodesCreated"`
	EdgesCreated  int    `json:"edgesCreated"`
	BlobsCreated  int    `json:"blobsCreated"`
}

type userBackupVerifyResult struct {
	CheckedEndpoints int   `json:"checkedEndpoints"`
	NodesImported    int64 `json:"nodesImported"`
	EdgesImported    int64 `json:"edgesImported"`
	BlobsImported    int64 `json:"blobsImported"`
}

type userBackupSafetyResult struct {
	ArchiveChecked             bool `json:"archiveChecked"`
	SourcePasswordRejected     bool `json:"sourcePasswordRejected"`
	ForbiddenArchiveTextAbsent bool `json:"forbiddenArchiveTextAbsent"`
}

func executeUserBackupEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, eventType string, target map[string]any, state map[string]UserBackupOperationResult) (UserBackupOperationResult, error) {
	node := env.NodeRef{Name: stringTarget(target, "node"), Ordinal: intTarget(target, "ordinal", 0)}
	result := UserBackupOperationResult{Operation: eventType, NodeName: node.Name, File: expandTargetPath(stringTarget(target, "file"), environment, resources), HostFile: expandTargetPath(stringTarget(target, "hostFile"), environment, resources)}
	switch eventType {
	case "user-backup-fixture":
		fixture, actorID, username, err := executeUserBackupFixture(ctx, driver, environment, node, resources, target)
		result.ActorID, result.Username, result.Fixture = actorID, username, &fixture
		return result, err
	case "user-backup-export", "user-backup-validate", "user-backup-import":
		return executeUserBackupCommandEvent(ctx, driver, environment, node, resources, eventType, target, result)
	case "user-backup-verify-restored":
		verification, err := executeUserBackupVerify(ctx, driver, environment, resources, target, state)
		result.TargetUsername = stringTarget(target, "targetUsername")
		result.Verification = &verification
		return result, err
	case "user-backup-assert-safety":
		safety, err := executeUserBackupSafety(ctx, driver, environment, node, resources, target)
		result.Safety = &safety
		return result, err
	default:
		return result, fmt.Errorf("unsupported user-backup event type %q", eventType)
	}
}

func executeUserBackupCommandEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, node env.NodeRef, resources *provision.ScenarioResources, eventType string, target map[string]any, result UserBackupOperationResult) (UserBackupOperationResult, error) {
	file := result.File
	if file == "" {
		return result, fmt.Errorf("%s requires target.file", eventType)
	}
	if eventType == "user-backup-import" && result.HostFile != "" {
		stager, ok := driver.(env.NodeFileStager)
		if !ok {
			return result, fmt.Errorf("driver %q does not support backup archive staging", driver.Name())
		}
		if _, err := stager.CopyToNode(ctx, environment, node, result.HostFile, file); err != nil {
			return result, err
		}
	}
	cmd, actorID, username, err := userBackupCommand(eventType, target, resources, file)
	if err != nil {
		return result, err
	}
	result.ActorID = actorID
	result.Username = username
	if eventType == "user-backup-import" {
		result.TargetUsername = username
	}
	execResult, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: cmd})
	result.Stdout = bounded(execResult.Stdout, 8192)
	result.Stderr = bounded(execResult.Stderr, 8192)
	if err != nil {
		return result, err
	}
	if eventType == "user-backup-export" && result.HostFile != "" {
		stager, ok := driver.(env.NodeFileStager)
		if !ok {
			return result, fmt.Errorf("driver %q does not support backup archive staging", driver.Name())
		}
		if _, err := stager.CopyFromNode(ctx, environment, node, file, result.HostFile); err != nil {
			return result, err
		}
	}
	if eventType == "user-backup-import" {
		var summary userBackupImportSummary
		if err := json.Unmarshal([]byte(execResult.Stdout), &summary); err != nil {
			return result, fmt.Errorf("decode user backup import result: %w", err)
		}
		result.Import = &summary
	}
	return result, nil
}

func executeUserBackupFixture(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, node env.NodeRef, resources *provision.ScenarioResources, target map[string]any) (userBackupFixtureResult, string, string, error) {
	actorID := firstNonEmpty(stringTarget(target, "actorId"), stringTarget(target, "sourceActorId"))
	assignment, ok := findAssignment(resources, actorID)
	if !ok {
		return userBackupFixtureResult{}, actorID, "", fmt.Errorf("actor assignment %q not found", actorID)
	}
	base := principalCLIBase(assignment.Username, assignment.Password)
	sessionOut, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: append(base, "session", "open", "--space-id", assignment.SpaceID, "--domain-id", assignment.DomainID)})
	if err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	sessionID, err := jsonStringField(sessionOut.Stdout, "session_id", "sessionId")
	if err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	txOut, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: append(base, "transaction", "begin", sessionID, "--mode", "read-write")})
	if err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	txID, err := jsonStringField(txOut.Stdout, "transaction_id", "transactionId")
	if err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	fixtureLabel := firstNonEmpty(stringTarget(target, "label"), "nt2-fixture")
	createNode := func(name string) (string, error) {
		out, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: append(base, "graph", "node", "create", "--transaction-id", txID, "--label", "NT2Fixture", "--content", name, "--props-json", fmt.Sprintf(`{"tags":["%s"],"fixture":"%s"}`, fixtureLabel, name))})
		if err != nil {
			return "", err
		}
		return jsonStringField(out.Stdout, "node_id", "nodeId")
	}
	nodeA, err := createNode("nt2-source-a")
	if err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	nodeB, err := createNode("nt2-source-b")
	if err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	if _, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: append(base, "graph", "edge", "create", "--transaction-id", txID, "--from", nodeA, "--to", nodeB, "--kind", "NT2_RESTORES", "--props-json", `{"fixture":"nt2"}`)}); err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	blobPath := "/tmp/mycel-lab-nt2-blob-fixture.txt"
	payload := firstNonEmpty(stringTarget(target, "blobPayload"), "mycel-lab nt2 blob payload")
	if _, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: []string{"sh", "-ec", "printf %s " + shellQuote(payload) + " > " + shellQuote(blobPath)}}); err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	if _, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: append(base, "graph", "blob-node", "create", blobPath, "--transaction-id", txID, "--mime-type", "text/plain", "--props-json", fmt.Sprintf(`{"tags":["%s"],"fixture":"blob"}`, fixtureLabel), "--payload-json", `{"text":"nt2 blob fixture"}`)}); err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	if _, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: append(base, "transaction", "commit", txID)}); err != nil {
		return userBackupFixtureResult{}, actorID, assignment.Username, err
	}
	_, _ = driver.Exec(ctx, environment, node, env.ExecRequest{Command: append(base, "session", "close", sessionID)})
	return userBackupFixtureResult{SessionID: sessionID, TransactionID: txID, NodesCreated: 3, EdgesCreated: 1, BlobsCreated: 1}, actorID, assignment.Username, nil
}

func executeUserBackupVerify(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any, state map[string]UserBackupOperationResult) (userBackupVerifyResult, error) {
	importResult, ok := state[userBackupKey(target)]
	if !ok || importResult.Import == nil {
		return userBackupVerifyResult{}, fmt.Errorf("no import result recorded for %q", userBackupKey(target))
	}
	summary := importResult.Import
	if min := intTarget(target, "minNodes", 1); summary.NodesImported < int64(min) {
		return userBackupVerifyResult{}, fmt.Errorf("restored nodes=%d, want >=%d", summary.NodesImported, min)
	}
	if min := intTarget(target, "minEdges", 1); summary.EdgesImported < int64(min) {
		return userBackupVerifyResult{}, fmt.Errorf("restored edges=%d, want >=%d", summary.EdgesImported, min)
	}
	if min := intTarget(target, "minBlobs", 1); summary.BlobsImported < int64(min) {
		return userBackupVerifyResult{}, fmt.Errorf("restored blobs=%d, want >=%d", summary.BlobsImported, min)
	}
	targetUsername := firstNonEmpty(stringTarget(target, "targetUsername"), summary.Username)
	password := firstNonEmpty(stringTarget(target, "password"), stringTarget(target, "newPassword"), defaultUserBackupRestorePassword)
	nodes, err := driver.Nodes(ctx, environment)
	if err != nil {
		return userBackupVerifyResult{}, err
	}
	checks := 0
	for _, node := range nodes {
		for _, spaceID := range summary.SpaceIDMap {
			for _, domainID := range summary.DomainIDMap {
				cmd := append(principalCLIBase(targetUsername, password), "query", "gql", "--space-id", spaceID, "--domain-id", domainID, "MATCH (n) RETURN count(n)")
				if _, err := driver.Exec(ctx, environment, env.NodeRef{Name: node.Name, Ordinal: node.Ordinal}, env.ExecRequest{Command: cmd}); err != nil {
					return userBackupVerifyResult{}, fmt.Errorf("restored query on %s: %w", node.Name, err)
				}
				checks++
			}
		}
	}
	return userBackupVerifyResult{CheckedEndpoints: checks, NodesImported: summary.NodesImported, EdgesImported: summary.EdgesImported, BlobsImported: summary.BlobsImported}, nil
}

func executeUserBackupSafety(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, node env.NodeRef, resources *provision.ScenarioResources, target map[string]any) (userBackupSafetyResult, error) {
	file := stringTarget(target, "file")
	if file == "" {
		return userBackupSafetyResult{}, fmt.Errorf("user-backup-assert-safety requires target.file")
	}
	actorID := firstNonEmpty(stringTarget(target, "actorId"), stringTarget(target, "sourceActorId"))
	assignment, ok := findAssignment(resources, actorID)
	if !ok {
		return userBackupSafetyResult{}, fmt.Errorf("actor assignment %q not found", actorID)
	}
	out := userBackupSafetyResult{ArchiveChecked: true}
	for _, forbidden := range []string{assignment.Password, "access_token", "refresh_token", "auth_session", "newPassword"} {
		if strings.TrimSpace(forbidden) == "" {
			continue
		}
		cmd := []string{"sh", "-ec", "! grep -a -F -- " + shellQuote(forbidden) + " " + shellQuote(file) + " >/dev/null"}
		if _, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: cmd}); err != nil {
			return out, fmt.Errorf("forbidden archive text %q found in %s", forbidden, file)
		}
	}
	out.ForbiddenArchiveTextAbsent = true
	if targetUsername := stringTarget(target, "targetUsername"); targetUsername != "" {
		cmd := append(principalCLIBase(targetUsername, assignment.Password), "space", "list")
		if _, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: cmd}); err == nil {
			return out, fmt.Errorf("source password unexpectedly authenticated for restored user %s", targetUsername)
		}
		out.SourcePasswordRejected = true
	}
	return out, nil
}

func userBackupCommand(eventType string, target map[string]any, resources *provision.ScenarioResources, file string) ([]string, string, string, error) {
	actorID := firstNonEmpty(stringTarget(target, "actorId"), stringTarget(target, "sourceActorId"))
	username := firstNonEmpty(stringTarget(target, "username"), stringTarget(target, "sourceUsername"))
	if username == "" && actorID != "" {
		assignment, ok := findAssignment(resources, actorID)
		if !ok {
			return nil, actorID, "", fmt.Errorf("actor assignment %q not found", actorID)
		}
		username = assignment.Username
	}
	compression := firstNonEmpty(stringTarget(target, "compression"), "auto")
	base := adminCLIBase()
	switch eventType {
	case "user-backup-export":
		if username == "" {
			return nil, actorID, username, fmt.Errorf("%s requires target.actorId, target.sourceActorId, target.username, or target.sourceUsername", eventType)
		}
		cmd := append(base, "admin", "user-backup", "export", "--source-username", username, "--file", file, "--include-blobs")
		if compression != "auto" && compression != "" {
			cmd = append(cmd, "--compression", compression)
		}
		if sourceLabel := stringTarget(target, "sourceLabel"); sourceLabel != "" {
			cmd = append(cmd, "--source-label", sourceLabel)
		}
		return cmd, actorID, username, nil
	case "user-backup-validate":
		cmd := append(base, "admin", "user-backup", "validate", "--file", file)
		if compression != "" {
			cmd = append(cmd, "--compression", compression)
		}
		return cmd, actorID, username, nil
	case "user-backup-import":
		targetUsername := stringTarget(target, "targetUsername")
		if targetUsername == "" {
			targetUsername = username
		}
		if targetUsername == "" {
			return nil, actorID, username, fmt.Errorf("%s requires target.targetUsername or a resolvable source username", eventType)
		}
		cmd := append(base, "admin", "user-backup", "import", "--file", file, "--target-username", targetUsername)
		if compression != "" {
			cmd = append(cmd, "--compression", compression)
		}
		if boolTarget(target, "createUser", false) {
			cmd = append(cmd, "--create-user", "--new-password", firstNonEmpty(stringTarget(target, "newPassword"), defaultUserBackupRestorePassword))
		}
		if boolTarget(target, "execute", true) {
			cmd = append(cmd, "--execute")
		}
		if mode := stringTarget(target, "domainImportMode"); mode != "" {
			cmd = append(cmd, "--domain-import-mode", mode)
		}
		return cmd, actorID, targetUsername, nil
	default:
		return nil, actorID, username, fmt.Errorf("unsupported user-backup event type %q", eventType)
	}
}

func adminCLIBase() []string {
	return []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", provision.DefaultAdminUsername, "--password", provision.DefaultAdminPassword, "--output", "json"}
}

func offlineCLIBase() []string {
	return []string{"mycel", "--output", "json"}
}

func principalCLIBase(username, password string) []string {
	return []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", username, "--password", password, "--output", "json"}
}

func userBackupKey(target map[string]any) string {
	return firstNonEmpty(stringTarget(target, "backup"), stringTarget(target, "name"), stringTarget(target, "file"), stringTarget(target, "hostFile"), "default")
}

func expandTargetPath(value string, environment env.Environment, resources *provision.ScenarioResources) string {
	if value == "" {
		return ""
	}
	runID := ""
	if resources != nil {
		runID = resources.RunID
	}
	out := strings.ReplaceAll(value, "{environment}", environment.Name)
	out = strings.ReplaceAll(out, "{runID}", runID)
	return out
}

func jsonStringField(raw string, names ...string) (string, error) {
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return "", fmt.Errorf("decode JSON output: %w", err)
	}
	for _, name := range names {
		if value, _ := doc[name].(string); value != "" {
			return value, nil
		}
	}
	return "", fmt.Errorf("JSON output missing %s", strings.Join(names, "/"))
}

func findAssignment(resources *provision.ScenarioResources, actorID string) (provision.ActorAssignment, bool) {
	if resources == nil {
		return provision.ActorAssignment{}, false
	}
	for _, assignment := range resources.Assignments {
		if assignment.ActorID == actorID {
			return assignment, true
		}
	}
	return provision.ActorAssignment{}, false
}

func stringTarget(target map[string]any, key string) string {
	if target == nil {
		return ""
	}
	if value, ok := target[key].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func intTarget(target map[string]any, key string, fallback int) int {
	if target == nil {
		return fallback
	}
	switch value := target[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return fallback
	}
}

func boolTarget(target map[string]any, key string, fallback bool) bool {
	if target == nil {
		return fallback
	}
	if value, ok := target[key].(bool); ok {
		return value
	}
	if value, ok := target[key].(string); ok {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes":
			return true
		case "0", "false", "no":
			return false
		}
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func bounded(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[len(value)-limit:]
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
