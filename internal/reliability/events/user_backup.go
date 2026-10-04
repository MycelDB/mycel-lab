package events

import (
	"context"
	"fmt"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

const defaultUserBackupRestorePassword = "mycel-lab-restore-password"

type UserBackupOperationResult struct {
	Operation string `json:"operation"`
	NodeName  string `json:"nodeName,omitempty"`
	ActorID   string `json:"actorId,omitempty"`
	Username  string `json:"username,omitempty"`
	File      string `json:"file"`
	Stdout    string `json:"stdout,omitempty"`
	Stderr    string `json:"stderr,omitempty"`
}

func executeUserBackupEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, eventType string, target map[string]any) (UserBackupOperationResult, error) {
	file := stringTarget(target, "file")
	if file == "" {
		return UserBackupOperationResult{}, fmt.Errorf("%s requires target.file", eventType)
	}
	node := env.NodeRef{Name: stringTarget(target, "node"), Ordinal: intTarget(target, "ordinal", 0)}
	result := UserBackupOperationResult{Operation: eventType, NodeName: node.Name, File: file}
	cmd, actorID, username, err := userBackupCommand(eventType, target, resources, file)
	if err != nil {
		return result, err
	}
	result.ActorID = actorID
	result.Username = username
	execResult, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: cmd})
	result.Stdout = bounded(execResult.Stdout, 8192)
	result.Stderr = bounded(execResult.Stderr, 8192)
	if err != nil {
		return result, err
	}
	return result, nil
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
	base := []string{"mycel", "--daemon-addr", "127.0.0.1:9091", "--username", provision.DefaultAdminUsername, "--password", provision.DefaultAdminPassword, "--output", "json", "admin", "user-backup"}
	switch eventType {
	case "user-backup-export":
		if username == "" {
			return nil, actorID, username, fmt.Errorf("%s requires target.actorId, target.sourceActorId, target.username, or target.sourceUsername", eventType)
		}
		cmd := append(base, "export", "--source-username", username, "--file", file, "--include-blobs")
		if compression != "auto" && compression != "" {
			cmd = append(cmd, "--compression", compression)
		}
		if sourceLabel := stringTarget(target, "sourceLabel"); sourceLabel != "" {
			cmd = append(cmd, "--source-label", sourceLabel)
		}
		return cmd, actorID, username, nil
	case "user-backup-validate":
		cmd := append(base, "validate", "--file", file)
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
		cmd := append(base, "import", "--file", file, "--target-username", targetUsername)
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
