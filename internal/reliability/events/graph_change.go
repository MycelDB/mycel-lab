package events

import (
	"context"
	"errors"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

const graphChangeNotificationPath = "/data/mycel/graph-change-notification"
const graphChangeNotificationFaultBackupPath = "/data/mycel/graph-change-notification.fault-backup"

// executeGraphChangeNotificationFault makes the node-local notification path
// unwritable while leaving graph Raft data and derived-consumer metadata intact.
// Graph commits should remain available; this is the handoff-loss reproduction.
func executeGraphChangeNotificationFault(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, target map[string]any) (map[string]any, error) {
	return executeGraphChangeNotificationPathMutation(ctx, driver, environment, target, "fault")
}

func executeGraphChangeNotificationRestore(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, target map[string]any) (map[string]any, error) {
	return executeGraphChangeNotificationPathMutation(ctx, driver, environment, target, "restore")
}

func executeGraphChangeNotificationPathMutation(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, target map[string]any, operation string) (map[string]any, error) {
	ordinals := intSliceTarget(target, "ordinals")
	if len(ordinals) == 0 {
		ordinals = []int{intTarget(target, "ordinal", 0)}
	}
	for _, ordinal := range ordinals {
		if ordinal < 0 {
			return nil, errors.New("graph-change notification ordinal must be nonnegative")
		}
		command := []string{"sh", "-ec"}
		switch operation {
		case "fault":
			command = append(command, "rm -rf -- "+graphChangeNotificationFaultBackupPath+"; if [ -e "+graphChangeNotificationPath+" ]; then mv -- "+graphChangeNotificationPath+" "+graphChangeNotificationFaultBackupPath+"; fi; : > "+graphChangeNotificationPath)
		case "restore":
			command = append(command, "rm -f -- "+graphChangeNotificationPath+"; if [ -d "+graphChangeNotificationFaultBackupPath+" ]; then mv -- "+graphChangeNotificationFaultBackupPath+" "+graphChangeNotificationPath+"; else mkdir -p -- "+graphChangeNotificationPath+"; fi")
		default:
			return nil, errors.New("unsupported graph-change notification path operation")
		}
		if _, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: command}); err != nil {
			return nil, errors.New("mutate graph-change notification path")
		}
	}
	return map[string]any{"ordinals": ordinals, "operation": operation, "graphChangeNotificationPathMutated": true}, nil
}
