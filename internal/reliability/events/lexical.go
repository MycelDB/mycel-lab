package events

import (
	"context"
	"errors"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

// executeLexicalLocalArtifactLoss removes only rebuildable lexical artifacts.
// Graph truth, graph-change history, and replicated projection metadata remain
// intact so the following restart exercises historical lexical replay.
func executeLexicalLocalArtifactLoss(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, target map[string]any) (map[string]any, error) {
	ordinals := intSliceTarget(target, "ordinals")
	if len(ordinals) == 0 {
		ordinal := intTarget(target, "ordinal", 0)
		ordinals = []int{ordinal}
	}
	for _, ordinal := range ordinals {
		if ordinal < 0 {
			return nil, errors.New("lexical local artifact ordinal must be nonnegative")
		}
		if _, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: []string{"sh", "-ec", "rm -rf -- /data/mycel/search/lexical"}}); err != nil {
			return nil, errors.New("remove local lexical projection artifacts")
		}
	}
	return map[string]any{"ordinals": ordinals, "artifactsRemoved": true}, nil
}
