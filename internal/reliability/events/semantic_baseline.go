package events

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

var semanticPathComponent = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// executeSemanticLocalBaselineLoss removes only the rebuildable semantic index
// for the provisioned rule on one node. Raft graph data and replicated
// projection control metadata remain untouched; the following restart must
// recover the local artifact from graph truth.
func executeSemanticLocalBaselineLoss(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) (map[string]any, error) {
	if resources == nil || resources.Semantic == nil {
		return nil, errors.New("semantic local baseline loss requires semantic resources")
	}
	spaceID := resources.Semantic.SpaceID
	ruleID := resources.Semantic.SemanticRuleID
	if !semanticPathComponent.MatchString(spaceID) || !semanticPathComponent.MatchString(ruleID) {
		return nil, errors.New("semantic local baseline identifiers are invalid")
	}
	ordinal := intTarget(target, "ordinal", 0)
	if ordinal < 0 {
		return nil, errors.New("semantic local baseline ordinal must be nonnegative")
	}
	path := fmt.Sprintf("/data/mycel/graphs/%s/semantic/indexes/%s", spaceID, ruleID)
	if _, err := driver.Exec(ctx, environment, env.NodeRef{Ordinal: ordinal}, env.ExecRequest{Command: []string{"sh", "-ec", "rm -rf -- " + shellQuote(path)}}); err != nil {
		return nil, errors.New("remove local semantic projection baseline")
	}
	return map[string]any{"ordinal": ordinal, "baselineRemoved": true}, nil
}
