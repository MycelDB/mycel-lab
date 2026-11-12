package events

import (
	"context"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

func TestExecuteSemanticLocalBaselineLossRemovesOnlyRebuildableIndex(t *testing.T) {
	driver := &recordingDriver{}
	resources := &provision.ScenarioResources{Semantic: &provision.SemanticResources{SpaceID: "space-1", SemanticRuleID: "rule-1"}}
	result, err := executeSemanticLocalBaselineLoss(context.Background(), driver, env.Environment{}, resources, map[string]any{"ordinal": 2})
	if err != nil {
		t.Fatalf("executeSemanticLocalBaselineLoss() error=%v", err)
	}
	if result["ordinal"] != 2 || result["baselineRemoved"] != true {
		t.Fatalf("unexpected result: %#v", result)
	}
	if len(driver.execCommands) != 1 || !strings.Contains(driver.execCommands[0], "/data/mycel/graphs/space-1/semantic/indexes/rule-1") {
		t.Fatalf("unexpected baseline removal command: %#v", driver.execCommands)
	}
	if len(driver.restarted) != 0 {
		t.Fatalf("baseline-loss primitive must not restart the node: %#v", driver.restarted)
	}
}

func TestExecuteSemanticLocalBaselineLossRejectsUnsafeIdentifiers(t *testing.T) {
	driver := &recordingDriver{}
	resources := &provision.ScenarioResources{Semantic: &provision.SemanticResources{SpaceID: "../space", SemanticRuleID: "rule-1"}}
	if _, err := executeSemanticLocalBaselineLoss(context.Background(), driver, env.Environment{}, resources, nil); err == nil {
		t.Fatal("unsafe semantic identifier unexpectedly accepted")
	}
}
