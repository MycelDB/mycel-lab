package catalog

import (
	"path/filepath"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestSemanticContractDisabledQuiescenceIsNarrowAndReenableStaysStrict(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "compose-semantic-graph-contract", "compose-semantic-graph-contract.yaml")
	loaded, err := spec.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	scenario := loaded.(spec.Scenario)
	phases := map[string]spec.PhaseSpec{}
	pausedCount := 0
	for _, phase := range scenario.Phases {
		phases[phase.Name] = phase
		for _, event := range phase.Events {
			if event.Type == "semantic-maintenance-await" && event.Target["completion"] == "disabled-quiescent" {
				pausedCount++
				if phase.Name != "disabled-rule-negative-control" {
					t.Fatal("disabled mode escaped its intentional negative-control phase")
				}
			}
		}
	}
	if len(scenario.Phases) != 11 || pausedCount != 1 {
		t.Fatal("contract scope or disabled-mode count changed")
	}
	baseline := phases["delete-and-converge"].Events
	captured := false
	for _, event := range baseline {
		if event.Type == "semantic-reliability-assert" && event.Target["requireFreshAtLatest"] == true && event.Target["captureGenerationAs"] == "before-disable-generation" && event.Target["captureFreshRevisionAs"] == "before-disable-fresh" {
			captured = true
		}
	}
	if !captured {
		t.Fatal("disabled boundary lost its previously fresh generation/revision certificate")
	}
	negative := phases["disabled-rule-negative-control"].Events
	if len(negative) != 4 || negative[0].Type != "semantic-rule-enable" || negative[0].Target["enabled"] != false || negative[1].Type != "semantic-node-create" || negative[1].Target["captureRevisionAs"] != "disabled-node-revision" {
		t.Fatal("public disable/acknowledged commit order changed")
	}
	wait := negative[2]
	if wait.Type != "semantic-maintenance-await" || wait.Target["completion"] != "disabled-quiescent" || wait.Target["expectGenerationFrom"] != "before-disable-generation" || wait.Target["expectFreshRevisionFrom"] != "before-disable-fresh" || wait.Target["expectRevisionFrom"] != "disabled-node-revision" {
		t.Fatal("quiescence is not bound to the captured state and commit")
	}
	if negative[3].Type != "semantic-search-assert" || negative[3].Target["expectFailureCode"] != "FailedPrecondition" {
		t.Fatal("disabled direct-search rejection was dropped")
	}
	reenable := phases["reenable-backfill-and-prove-catch-up"].Events
	if reenable[0].Type != "semantic-rule-enable" || reenable[0].Target["enabled"] != true || reenable[1].Type != "semantic-backfill" || reenable[2].Type != "semantic-maintenance-await" || reenable[2].Target["completion"] != nil {
		t.Fatal("re-enable stopped using ordinary indexed completion")
	}
	fresh, search := false, false
	for _, event := range reenable {
		if event.Type == "semantic-reliability-assert" && event.Target["requireFreshAtLatest"] == true {
			fresh = true
		}
		if event.Type == "semantic-search-assert" && event.Target["requireProvenance"] == true {
			search = true
		}
	}
	if !fresh || !search {
		t.Fatal("re-enable freshness/direct-provenance assertions were weakened")
	}
}
