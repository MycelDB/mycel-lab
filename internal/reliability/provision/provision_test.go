package provision

import (
	"path/filepath"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
)

func TestPlanScenarioCreatesWriterResourcesAndReaderAssignments(t *testing.T) {
	scenario, err := catalog.ResolveScenarioFile(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "raft-3-node-short-outage.yaml"), catalog.ResolveOptions{})
	if err != nil {
		t.Fatalf("ResolveScenarioFile() error = %v", err)
	}
	resources, err := PlanScenario(scenario, Options{RunID: "raft-3-node-short-outage-20260912-000000", DaemonAddrs: []string{"127.0.0.1:19091", "127.0.0.1:19092", "127.0.0.1:19093"}, DryRun: true})
	if err != nil {
		t.Fatalf("PlanScenario() error = %v", err)
	}
	if len(resources.Principals) != 6 {
		t.Fatalf("principals=%d, want 6", len(resources.Principals))
	}
	if len(resources.Spaces) != 4 || len(resources.Domains) != 4 {
		t.Fatalf("spaces=%d domains=%d, want 4/4", len(resources.Spaces), len(resources.Domains))
	}
	if len(resources.Assignments) != 6 {
		t.Fatalf("assignments=%d, want 6", len(resources.Assignments))
	}
	writers := 0
	readers := 0
	for _, assignment := range resources.Assignments {
		switch assignment.BehaviorType {
		case "graph-transaction":
			writers++
			if assignment.TargetActorID != "" {
				t.Fatalf("writer has target actor: %+v", assignment)
			}
		case "gql-read":
			readers++
			if assignment.TargetActorID == "" || assignment.TargetGroup != "journal-users" {
				t.Fatalf("reader missing writer target: %+v", assignment)
			}
		default:
			t.Fatalf("unexpected behavior type %q", assignment.BehaviorType)
		}
		if assignment.DaemonAddr == "" || assignment.Username == "" {
			t.Fatalf("assignment missing addr/username: %+v", assignment)
		}
	}
	if writers != 4 || readers != 2 {
		t.Fatalf("writers=%d readers=%d, want 4/2", writers, readers)
	}
}
