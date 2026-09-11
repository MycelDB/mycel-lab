package report

import (
	"strings"
	"testing"
	"time"
)

func TestMarkdownIncludesPhasesAndArtifacts(t *testing.T) {
	md := Markdown(RunSummary{RunID: "run-1", ScenarioName: "example", Status: "passed", StartedAt: time.Unix(1, 0).UTC(), FinishedAt: time.Unix(2, 0).UTC(), DryRun: true, Phases: []PhaseSummary{{Name: "warmup", Status: "passed", PlannedDuration: time.Minute, ActualDuration: time.Second}}, Artifacts: []string{"result.json"}})
	for _, want := range []string{"# Mycel Lab Run run-1", "Scenario: `example`", "Dry run: phase waits", "| warmup | passed | 1m0s | 1s |", "`result.json`"} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n%s", want, md)
		}
	}
}
