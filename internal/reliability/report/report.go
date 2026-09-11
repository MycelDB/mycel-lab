package report

import (
	"fmt"
	"strings"
	"time"
)

type PhaseSummary struct {
	Name     string        `json:"name"`
	Status   string        `json:"status"`
	Duration time.Duration `json:"duration"`
}

type RunSummary struct {
	RunID        string         `json:"runId"`
	ScenarioName string         `json:"scenarioName"`
	Status       string         `json:"status"`
	StartedAt    time.Time      `json:"startedAt"`
	FinishedAt   time.Time      `json:"finishedAt"`
	DryRun       bool           `json:"dryRun"`
	Phases       []PhaseSummary `json:"phases"`
	Artifacts    []string       `json:"artifacts"`
}

func Markdown(summary RunSummary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Mycel Lab Run %s\n\n", summary.RunID)
	fmt.Fprintf(&b, "- Scenario: `%s`\n", summary.ScenarioName)
	fmt.Fprintf(&b, "- Status: `%s`\n", summary.Status)
	fmt.Fprintf(&b, "- Dry run: `%t`\n", summary.DryRun)
	fmt.Fprintf(&b, "- Started: `%s`\n", summary.StartedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "- Finished: `%s`\n\n", summary.FinishedAt.Format(time.RFC3339))
	b.WriteString("## Phases\n\n")
	b.WriteString("| Phase | Status | Planned duration |\n")
	b.WriteString("| --- | --- | --- |\n")
	for _, phase := range summary.Phases {
		fmt.Fprintf(&b, "| %s | %s | %s |\n", phase.Name, phase.Status, phase.Duration)
	}
	if len(summary.Artifacts) > 0 {
		b.WriteString("\n## Artifacts\n\n")
		for _, artifact := range summary.Artifacts {
			fmt.Fprintf(&b, "- `%s`\n", artifact)
		}
	}
	return b.String()
}
