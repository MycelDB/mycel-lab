package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/events"
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
	"github.com/MycelDB/mycel-lab/internal/reliability/semanticbench"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestSemanticScalingCanonicalPlansUsePinnedCredentialFreeCompose(t *testing.T) {
	for _, sweep := range []string{spec.ScaleIndexed, spec.ScaleGraph} {
		t.Run(sweep, func(t *testing.T) {
			s := canonicalComposeScenario(t, "compose-semantic-search-scaling-"+sweep)
			if err := validateSemanticScalingScenario(s, s.Cluster.Image, false); err != nil {
				t.Fatal(err)
			}
			if err := validateOperationTimeoutBudget(s); err != nil {
				t.Fatal(err)
			}
			plan, err := spec.ParseSemanticScalingTarget(s.Phases[0].Events[0].Target)
			if err != nil || plan.Sweep != sweep {
				t.Fatalf("sweep plan didn't match the fixture: %+v %v", plan, err)
			}
			if err := validateSemanticScalingScenario(s, "myceldb/mycel:latest", false); err == nil {
				t.Fatal("accepted mutable tag")
			}
			if err := validateSemanticScalingScenario(s, s.Cluster.Image, true); err == nil {
				t.Fatal("accepted report-only experiment in a release bundle")
			}
			s.ProviderProfile.Profile.Mode = provider.ModeLive
			if err := validateSemanticScalingScenario(s, s.Cluster.Image, false); err == nil {
				t.Fatal("accepted live provider")
			}
		})
	}
}

func TestSemanticScalingReportArtifactsRetainInconclusiveEvidence(t *testing.T) {
	sink, err := artifacts.NewSanitizedSink(t.TempDir(), provider.NewRedactor(nil))
	if err != nil {
		t.Fatal(err)
	}
	run := &runRecorder{sink: sink, harnessIdentity: harnessBuildIdentity{ExecutableSHA256: "test-build", VCS: map[string]string{"vcs.revision": "test-revision"}}}
	report := semanticbench.NewReport("run-138", spec.SemanticScalingTarget{Sweep: spec.ScaleIndexed, Sizes: []int{1000, 3000}})
	report.Points[0].Reason = "index-not-fresh"
	report.Finish()
	err = run.RecordRuntimeEvent(context.Background(), events.Record{Type: "event-completed", EventType: spec.SemanticScalingEvent, Payload: map[string]any{"semanticSearchScaling": report}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(sink.Root(), "benchmark", "semantic-search-scaling.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded semanticbench.Report
	if err := json.Unmarshal(raw, &recorded); err != nil || recorded.Status != semanticbench.Inconclusive || recorded.Environment.LabRevision != "test-revision" || recorded.Points[1].Reason != "not-reached" {
		t.Fatalf("invalid JSON report: %s, %v", raw, err)
	}
	markdown, err := os.ReadFile(filepath.Join(sink.Root(), "benchmark", "semantic-search-scaling.md"))
	if err != nil || !strings.Contains(string(markdown), "No performance threshold applied") {
		t.Fatalf("invalid Markdown report: %s, %v", markdown, err)
	}
}
