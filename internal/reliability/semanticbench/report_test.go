package semanticbench

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestSummarizeSuccessfulLatencyAndTail(t *testing.T) {
	var samples []Sample
	for i := 1000; i > 0; i-- {
		samples = append(samples, Sample{Elapsed: time.Duration(i) * time.Millisecond})
	}
	samples = append(samples, Sample{Elapsed: time.Hour, Outcome: Timeout}, Sample{Elapsed: time.Hour, Outcome: RPCError})
	w := Summarize(8, 1002, 10*time.Second, samples, "")
	if w.Status != Complete || w.Attempts != 1002 || w.Successes != 1000 || w.Errors != 1 || w.Timeouts != 1 || *w.P50 != 500 || *w.P95 != 950 || *w.P99 != 990 || w.QPS != 100 {
		t.Fatalf("bad aggregation: %+v", w)
	}
	w = Summarize(8, 999, time.Second, samples[:999], "")
	if w.Status != Complete || w.P99 != nil || w.P95 == nil {
		t.Fatalf("small p99 sample was published: %+v", w)
	}
}

func TestSummarizeInvalidAndPartial(t *testing.T) {
	samples := make([]Sample, 100)
	for i := range samples {
		samples[i].Elapsed = time.Second
	}
	w := Summarize(1, 100, time.Minute, samples, "")
	if w.Status != Complete {
		t.Fatalf("slow results must not fail a measurement: %+v", w)
	}
	w = Summarize(1, 100, time.Minute, append(samples, Sample{Outcome: UnexpectedResponse}), "measurement-budget-expired")
	if w.Status != Inconclusive || w.Reason != "unexpected-search-results" || w.Successes != 100 || w.WrongHits != 1 || w.Errors != 1 || w.P95 == nil {
		t.Fatalf("missing partial invalid-query evidence: %+v", w)
	}
	w = Summarize(1, 100, time.Minute, samples[:50], "measurement-budget-expired")
	if w.Status != Inconclusive || w.Attempts != 50 || w.P95 != nil || w.P50 == nil {
		t.Fatalf("incorrect interrupted result: %+v", w)
	}
	w = Summarize(1, 50, time.Second, samples[:50], "")
	if w.Reason != "insufficient-success-samples" {
		t.Fatalf("small sample treated as baseline: %+v", w)
	}
}

func fixtureReport(id string, multiplier float64) Report {
	r := NewReport(id, spec.SemanticScalingTarget{Sweep: spec.ScaleGraph, Sizes: []int{1000, 3000, 10000}, Samples: 1024})
	r.Environment = Environment{HarnessSHA256: "harness", DockerEngineSHA256: "engine", DockerCPUs: 4}
	for i := range r.Points {
		p := &r.Points[i]
		p.Reason = ""
		p.Counts = &Counts{Nodes: p.Size, Records: 1000, Vectors: 1000}
		for j := range p.Workloads {
			p50, p95 := float64(i+1)*multiplier, 2*float64(i+1)*multiplier
			p.Workloads[j] = Workload{Clients: p.Workloads[j].Clients, Attempts: 1024, Successes: 1024, Status: Complete, P50: &p50, P95: &p95}
		}
	}
	r.Finish()
	return r
}

func TestComparisonRepeatsAndAdjacentRatios(t *testing.T) {
	var reports []Report
	for i, multiple := range []float64{1, 1.5, 2} {
		reports = append(reports, fixtureReport(fmt.Sprintf("run-%d", i), multiple))
	}
	markdown, err := CompareMarkdown(reports)
	if err != nil || !strings.Contains(markdown, "| 3000 | 3000 / 0 | 1000 / 1000 | 1 | 3 / 3 | 3.000 [2.000–4.000] | 6.000 [4.000–8.000] | 3.00× | 1.00× | 2.00× | 2.00× |") {
		t.Fatalf("comparison: %v\n%s", err, markdown)
	}
	reports[0].Points[1].Workloads[0].Status = Inconclusive
	markdown, err = CompareMarkdown(reports)
	if err != nil || !strings.Contains(markdown, "| 10000 | 10000 / 0 | 1000 / 1000 | 1 | 3 / 3 | 4.500 [3.000–6.000] | 9.000 [6.000–12.000] | — | — | — | — |") {
		t.Fatalf("comparison bridged an inconclusive point: %v\n%s", err, markdown)
	}
}

func TestComparisonRefusesUnlikeExperiments(t *testing.T) {
	original := fixtureReport("first", 1)
	for name, change := range map[string]func(*Report){
		"duplicate run":      func(r *Report) { r.RunID = original.RunID },
		"different engine":   func(r *Report) { r.Environment.DockerEngineSHA256 = "other" },
		"different corpus":   func(r *Report) { r.Corpus += "-other" },
		"different nodes":    func(r *Report) { r.Points[0].Counts.Nodes++ },
		"missing percentile": func(r *Report) { r.Points[0].Workloads[0].P95 = nil },
	} {
		t.Run(name, func(t *testing.T) {
			other := fixtureReport("second", 2)
			change(&other)
			if _, err := CompareMarkdown([]Report{original, other}); err == nil {
				t.Fatal("accepted incompatible report")
			}
		})
	}
}

func TestReportRetainsUnavailablePoints(t *testing.T) {
	r := NewReport("run", spec.SemanticScalingTarget{Sweep: spec.ScaleIndexed, Sizes: []int{1000, 3000}})
	r.Points[0].Reason = "index-not-fresh"
	r.Finish()
	markdown := r.Markdown()
	if r.Status != Inconclusive || !strings.Contains(markdown, "index-not-fresh") || !strings.Contains(markdown, "| 3000 | — | — | 8 |") || !strings.Contains(markdown, "No performance threshold applied") {
		t.Fatalf("lost failure context: %s", markdown)
	}
}
