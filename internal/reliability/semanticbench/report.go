// Package semanticbench records report-only semantic search measurements.
package semanticbench

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

const (
	SchemaVersion    = 1
	CorpusVersion    = "axis-documents-v1"
	QueryMixVersion  = "four-axis-top10-v1"
	WarmupsPerClient = 8
	MinimumP95       = 100
	MinimumP99       = 1000
	Complete         = "measurement-complete"
	Inconclusive     = "inconclusive"
)

func Concurrencies() [2]int { return [2]int{1, 8} }

type Environment struct {
	Image              string   `json:"daemonImage"`
	LabRevision        string   `json:"labRevision"`
	LabModified        bool     `json:"labModified"`
	HarnessSHA256      string   `json:"harnessSha256"`
	Driver             string   `json:"driver"`
	DaemonCount        int      `json:"daemonCount"`
	RaftPartitions     int      `json:"raftPartitions"`
	RaftReplicaFactor  int      `json:"raftReplicaFactor"`
	ComposeSHA256      []string `json:"composeSha256"`
	DockerCPUs         int      `json:"dockerCpus"`
	DockerMemory       int64    `json:"dockerMemoryBytes"`
	DockerArch         string   `json:"dockerArchitecture"`
	DockerVersion      string   `json:"dockerVersion"`
	DockerEngineSHA256 string   `json:"dockerEngineSha256"`
	ClientPlatform     string   `json:"clientPlatform"`
	ClientCPUs         int      `json:"clientCpus"`
	Provider           string   `json:"embeddingFixture"`
	ProviderSHA256     string   `json:"embeddingFixtureSha256"`
	VectorDimension    int      `json:"vectorDimension"`
	VectorBackend      string   `json:"vectorBackend"`
}

type Report struct {
	Version     int                        `json:"schemaVersion"`
	RunID       string                     `json:"runId"`
	Status      string                     `json:"status"`
	Reason      string                     `json:"reason,omitempty"`
	Corpus      string                     `json:"corpusVersion"`
	QueryMix    string                     `json:"queryMixVersion"`
	TopK        int                        `json:"topK"`
	MinScore    float64                    `json:"minimumScore"`
	Warmups     int                        `json:"warmupsPerClient"`
	Environment Environment                `json:"environment"`
	Plan        spec.SemanticScalingTarget `json:"plan"`
	Points      []Point                    `json:"points"`
}

type Counts struct {
	Nodes   int `json:"graphNodes"`
	Edges   int `json:"graphEdges"`
	Records int `json:"indexedRecords"`
	Vectors int `json:"indexedVectors"`
}

type Point struct {
	Size      int        `json:"plannedSize"`
	Counts    *Counts    `json:"observedCounts,omitempty"`
	SetupMS   *float64   `json:"setupMs,omitempty"`
	Workloads []Workload `json:"workloads"`
	Reason    string     `json:"reason,omitempty"`
}

type Workload struct {
	Clients    int      `json:"clients"`
	Status     string   `json:"status"`
	Reason     string   `json:"reason,omitempty"`
	Attempts   int      `json:"attempts"`
	Successes  int      `json:"successes"`
	Errors     int      `json:"errors"`
	Timeouts   int      `json:"timeouts"`
	WrongHits  int      `json:"unexpectedResponses"`
	DurationMS float64  `json:"durationMs"`
	QPS        float64  `json:"successfulQueriesPerSecond"`
	P50        *float64 `json:"p50Ms,omitempty"`
	P95        *float64 `json:"p95Ms,omitempty"`
	P99        *float64 `json:"p99Ms,omitempty"`
}

type Outcome uint8

const (
	Success Outcome = iota
	RPCError
	Timeout
	UnexpectedResponse
)

type Sample struct {
	Elapsed time.Duration
	Outcome Outcome
}

func Milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func NewReport(runID string, plan spec.SemanticScalingTarget) Report {
	r := Report{Version: SchemaVersion, RunID: runID, Status: Inconclusive, Corpus: CorpusVersion, QueryMix: QueryMixVersion, TopK: spec.ScalingTopK, MinScore: spec.ScalingMinScore, Warmups: WarmupsPerClient, Plan: plan}
	for _, size := range plan.Sizes {
		point := Point{Size: size, Reason: "not-reached"}
		for _, clients := range Concurrencies() {
			point.Workloads = append(point.Workloads, Workload{Clients: clients, Status: Inconclusive, Reason: "not-reached"})
		}
		r.Points = append(r.Points, point)
	}
	return r
}

func (r *Report) Finish() {
	if r.Reason != "" {
		return
	}
	for _, point := range r.Points {
		for _, workload := range point.Workloads {
			if point.Reason != "" || workload.Status != Complete {
				r.Reason = "one-or-more-points-inconclusive"
				return
			}
		}
	}
	r.Status = Complete
}

// Summarize uses nearest rank on successful responses only. Errors exclude
// timeouts but include unexpected responses, which are also counted separately.
func Summarize(clients, goal int, elapsed time.Duration, samples []Sample, interruption string) Workload {
	w := Workload{Clients: clients, Attempts: len(samples), DurationMS: Milliseconds(elapsed), Status: Inconclusive}
	var latencies []float64
	for _, sample := range samples {
		switch sample.Outcome {
		case Success:
			latencies = append(latencies, Milliseconds(sample.Elapsed))
		case Timeout:
			w.Timeouts++
		case UnexpectedResponse:
			w.WrongHits++
			w.Errors++
		default:
			w.Errors++
		}
	}
	w.Successes = len(latencies)
	if elapsed > 0 {
		w.QPS = float64(w.Successes) / elapsed.Seconds()
	}
	slices.Sort(latencies)
	w.P50 = percentile(latencies, 0.50, 1)
	w.P95 = percentile(latencies, 0.95, MinimumP95)
	w.P99 = percentile(latencies, 0.99, MinimumP99)
	switch {
	case w.WrongHits != 0:
		w.Reason = "unexpected-search-results"
	case interruption != "":
		w.Reason = interruption
	case w.Attempts != goal:
		w.Reason = "attempt-budget-not-reached"
	case w.Successes < MinimumP95:
		w.Reason = "insufficient-success-samples"
	default:
		w.Status = Complete
	}
	return w
}

func percentile(sorted []float64, p float64, minimum int) *float64 {
	if len(sorted) < minimum {
		return nil
	}
	v := sorted[int(math.Ceil(p*float64(len(sorted))))-1]
	return &v
}

func (r Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Semantic search scaling: %s\n\n**%s**", r.Plan.Sweep, r.Status)
	if r.Reason != "" {
		fmt.Fprintf(&b, " (%s)", r.Reason)
	}
	fmt.Fprintf(&b, ". No performance threshold applied.\n\nDaemon: `%s` · Lab: `%s` · %d daemons, %d Raft partitions, RF %d\n\n", r.Environment.Image, r.Environment.LabRevision, r.Environment.DaemonCount, r.Environment.RaftPartitions, r.Environment.RaftReplicaFactor)
	fmt.Fprintf(&b, "Docker: %s, %d CPUs, %d bytes RAM. Fixture: `%s`, %d dimensions, top %d, min score %.2f.\n\n", r.Environment.DockerArch, r.Environment.DockerCPUs, r.Environment.DockerMemory, r.Environment.Provider, r.Environment.VectorDimension, r.TopK, r.MinScore)
	b.WriteString("| Target | Graph nodes / edges | Index records / vectors | Clients | Attempts / successes | p50 ms | p95 ms | p99 ms | Success/s | Errors | Timeouts | Measurement |\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
	for _, p := range r.Points {
		graph, index := countLabels(p.Counts)
		for _, w := range p.Workloads {
			state := w.Status
			if p.Reason != "" {
				state += ": " + p.Reason
			} else if w.Reason != "" {
				state += ": " + w.Reason
			}
			fmt.Fprintf(&b, "| %d | %s | %s | %d | %d / %d | %s | %s | %s | %.2f | %d | %d | %s |\n", p.Size, graph, index, w.Clients, w.Attempts, w.Successes, value(w.P50), value(w.P95), value(w.P99), w.QPS, w.Errors, w.Timeouts, state)
		}
	}
	fmt.Fprintf(&b, "\nPercentiles use successful queries only (nearest rank); p95/p99 require at least %d/%d successes. Errors include unexpected responses, but not timeouts. See JSON for setup time, load duration, and the workload identity.\n", MinimumP95, MinimumP99)
	return b.String()
}

func value(n *float64) string {
	if n == nil {
		return "—"
	}
	return fmt.Sprintf("%.3f", *n)
}

func countLabels(c *Counts) (string, string) {
	if c == nil {
		return "—", "—"
	}
	return fmt.Sprintf("%d / %d", c.Nodes, c.Edges), fmt.Sprintf("%d / %d", c.Records, c.Vectors)
}

// CompareMarkdown holds the entire experimental setup fixed and compares
// independently provisioned repetitions of one sweep. It does not pool samples
// or interpolate across missing sizes.
func CompareMarkdown(reports []Report) (string, error) {
	if len(reports) < 2 {
		return "", errors.New("compare requires at least two independent reports of the same sweep")
	}
	base := reports[0]
	seen := map[string]bool{}
	for _, r := range reports {
		if r.Version != SchemaVersion || r.RunID == "" || seen[r.RunID] || len(r.Points) != len(r.Plan.Sizes) || len(r.Points) < 2 {
			return "", errors.New("comparison requires distinct run IDs and valid v1 reports")
		}
		seen[r.RunID] = true
		if r.Corpus != base.Corpus || r.QueryMix != base.QueryMix || r.TopK != base.TopK || r.MinScore != base.MinScore || r.Warmups != base.Warmups || !reflect.DeepEqual(r.Plan, base.Plan) || !reflect.DeepEqual(r.Environment, base.Environment) || r.Environment.HarnessSHA256 == "" || r.Environment.DockerEngineSHA256 == "" || r.Environment.DockerCPUs < 1 {
			return "", errors.New("reports do not have the same verified workload and environment identity")
		}
		for i, p := range r.Points {
			if p.Size != r.Plan.Sizes[i] || len(p.Workloads) != len(Concurrencies()) {
				return "", errors.New("report does not match its size or concurrency plan")
			}
			for j, clients := range Concurrencies() {
				w := p.Workloads[j]
				if w.Clients != clients || (w.Status == Complete && (p.Counts == nil || p.Reason != "" || w.WrongHits != 0 || w.Attempts != r.Plan.Samples || w.Successes < MinimumP95 || w.P50 == nil || w.P95 == nil || *w.P50 <= 0 || *w.P95 < *w.P50)) {
					return "", errors.New("report contains inconsistent measurement data")
				}
			}
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Semantic search scaling: %s (%d independent runs)\n\nMedian of run percentiles [min–max]. Adjacent ratios use these medians, only when every repetition at both points is comparable. No performance threshold applied.\n\n", base.Plan.Sweep, len(reports))
	b.WriteString("| Target | Graph nodes / edges | Index records / vectors | Clients | Runs usable | p50 ms | p95 ms | Nodes × | Vectors × | p50 × | p95 × | Attempts | Errors | Timeouts |\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for j, clients := range Concurrencies() {
		var prev *Counts
		var prev50, prev95 *float64
		for i, size := range base.Plan.Sizes {
			var counts *Counts
			var p50, p95 []float64
			var attempts, failures, timeouts int
			for _, r := range reports {
				p, w := r.Points[i], r.Points[i].Workloads[j]
				attempts, failures, timeouts = attempts+w.Attempts, failures+w.Errors, timeouts+w.Timeouts
				if p.Counts != nil && p.Reason == "" {
					if counts != nil && *counts != *p.Counts {
						return "", errors.New("observed graph/index counts differ at the same size point")
					}
					counts = p.Counts
				}
				if w.Status == Complete {
					p50, p95 = append(p50, *w.P50), append(p95, *w.P95)
				}
			}
			graph, index := countLabels(counts)
			label50, mid50 := spread(p50)
			label95, mid95 := spread(p95)
			nodeRatio, vecRatio, r50, r95 := "—", "—", "—", "—"
			if len(p50) == len(reports) && prev != nil && prev.Nodes > 0 && prev.Vectors > 0 {
				nodeRatio = ratio(float64(counts.Nodes), float64(prev.Nodes))
				vecRatio = ratio(float64(counts.Vectors), float64(prev.Vectors))
				r50, r95 = ratio(*mid50, *prev50), ratio(*mid95, *prev95)
			}
			fmt.Fprintf(&b, "| %d | %s | %s | %d | %d / %d | %s | %s | %s | %s | %s | %s | %d | %d | %d |\n", size, graph, index, clients, len(p50), len(reports), label50, label95, nodeRatio, vecRatio, r50, r95, attempts, failures, timeouts)
			prev, prev50, prev95 = nil, nil, nil
			if len(p50) == len(reports) {
				prev, prev50, prev95 = counts, mid50, mid95
			}
		}
	}
	b.WriteString("\nAttempt, error, and timeout totals include inconclusive repetitions. See per-run reports for details. These ratios describe the measured range only; they are not an asymptotic bound.\n")
	return b.String(), nil
}

func spread(samples []float64) (string, *float64) {
	slices.Sort(samples)
	n := len(samples)
	if n == 0 {
		return "—", nil
	}
	median := (samples[(n-1)/2] + samples[n/2]) / 2
	return fmt.Sprintf("%.3f [%.3f–%.3f]", median, samples[0], samples[n-1]), &median
}

func ratio(after, before float64) string { return fmt.Sprintf("%.2f×", after/before) }
