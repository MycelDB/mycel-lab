package metrics

import (
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/store"
)

type Sample struct {
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

type Summary struct {
	RunID                     string  `json:"runId,omitempty"`
	CommitsPerSecond          float64 `json:"commitsPerSecond"`
	TransactionLatencyP50MS   float64 `json:"transactionLatencyP50Ms"`
	TransactionLatencyP95MS   float64 `json:"transactionLatencyP95Ms"`
	TransactionLatencyP99MS   float64 `json:"transactionLatencyP99Ms"`
	ReadCheckLatencyP50MS     float64 `json:"readCheckLatencyP50Ms"`
	ReadCheckLatencyP95MS     float64 `json:"readCheckLatencyP95Ms"`
	ReadCheckLatencyP99MS     float64 `json:"readCheckLatencyP99Ms"`
	TransientErrors           int     `json:"transientErrors"`
	PermanentErrors           int     `json:"permanentErrors"`
	ExpectedDegradationEvents int     `json:"expectedDegradationEvents"`
	PodRestartEvents          int     `json:"podRestartEvents"`
	LeaderChanges             int     `json:"leaderChanges"`
}

type Comparison struct {
	A     Summary `json:"a"`
	B     Summary `json:"b"`
	Delta Summary `json:"delta"`
}

func Aggregate(samples []Sample, duration time.Duration) Summary {
	var summary Summary
	var txLatencies, readLatencies []float64
	var commits float64
	for _, sample := range samples {
		switch sample.Name {
		case "commits":
			commits += sample.Value
		case "transaction_latency_ms":
			txLatencies = append(txLatencies, sample.Value)
		case "read_check_latency_ms":
			readLatencies = append(readLatencies, sample.Value)
		case "errors":
			switch sample.Labels["class"] {
			case "transient", "unavailable":
				summary.TransientErrors += int(sample.Value)
			case "permanent":
				summary.PermanentErrors += int(sample.Value)
			}
		case "expected_degradation":
			summary.ExpectedDegradationEvents += int(sample.Value)
		case "pod_restarts":
			summary.PodRestartEvents += int(sample.Value)
		case "leader_changes":
			summary.LeaderChanges += int(sample.Value)
		}
	}
	if duration > 0 {
		summary.CommitsPerSecond = round(commits / duration.Seconds())
	}
	summary.TransactionLatencyP50MS = percentile(txLatencies, 0.50)
	summary.TransactionLatencyP95MS = percentile(txLatencies, 0.95)
	summary.TransactionLatencyP99MS = percentile(txLatencies, 0.99)
	summary.ReadCheckLatencyP50MS = percentile(readLatencies, 0.50)
	summary.ReadCheckLatencyP95MS = percentile(readLatencies, 0.95)
	summary.ReadCheckLatencyP99MS = percentile(readLatencies, 0.99)
	return summary
}

func FromRunDetails(details store.RunDetails) Summary {
	samples := make([]Sample, 0, len(details.Metrics))
	for _, metric := range details.Metrics {
		labels := map[string]string{}
		if len(metric.Labels) != 0 {
			_ = json.Unmarshal(metric.Labels, &labels)
		}
		samples = append(samples, Sample{Name: metric.Name, Value: metric.Value, Labels: labels})
	}
	duration := time.Duration(0)
	if details.Run.FinishedAt != nil {
		duration = details.Run.FinishedAt.Sub(details.Run.StartedAt)
	}
	summary := Aggregate(samples, duration)
	summary.RunID = details.Run.ID
	return summary
}

func Compare(a, b Summary) Comparison {
	return Comparison{A: a, B: b, Delta: Summary{CommitsPerSecond: round(b.CommitsPerSecond - a.CommitsPerSecond), TransactionLatencyP50MS: round(b.TransactionLatencyP50MS - a.TransactionLatencyP50MS), TransactionLatencyP95MS: round(b.TransactionLatencyP95MS - a.TransactionLatencyP95MS), TransactionLatencyP99MS: round(b.TransactionLatencyP99MS - a.TransactionLatencyP99MS), ReadCheckLatencyP50MS: round(b.ReadCheckLatencyP50MS - a.ReadCheckLatencyP50MS), ReadCheckLatencyP95MS: round(b.ReadCheckLatencyP95MS - a.ReadCheckLatencyP95MS), ReadCheckLatencyP99MS: round(b.ReadCheckLatencyP99MS - a.ReadCheckLatencyP99MS), TransientErrors: b.TransientErrors - a.TransientErrors, PermanentErrors: b.PermanentErrors - a.PermanentErrors, ExpectedDegradationEvents: b.ExpectedDegradationEvents - a.ExpectedDegradationEvents, PodRestartEvents: b.PodRestartEvents - a.PodRestartEvents, LeaderChanges: b.LeaderChanges - a.LeaderChanges}}
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return round(sorted[0])
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return round(sorted[idx])
}

func round(value float64) float64 {
	return math.Round(value*1000) / 1000
}
