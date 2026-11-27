package events

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/semanticbench"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
)

func TestScalingFixtureCandidateCounts(t *testing.T) {
	queries := scalingQueries(&provision.SemanticResources{})
	for _, indexed := range []int{40, 43, 1000, 10000, 30000} {
		var counts [spec.ScalingAxes]int
		for i := 0; i < indexed+64; i++ {
			node := scalingNode(i, indexed)
			if i >= indexed {
				if node.GetLabels()[0] != scalingOtherLabel {
					t.Fatal("graph-only growth includes an eligible node")
				}
				continue
			}
			if node.GetLabels()[0] != scalingIndexLabel {
				t.Fatal("indexed growth includes an excluded node")
			}
			for axis, request := range queries {
				if strings.Contains(node.GetPayload().GetFields()["text"].GetStringValue(), request.GetQuery()) {
					counts[axis]++
				}
			}
		}
		for axis, want := range counts {
			if got := scalingCandidateCount(indexed, axis); got != want || got >= 10000 {
				t.Fatalf("size %d axis %d: candidates %d want %d (and below the API limit)", indexed, axis, got, want)
			}
		}
	}
}

func TestScalingProbeRequiresExactResponse(t *testing.T) {
	requests := scalingQueries(&provision.SemanticResources{SpaceID: "space", DomainID: "domain", SemanticRuleID: "rule", EmbeddingBindingKey: "binding"})
	response := func() *clientv1.SemanticSearchResponse {
		r := &clientv1.SemanticSearchResponse{ExaminedSemanticCandidates: 250, EligibleSemanticCandidates: 250, NextPageToken: "opaque"}
		for i := 0; i < spec.ScalingTopK; i++ {
			fixture := scalingNode(i, 1000)
			r.Results = append(r.Results, &clientv1.SemanticSearchResult{NodeId: fixture.GetNodeId(), SemanticRuleId: "rule", EmbeddingBindingKey: "binding", Score: 1, RecordId: "record", Node: &clientv1.Node{NodeId: fixture.GetNodeId(), DomainId: "domain", Payload: fixture.GetPayload()}, Provenance: &clientv1.SemanticResultProvenance{Dimensions: 8, SemanticGeneration: 2}})
		}
		return r
	}
	if !validScalingHits(response(), requests[0], 0, 2, 250) {
		t.Fatal("expected response was rejected")
	}
	for name, corrupt := range map[string]func(*clientv1.SemanticSearchResponse){
		"warning":           func(r *clientv1.SemanticSearchResponse) { r.Warnings = []string{"index unavailable"} },
		"wrong order":       func(r *clientv1.SemanticSearchResponse) { r.Results[0], r.Results[1] = r.Results[1], r.Results[0] },
		"missing candidate": func(r *clientv1.SemanticSearchResponse) { r.ExaminedSemanticCandidates-- },
		"short page":        func(r *clientv1.SemanticSearchResponse) { r.Results = r.Results[:1] },
		"wrong domain":      func(r *clientv1.SemanticSearchResponse) { r.Results[0].Node.DomainId = "other" },
		"stale generation":  func(r *clientv1.SemanticSearchResponse) { r.Results[0].Provenance.SemanticGeneration = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			r := response()
			corrupt(r)
			if validScalingHits(r, requests[0], 0, 2, 250) {
				t.Fatal("invalid or incomplete response counted as successful")
			}
		})
	}
}

func TestScalingLoadBalancesQueriesAndExcludesWarmup(t *testing.T) {
	var counts [8][spec.ScalingAxes]atomic.Int32
	w := measureScalingWorkload(context.Background(), 8, 128, time.Second, func(ctx context.Context, client, axis int) semanticbench.Sample {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("no request deadline")
		}
		if counts[client][axis].Add(1) <= 2 {
			return semanticbench.Sample{Elapsed: time.Hour}
		}
		return semanticbench.Sample{Elapsed: time.Millisecond}
	})
	if w.Status != semanticbench.Complete || w.Attempts != 128 || w.P95 == nil || *w.P95 != 1 {
		t.Fatalf("incorrect timed sample accounting: %+v", w)
	}
	for client := range counts {
		for axis := range counts[client] {
			if n := counts[client][axis].Load(); n != 6 { // Two warmups plus four timed queries.
				t.Fatalf("client %d query %d: %d repetitions; want six", client, axis, n)
			}
		}
	}
}

func TestScalingWarmupFailureDoesNotProduceTimedResults(t *testing.T) {
	w := measureScalingWorkload(context.Background(), 1, 128, time.Second, func(context.Context, int, int) semanticbench.Sample {
		return semanticbench.Sample{Outcome: semanticbench.UnexpectedResponse}
	})
	if w.Status != semanticbench.Inconclusive || w.Reason != "warmup-response-unverified" || w.Attempts != 0 || w.P50 != nil {
		t.Fatalf("invalid warmup opened the timed window: %+v", w)
	}
}

func TestScalingInterruptedLoadKeepsPartialSamples(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	w := measureScalingWorkload(ctx, 1, 128, time.Second, func(context.Context, int, int) semanticbench.Sample {
		calls++
		if calls == semanticbench.WarmupsPerClient+3 {
			cancel()
		}
		return semanticbench.Sample{Elapsed: time.Millisecond}
	})
	if w.Status != semanticbench.Inconclusive || w.Reason != "measurement-budget-expired" || w.Attempts != 3 || w.Successes != 3 || w.P50 == nil {
		t.Fatalf("interrupted workload discarded partial results: %+v", w)
	}
}
