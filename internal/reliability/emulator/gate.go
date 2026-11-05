package emulator

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

// GateState exposes only a synthetic gate identity and request sequence. It
// never includes request bodies, headers, provider responses, or credentials.
type GateState struct {
	SchemaVersion int    `json:"schemaVersion"`
	Name          string `json:"name"`
	Sequence      int    `json:"sequence"`
	State         string `json:"state"`
}

type responseGate struct {
	state   GateState
	release chan struct{}
}

func (h *OpenAIHandler) serveGate(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/control/gates/")
	if !provider.ValidEmulatorGate(name) {
		http.NotFound(w, r)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	gate := h.gates[name]
	if gate == nil {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		if gate.state.State != "waiting" {
			http.Error(w, "gate is not waiting", http.StatusConflict)
			return
		}
		gate.state.State = "released"
		close(gate.release)
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(gate.state)
}

func (h *OpenAIHandler) waitAtGate(ctx context.Context, outcome provider.ProtocolEmulatorOutcome, observation *Observation) bool {
	if outcome.Gate == "" {
		return true
	}
	h.mu.Lock()
	gate := h.gates[outcome.Gate]
	if gate == nil || gate.state.State != "idle" {
		h.mu.Unlock()
		observation.Outcome, observation.Status = "gate-timeout", http.StatusGatewayTimeout
		return false
	}
	gate.state.Sequence, gate.state.State = observation.Sequence, "waiting"
	h.mu.Unlock()
	timer := time.NewTimer(time.Duration(outcome.GateTimeoutMS) * time.Millisecond)
	defer timer.Stop()
	state := "released"
	select {
	case <-ctx.Done():
		state = "canceled"
		observation.Outcome, observation.Status, observation.Canceled = "canceled", 499, true
	case <-timer.C:
		state = "timed_out"
		observation.Outcome, observation.Status = "gate-timeout", http.StatusGatewayTimeout
	case <-gate.release:
	}
	h.mu.Lock()
	gate.state.State = state
	h.mu.Unlock()
	return state == "released"
}
