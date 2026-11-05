package emulator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

func gateStatus(t *testing.T, server string, name string) GateState {
	t.Helper()
	response, err := http.Get(server + "/control/gates/" + name)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var state GateState
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&state) != nil {
		t.Fatal("invalid control response")
	}
	return state
}
func waitGate(t *testing.T, server, name, want string) GateState {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state := gateStatus(t, server, name)
		if state.State == want {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("gate did not reach required state")
	return GateState{}
}
func TestControlledGateProvesNewerResponseBeforeOlderCompletion(t *testing.T) {
	h := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", MaxRequests: 2, RequestSequence: []provider.ProtocolEmulatorOutcome{{Kind: "success", Gate: "older-response", GateTimeoutMS: 2000}, {Kind: "success"}}})
	server := httptest.NewServer(h)
	defer server.Close()
	early, _ := http.Post(server.URL+"/control/gates/older-response", "application/json", nil)
	if early.StatusCode != 409 {
		t.Fatal("unentered gate could be released")
	}
	early.Body.Close()
	old := make(chan error, 1)
	go func() {
		response, err := http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(`{"model":"embedding-v1","input":"axis-0"}`))
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		old <- err
	}()
	state := waitGate(t, server.URL, "older-response", "waiting")
	if state.SchemaVersion != 1 || state.Sequence != 1 {
		t.Fatal("missing older request witness")
	}
	newer, err := http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(`{"model":"embedding-v1","input":"axis-2"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, newer.Body)
	newer.Body.Close()
	select {
	case <-old:
		t.Fatal("older request escaped before explicit release")
	default:
	}
	observations := h.observationSnapshot()
	if len(observations) != 1 || observations[0].Sequence != 2 {
		t.Fatal("newer request was not completed while older was held")
	}
	release, err := http.Post(server.URL+"/control/gates/older-response", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	release.Body.Close()
	if release.StatusCode != 200 {
		t.Fatal("release failed")
	}
	if err := <-old; err != nil {
		t.Fatal(err)
	}
	observations = h.observationSnapshot()
	if len(observations) != 2 || observations[0].Sequence != 2 || observations[1].Sequence != 1 {
		t.Fatal("out-of-order completion was not real")
	}
	raw, _ := json.Marshal(observations)
	if strings.Contains(string(raw), "axis-") {
		t.Fatal("provider inputs leaked")
	}
}
func TestGateCancellationAndDeadlineAreBounded(t *testing.T) {
	for _, cancelRequest := range []bool{true, false} {
		t.Run(map[bool]string{true: "cancel", false: "deadline"}[cancelRequest], func(t *testing.T) {
			timeout := 30
			if cancelRequest {
				timeout = 2000
			}
			h := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", MaxRequests: 1, RequestSequence: []provider.ProtocolEmulatorOutcome{{Kind: "success", Gate: "response", GateTimeoutMS: timeout}}})
			server := httptest.NewServer(h)
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/embeddings", strings.NewReader(`{"model":"embedding-v1","input":"axis-0"}`))
				response, err := http.DefaultClient.Do(request)
				if err == nil {
					response.Body.Close()
				}
			}()
			if cancelRequest {
				waitGate(t, server.URL, "response", "waiting")
				cancel()
				waitGate(t, server.URL, "response", "canceled")
			} else {
				waitGate(t, server.URL, "response", "timed_out")
			}
			<-done
			observations := h.observationSnapshot()
			if len(observations) != 1 {
				t.Fatal("lost interrupted request")
			}
			want := "gate-timeout"
			if cancelRequest {
				want = "canceled"
			}
			if observations[0].Outcome != want {
				t.Fatalf("unexpected interruption: %+v", observations)
			}
		})
	}
}
func TestConnectionDropIsRealTransportFailure(t *testing.T) {
	h := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", MaxRequests: 1, RequestSequence: []provider.ProtocolEmulatorOutcome{{Kind: "connection-drop"}}})
	server := httptest.NewServer(h)
	defer server.Close()
	response, err := http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(`{"model":"embedding-v1","input":"axis-0"}`))
	if response != nil {
		response.Body.Close()
	}
	if err == nil {
		t.Fatal("connection drop became an ordinary HTTP response")
	}
	observations := h.observationSnapshot()
	if len(observations) != 1 || observations[0].Outcome != "connection-drop" {
		t.Fatal("transport evidence missing")
	}
}
