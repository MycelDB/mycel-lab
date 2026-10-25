package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

type recordingEndpointRecovery struct {
	driver       *recordingDriver
	snapshotNode string
	refreshNode  string
	after        uint64
}

func (r *recordingEndpointRecovery) SnapshotEndpoint(nodeName string) (env.EndpointForwardSnapshot, bool) {
	r.snapshotNode = nodeName
	return env.EndpointForwardSnapshot{NodeName: nodeName, DaemonAddr: "127.0.0.1:20091", Generation: 4, Ready: true}, true
}

func (r *recordingEndpointRecovery) RefreshAndAwaitEndpoint(_ context.Context, nodeName string, afterGeneration uint64) (env.EndpointRecoveryEvidence, error) {
	if len(r.driver.restarted) != 1 {
		return env.EndpointRecoveryEvidence{}, errors.New("endpoint recovery ran before node restart")
	}
	r.refreshNode = nodeName
	r.after = afterGeneration
	return env.EndpointRecoveryEvidence{NodeName: nodeName, DaemonAddr: "127.0.0.1:20091", PreviousGeneration: afterGeneration, RefreshGeneration: afterGeneration, ReadyGeneration: afterGeneration + 1, Reconnected: true, Status: "transport-ready"}, nil
}

func TestDriverRuntimeRestartsEndpointAndProbesExternalGRPCBeforeCompleting(t *testing.T) {
	driver := &recordingDriver{}
	recovery := &recordingEndpointRecovery{driver: driver}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(recovery)
	probeAddr := ""
	runtime.endpointProbe = func(_ context.Context, _ *provision.ScenarioResources, daemonAddr string) (int, error) {
		probeAddr = daemonAddr
		return 2, nil
	}
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "node-restart", Target: map[string]any{"ordinal": 0}})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := payload["endpointRecovery"].(endpointRecoveryResult)
	if !ok {
		t.Fatalf("endpoint recovery payload=%+v", payload)
	}
	if recovery.snapshotNode != "myceld-0" || recovery.refreshNode != "myceld-0" || recovery.after != 4 || probeAddr != "127.0.0.1:20091" {
		t.Fatalf("recovery=%+v probeAddr=%q", recovery, probeAddr)
	}
	if result.TransportStatus != "transport-ready" || result.GRPCProbeStatus != "completed" || result.PreviousGeneration != 4 || result.RefreshGeneration != 4 || result.ReadyGeneration != 5 || result.ProbeAttempts != 2 || !result.Reconnected {
		t.Fatalf("endpoint recovery result=%+v", result)
	}
}

func TestDriverRuntimePersistsClosedEndpointRecoveryEvidence(t *testing.T) {
	driver := &recordingDriver{}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(&recordingEndpointRecovery{driver: driver})
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) { return 1, nil }
	recorder := &recordingEventRecorder{}
	phase := spec.PhaseSpec{Name: "restart", Events: []spec.EventSpec{{Type: "node-restart", Target: map[string]any{"ordinal": 0}}}}
	if err := runtime.ExecutePhaseEvents(context.Background(), phase, false, recorder); err != nil {
		t.Fatal(err)
	}
	if len(recorder.records) != 2 || recorder.records[1].Type != "event-completed" {
		t.Fatalf("records=%+v", recorder.records)
	}
	encoded, err := json.Marshal(recorder.records[1].Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"nodeName":"myceld-0"`, `"previousGeneration":4`, `"refreshGeneration":4`, `"readyGeneration":5`, `"transportStatus":"transport-ready"`, `"grpcProbeStatus":"completed"`, `"probeAttempts":1`} {
		if !bytes.Contains(encoded, []byte(required)) {
			t.Fatalf("endpoint recovery evidence missing %s: %s", required, encoded)
		}
	}
	for _, prohibited := range []string{"AccessToken", "RefreshToken", "raw probe failure"} {
		if bytes.Contains(encoded, []byte(prohibited)) {
			t.Fatalf("endpoint recovery evidence leaked %q: %s", prohibited, encoded)
		}
	}
}

func TestDriverRuntimeFailsClosedWhenExternalGRPCProbeFails(t *testing.T) {
	driver := &recordingDriver{}
	runtime := NewDriverRuntimeWithResources(driver, env.Environment{Name: "test", Driver: "k3d"}, &provision.ScenarioResources{})
	runtime.SetEndpointRecovery(&recordingEndpointRecovery{driver: driver})
	runtime.endpointProbe = func(context.Context, *provision.ScenarioResources, string) (int, error) {
		return 3, errors.New("raw probe failure")
	}
	payload, err := runtime.applyEvent(context.Background(), spec.EventSpec{Type: "node-restart", Target: map[string]any{"ordinal": 0}})
	if err == nil || err.Error() != "external endpoint did not become ready" {
		t.Fatalf("applyEvent() error=%v", err)
	}
	result := payload["endpointRecovery"].(endpointRecoveryResult)
	if result.GRPCProbeStatus != "failed" || result.ProbeAttempts != 3 {
		t.Fatalf("endpoint recovery result=%+v", result)
	}
}
