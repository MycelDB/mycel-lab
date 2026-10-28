package env

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConsolePortForwardRefreshRequiresAcknowledgedReplacement(t *testing.T) {
	state := &endpointForwardState{endpoint: ConsoleEndpoint{NodeName: "myceld-0", DaemonAddr: "127.0.0.1:20091"}, refresh: make(chan endpointRefreshRequest)}
	session := &ConsolePortForwardSession{cancel: func() {}, done: make(chan struct{}), states: map[string]*endpointForwardState{"myceld-0": state}, notify: make(chan struct{})}
	type refreshResult struct {
		evidence EndpointRecoveryEvidence
		err      error
	}
	result := make(chan refreshResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		evidence, err := session.RefreshAndAwaitEndpoint(ctx, "myceld-0", 0)
		result <- refreshResult{evidence: evidence, err: err}
	}()
	request := <-state.refresh
	// This readiness belongs to the already-starting process and must not
	// satisfy the refresh request that has not yet been acknowledged.
	session.markEndpointReady(state)
	select {
	case got := <-result:
		t.Fatalf("pre-refresh readiness completed request: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	session.markEndpointUnavailable(state)
	request.acknowledged <- session.endpointGeneration(state)
	select {
	case got := <-result:
		t.Fatalf("acknowledged old generation completed request: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	session.markEndpointReady(state)
	got := <-result
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.evidence.RefreshGeneration != 1 || got.evidence.ReadyGeneration != 2 {
		t.Fatalf("evidence=%+v", got.evidence)
	}
}

func TestConsolePortForwardSessionRefreshesEndpointGeneration(t *testing.T) {
	binDir := t.TempDir()
	invocations := filepath.Join(t.TempDir(), "invocations")
	kubectl := filepath.Join(binDir, "kubectl")
	script := "#!/bin/sh\nprintf '%s\\n' \"$$\" >> \"$FAKE_PORT_FORWARD_INVOCATIONS\"\nprintf 'Forwarding from 127.0.0.1:20091 -> 9091\\n'\nwhile :; do sleep 60; done\n"
	if err := os.WriteFile(kubectl, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_PORT_FORWARD_INVOCATIONS", invocations)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	endpoint := ConsoleEndpoint{NodeName: "myceld-0", ServiceName: "myceld-0-client", LocalAddress: "127.0.0.1", LocalPort: 20091, RemotePort: 9091, DaemonAddr: "127.0.0.1:20091"}
	session, err := StartConsolePortForwards(ctx, Environment{Context: "k3d-test", Namespace: "default"}, []ConsoleEndpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := session.SnapshotEndpoint("myceld-0")
	if !ok || !snapshot.Ready || snapshot.Generation != 1 || snapshot.DaemonAddr != endpoint.DaemonAddr {
		t.Fatalf("initial snapshot=%+v ok=%t", snapshot, ok)
	}
	recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	evidence, err := session.RefreshAndAwaitEndpoint(recoveryCtx, "myceld-0", snapshot.Generation)
	recoveryCancel()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Status != "transport-ready" || !evidence.Reconnected || evidence.PreviousGeneration != 1 || evidence.RefreshGeneration != 1 || evidence.ReadyGeneration != 2 || evidence.DaemonAddr != endpoint.DaemonAddr {
		t.Fatalf("recovery evidence=%+v", evidence)
	}
	// A stale caller generation must still force a new process after the
	// currently ready generation, so a pre-restart reconnect cannot satisfy
	// post-restart transport readiness.
	recoveryCtx, recoveryCancel = context.WithTimeout(context.Background(), 5*time.Second)
	evidence, err = session.RefreshAndAwaitEndpoint(recoveryCtx, "myceld-0", snapshot.Generation)
	recoveryCancel()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.PreviousGeneration != 1 || evidence.RefreshGeneration != 2 || evidence.ReadyGeneration != 3 {
		t.Fatalf("stale-generation recovery evidence=%+v", evidence)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := session.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	stopCancel()
	raw, err := os.ReadFile(invocations)
	if err != nil {
		t.Fatal(err)
	}
	if count := len(strings.Fields(string(raw))); count != 3 {
		t.Fatalf("port-forward invocation count=%d, want 3: %s", count, raw)
	}
}
