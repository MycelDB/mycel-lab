package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
	"github.com/MycelDB/mycel-lab/internal/reliability/env"
)

func TestSemanticOrderingPublishesNewerBeforeReleasingOlder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release := make(chan struct{})
	started := make(chan struct{})
	var mu sync.Mutex
	newerPublished := false
	gate := func(ctx context.Context, action string) (emulator.GateState, error) {
		select {
		case <-started:
		case <-ctx.Done():
			return emulator.GateState{}, ctx.Err()
		}
		state := "waiting"
		if action == "release" {
			mu.Lock()
			published := newerPublished
			mu.Unlock()
			if !published {
				return emulator.GateState{}, errors.New("early release")
			}
			close(release)
			state = "released"
		}
		return emulator.GateState{SchemaVersion: 1, Name: "older", Sequence: 3, State: state}, nil
	}
	older := func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	newer := func(context.Context) error { mu.Lock(); defer mu.Unlock(); newerPublished = true; return nil }
	sequence, err := runSemanticOrdering(ctx, time.Millisecond, gate, older, newer)
	if err != nil || sequence != 3 {
		t.Fatalf("controlled publication order failed: %d %v", sequence, err)
	}
}
func TestSemanticOrderingFailsWithoutItsHappensBeforeWitness(t *testing.T) {
	for _, mode := range []string{"newer-fails", "escaped", "wrong-release", "never-entered"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			calls := 0
			released := false
			gate := func(_ context.Context, action string) (emulator.GateState, error) {
				calls++
				state := "waiting"
				sequence := 1
				if mode == "never-entered" {
					state = "idle"
					sequence = 0
				}
				if mode == "escaped" && calls >= 2 {
					state = "released"
				}
				if action == "release" {
					state = "released"
					released = true
					if mode == "wrong-release" {
						sequence = 2
					}
				}
				return emulator.GateState{SchemaVersion: 1, Name: "older", Sequence: sequence, State: state}, nil
			}
			older := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
			newer := func(context.Context) error {
				if mode == "newer-fails" {
					return errors.New("newer did not publish")
				}
				return nil
			}
			if _, err := runSemanticOrdering(ctx, time.Millisecond, gate, older, newer); err == nil {
				t.Fatal("false ordering proof passed")
			}
			if (mode == "newer-fails" || mode == "escaped" || mode == "never-entered") && released {
				t.Fatal("unsafe boundary was released")
			}
		})
	}
}

type gateReply struct{ raw string }

func (g gateReply) ProviderEmulatorGate(context.Context, env.Environment, string, string) (env.ExecResult, error) {
	return env.ExecResult{Stdout: g.raw}, nil
}
func TestProviderGateEvidenceIsStrictVersionedAndCanonical(t *testing.T) {
	valid, _ := json.Marshal(emulator.GateState{SchemaVersion: 1, Name: "older", Sequence: 1, State: "waiting"})
	if _, err := controlledProviderGate(gateReply{string(valid)}, env.Environment{}, "older")(context.Background(), "get"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{}`, `{"schemaVersion":2,"name":"older","sequence":1,"state":"waiting"}`, `{"schemaVersion":1,"name":"older","sequence":1,"state":"waiting","rawBody":"private"}`, `{"schemaVersion":1,"schemaVersion":1,"name":"older","sequence":1,"state":"waiting"}`, string(valid) + ` {}`} {
		if _, err := controlledProviderGate(gateReply{raw}, env.Environment{}, "older")(context.Background(), "get"); err == nil {
			t.Fatal("invalid gate evidence accepted")
		}
	}
}
