package actors

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
)

type ReaderActor struct {
	GroupName  string
	Index      int
	ID         string
	Seed       int64
	Rate       spec.RateSpec
	Assignment provision.ActorAssignment
	Recorder   EventRecorder

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewReaderActor(group spec.ResolvedActorGroup, index int, seed int64, rate spec.RateSpec, assignment provision.ActorAssignment, recorder EventRecorder) *ReaderActor {
	return &ReaderActor{GroupName: group.Name, Index: index, ID: fmt.Sprintf("%s-%d", group.Name, index), Seed: seed, Rate: rate, Assignment: assignment, Recorder: recorder}
}

func (a *ReaderActor) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.done != nil {
		a.mu.Unlock()
		return nil
	}
	child, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.done = make(chan struct{})
	a.mu.Unlock()
	if a.Recorder != nil {
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-started", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"targetActorId": a.Assignment.TargetActorID}})
	}
	go a.loop(child)
	return nil
}

func (a *ReaderActor) UpdateRate(ctx context.Context, rate spec.RateSpec) error {
	a.mu.Lock()
	a.Rate = rate
	a.mu.Unlock()
	if a.Recorder != nil {
		return a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-rate-updated", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"queriesPerSecond": rate.QueriesPerSecond}})
	}
	return nil
}

func (a *ReaderActor) Stop(ctx context.Context) error {
	a.mu.Lock()
	cancel := a.cancel
	done := a.done
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if a.Recorder != nil {
		return a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "actor-stopped", ActorID: a.ID, GroupName: a.GroupName})
	}
	return nil
}

func (a *ReaderActor) loop(ctx context.Context) {
	defer close(a.done)
	for {
		interval := a.interval()
		if interval <= 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			opCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			a.read(opCtx)
			cancel()
		}
	}
}

func (a *ReaderActor) interval() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Rate.QueriesPerSecond <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / a.Rate.QueriesPerSecond)
}

func (a *ReaderActor) read(ctx context.Context) {
	assignment := a.currentAssignment()
	if !assignmentCanUseRealClient(assignment) {
		if a.Recorder != nil {
			_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-read-check", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"ok": true, "simulated": true}})
		}
		return
	}
	erredAfterReauth := false
	reauthenticated := false
	err := a.readOnce(ctx, false)
	if shouldReauthenticate(err, a.currentAssignment()) {
		reauthenticated = true
		err = a.readOnce(ctx, true)
		erredAfterReauth = err != nil
	}
	if a.Recorder != nil {
		payload := map[string]any{"ok": err == nil, "spaceId": assignment.SpaceID, "domainId": assignment.DomainID, "targetActorId": assignment.TargetActorID}
		if reauthenticated {
			payload["reauthenticated"] = true
			payload["reauthenticationRetryFailed"] = erredAfterReauth
		}
		if err != nil {
			payload["error"] = err.Error()
			payload["class"] = ClassifyError(err)
		}
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-read-check", ActorID: a.ID, GroupName: a.GroupName, Payload: payload})
	}
}

func (a *ReaderActor) readOnce(ctx context.Context, forceLogin bool) error {
	assignment := a.currentAssignment()
	client, err := a.dialClient(ctx, forceLogin)
	if err != nil {
		return err
	}
	defer client.Close()
	defer a.captureClientTokens(client)
	_, err = client.QueryGQLReadOnly(ctx, assignment.SpaceID, assignment.DomainID, "MATCH (n) RETURN count(n)", 10)
	return err
}

func (a *ReaderActor) dialClient(ctx context.Context, forceLogin bool) (*mycel.Client, error) {
	assignment := a.currentAssignment()
	if forceLogin {
		recordReauthenticationEvent(ctx, a.Recorder, "actor-reauthentication-attempted", a.ID, a.GroupName, map[string]any{"daemonAddr": assignment.DaemonAddr, "targetActorId": assignment.TargetActorID})
	}
	client, err := mycel.Dial(ctx, clientConfigForAssignment(assignment, forceLogin))
	if err != nil {
		if forceLogin {
			recordReauthenticationEvent(ctx, a.Recorder, "actor-reauthentication-failed", a.ID, a.GroupName, map[string]any{"daemonAddr": assignment.DaemonAddr, "targetActorId": assignment.TargetActorID, "error": err.Error(), "class": ClassifyError(err)})
		}
		return nil, err
	}
	if forceLogin {
		a.captureClientTokens(client)
		recordReauthenticationEvent(ctx, a.Recorder, "actor-reauthenticated", a.ID, a.GroupName, map[string]any{"daemonAddr": assignment.DaemonAddr, "targetActorId": assignment.TargetActorID})
	}
	return client, nil
}

func (a *ReaderActor) currentAssignment() provision.ActorAssignment {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Assignment
}

func (a *ReaderActor) captureClientTokens(client *mycel.Client) {
	a.mu.Lock()
	defer a.mu.Unlock()
	captureClientTokens(&a.Assignment, client)
}
