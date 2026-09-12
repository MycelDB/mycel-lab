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
	if a.Assignment.DaemonAddr == "" || a.Assignment.SpaceID == "" || a.Assignment.DomainID == "" || a.Assignment.Username == "" || a.Assignment.Password == "" {
		if a.Recorder != nil {
			_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-read-check", ActorID: a.ID, GroupName: a.GroupName, Payload: map[string]any{"ok": true, "simulated": true}})
		}
		return
	}
	cfg := mycel.Config{Addr: a.Assignment.DaemonAddr, Username: a.Assignment.Username, Password: a.Assignment.Password, ClientName: "mycel-lab"}
	if a.Assignment.AccessToken != "" {
		cfg.Username = ""
		cfg.Password = ""
		cfg.AccessToken = a.Assignment.AccessToken
		cfg.RefreshToken = a.Assignment.RefreshToken
		cfg.AccessTokenExpireTime = a.Assignment.TokenExpires
	}
	client, err := mycel.Dial(ctx, cfg)
	if err == nil {
		_, err = client.QueryGQLReadOnly(ctx, a.Assignment.SpaceID, a.Assignment.DomainID, "MATCH (n) RETURN count(n)", 10)
		_ = client.Close()
	}
	if a.Recorder != nil {
		payload := map[string]any{"ok": err == nil, "spaceId": a.Assignment.SpaceID, "domainId": a.Assignment.DomainID, "targetActorId": a.Assignment.TargetActorID}
		if err != nil {
			payload["error"] = err.Error()
			payload["class"] = ClassifyError(err)
		}
		_ = a.Recorder.RecordActorEvent(ctx, ActorEvent{Type: "graph-read-check", ActorID: a.ID, GroupName: a.GroupName, Payload: payload})
	}
}
