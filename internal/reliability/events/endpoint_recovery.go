package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type endpointRecoveryResult struct {
	NodeName           string `json:"nodeName"`
	PreviousGeneration uint64 `json:"previousGeneration"`
	RefreshGeneration  uint64 `json:"refreshGeneration"`
	ReadyGeneration    uint64 `json:"readyGeneration"`
	Reconnected        bool   `json:"reconnected"`
	TransportStatus    string `json:"transportStatus"`
	GRPCProbeStatus    string `json:"grpcProbeStatus"`
	ProbeAttempts      int    `json:"probeAttempts"`
}

func (r *DriverRuntime) restartNodeAndRecoverEndpoint(ctx context.Context, target map[string]any) (map[string]any, error) {
	node := nodeName(target)
	ordinal := nodeOrdinal(target)
	if node == "" && ordinal >= 0 {
		node = fmt.Sprintf("myceld-%d", ordinal)
	}
	var snapshot env.EndpointForwardSnapshot
	managed := false
	if r.EndpointRecovery != nil && node != "" {
		snapshot, managed = r.EndpointRecovery.SnapshotEndpoint(node)
	}
	if err := r.Driver.RestartNode(ctx, r.Environment, env.NodeRef{Name: node, Ordinal: ordinal}); err != nil {
		return nil, err
	}
	if !managed {
		return nil, nil
	}
	recoveryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := r.recoverEndpoint(recoveryCtx, snapshot)
	return map[string]any{"endpointRecovery": result}, err
}

func (r *DriverRuntime) rollingRestartAndRecoverEndpoints(ctx context.Context) (map[string]any, error) {
	snapshots := r.managedEndpointSnapshots()
	if err := r.Driver.RollingRestart(ctx, r.Environment); err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	recoveries, err := r.recoverEndpoints(ctx, snapshots)
	return map[string]any{"endpointRecoveries": recoveries}, err
}

func (r *DriverRuntime) managedEndpointSnapshots() []env.EndpointForwardSnapshot {
	if r.EndpointRecovery == nil {
		return nil
	}
	return r.EndpointRecovery.SnapshotEndpoints()
}

func (r *DriverRuntime) recoverNodeFromSnapshots(ctx context.Context, snapshots []env.EndpointForwardSnapshot, nodeName string) (endpointRecoveryResult, bool, error) {
	for _, snapshot := range snapshots {
		if snapshot.NodeName != nodeName {
			continue
		}
		recoveryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		result, err := r.recoverEndpoint(recoveryCtx, snapshot)
		return result, true, err
	}
	return endpointRecoveryResult{}, false, nil
}

func (r *DriverRuntime) recoverEndpoints(ctx context.Context, snapshots []env.EndpointForwardSnapshot) ([]endpointRecoveryResult, error) {
	recoveryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	results := make([]endpointRecoveryResult, 0, len(snapshots))
	for _, snapshot := range snapshots {
		result, err := r.recoverEndpoint(recoveryCtx, snapshot)
		results = append(results, result)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

func (r *DriverRuntime) recoverEndpoint(ctx context.Context, snapshot env.EndpointForwardSnapshot) (endpointRecoveryResult, error) {
	failed := endpointRecoveryResult{NodeName: snapshot.NodeName, PreviousGeneration: snapshot.Generation, TransportStatus: "failed", GRPCProbeStatus: "not-started"}
	transport, err := r.EndpointRecovery.RefreshAndAwaitEndpoint(ctx, snapshot.NodeName, snapshot.Generation)
	if err != nil {
		return failed, err
	}
	probe := r.endpointProbe
	if probe == nil {
		probe = probeExternalGraphEndpoint
	}
	attempts, err := probe(ctx, r.Resources, transport.DaemonAddr)
	result := endpointRecoveryResult{
		NodeName:           snapshot.NodeName,
		PreviousGeneration: snapshot.Generation,
		RefreshGeneration:  transport.RefreshGeneration,
		ReadyGeneration:    transport.ReadyGeneration,
		Reconnected:        transport.Reconnected,
		TransportStatus:    transport.Status,
		GRPCProbeStatus:    "completed",
		ProbeAttempts:      attempts,
	}
	if err != nil {
		result.GRPCProbeStatus = "failed"
		return result, errors.New("external endpoint did not become ready")
	}
	return result, nil
}

func probeExternalGraphEndpoint(ctx context.Context, resources *provision.ScenarioResources, daemonAddr string) (int, error) {
	assignment, ok := endpointProbeAssignment(resources, daemonAddr)
	if !ok {
		return 0, errors.New("external endpoint probe assignment is unavailable")
	}
	attempts := 0
	for {
		if err := ctx.Err(); err != nil {
			return attempts, err
		}
		attempts++
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		client, err := mycel.Dial(attemptCtx, mycel.Config{Addr: daemonAddr, AccessToken: assignment.AccessToken, ClientName: "mycel-lab-endpoint-readiness"})
		if err == nil {
			callCtx, callCancel := client.AuthCallContext(attemptCtx)
			_, err = client.Space.GetSpace(callCtx, &clientv1.GetSpaceRequest{SpaceId: assignment.SpaceID}, grpc.WaitForReady(true))
			callCancel()
			_ = client.Close()
		}
		cancel()
		if err == nil {
			return attempts, nil
		}
		code := status.Code(err)
		if code != codes.Unavailable && code != codes.DeadlineExceeded && code != codes.Canceled {
			return attempts, errors.New("external endpoint readiness probe was rejected")
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempts, ctx.Err()
		case <-timer.C:
		}
	}
}

func endpointProbeAssignment(resources *provision.ScenarioResources, daemonAddr string) (provision.ActorAssignment, bool) {
	if resources == nil || strings.TrimSpace(daemonAddr) == "" {
		return provision.ActorAssignment{}, false
	}
	var fallback provision.ActorAssignment
	for _, assignment := range resources.Assignments {
		if assignment.AccessToken == "" || assignment.SpaceID == "" {
			continue
		}
		if fallback.AccessToken == "" {
			fallback = assignment
		}
		if assignment.DaemonAddr == daemonAddr {
			return assignment, true
		}
	}
	return fallback, fallback.AccessToken != ""
}
