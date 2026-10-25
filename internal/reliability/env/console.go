package env

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/executil"
)

const (
	DefaultConsolePortBase = 19091
	DefaultDaemonGRPCPort  = 9091
)

type ConsoleEndpoint struct {
	NodeName     string `json:"nodeName"`
	ServiceName  string `json:"serviceName"`
	LocalAddress string `json:"localAddress"`
	LocalPort    int    `json:"localPort"`
	RemotePort   int    `json:"remotePort"`
	DaemonAddr   string `json:"daemonAddr"`
}

type EndpointForwardSnapshot struct {
	NodeName   string `json:"nodeName"`
	DaemonAddr string `json:"daemonAddr"`
	Generation uint64 `json:"generation"`
	Ready      bool   `json:"ready"`
}

type EndpointRecoveryEvidence struct {
	NodeName           string `json:"nodeName"`
	DaemonAddr         string `json:"daemonAddr"`
	PreviousGeneration uint64 `json:"previousGeneration"`
	RefreshGeneration  uint64 `json:"refreshGeneration"`
	ReadyGeneration    uint64 `json:"readyGeneration"`
	Reconnected        bool   `json:"reconnected"`
	Status             string `json:"status"`
}

type EndpointRecovery interface {
	SnapshotEndpoint(nodeName string) (EndpointForwardSnapshot, bool)
	SnapshotEndpoints() []EndpointForwardSnapshot
	RefreshAndAwaitEndpoint(ctx context.Context, nodeName string, afterGeneration uint64) (EndpointRecoveryEvidence, error)
}

type endpointRefreshRequest struct {
	acknowledged chan uint64
}

type endpointForwardState struct {
	endpoint   ConsoleEndpoint
	refresh    chan endpointRefreshRequest
	refreshMu  sync.Mutex
	generation uint64
	ready      bool
}

type ConsolePortForwardSession struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	states map[string]*endpointForwardState
	notify chan struct{}
}

func ConsoleEndpointsForNodeCount(nodeCount, portBase int) []ConsoleEndpoint {
	if portBase <= 0 {
		portBase = DefaultConsolePortBase
	}
	endpoints := make([]ConsoleEndpoint, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeName := fmt.Sprintf("myceld-%d", i)
		localPort := portBase + i
		endpoints = append(endpoints, ConsoleEndpoint{
			NodeName:     nodeName,
			ServiceName:  nodeName + "-client",
			LocalAddress: "127.0.0.1",
			LocalPort:    localPort,
			RemotePort:   DefaultDaemonGRPCPort,
			DaemonAddr:   fmt.Sprintf("127.0.0.1:%d", localPort),
		})
	}
	return endpoints
}

func StartConsolePortForwards(ctx context.Context, environment Environment, endpoints []ConsoleEndpoint) (*ConsolePortForwardSession, error) {
	if len(endpoints) == 0 {
		return &ConsolePortForwardSession{cancel: func() {}, done: closedChan(), states: map[string]*endpointForwardState{}, notify: make(chan struct{})}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	session := &ConsolePortForwardSession{cancel: cancel, done: make(chan struct{}), states: map[string]*endpointForwardState{}, notify: make(chan struct{})}
	ready := make(chan error, len(endpoints))
	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		endpoint := endpoint
		state := &endpointForwardState{endpoint: endpoint, refresh: make(chan endpointRefreshRequest)}
		session.states[endpoint.NodeName] = state
		wg.Add(1)
		go func() {
			defer wg.Done()
			runPortForwardLoop(ctx, environment, session, state, ready)
		}()
	}
	go func() {
		wg.Wait()
		close(session.done)
	}()

	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()
	for range endpoints {
		select {
		case err := <-ready:
			if err != nil {
				cancel()
				<-session.done
				return nil, err
			}
		case <-timeout.C:
			cancel()
			<-session.done
			return nil, fmt.Errorf("timed out waiting for console port-forwards to become ready")
		case <-ctx.Done():
			cancel()
			<-session.done
			return nil, ctx.Err()
		}
	}
	return session, nil
}

func (s *ConsolePortForwardSession) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.cancel()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ConsolePortForwardSession) SnapshotEndpoint(nodeName string) (EndpointForwardSnapshot, bool) {
	if s == nil {
		return EndpointForwardSnapshot{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[nodeName]
	if !ok {
		return EndpointForwardSnapshot{}, false
	}
	return EndpointForwardSnapshot{NodeName: nodeName, DaemonAddr: state.endpoint.DaemonAddr, Generation: state.generation, Ready: state.ready}, true
}

func (s *ConsolePortForwardSession) SnapshotEndpoints() []EndpointForwardSnapshot {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.states))
	for name := range s.states {
		names = append(names, name)
	}
	sort.Strings(names)
	snapshots := make([]EndpointForwardSnapshot, 0, len(names))
	for _, name := range names {
		state := s.states[name]
		snapshots = append(snapshots, EndpointForwardSnapshot{NodeName: name, DaemonAddr: state.endpoint.DaemonAddr, Generation: state.generation, Ready: state.ready})
	}
	return snapshots
}

func (s *ConsolePortForwardSession) RefreshAndAwaitEndpoint(ctx context.Context, nodeName string, afterGeneration uint64) (EndpointRecoveryEvidence, error) {
	if s == nil {
		return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint recovery session is unavailable")
	}
	s.mu.Lock()
	state, ok := s.states[nodeName]
	s.mu.Unlock()
	if !ok {
		return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint recovery target is unavailable")
	}
	state.refreshMu.Lock()
	defer state.refreshMu.Unlock()
	request := endpointRefreshRequest{acknowledged: make(chan uint64, 1)}
	select {
	case state.refresh <- request:
	case <-ctx.Done():
		return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint transport did not become ready")
	case <-s.done:
		return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint recovery session stopped")
	}
	var refreshGeneration uint64
	select {
	case refreshGeneration = <-request.acknowledged:
	case <-ctx.Done():
		return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint transport did not become ready")
	case <-s.done:
		return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint recovery session stopped")
	}
	if refreshGeneration < afterGeneration {
		refreshGeneration = afterGeneration
	}
	s.mu.Lock()
	current, ok := s.states[nodeName]
	if !ok || current != state {
		s.mu.Unlock()
		return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint recovery target is unavailable")
	}
	for {
		if state.ready && state.generation > refreshGeneration {
			evidence := EndpointRecoveryEvidence{NodeName: nodeName, DaemonAddr: state.endpoint.DaemonAddr, PreviousGeneration: afterGeneration, RefreshGeneration: refreshGeneration, ReadyGeneration: state.generation, Reconnected: true, Status: "transport-ready"}
			s.mu.Unlock()
			return evidence, nil
		}
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint transport did not become ready")
		case <-s.done:
			return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint recovery session stopped")
		case <-notify:
		}
		s.mu.Lock()
		state, ok = s.states[nodeName]
		if !ok {
			s.mu.Unlock()
			return EndpointRecoveryEvidence{}, fmt.Errorf("endpoint recovery target is unavailable")
		}
	}
}

func (s *ConsolePortForwardSession) markEndpointReady(state *endpointForwardState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.generation++
	state.ready = true
	s.signalEndpointChangeLocked()
}

func (s *ConsolePortForwardSession) markEndpointUnavailable(state *endpointForwardState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.ready {
		state.ready = false
		s.signalEndpointChangeLocked()
	}
}

func (s *ConsolePortForwardSession) endpointGeneration(state *endpointForwardState) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return state.generation
}

func (s *ConsolePortForwardSession) signalEndpointChangeLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func runPortForwardLoop(ctx context.Context, environment Environment, session *ConsolePortForwardSession, state *endpointForwardState, initialReady chan<- error) {
	reportedInitial := false
	for {
		if ctx.Err() != nil {
			return
		}
		runCtx, cancelRun := context.WithCancel(ctx)
		result := make(chan error, 1)
		go func() {
			result <- runOnePortForward(runCtx, environment, state.endpoint, func() {
				session.markEndpointReady(state)
				if !reportedInitial {
					reportedInitial = true
					initialReady <- nil
				}
			})
		}()
		var refreshRequest *endpointRefreshRequest
		var err error
		select {
		case err = <-result:
		case request := <-state.refresh:
			refreshRequest = &request
			cancelRun()
			err = <-result
		case <-ctx.Done():
			cancelRun()
			<-result
			return
		}
		cancelRun()
		session.markEndpointUnavailable(state)
		if refreshRequest != nil {
			refreshRequest.acknowledged <- session.endpointGeneration(state)
		}
		if !reportedInitial {
			if ctx.Err() != nil {
				return
			}
			reportedInitial = true
			initialReady <- err
		}
		if ctx.Err() != nil {
			return
		}
		if refreshRequest != nil {
			continue
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case request := <-state.refresh:
			timer.Stop()
			request.acknowledged <- session.endpointGeneration(state)
			continue
		case <-timer.C:
		}
	}
}

func runOnePortForward(ctx context.Context, environment Environment, endpoint ConsoleEndpoint, markReady func()) error {
	args := []string{"--context", environment.Context, "-n", environment.Namespace, "port-forward", "--address", endpoint.LocalAddress, "service/" + endpoint.ServiceName, fmt.Sprintf("%d:%d", endpoint.LocalPort, endpoint.RemotePort)}
	cmd := executil.CommandContext(ctx, "kubectl", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	ready := make(chan struct{}, 1)
	var output strings.Builder
	var outputMu sync.Mutex
	if err := cmd.Start(); err != nil {
		return err
	}
	scan := func(scanner *bufio.Scanner) {
		for scanner.Scan() {
			line := scanner.Text()
			outputMu.Lock()
			output.WriteString(line)
			output.WriteByte('\n')
			outputMu.Unlock()
			if strings.Contains(line, "Forwarding from") {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
		}
	}
	go scan(bufio.NewScanner(stdout))
	go scan(bufio.NewScanner(stderr))
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	select {
	case <-ready:
		markReady()
		err := <-wait
		if ctx.Err() != nil {
			return nil
		}
		return err
	case err := <-wait:
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("port-forward exited before becoming ready")
		}
		outputMu.Lock()
		capturedOutput := output.String()
		outputMu.Unlock()
		return fmt.Errorf("%s: %w\n%s", executil.ShellCommand("kubectl", args...), err, capturedOutput)
	case <-ctx.Done():
		<-wait
		return nil
	}
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
