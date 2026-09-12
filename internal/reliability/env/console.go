package env

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
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

type ConsolePortForwardSession struct {
	cancel context.CancelFunc
	done   chan struct{}
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
		return &ConsolePortForwardSession{cancel: func() {}, done: closedChan()}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	session := &ConsolePortForwardSession{cancel: cancel, done: make(chan struct{})}
	ready := make(chan error, len(endpoints))
	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		endpoint := endpoint
		wg.Add(1)
		go func() {
			defer wg.Done()
			runPortForwardLoop(ctx, environment, endpoint, ready)
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

func runPortForwardLoop(ctx context.Context, environment Environment, endpoint ConsoleEndpoint, initialReady chan<- error) {
	reportedInitial := false
	for {
		if ctx.Err() != nil {
			return
		}
		err := runOnePortForward(ctx, environment, endpoint, func() {
			if !reportedInitial {
				reportedInitial = true
				initialReady <- nil
			}
		})
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
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func runOnePortForward(ctx context.Context, environment Environment, endpoint ConsoleEndpoint, markReady func()) error {
	args := []string{"--context", environment.Context, "-n", environment.Namespace, "port-forward", "--address", endpoint.LocalAddress, "service/" + endpoint.ServiceName, fmt.Sprintf("%d:%d", endpoint.LocalPort, endpoint.RemotePort)}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
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
