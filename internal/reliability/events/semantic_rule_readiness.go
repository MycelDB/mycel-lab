package events

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	semanticRuleReadinessTimeout  = 5 * time.Second
	semanticRuleReadinessInterval = 100 * time.Millisecond
)

type semanticRuleReadinessProbe func(context.Context) error

type semanticRuleMutation func(context.Context) error

func executeSemanticRuleMutation(ctx context.Context, probe semanticRuleReadinessProbe, mutation semanticRuleMutation) (int, bool, error) {
	return executeSemanticRuleMutationWithPolicy(ctx, semanticRuleReadinessTimeout, semanticRuleReadinessInterval, probe, mutation)
}

func executeSemanticRuleMutationWithPolicy(ctx context.Context, timeout, interval time.Duration, probe semanticRuleReadinessProbe, mutation semanticRuleMutation) (int, bool, error) {
	attempts, err := awaitSemanticRuleReadinessWithPolicy(ctx, timeout, interval, probe)
	if err != nil {
		return attempts, false, err
	}
	if mutation == nil {
		return attempts, false, errors.New("semantic rule mutation is unavailable")
	}
	return attempts, true, mutation(ctx)
}

func awaitSemanticRuleReadinessWithPolicy(ctx context.Context, timeout, interval time.Duration, probe semanticRuleReadinessProbe) (int, error) {
	if probe == nil {
		return 0, errors.New("semantic rule readiness probe is unavailable")
	}
	if timeout <= 0 {
		return 0, errors.New("semantic rule readiness timeout must be positive")
	}
	if interval <= 0 {
		return 0, errors.New("semantic rule readiness interval must be positive")
	}

	readinessCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	attempts := 0
	for {
		attempts++
		err := probe(readinessCtx)
		if err == nil {
			return attempts, nil
		}
		code := status.Code(err)
		if code != codes.Unauthenticated {
			return attempts, status.Errorf(code, "semantic rule readiness failed after %d attempt(s)", attempts)
		}

		timer := time.NewTimer(interval)
		select {
		case <-readinessCtx.Done():
			timer.Stop()
			if err := ctx.Err(); err != nil {
				return attempts, err
			}
			return attempts, status.Errorf(codes.Unauthenticated, "semantic rule readiness timed out after %d attempt(s)", attempts)
		case <-timer.C:
		}
	}
}

func semanticRuleReadinessError(attempts int, err error) error {
	if err == nil {
		return nil
	}
	code := status.Code(err)
	return status.Errorf(code, "semantic rule readiness failed after %d attempt(s) (%s)", attempts, canonicalProbeCode(code))
}
