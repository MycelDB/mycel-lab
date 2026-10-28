package events

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecuteSemanticRuleMutationWaitsForReadinessAndMutatesOnce(t *testing.T) {
	t.Parallel()
	probeCalls := 0
	mutationCalls := 0
	attempts, mutationAttempted, err := executeSemanticRuleMutationWithPolicy(context.Background(), 100*time.Millisecond, time.Millisecond, func(context.Context) error {
		probeCalls++
		if probeCalls < 3 {
			return status.Error(codes.Unauthenticated, "session is not visible yet")
		}
		return nil
	}, func(context.Context) error {
		mutationCalls++
		return nil
	})
	if err != nil {
		t.Fatalf("execute semantic admin mutation: %v", err)
	}
	if attempts != 3 || probeCalls != 3 {
		t.Fatalf("readiness attempts = %d, probe calls = %d; want 3, 3", attempts, probeCalls)
	}
	if !mutationAttempted || mutationCalls != 1 {
		t.Fatalf("mutation attempted = %v, calls = %d; want true, 1", mutationAttempted, mutationCalls)
	}
}

func TestExecuteSemanticRuleMutationDoesNotRetryMutationError(t *testing.T) {
	t.Parallel()
	mutationErr := errors.New("mutation response was ambiguous")
	mutationCalls := 0
	attempts, mutationAttempted, err := executeSemanticRuleMutationWithPolicy(context.Background(), 100*time.Millisecond, time.Millisecond, func(context.Context) error {
		return nil
	}, func(context.Context) error {
		mutationCalls++
		return mutationErr
	})
	if !errors.Is(err, mutationErr) {
		t.Fatalf("error = %v, want mutation error", err)
	}
	if attempts != 1 || !mutationAttempted || mutationCalls != 1 {
		t.Fatalf("attempts = %d, mutation attempted = %v, calls = %d; want 1, true, 1", attempts, mutationAttempted, mutationCalls)
	}
}

func TestExecuteSemanticRuleMutationFailsClosedBeforeMutation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		code codes.Code
	}{
		{name: "permission denied", code: codes.PermissionDenied},
		{name: "unavailable", code: codes.Unavailable},
		{name: "deadline exceeded", code: codes.DeadlineExceeded},
		{name: "internal", code: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			probeCalls := 0
			mutationCalls := 0
			attempts, mutationAttempted, err := executeSemanticRuleMutationWithPolicy(context.Background(), 100*time.Millisecond, time.Millisecond, func(context.Context) error {
				probeCalls++
				return status.Error(tt.code, "do not persist this diagnostic marker")
			}, func(context.Context) error {
				mutationCalls++
				return nil
			})
			if status.Code(err) != tt.code {
				t.Fatalf("error code = %s, want %s", status.Code(err), tt.code)
			}
			if strings.Contains(err.Error(), "diagnostic marker") {
				t.Fatalf("readiness error retained the daemon diagnostic: %v", err)
			}
			if attempts != 1 || probeCalls != 1 || mutationAttempted || mutationCalls != 0 {
				t.Fatalf("attempts = %d, probe calls = %d, mutation attempted = %v, mutation calls = %d; want 1, 1, false, 0", attempts, probeCalls, mutationAttempted, mutationCalls)
			}
		})
	}
}

func TestExecuteSemanticRuleMutationBoundsUnauthenticatedReadiness(t *testing.T) {
	t.Parallel()
	mutationCalls := 0
	started := time.Now()
	attempts, mutationAttempted, err := executeSemanticRuleMutationWithPolicy(context.Background(), 20*time.Millisecond, time.Millisecond, func(context.Context) error {
		return status.Error(codes.Unauthenticated, "session is not visible")
	}, func(context.Context) error {
		mutationCalls++
		return nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error code = %s, want unauthenticated", status.Code(err))
	}
	if attempts < 2 {
		t.Fatalf("readiness attempts = %d, want at least 2", attempts)
	}
	if mutationAttempted || mutationCalls != 0 {
		t.Fatalf("mutation attempted = %v, calls = %d; want false, 0", mutationAttempted, mutationCalls)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded readiness took %s", elapsed)
	}
}

func TestExecuteSemanticRuleMutationHonorsParentCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts, mutationAttempted, err := executeSemanticRuleMutationWithPolicy(ctx, time.Second, time.Millisecond, func(context.Context) error {
		return status.Error(codes.Unauthenticated, "session is not visible")
	}, func(context.Context) error {
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if attempts != 1 || mutationAttempted {
		t.Fatalf("attempts = %d, mutation attempted = %v; want 1, false", attempts, mutationAttempted)
	}
}

func TestSemanticRuleReadinessErrorPreservesCodeAndRedactsCause(t *testing.T) {
	t.Parallel()
	err := semanticRuleReadinessError(2, status.Error(codes.Unauthenticated, "sensitive daemon diagnostic"))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error code = %s, want unauthenticated", status.Code(err))
	}
	if strings.Contains(err.Error(), "sensitive daemon diagnostic") {
		t.Fatalf("error retained daemon diagnostic: %v", err)
	}
}
