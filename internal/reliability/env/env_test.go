package env

import (
	"context"
	"strings"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/artifacts"
	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

func TestRequireDestructiveConfirmation(t *testing.T) {
	if err := RequireDestructiveConfirmation(Options{}); err == nil {
		t.Fatal("RequireDestructiveConfirmation() error=nil, want error")
	}
	if err := RequireDestructiveConfirmation(Options{ConfirmDestructive: true}); err != nil {
		t.Fatalf("RequireDestructiveConfirmation(confirmed) error=%v", err)
	}
	if err := RequireDestructiveConfirmation(Options{DryRun: true}); err != nil {
		t.Fatalf("RequireDestructiveConfirmation(dry-run) error=%v", err)
	}
}

func TestDryRunDriverLifecycle(t *testing.T) {
	driver := DryRunDriver{}
	if err := driver.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight() error=%v", err)
	}
	environment, err := driver.Create(context.Background(), spec.ResolvedScenario{Metadata: spec.Metadata{Name: "example"}, Environment: spec.EnvironmentSpec{Namespace: "test-ns"}})
	if err != nil {
		t.Fatalf("Create() error=%v", err)
	}
	if environment.Name != "example" || environment.Namespace != "test-ns" {
		t.Fatalf("environment=%+v", environment)
	}
	sink, err := artifacts.NewSink(t.TempDir())
	if err != nil {
		t.Fatalf("NewSink() error=%v", err)
	}
	if err := driver.CaptureState(context.Background(), environment, sink); err != nil {
		t.Fatalf("CaptureState() error=%v", err)
	}
}

func TestK3DPreflightRequiresConfirmation(t *testing.T) {
	err := (K3DDriver{}).Preflight(context.Background())
	if err == nil {
		t.Fatal("Preflight() error=nil, want confirmation error")
	}
	if !strings.Contains(err.Error(), "--confirm-destructive") {
		t.Fatalf("Preflight() error=%v, want confirmation message", err)
	}
}
