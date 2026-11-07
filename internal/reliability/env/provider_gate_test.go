package env

import (
	"context"
	"testing"
)

func TestProviderGateControlCannotEscapeItsFiniteInterface(t *testing.T) {
	good := Environment{Metadata: map[string]string{"providerEmulator": "true"}}
	for _, args := range [][2]string{{"../escape", "get"}, {"older", "delete"}, {"older;command", "release"}, {"", "get"}} {
		if _, err := gateArguments(good, args[0], args[1]); err == nil {
			t.Fatal("arbitrary control accepted")
		}
	}
	if _, err := (ComposeDriver{}).ProviderEmulatorGate(context.Background(), Environment{}, "older", "get"); err == nil {
		t.Fatal("unconfigured environment reached execution")
	}
	args, err := gateArguments(good, "older-response", "release")
	if err != nil || len(args) != 6 || args[0] != "/mycel-provider-emulator" {
		t.Fatal("invalid fixed executable/control arguments")
	}
}
