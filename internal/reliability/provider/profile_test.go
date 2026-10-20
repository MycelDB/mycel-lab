package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDeterministicProfileResolvesWithoutCredentials(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", "deterministic-token-basis-v1.yaml")
	profile, source, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	var lookups atomic.Int64
	resolved, err := Resolve(profile, source, ResolveOptions{LookupEnv: func(string) (string, bool) {
		lookups.Add(1)
		return "", false
	}})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if lookups.Load() != 0 {
		t.Fatalf("credential lookups = %d, want 0", lookups.Load())
	}
	hash := sha256.Sum256(source)
	if want := "sha256:" + hex.EncodeToString(hash[:]); resolved.SourceSHA256 != want {
		t.Fatalf("source hash = %q, want %q", resolved.SourceSHA256, want)
	}
	if resolved.Profile.Capabilities.Embedding.FixtureStrategy != "token-basis-v1" {
		t.Fatalf("unexpected resolved profile: %#v", resolved.Profile)
	}
	again, err := Resolve(profile, source, ResolveOptions{})
	if err != nil || !reflect.DeepEqual(resolved, again) {
		t.Fatalf("profile resolution is not deterministic: first=%#v second=%#v error=%v", resolved, again, err)
	}
}

func TestLiveProfileDryRunDoesNotReadCredential(t *testing.T) {
	profile, source := mustLoadProfile(t, "openai-compatible-live.yaml")
	var lookups atomic.Int64
	resolved, err := Resolve(profile, source, ResolveOptions{
		DryRun:    true,
		Manifest:  fullProviderManifest(),
		LookupEnv: func(string) (string, bool) { lookups.Add(1); return "", false },
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if lookups.Load() != 0 || len(resolved.RuntimeSecretValues()) != 0 {
		t.Fatalf("dry run read credentials: lookups=%d secrets=%d", lookups.Load(), len(resolved.RuntimeSecretValues()))
	}
}

func TestLiveProfileRequiresCredentialOutsideDryRun(t *testing.T) {
	profile, source := mustLoadProfile(t, "openai-compatible-live.yaml")
	if _, err := Resolve(profile, source, ResolveOptions{Manifest: fullProviderManifest(), LookupEnv: func(string) (string, bool) { return "", false }}); err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("Resolve() error = %v, want missing credential", err)
	}
	const secret = "opaque-fixture-value"
	resolved, err := Resolve(profile, source, ResolveOptions{Manifest: fullProviderManifest(), LookupEnv: func(name string) (string, bool) {
		return secret, name == "OPENAI_API_KEY"
	}})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	raw, err := json.Marshal(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) || strings.Contains(string(raw), "RuntimeSecret") {
		t.Fatalf("resolved profile leaked credential: %s", raw)
	}
	if got := resolved.RuntimeSecretValues(); len(got) != 1 || got[0] != secret {
		t.Fatalf("runtime secrets = %#v", got)
	}
}

func TestRequiredProviderEnforcementFailsClosed(t *testing.T) {
	profile, source := mustLoadProfile(t, "openai-compatible-live.yaml")
	_, err := Resolve(profile, source, ResolveOptions{DryRun: true})
	if err == nil || !strings.Contains(err.Error(), "lacks advertised enforcement capability") {
		t.Fatalf("Resolve() error = %v", err)
	}
}

func TestAdvisoryEnforcementRecordsUnavailableCapabilities(t *testing.T) {
	profile, source := mustLoadProfile(t, "openai-compatible-emulator.yaml")
	resolved, err := Resolve(profile, source, ResolveOptions{})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	found := false
	for _, status := range resolved.EnforcementStatus {
		if status.Capability == EnforceProviderRequests {
			found = true
			if status.Status != "advisory" {
				t.Fatalf("status = %#v", status)
			}
		}
	}
	if !found {
		t.Fatal("provider request enforcement status missing")
	}
}

func TestLoadRejectsUnknownFieldsAndCredentialedDeterministicProfile(t *testing.T) {
	base := `apiVersion: reliability.mycel.io/v1alpha1
kind: ProviderProfile
metadata:
  name: invalid
mode: deterministic
capabilities:
  embedding:
    connectorType: fake
    endpointKey: fake
    modelKey: fake
    dimensions: 2
    vectorSpaceKey: fake/2
    fixtureStrategy: token-basis-v1
    batching: {mode: single, maxItems: 1}
budgets:
  enforcement: required
  maxOperations: 2
  maxConcurrentOperations: 1
  requestTimeout: 1s
  totalTimeout: 2s
artifacts: {captureBodies: false, captureHeaders: false}
`
	if _, err := Load([]byte(base + "unknownField: true\n")); err == nil {
		t.Fatal("Load() accepted unknown field")
	}
	credentialed := strings.Replace(base, "    batching:", "    secretRef: {env: SHOULD_NOT_EXIST}\n    batching:", 1)
	if _, err := Load([]byte(credentialed)); err == nil || !strings.Contains(err.Error(), "cannot declare secretRef") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestProfileValidationRejectsInvalidDimensionsBudgetsAndModes(t *testing.T) {
	base, _ := mustLoadProfile(t, "deterministic-token-basis-v1.yaml")
	tests := []struct {
		name   string
		mutate func(*Profile)
	}{
		{name: "dimensions", mutate: func(profile *Profile) { profile.Capabilities.Embedding.Dimensions = 0 }},
		{name: "deterministic-dimensions-limit", mutate: func(profile *Profile) { profile.Capabilities.Embedding.Dimensions = 4097 }},
		{name: "operations", mutate: func(profile *Profile) { profile.Budgets.MaxOperations = 0 }},
		{name: "timeout-order", mutate: func(profile *Profile) {
			profile.Budgets.RequestTimeout = profile.Budgets.TotalTimeout
			profile.Budgets.TotalTimeout.Duration--
		}},
		{name: "mode", mutate: func(profile *Profile) { profile.Mode = "unknown" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := base
			embedding := *base.Capabilities.Embedding
			profile.Capabilities.Embedding = &embedding
			test.mutate(&profile)
			if err := profile.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestLiveProfileRequiresFiniteProviderBounds(t *testing.T) {
	profile, _ := mustLoadProfile(t, "openai-compatible-live.yaml")
	profile.Budgets.MaxRequests = 0
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "live profiles require") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestResolveRejectsUnavailableDeclaredCapability(t *testing.T) {
	profile, source := mustLoadProfile(t, "deterministic-token-basis-v1.yaml")
	copy := *profile.Capabilities.Embedding
	copy.Dimensions = 0
	copy.VectorSpaceKey = ""
	profile.Capabilities.Generation = &copy
	if _, err := Resolve(profile, source, ResolveOptions{}); err == nil || !strings.Contains(err.Error(), "generation") {
		t.Fatalf("Resolve() error = %v", err)
	}
}

func mustLoadProfile(t *testing.T, name string) (Profile, []byte) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", name)
	profile, source, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile(%q) error = %v", name, err)
	}
	return profile, source
}

func fullProviderManifest() CapabilityManifest {
	manifest := CapabilityManifest{}
	for _, capability := range []EnforcementCapability{
		EnforceProviderRequests,
		EnforceProviderConcurrency,
		EnforceProviderRetries,
		EnforceProviderInputTokens,
		EnforceProviderOutputTokens,
		EnforceProviderEstimatedCost,
		EnforceProviderBatchItems,
	} {
		manifest = manifest.With(capability, "test-provider")
	}
	return manifest
}
