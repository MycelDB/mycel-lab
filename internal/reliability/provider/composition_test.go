package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func compositionSources(t *testing.T) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, name := range []string{"deterministic-token-basis-v1", "openai-compatible-procedure-emulator"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "reliability", "provider-profiles", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		out[name] = raw
	}
	return out
}
func compositionStages() []StageSpec {
	return []StageSpec{{Name: "enrich", Capability: "generation", ProfileRef: "openai-compatible-procedure-emulator", InputFrom: "graph"}, {Name: "embed", Capability: "embedding", ProfileRef: "deterministic-token-basis-v1", InputFrom: "enrich"}}
}
func TestCompositionDoesNotAssumeGenerationProviderEmbeds(t *testing.T) {
	sources := compositionSources(t)
	plan, err := PlanComposition(compositionStages(), sources, SemanticStageAdapters())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Stages) != 2 || plan.Stages[0].Dimensions != 0 || plan.Stages[1].Dimensions != 8 || plan.Stages[0].OutputKind != "text" || plan.Stages[1].OutputKind != "vector" || plan.Status != "planned" || plan.MaximumWallClockMilliseconds <= 0 {
		t.Fatalf("invalid stage contract: %+v", plan)
	}
	sum := sha256.Sum256(sources["openai-compatible-procedure-emulator"])
	if plan.Stages[0].ProfileSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("stage provenance is not bound to exact source")
	}
	var missing *CompositionCapabilityError
	if !errors.As(plan.RequireExecutable(), &missing) || len(missing.Missing) != 1 || missing.Missing[0] != "semantic-stage/generation/openai-compatible" {
		t.Fatal("unsupported native enrichment silently passed")
	}
	raw, _ := json.Marshal(plan)
	var object map[string]any
	if json.Unmarshal(raw, &object) != nil || object["status"] == "passed" {
		t.Fatal("planning was represented as runtime evidence")
	}
}
func TestCompositionDirectEmbeddingUsesExistingAdapter(t *testing.T) {
	stages := []StageSpec{{Name: "embed", Capability: "embedding", ProfileRef: "deterministic-token-basis-v1", InputFrom: "graph"}}
	plan, err := PlanComposition(stages, compositionSources(t), SemanticStageAdapters())
	if err != nil || plan.RequireExecutable() != nil {
		t.Fatalf("direct embedding rejected: %+v %v", plan, err)
	}
}
func TestCompositionRejectsInvalidDataflowAndCapabilities(t *testing.T) {
	for name, mutate := range map[string]func([]StageSpec){
		"cycle":                  func(s []StageSpec) { s[0].InputFrom = "embed" },
		"implicit input":         func(s []StageSpec) { s[1].InputFrom = "" },
		"duplicate name":         func(s []StageSpec) { s[1].Name = "enrich" },
		"missing capability":     func(s []StageSpec) { s[0].Capability = "enrichment" },
		"unknown capability":     func(s []StageSpec) { s[0].Capability = "vendor-magic" },
		"embedding not terminal": func(s []StageSpec) { s[0].Capability = "embedding" },
		"missing profile":        func(s []StageSpec) { s[0].ProfileRef = "missing" },
		"path control":           func(s []StageSpec) { s[0].Name = "../gate" },
	} {
		t.Run(name, func(t *testing.T) {
			s := compositionStages()
			mutate(s)
			if _, err := PlanComposition(s, compositionSources(t), SemanticStageAdapters()); err == nil {
				t.Fatal("invalid composition accepted")
			}
		})
	}
}
func TestCompositionStageYAMLRejectsUnknownAndUntypedFields(t *testing.T) {
	for _, raw := range []string{"name: enrich\ncapability: generation\nprofileRef: p\ninputFrom: graph\nfallbackProfileRef: hidden\n", "name: true\ncapability: generation\nprofileRef: p\ninputFrom: graph\n", "name: enrich\nname: alternate\n"} {
		var s StageSpec
		if yaml.Unmarshal([]byte(raw), &s) == nil {
			t.Fatal("ambiguous stage contract accepted")
		}
	}
}
func TestProtocolSequenceAndGatesRejectAmbiguousContracts(t *testing.T) {
	good := ProtocolEmulatorScript{Version: "v1", MaxRequests: 1, RequestSequence: []ProtocolEmulatorOutcome{{Kind: "success", Gate: "older", GateTimeoutMS: 1000}}}
	if err := ValidateProtocolEmulatorScript(&good); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ProtocolEmulatorScript){
		func(s *ProtocolEmulatorScript) { s.MaxRequests = 2 },
		func(s *ProtocolEmulatorScript) { s.RequestSequence = []ProtocolEmulatorOutcome{} },
		func(s *ProtocolEmulatorScript) { s.RequireMatch = true },
		func(s *ProtocolEmulatorScript) { s.RequestSequence[0].Gate = "../escape" },
		func(s *ProtocolEmulatorScript) { s.RequestSequence[0].GateTimeoutMS = 0 },
		func(s *ProtocolEmulatorScript) { s.RequestSequence[0].DelayMS = 1 },
		func(s *ProtocolEmulatorScript) { s.RequestSequence[0].Kind = "refusal" },
	} {
		s := good
		s.RequestSequence = append([]ProtocolEmulatorOutcome(nil), good.RequestSequence...)
		mutate(&s)
		if ValidateProtocolEmulatorScript(&s) == nil {
			t.Fatal("ambiguous sequence/gate accepted")
		}
	}
}
