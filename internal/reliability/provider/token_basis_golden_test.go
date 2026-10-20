package provider

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type tokenBasisGoldens struct {
	SchemaVersion string `json:"schemaVersion"`
	Strategy      string `json:"strategy"`
	Dimensions    int    `json:"dimensions"`
	Vectors       []struct {
		Input           string    `json:"input"`
		NormalizedInput string    `json:"normalizedInput"`
		Expected        []float64 `json:"expected"`
	} `json:"vectors"`
	Scripts []struct {
		Name             string `json:"name"`
		Input            string `json:"input"`
		ExhaustedOutcome string `json:"exhaustedOutcome"`
		Reset            string `json:"reset"`
		Outcomes         []struct {
			Kind       string `json:"kind"`
			DelayMS    int    `json:"delay_ms"`
			Dimensions int    `json:"dimensions"`
		} `json:"outcomes"`
	} `json:"scripts"`
}

func TestTokenBasisV1IndependentGoldensAndScripts(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "reliability", "testdata", "semantic", "v1alpha1", "fixtures", "token-basis-v1-goldens.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var goldens tokenBasisGoldens
	if err := json.Unmarshal(raw, &goldens); err != nil {
		t.Fatal(err)
	}
	if goldens.SchemaVersion != "reliability.mycel.io/semantic-token-basis-goldens/v1alpha1" || goldens.Strategy != "token-basis-v1" {
		t.Fatalf("unexpected golden identity: %#v", goldens)
	}
	for _, golden := range goldens.Vectors {
		got := independentTokenBasis(golden.Input, goldens.Dimensions)
		if !reflect.DeepEqual(got, golden.Expected) {
			t.Fatalf("input %q vector = %#v, want %#v", golden.Input, got, golden.Expected)
		}
		if golden.NormalizedInput != "" && normalizeGoldenInput(golden.Input) != golden.NormalizedInput {
			t.Fatalf("input %q normalized to %q, want %q", golden.Input, normalizeGoldenInput(golden.Input), golden.NormalizedInput)
		}
	}
	allowed := map[string]bool{"success": true, "error": true, "retryable-error": true, "blocked": true, "refused": true, "wrong-dimensions": true}
	for _, script := range goldens.Scripts {
		if script.Name == "" || script.Input == "" || len(script.Outcomes) == 0 || len(script.Outcomes) > 256 {
			t.Fatalf("unbounded or incomplete script: %#v", script)
		}
		if script.ExhaustedOutcome != "error:fake_fixture_script_exhausted" || script.Reset != "process-restart" {
			t.Fatalf("script lacks explicit exhaustion/reset semantics: %#v", script)
		}
		for _, outcome := range script.Outcomes {
			if !allowed[outcome.Kind] {
				t.Fatalf("unsupported golden outcome %q", outcome.Kind)
			}
			if delay := time.Duration(outcome.DelayMS) * time.Millisecond; outcome.DelayMS < 0 || delay > 30*time.Second {
				t.Fatalf("invalid bounded delay_ms %d", outcome.DelayMS)
			}
			if outcome.Kind == "wrong-dimensions" && (outcome.Dimensions <= 0 || outcome.Dimensions > 65536) {
				t.Fatalf("invalid wrong dimensions outcome: %#v", outcome)
			}
		}
	}
}

// independentTokenBasis deliberately duplicates only the published fixture
// contract. It imports no daemon connector code, so drift is visible when the
// daemon and harness golden suites run together.
func independentTokenBasis(input string, dimensions int) []float64 {
	normalized := normalizeGoldenInput(input)
	vector := make([]float64, dimensions)
	recognized := false
	for _, token := range strings.FieldsFunc(normalized, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-')
	}) {
		if strings.HasPrefix(token, "axis-") {
			rawAxis := strings.TrimPrefix(token, "axis-")
			if axis, err := strconv.Atoi(rawAxis); err == nil && rawAxis != "" && (len(rawAxis) == 1 || rawAxis[0] != '0') && axis >= 0 && axis < dimensions {
				vector[axis]++
				recognized = true
			}
		}
	}
	if !recognized {
		for offset, block := 0, uint32(0); offset < len(vector); block++ {
			hasher := sha256.New()
			_, _ = hasher.Write([]byte(normalized))
			_, _ = hasher.Write([]byte{0})
			var suffix [4]byte
			binary.BigEndian.PutUint32(suffix[:], block)
			_, _ = hasher.Write(suffix[:])
			digest := hasher.Sum(nil)
			for index := 0; index+4 <= len(digest) && offset < len(vector); index, offset = index+4, offset+1 {
				value := int32(binary.BigEndian.Uint32(digest[index : index+4]))
				vector[offset] = float64(value) / 2147483648.0
			}
		}
	}
	var squared float64
	for _, value := range vector {
		squared += value * value
	}
	if squared == 0 {
		vector[0] = 1
		return vector
	}
	norm := math.Sqrt(squared)
	for index := range vector {
		vector[index] /= norm
	}
	return vector
}

func normalizeGoldenInput(input string) string {
	var out strings.Builder
	out.Grow(len(input))
	space := true
	for index := 0; index < len(input); index++ {
		value := input[index]
		switch value {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			if !space {
				out.WriteByte(' ')
				space = true
			}
		default:
			if value >= 'A' && value <= 'Z' {
				value += 'a' - 'A'
			}
			out.WriteByte(value)
			space = false
		}
	}
	return strings.TrimSpace(out.String())
}
