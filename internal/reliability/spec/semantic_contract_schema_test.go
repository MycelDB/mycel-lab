package spec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestSemanticContractSchemasAcceptExamples(t *testing.T) {
	root := semanticContractTestdataRoot(t)
	cases := []struct {
		name    string
		schema  string
		example string
	}{
		{name: "corpus", schema: "semantic-corpus.schema.json", example: "minimal-corpus.json"},
		{name: "evidence", schema: "semantic-evidence.schema.json", example: "search-evidence.json"},
		{name: "final report", schema: "semantic-final-report.schema.json", example: "final-report.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sch := compileSemanticSchema(t, filepath.Join(root, "schemas", tc.schema))
			instance := readJSONValue(t, filepath.Join(root, "examples", tc.example))
			if err := sch.Validate(instance); err != nil {
				t.Fatalf("validate positive example: %v", err)
			}
		})
	}
}

func TestSemanticContractSchemasRejectNegativeFixtures(t *testing.T) {
	root := semanticContractTestdataRoot(t)
	cases := []struct {
		name    string
		schema  string
		fixture string
	}{
		{name: "false success verdict", schema: "semantic-final-report.schema.json", fixture: "false-success-report.json"},
		{name: "invalid operation shape", schema: "semantic-corpus.schema.json", fixture: "invalid-operation-shape-corpus.json"},
		{name: "unsafe artifact classification", schema: "semantic-corpus.schema.json", fixture: "unsafe-classification-corpus.json"},
		{name: "bounded field", schema: "semantic-final-report.schema.json", fixture: "oversized-field-report.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sch := compileSemanticSchema(t, filepath.Join(root, "schemas", tc.schema))
			instance := readJSONValue(t, filepath.Join(root, "negative", tc.fixture))
			if err := sch.Validate(instance); err == nil {
				t.Fatal("negative fixture unexpectedly validated")
			}
		})
	}
}

func TestSemanticContractSchemasDeclareDecoderLimits(t *testing.T) {
	root := semanticContractTestdataRoot(t)
	for _, name := range []string{
		"semantic-corpus.schema.json",
		"semantic-evidence.schema.json",
		"semantic-final-report.schema.json",
	} {
		t.Run(name, func(t *testing.T) {
			value := readJSONValue(t, filepath.Join(root, "schemas", name))
			object, ok := value.(map[string]any)
			if !ok {
				t.Fatal("schema root is not an object")
			}
			limits, ok := object["x-mycel-decoder-limits"].(map[string]any)
			if !ok || len(limits) == 0 {
				t.Fatal("schema does not declare decoder limits")
			}
			for _, key := range []string{"maxDocumentBytes", "maxNestingDepth", "maxStringUtf8Bytes"} {
				limit, ok := limits[key].(float64)
				if !ok || limit <= 0 {
					t.Fatalf("decoder limit %s is absent or non-positive", key)
				}
			}
		})
	}
}

func semanticContractTestdataRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", "tests", "reliability", "testdata", "semantic", "v1alpha1"))
	if err != nil {
		t.Fatalf("resolve semantic contract testdata: %v", err)
	}
	return root
}

func compileSemanticSchema(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	sch, err := jsonschema.NewCompiler().Compile(path)
	if err != nil {
		t.Fatalf("compile schema %s: %v", path, err)
	}
	return sch
}

func readJSONValue(t *testing.T, path string) any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return value
}
