package catalog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"github.com/MycelDB/mycel-lab/internal/reliability/store"
)

func TestImportPathLifecycleAndExportRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	root := copyReliabilityFixtures(t)

	first, err := ImportPath(ctx, st, root, ImportOptions{})
	if err != nil {
		t.Fatalf("ImportPath(first) error = %v", err)
	}
	assertNoImportErrors(t, first)
	if !containsAction(first, ImportCreated) {
		t.Fatalf("first import actions = %+v, want created", first)
	}

	second, err := ImportPath(ctx, st, root, ImportOptions{})
	if err != nil {
		t.Fatalf("ImportPath(second) error = %v", err)
	}
	assertNoImportErrors(t, second)
	if !allActions(second, ImportNoop) {
		t.Fatalf("second import actions = %+v, want all noop", second)
	}

	actorPath := filepath.Join(root, "actor-profiles", "gql-reader.yaml")
	appendFile(t, actorPath, "\n  note: changed-before-use\n")
	third, err := ImportPath(ctx, st, actorPath, ImportOptions{ProfileDirs: []string{root}})
	if err != nil {
		t.Fatalf("ImportPath(third) error = %v", err)
	}
	assertNoImportErrors(t, third)
	if len(third) != 1 || third[0].Action != ImportUpdated || third[0].Version != 1 {
		t.Fatalf("third import = %+v, want updated v1", third)
	}

	if err := st.MarkDefinitionUsed(ctx, store.KindActorProfile, "gql-reader", 1); err != nil {
		t.Fatalf("MarkDefinitionUsed() error = %v", err)
	}
	appendFile(t, actorPath, "  note2: changed-after-use\n")
	fourth, err := ImportPath(ctx, st, actorPath, ImportOptions{ProfileDirs: []string{root}})
	if err != nil {
		t.Fatalf("ImportPath(fourth) error = %v", err)
	}
	assertNoImportErrors(t, fourth)
	if len(fourth) != 1 || fourth[0].Action != ImportCreated || fourth[0].Version != 2 {
		t.Fatalf("fourth import = %+v, want created v2", fourth)
	}
	if err := st.DeleteDefinition(ctx, store.KindActorProfile, "gql-reader", 1); !errors.Is(err, store.ErrDefinitionInUse) {
		t.Fatalf("DeleteDefinition(used) error = %v, want ErrDefinitionInUse", err)
	}

	exported, err := ExportDefinition(ctx, st, store.KindScenario, "example", 0)
	if err != nil {
		t.Fatalf("ExportDefinition() error = %v", err)
	}
	loaded, err := spec.Load(exported)
	if err != nil {
		t.Fatalf("exported YAML does not round-trip: %v\n%s", err, exported)
	}
	if loaded.(spec.Scenario).Metadata.Name != "example" {
		t.Fatalf("exported scenario name = %q", loaded.(spec.Scenario).Metadata.Name)
	}
}

func TestImportDryRunDoesNotRequireStore(t *testing.T) {
	results, err := ImportPath(context.Background(), nil, filepath.Join("..", "..", "..", "tests", "reliability"), ImportOptions{DryRun: true})
	if err != nil {
		t.Fatalf("ImportPath(dry-run) error = %v", err)
	}
	assertNoImportErrors(t, results)
	if len(results) == 0 {
		t.Fatal("dry-run returned no results")
	}
	if !allActions(results, ImportWouldImport) {
		t.Fatalf("dry-run results = %+v, want would-import", results)
	}
}

func TestJSONToYAMLRoundTrip(t *testing.T) {
	yamlData, err := JSONToYAML([]byte(`{"apiVersion":"myceldb.io/reliability/v1","kind":"Suite","metadata":{"name":"baseline"},"scenarios":[{"path":"scenario.yaml"}],"execution":{"mode":"sequential","stopOnFailure":true}}`))
	if err != nil {
		t.Fatalf("JSONToYAML() error = %v", err)
	}
	loaded, err := spec.Load(yamlData)
	if err != nil {
		t.Fatalf("Load(JSONToYAML()) error = %v\n%s", err, yamlData)
	}
	if loaded.(spec.Suite).Metadata.Name != "baseline" {
		t.Fatalf("suite name = %q", loaded.(spec.Suite).Metadata.Name)
	}
}

func copyReliabilityFixtures(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "..", "..", "tests", "reliability")
	dst := t.TempDir()
	if err := filepath.WalkDir(src, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	}); err != nil {
		t.Fatalf("copy fixtures: %v", err)
	}
	return dst
}

func appendFile(t *testing.T, path string, content string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	if _, err := file.WriteString(content); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
}

func assertNoImportErrors(t *testing.T, results []ImportResult) {
	t.Helper()
	for _, result := range results {
		if result.Error != nil {
			t.Fatalf("import result for %s has error: %v", result.Path, result.Error)
		}
	}
}

func containsAction(results []ImportResult, action ImportAction) bool {
	for _, result := range results {
		if result.Action == action {
			return true
		}
	}
	return false
}

func allActions(results []ImportResult, action ImportAction) bool {
	if len(results) == 0 {
		return false
	}
	for _, result := range results {
		if result.Action != action {
			return false
		}
	}
	return true
}

func TestCanonicalHashChangesOnContentChange(t *testing.T) {
	first, err := BuildDefinition(filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", "example", "example.yaml"), nil)
	if err != nil {
		t.Fatalf("BuildDefinition(first) error = %v", err)
	}
	root := copyReliabilityFixtures(t)
	path := filepath.Join(root, "scenarios", "example", "example.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read copied scenario: %v", err)
	}
	data = bytes.Replace(data, []byte("seed: 7"), []byte("seed: 8"), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write copied scenario: %v", err)
	}
	second, err := BuildDefinition(path, []string{root})
	if err != nil {
		t.Fatalf("BuildDefinition(second) error = %v", err)
	}
	if first.SpecHash == second.SpecHash {
		t.Fatalf("hash did not change: %s", first.SpecHash)
	}
}
