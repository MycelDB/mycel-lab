package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"github.com/MycelDB/mycel-lab/internal/reliability/store"
	"gopkg.in/yaml.v3"
)

type ImportAction string

const (
	ImportWouldImport ImportAction = "would-import"
	ImportCreated     ImportAction = "created"
	ImportUpdated     ImportAction = "updated"
	ImportNoop        ImportAction = "noop"
)

type ImportResult struct {
	Path    string
	Kind    store.DefinitionKind
	Name    string
	Version int
	Hash    string
	Action  ImportAction
	Error   error
}

type ImportOptions struct {
	DryRun      bool
	ProfileDirs []string
}

func ImportPath(ctx context.Context, st store.Store, path string, opts ImportOptions) ([]ImportResult, error) {
	files, err := discoverYAMLFiles(path)
	if err != nil {
		return nil, err
	}
	root := path
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		root = filepath.Dir(path)
	}
	profileDirs := append([]string{root}, opts.ProfileDirs...)
	var results []ImportResult
	for _, file := range orderDefinitionFiles(files) {
		def, err := BuildDefinition(file, profileDirs)
		if err != nil {
			results = append(results, ImportResult{Path: file, Error: err})
			continue
		}
		result := ImportResult{Path: file, Kind: def.Kind, Name: def.Name, Hash: def.SpecHash, Action: ImportWouldImport}
		if !opts.DryRun {
			if st == nil {
				return nil, fmt.Errorf("store is required when dry-run is false")
			}
			upserted, err := st.UpsertDefinition(ctx, def)
			if err != nil {
				result.Error = err
			} else {
				result.Version = upserted.Definition.Version
				result.Action = importActionForUpsert(upserted.Status)
			}
		}
		results = append(results, result)
	}
	return results, nil
}

func BuildDefinition(path string, profileDirs []string) (store.Definition, error) {
	loaded, err := spec.LoadFile(path)
	if err != nil {
		return store.Definition{}, err
	}
	kind, name, err := kindAndName(loaded)
	if err != nil {
		return store.Definition{}, err
	}
	specJSON, err := canonicalJSON(loaded)
	if err != nil {
		return store.Definition{}, err
	}
	var resolvedJSON []byte
	if scenario, ok := loaded.(spec.Scenario); ok {
		resolved, err := ResolveScenario(scenario, filepath.Dir(path), ResolveOptions{ProfileDirs: profileDirs})
		if err == nil {
			resolvedJSON, err = canonicalJSON(resolved)
			if err != nil {
				return store.Definition{}, err
			}
		}
	}
	return store.Definition{Kind: kind, Name: name, SpecHash: hashJSON(specJSON), SpecJSON: specJSON, ResolvedJSON: resolvedJSON}, nil
}

func ExportDefinition(ctx context.Context, st store.Store, kind store.DefinitionKind, name string, version int) ([]byte, error) {
	var def store.Definition
	var err error
	if version == 0 {
		def, err = st.LatestDefinition(ctx, kind, name)
	} else {
		def, err = st.GetDefinition(ctx, kind, name, version)
	}
	if err != nil {
		return nil, err
	}
	return JSONToYAML(def.SpecJSON)
}

func JSONToYAML(raw []byte) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return yaml.Marshal(value)
}

func canonicalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

func hashJSON(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func kindAndName(value any) (store.DefinitionKind, string, error) {
	switch v := value.(type) {
	case spec.ClusterProfile:
		return store.KindClusterProfile, v.Metadata.Name, nil
	case spec.ActorProfile:
		return store.KindActorProfile, v.Metadata.Name, nil
	case spec.Scenario:
		return store.KindScenario, v.Metadata.Name, nil
	case spec.Suite:
		return store.KindSuite, v.Metadata.Name, nil
	default:
		return "", "", fmt.Errorf("unsupported definition type %T", value)
	}
}

func discoverYAMLFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	var files []string
	if !info.IsDir() {
		if isYAML(path) {
			return []string{path}, nil
		}
		return nil, fmt.Errorf("%s is not a YAML file", path)
	}
	err = filepath.WalkDir(path, func(p string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if isYAML(p) {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func orderDefinitionFiles(files []string) []string {
	ordered := append([]string(nil), files...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return kindOrderForPath(ordered[i]) < kindOrderForPath(ordered[j])
	})
	return ordered
}

func kindOrderForPath(path string) int {
	parts := strings.Split(filepath.ToSlash(path), "/")
	for _, part := range parts {
		switch part {
		case "clusters":
			return 0
		case "actor-profiles":
			return 1
		case "scenarios":
			return 2
		case "suites":
			return 3
		}
	}
	return 4
}

func isYAML(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".yaml" || ext == ".yml"
}

func importActionForUpsert(status store.UpsertStatus) ImportAction {
	switch status {
	case store.UpsertCreated:
		return ImportCreated
	case store.UpsertUpdated:
		return ImportUpdated
	case store.UpsertNoop:
		return ImportNoop
	default:
		return ImportAction(status)
	}
}
