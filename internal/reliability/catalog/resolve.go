package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"gopkg.in/yaml.v3"
)

type ResolveOptions struct {
	ProfileDirs []string
}

func ResolveScenarioFile(path string, opts ResolveOptions) (spec.ResolvedScenario, error) {
	loaded, err := spec.LoadFile(path)
	if err != nil {
		return spec.ResolvedScenario{}, err
	}
	scenario, ok := loaded.(spec.Scenario)
	if !ok {
		return spec.ResolvedScenario{}, fmt.Errorf("%s is %T, want Scenario", path, loaded)
	}
	return ResolveScenario(scenario, filepath.Dir(path), opts)
}

func ResolveScenario(scenario spec.Scenario, scenarioDir string, opts ResolveOptions) (spec.ResolvedScenario, error) {
	clusterProfile, err := resolveClusterProfile(scenario.ClusterRef, scenarioDir, opts.ProfileDirs)
	if err != nil {
		return spec.ResolvedScenario{}, err
	}

	cluster, err := mergeCluster(clusterProfile.Cluster, scenario.ClusterOverrides)
	if err != nil {
		return spec.ResolvedScenario{}, fmt.Errorf("apply clusterOverrides: %w", err)
	}
	if err := cluster.Validate(); err != nil {
		return spec.ResolvedScenario{}, fmt.Errorf("resolved cluster invalid: %w", err)
	}

	groups := make([]spec.ResolvedActorGroup, 0, len(scenario.ActorGroups))
	for _, group := range scenario.ActorGroups {
		profile, err := resolveActorProfile(group.ProfileRef, scenarioDir, opts.ProfileDirs)
		if err != nil {
			return spec.ResolvedScenario{}, fmt.Errorf("actor group %q: %w", group.Name, err)
		}
		profile, err = applyActorOverrides(profile, group.Overrides)
		if err != nil {
			return spec.ResolvedScenario{}, fmt.Errorf("actor group %q overrides: %w", group.Name, err)
		}
		groups = append(groups, spec.ResolvedActorGroup{ActorGroupSpec: group, Profile: profile})
	}

	return spec.ResolvedScenario{
		TypeMeta:    spec.TypeMeta{APIVersion: spec.APIVersion, Kind: "ResolvedScenario"},
		Metadata:    scenario.Metadata,
		Seed:        scenario.Seed,
		Environment: scenario.Environment,
		ClusterRef:  scenario.ClusterRef,
		Cluster:     cluster,
		ActorGroups: groups,
		Phases:      scenario.Phases,
		Assertions:  scenario.Assertions,
		Artifacts:   scenario.Artifacts,
	}, nil
}

func resolveClusterProfile(ref, scenarioDir string, profileDirs []string) (spec.ClusterProfile, error) {
	path, err := resolveRef(ref, scenarioDir, profileDirs, "clusters")
	if err != nil {
		return spec.ClusterProfile{}, err
	}
	loaded, err := spec.LoadFile(path)
	if err != nil {
		return spec.ClusterProfile{}, err
	}
	profile, ok := loaded.(spec.ClusterProfile)
	if !ok {
		return spec.ClusterProfile{}, fmt.Errorf("%s is %T, want ClusterProfile", path, loaded)
	}
	return profile, nil
}

func resolveActorProfile(ref, scenarioDir string, profileDirs []string) (spec.ActorProfile, error) {
	path, err := resolveRef(ref, scenarioDir, profileDirs, "actor-profiles")
	if err != nil {
		return spec.ActorProfile{}, err
	}
	loaded, err := spec.LoadFile(path)
	if err != nil {
		return spec.ActorProfile{}, err
	}
	profile, ok := loaded.(spec.ActorProfile)
	if !ok {
		return spec.ActorProfile{}, fmt.Errorf("%s is %T, want ActorProfile", path, loaded)
	}
	return profile, nil
}

func resolveRef(ref, scenarioDir string, profileDirs []string, subdir string) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("empty reference")
	}
	candidates := candidatePaths(ref, scenarioDir, profileDirs, subdir)
	for _, candidate := range candidates {
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("reference %q not found; tried %s", ref, strings.Join(candidates, ", "))
}

func candidatePaths(ref, scenarioDir string, profileDirs []string, subdir string) []string {
	var candidates []string
	add := func(p string) { candidates = append(candidates, filepath.Clean(p)) }

	if strings.Contains(ref, string(filepath.Separator)) || strings.HasSuffix(ref, ".yaml") || strings.HasSuffix(ref, ".yml") {
		if filepath.IsAbs(ref) {
			add(ref)
		} else {
			add(filepath.Join(scenarioDir, ref))
			add(ref)
		}
		return candidates
	}

	roots := defaultProfileRoots(scenarioDir)
	roots = append(roots, profileDirs...)
	for _, root := range roots {
		add(filepath.Join(root, subdir, ref+".yaml"))
		add(filepath.Join(root, subdir, ref+".yml"))
		add(filepath.Join(root, ref+".yaml"))
		add(filepath.Join(root, ref+".yml"))
	}
	return dedupe(candidates)
}

func defaultProfileRoots(scenarioDir string) []string {
	var roots []string
	add := func(p string) { roots = append(roots, filepath.Clean(p)) }
	add(filepath.Dir(scenarioDir))
	add(scenarioDir)
	if cwd, err := os.Getwd(); err == nil {
		add(filepath.Join(cwd, "tests", "reliability"))
		add(cwd)
	}
	return dedupe(roots)
}

func dedupe(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func applyActorOverrides(base spec.ActorProfile, overrides map[string]any) (spec.ActorProfile, error) {
	if len(overrides) == 0 {
		return base, nil
	}
	if base.Behavior == nil {
		base.Behavior = map[string]any{}
	}
	behaviorOverrides := overrides
	if nested, ok := overrides["behavior"].(map[string]any); ok {
		behaviorOverrides = nested
	}
	deepMerge(base.Behavior, behaviorOverrides)
	if err := base.Validate(); err != nil {
		return spec.ActorProfile{}, err
	}
	return base, nil
}

func mergeCluster(base spec.ClusterSpec, overrides map[string]any) (spec.ClusterSpec, error) {
	if len(overrides) == 0 {
		return base, nil
	}
	var merged map[string]any
	raw, err := yaml.Marshal(base)
	if err != nil {
		return spec.ClusterSpec{}, err
	}
	if err := yaml.Unmarshal(raw, &merged); err != nil {
		return spec.ClusterSpec{}, err
	}
	deepMerge(merged, overrides)
	outRaw, err := yaml.Marshal(merged)
	if err != nil {
		return spec.ClusterSpec{}, err
	}
	var out spec.ClusterSpec
	if err := yaml.Unmarshal(outRaw, &out); err != nil {
		return spec.ClusterSpec{}, err
	}
	return out, nil
}

func deepMerge(dst map[string]any, src map[string]any) {
	for key, value := range src {
		if srcMap, ok := value.(map[string]any); ok {
			if dstMap, ok := dst[key].(map[string]any); ok {
				deepMerge(dstMap, srcMap)
				continue
			}
		}
		dst[key] = value
	}
}
