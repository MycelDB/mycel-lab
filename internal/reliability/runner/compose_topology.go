package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	"gopkg.in/yaml.v3"
)

// validateComposeRaftTopology compares the resolved Raft assertion shape with
// the actual Compose input files, including overlays and inherited service
// environment. It runs for every suite member before the first deployment and
// again for a standalone scenario before Compose preflight/provisioning.
// Unknown or non-numeric interpolation is rejected, never assumed to match.
func validateComposeRaftTopology(scenario spec.ResolvedScenario, sourcePath string) error {
	if scenario.Environment.Driver != "compose" {
		return nil
	}
	options := scenario.Environment.Options
	services, err := topologyStringSlice(options["serviceNames"])
	if err != nil {
		return err
	}
	// Synthetic driver fixtures without Compose files are outside the Raft
	// contract; any real Compose input must name its daemon services.
	if len(services) == 0 {
		if options["composeFiles"] != nil || options["composeFile"] != nil {
			return fmt.Errorf("compose Raft topology for %s: serviceNames is required", scenario.Metadata.Name)
		}
		return nil
	}
	files, err := topologyStringSlice(options["composeFiles"])
	if err != nil {
		return err
	}
	if len(files) == 0 {
		if one, ok := options["composeFile"].(string); ok && strings.TrimSpace(one) != "" {
			files = []string{one}
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("compose Raft topology for %s: no Compose input files", scenario.Metadata.Name)
	}
	// No .env-supplied topology may silently override the parsed fixture. The
	// release candidate uses a clean workspace and an allowlisted environment.
	if value := os.Getenv("COMPOSE_ENV_FILES"); value != "" {
		return fmt.Errorf("compose Raft topology for %s: COMPOSE_ENV_FILES is unsupported by the static check", scenario.Metadata.Name)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	if raw, err := os.ReadFile(filepath.Join(cwd, ".env")); err == nil && strings.Contains(string(raw), "MYCELD_CLUSTER_RAFT_") {
		return fmt.Errorf("compose Raft topology for %s: .env supplies Raft settings; cannot prove fixture consistency", scenario.Metadata.Name)
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("compose Raft topology for %s: inspect .env: %w", scenario.Metadata.Name, err)
	}
	keys := []struct {
		name string
		want int
	}{
		{"MYCELD_CLUSTER_RAFT_NODE_COUNT", scenario.Cluster.Raft.NodeCount},
		{"MYCELD_CLUSTER_RAFT_PARTITION_COUNT", scenario.Cluster.Raft.PartitionCount},
		{"MYCELD_CLUSTER_RAFT_REPLICA_FACTOR", scenario.Cluster.Raft.ReplicaFactor},
	}
	values := make(map[string]map[string]string, len(services))
	for _, name := range services {
		values[name] = make(map[string]string)
	}
	for _, input := range files {
		path, err := topologyFilePath(input, sourcePath)
		if err != nil {
			return fmt.Errorf("compose Raft topology for %s: %w", scenario.Metadata.Name, err)
		}
		file, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("compose Raft topology for %s: read fixture %s: %w", scenario.Metadata.Name, path, err)
		}
		var doc struct {
			Services map[string]struct {
				Environment map[string]any `yaml:"environment"`
				EnvFile     any            `yaml:"env_file"`
				Extends     any            `yaml:"extends"`
			} `yaml:"services"`
		}
		if err := yaml.Unmarshal(file, &doc); err != nil {
			return fmt.Errorf("compose Raft topology for %s: decode fixture %s: %w", scenario.Metadata.Name, path, err)
		}
		for _, name := range services {
			service, ok := doc.Services[name]
			if !ok {
				continue
			}
			if service.EnvFile != nil || service.Extends != nil {
				return fmt.Errorf("compose Raft topology for %s: %s uses env_file/extends; cannot prove fixture consistency", scenario.Metadata.Name, name)
			}
			for _, field := range keys {
				if raw, present := service.Environment[field.name]; present {
					value, ok := raw.(string)
					if !ok {
						return fmt.Errorf("compose Raft topology for %s: %s on %s is not a string", scenario.Metadata.Name, field.name, name)
					}
					values[name][field.name] = value
				}
			}
		}
	}
	for _, name := range services {
		for _, field := range keys {
			value, err := resolveRaftComposeValue(values[name][field.name])
			if err != nil {
				return fmt.Errorf("compose Raft topology for %s: %s on %s: %w", scenario.Metadata.Name, field.name, name, err)
			}
			if value != field.want {
				return fmt.Errorf("compose Raft topology mismatch for %s: %s on %s is %d but resolved cluster profile declares %d", scenario.Metadata.Name, field.name, name, value, field.want)
			}
		}
	}
	return nil
}

func topologyStringSlice(value any) ([]string, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		return []string{v}, nil
	case []string:
		return v, nil
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text, ok := item.(string)
			if !ok || text == "" {
				return nil, fmt.Errorf("compose Raft topology: list contains a non-string/empty entry")
			}
			out = append(out, text)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("compose Raft topology: unsupported list type %T", value)
	}
}

func topologyFilePath(path, sourcePath string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	// A canonical scenario's Compose paths are workspace-relative, not relative
	// to its scenario YAML. Locate the Lab root from the scenario source or cwd.
	for _, start := range []string{filepath.Dir(sourcePath), "."} {
		abs, err := filepath.Abs(start)
		if err != nil {
			return "", err
		}
		for dir := abs; ; dir = filepath.Dir(dir) {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				candidate := filepath.Clean(filepath.Join(dir, path))
				if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
					return candidate, nil
				}
			}
			if parent := filepath.Dir(dir); parent == dir {
				break
			}
		}
	}
	return "", fmt.Errorf("Compose input %q not found relative to Lab source root", path)
}

func resolveRaftComposeValue(raw string) (int, error) {
	if raw == "" {
		return 0, fmt.Errorf("Raft setting absent from selected Compose services")
	}
	if strings.HasPrefix(raw, "${") && strings.HasSuffix(raw, "}") {
		parts := strings.SplitN(raw[2:len(raw)-1], ":-", 2)
		if len(parts) != 2 || !strings.HasPrefix(parts[0], "MYCELD_CLUSTER_RAFT_") || parts[1] == "" {
			return 0, fmt.Errorf("unsupported Compose interpolation for Raft setting")
		}
		raw = parts[1]
		if supplied := os.Getenv(parts[0]); supplied != "" {
			raw = supplied
		}
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("Raft setting is not a positive integer")
	}
	return value, nil
}
