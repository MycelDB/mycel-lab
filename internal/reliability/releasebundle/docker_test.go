package releasebundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type commandRunnerFunc func(context.Context, string, ...string) ([]byte, error)

func (function commandRunnerFunc) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return function(ctx, name, args...)
}

type buildxEnvironmentRunner struct {
	run    commandRunnerFunc
	runEnv func(context.Context, []string, string, ...string) ([]byte, error)
}

func (runner buildxEnvironmentRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return runner.run(ctx, name, args...)
}

func (runner buildxEnvironmentRunner) RunWithEnv(ctx context.Context, environment []string, name string, args ...string) ([]byte, error) {
	return runner.runEnv(ctx, environment, name, args...)
}

func TestDockerInspectorResolvesRegistryPlatformsAndSideManifestsAnonymously(t *testing.T) {
	const manifest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const config = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const side = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	indexRaw := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"` + manifest + `","platform":{"os":"linux","architecture":"amd64"}},{"digest":"` + side + `","platform":{"os":"unknown","architecture":"unknown"}}]}`)
	indexSum := sha256.Sum256(indexRaw)
	indexDigest := "sha256:" + hex.EncodeToString(indexSum[:])
	reference := "docker.io/myceldb/mycel@" + indexDigest
	var commands []string
	runner := commandRunnerFunc(func(_ context.Context, name string, args ...string) ([]byte, error) {
		command := name + " " + strings.Join(args, " ")
		commands = append(commands, command)
		switch {
		case strings.Contains(command, "--raw "+reference):
			return indexRaw, nil
		case strings.Contains(command, "--raw docker.io/myceldb/mycel@"+manifest):
			return []byte(`{"schemaVersion":2,"config":{"digest":"` + config + `"}}`), nil
		case strings.Contains(command, "docker.io/myceldb/mycel@"+manifest+" --format"):
			return []byte(`{"architecture":"amd64","os":"linux","config":{"Labels":{"org.opencontainers.image.source":"https://github.com/MycelDB/mycel","org.opencontainers.image.revision":"1111111111111111111111111111111111111111","io.myceldb.source.dirty":"false"}}}`), nil
		default:
			return nil, fmt.Errorf("unexpected command %s", command)
		}
	})
	resolved, err := NewDockerInspector(runner).Inspect(context.Background(), ImageIdentity{Name: "daemon", Reference: reference, IndexDigest: indexDigest})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.IndexDigest != indexDigest || len(resolved.Platforms) != 1 || resolved.Platforms[0].ConfigDigest != config {
		t.Fatalf("resolved=%+v", resolved)
	}
	if len(resolved.SideManifests) != 1 || resolved.SideManifests[0] != side {
		t.Fatalf("side manifests=%v", resolved.SideManifests)
	}
	for _, command := range commands {
		if !strings.Contains(command, "docker --config ") {
			t.Fatalf("registry inspection did not use anonymous Docker config: %s", command)
		}
	}
}

func TestDockerInspectorUsesExactBuildxWithAnonymousConfig(t *testing.T) {
	const manifest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const config = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	indexRaw := []byte(`{"schemaVersion":2,"manifests":[{"digest":"` + manifest + `","platform":{"os":"linux","architecture":"amd64"}}]}`)
	indexSum := sha256.Sum256(indexRaw)
	indexDigest := "sha256:" + hex.EncodeToString(indexSum[:])
	reference := "docker.io/myceldb/mycel@" + indexDigest
	dir := t.TempDir()
	dockerPath := filepath.Join(dir, "docker")
	buildxPath := filepath.Join(dir, "docker-buildx")
	for _, path := range []string{dockerPath, buildxPath} {
		if err := os.WriteFile(path, []byte("test executable"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	dockerPath, _ = filepath.EvalSymlinks(dockerPath)
	buildxPath, _ = filepath.EvalSymlinks(buildxPath)
	var anonymousCalls int
	runner := buildxEnvironmentRunner{
		run: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name == dockerPath && strings.Join(args, " ") == `info --format {{range .ClientInfo.Plugins}}{{if eq .Name "buildx"}}{{.Path}}{{end}}{{end}}` {
				return []byte(buildxPath + "\n"), nil
			}
			return nil, fmt.Errorf("unexpected ordinary command %s %s", name, strings.Join(args, " "))
		},
		runEnv: func(_ context.Context, environment []string, name string, args ...string) ([]byte, error) {
			if name != buildxPath || len(environment) != 1 || !strings.HasPrefix(environment[0], "DOCKER_CONFIG=") {
				return nil, fmt.Errorf("non-hermetic Buildx invocation: %s env=%v", name, environment)
			}
			anonymousCalls++
			command := strings.Join(args, " ")
			switch {
			case strings.Contains(command, "--raw "+reference):
				return indexRaw, nil
			case strings.Contains(command, "--raw docker.io/myceldb/mycel@"+manifest):
				return []byte(`{"schemaVersion":2,"config":{"digest":"` + config + `"}}`), nil
			case strings.Contains(command, "docker.io/myceldb/mycel@"+manifest+" --format"):
				return []byte(`{"architecture":"amd64","os":"linux","config":{"Labels":{}}}`), nil
			default:
				return nil, fmt.Errorf("unexpected Buildx command %s", command)
			}
		},
	}
	resolved, err := NewDockerInspectorWithToolPaths(runner, dockerPath, buildxPath).Inspect(context.Background(), ImageIdentity{Name: "daemon", Reference: reference, IndexDigest: indexDigest})
	if err != nil {
		t.Fatal(err)
	}
	if anonymousCalls != 3 || resolved.IndexDigest != indexDigest || len(resolved.Platforms) != 1 {
		t.Fatalf("calls=%d resolved=%+v", anonymousCalls, resolved)
	}
}

func TestDockerInspectorResolvesLocalImageIDAndLabels(t *testing.T) {
	const imageID = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	runner := commandRunnerFunc(func(_ context.Context, name string, args ...string) ([]byte, error) {
		return []byte(`[{"Id":"` + imageID + `","Architecture":"arm64","Os":"linux","Config":{"Labels":{"io.myceldb.source.dirty":"false"}}}]`), nil
	})
	resolved, err := NewDockerInspector(runner).Inspect(context.Background(), ImageIdentity{Name: "emulator", Reference: "local/emulator:candidate-1", Local: true})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ImageID != imageID || len(resolved.Platforms) != 1 || resolved.Platforms[0].ConfigDigest != imageID {
		t.Fatalf("resolved=%+v", resolved)
	}
}
