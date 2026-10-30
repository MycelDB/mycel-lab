package releasebundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

type commandRunnerFunc func(context.Context, string, ...string) ([]byte, error)

func (function commandRunnerFunc) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return function(ctx, name, args...)
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
