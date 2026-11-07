package env

import (
	"strings"
	"testing"
)

func TestImageProvenanceRejectsInvalidBuildCommandSentinel(t *testing.T) {
	source := "https://github.com/MycelDB/mycel"
	base := imageSourceProvenance{CaptureStatus: "completed", Source: source, Dirty: "false", Revision: strings.Repeat("a", 40), ContextDigest: strings.Repeat("b", 64), BuildCommand: "github-actions-buildx-multiarch"}
	if !validImageProvenance(base, source) {
		t.Fatal("valid provenance rejected")
	}
	for _, command := range []string{"", "invalid", "arbitrary-build", "github-actions-buildx-multiarch-extra"} {
		p := base
		p.BuildCommand = command
		if validImageProvenance(p, source) {
			t.Fatalf("untrusted build command %q accepted", command)
		}
	}
}
