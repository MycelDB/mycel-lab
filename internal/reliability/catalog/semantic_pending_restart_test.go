package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

const semanticV0214Image = "docker.io/myceldb/mycel@sha256:0973cddd9c282abff40d8edeb2e8f7a801e58d40bfbab4c8507ce2d9daae2dd7"

func TestCanonicalSemanticScenariosPinV0214ForManifestAndCompose(t *testing.T) {
	for _, name := range []string{"compose-semantic-graph-contract", "compose-semantic-graph-recovery", "compose-semantic-pending-restart", "compose-semantic-openai-emulator", "compose-semantic-live-provider"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", name, name+".yaml")
			scenario, err := ResolveScenarioFile(path, ResolveOptions{ProviderDryRun: true, DeferLiveProviderValidation: true, ProviderLookupEnv: func(string) (string, bool) { t.Fatal("planning resolved a credential"); return "", false }})
			if err != nil {
				t.Fatal(err)
			}
			if scenario.Cluster.Image != semanticV0214Image || scenario.Environment.Options["imageOverride"] != semanticV0214Image {
				t.Fatalf("daemon identity drift: manifest=%q Compose=%v", scenario.Cluster.Image, scenario.Environment.Options["imageOverride"])
			}
		})
	}
}

func TestPendingRestartScenarioCannotReconstructMissingWorkAfterRestart(t *testing.T) {
	name := "compose-semantic-pending-restart"
	path := filepath.Join("..", "..", "..", "tests", "reliability", "scenarios", name, name+".yaml")
	scenario, err := ResolveScenarioFile(path, ResolveOptions{ProviderDryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(scenario.ActorGroups) != 1 || scenario.ActorGroups[0].Count != 3 {
		t.Fatal("restart probes need three endpoint-local controller assignments")
	}
	pendingCaptured, restarts, readonlyChecks, completionChecks := false, 0, 0, 0
	pendingPreserved, resumed := false, false
	for _, phase := range scenario.Phases {
		for _, event := range phase.Events {
			if event.Type == "rolling-restart" {
				if !pendingCaptured || restarts > 0 && completionChecks == 0 {
					t.Fatal("restart precedes its required pending/completed checkpoint")
				}
				restarts++
			}
			if pendingCaptured && event.Type == "semantic-maintenance-await" {
				if !pendingPreserved || resumed || restarts != 1 {
					t.Fatal("maintenance resumed before read-only preservation or after the terminal boundary")
				}
				resumed = true
			}
			if pendingCaptured && (event.Type == "semantic-backfill" || event.Type == "semantic-rebuild" || event.Type == "environment-reset" || event.Type == "host-command") {
				t.Fatal("pending preservation includes repair/reset/unbounded host behavior")
			}
			if event.Type != "semantic-work-assert" {
				continue
			}
			if event.Target["captureWorkAs"] == "pending-before-restart" {
				if restarts != 0 || event.Target["analyze"] != true || event.Target["expectedStatus"] != "pending" {
					t.Fatal("pending boundary was not explicitly captured before restart")
				}
				pendingCaptured = true
			} else {
				if restarts == 0 || event.Target["analyze"] == true || event.Target["expectWorkFrom"] == nil {
					t.Fatal("post-restart checkpoint could reconstruct work")
				}
				readonlyChecks++
				if event.Target["expectedStatus"] == "pending" {
					if resumed || restarts != 1 {
						t.Fatal("pending preservation did not precede normal maintenance resumption")
					}
					pendingPreserved = true
				} else if event.Target["expectedStatus"] == "complete" {
					if !resumed {
						t.Fatal("completion was not reached through the controlled maintenance boundary")
					}
					completionChecks++
				}
			}
			if event.Target["expectBackgroundMaintenanceEnabled"] != false {
				t.Fatal("controlled background scheduling is not asserted")
			}
			if event.Target["expectRevisionFrom"] != "pending-commit" {
				t.Fatal("work is not bound to the acknowledged commit")
			}
		}
	}
	if !pendingCaptured || restarts != 2 || readonlyChecks != 3 || completionChecks != 2 {
		t.Fatalf("missing lifecycle proof: captured=%v restarts=%d read-only=%d completion=%d", pendingCaptured, restarts, readonlyChecks, completionChecks)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "compose", "semantic-pending-restart.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var overlay struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &overlay); err != nil {
		t.Fatal(err)
	}
	for _, service := range []string{"myceld-a", "myceld-b", "myceld-c"} {
		if overlay.Services[service].Environment["MYCELD_SEMANTIC_MAINTENANCE_ENABLED"] != "false" {
			t.Fatalf("uncontrolled pending boundary on %s", service)
		}
	}
}
