package runner

import (
	"fmt"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
)

// validateOperationTimeoutBudget prevents a declared event timeout from being
// silently shortened by the harness's per-operation request context. Every
// runtime event reserves an operation against this budget, including waits
// that make no outbound provider request. It is checked for every suite member
// before provisioning the first environment, and again for standalone runs.
func validateOperationTimeoutBudget(scenario spec.ResolvedScenario) error {
	if scenario.ProviderProfile == nil {
		return nil
	}
	limit := scenario.ProviderProfile.Profile.Budgets.RequestTimeout.Duration
	for _, phase := range scenario.Phases {
		for _, event := range phase.Events {
			raw, present := event.Target["timeout"]
			if !present {
				if event.Type == "cluster-recovery-wait" {
					return fmt.Errorf("scenario %s phase %s: cluster-recovery-wait needs an explicit timeout below the provider operation budget %s", scenario.Metadata.Name, phase.Name, limit)
				}
				continue
			}
			value, ok := raw.(string)
			if !ok {
				return fmt.Errorf("scenario %s phase %s event %s: timeout must be a duration string", scenario.Metadata.Name, phase.Name, event.Type)
			}
			maxWait, err := time.ParseDuration(value)
			if err != nil || maxWait <= 0 {
				return fmt.Errorf("scenario %s phase %s event %s: timeout must be a positive duration", scenario.Metadata.Name, phase.Name, event.Type)
			}
			if maxWait >= limit {
				return fmt.Errorf("scenario %s phase %s event %s: timeout %s must be below the provider operation requestTimeout %s (all runtime events reserve an operation)", scenario.Metadata.Name, phase.Name, event.Type, maxWait, limit)
			}
			if event.Type == "cluster-recovery-wait" {
				if rawInit, configured := event.Target["sessionInitTimeout"]; configured {
					if event.Target["reuseEndpointSessions"] != true {
						return fmt.Errorf("scenario %s phase %s: sessionInitTimeout requires reusable endpoint sessions", scenario.Metadata.Name, phase.Name)
					}
					text, ok := rawInit.(string)
					initTimeout, err := time.ParseDuration(text)
					if !ok || err != nil || initTimeout <= 0 || scenario.Cluster.Nodes <= 0 {
						return fmt.Errorf("scenario %s phase %s: sessionInitTimeout must leave time for concurrent endpoint logins and recovery polling within %s", scenario.Metadata.Name, phase.Name, maxWait)
					}
					commandBudget := 10 * time.Second
					if rawCommand, configured := event.Target["commandTimeout"]; configured {
						if commandText, valid := rawCommand.(string); valid {
							if parsed, parseErr := time.ParseDuration(commandText); parseErr == nil && parsed > 0 {
								commandBudget = parsed
							}
						}
					}
					pollInterval := 5 * time.Second
					if rawInterval, configured := event.Target["interval"]; configured {
						if intervalText, valid := rawInterval.(string); valid {
							if parsed, parseErr := time.ParseDuration(intervalText); parseErr == nil && parsed > 0 {
								pollInterval = parsed
							}
						}
					}
					stablePolls := 1
					if rawStablePolls, configured := event.Target["minimumStablePolls"]; configured {
						switch value := rawStablePolls.(type) {
						case int:
							stablePolls = value
						case int64:
							stablePolls = int(value)
						case float64:
							stablePolls = int(value)
						}
					}
					if stablePolls < 1 {
						stablePolls = 1
					}
					recoveryBudget := commandBudget + pollInterval*time.Duration(stablePolls-1)
					if recoveryBudget >= maxWait || initTimeout >= maxWait-recoveryBudget {
						return fmt.Errorf("scenario %s phase %s: sessionInitTimeout must leave time for concurrent endpoint logins and recovery polling within %s", scenario.Metadata.Name, phase.Name, maxWait)
					}
				}
			}
		}
	}
	return nil
}
