package events

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/env"
	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

type CommonFolioAutomationResult struct {
	Operation   string                            `json:"operation"`
	WriterGroup string                            `json:"writerGroup,omitempty"`
	Configured  int                               `json:"configured"`
	Diagnostics bool                              `json:"diagnostics,omitempty"`
	Assignments []CommonFolioAutomationAssignment `json:"assignments,omitempty"`
}

type CommonFolioAutomationAssignment struct {
	ActorID     string `json:"actorId"`
	Ordinal     int    `json:"ordinal"`
	SpaceID     string `json:"spaceId,omitempty"`
	DomainID    string `json:"domainId,omitempty"`
	PrincipalID string `json:"principalId,omitempty"`
	ProcedureID string `json:"procedureId,omitempty"`
	BindingID   string `json:"bindingId,omitempty"`
	Stdout      string `json:"stdout,omitempty"`
	Stderr      string `json:"stderr,omitempty"`
}

type CommonFolioTerminalFixtureResult struct {
	WriterGroup          string                                 `json:"writerGroup"`
	MinimumPerAssignment int                                    `json:"minimumPerAssignment"`
	Attempts             int                                    `json:"attempts"`
	Timeout              string                                 `json:"timeout"`
	Interval             string                                 `json:"interval"`
	Ready                bool                                   `json:"ready"`
	Assignments          []CommonFolioTerminalFixtureAssignment `json:"assignments"`
}

type CommonFolioTerminalFixtureAssignment struct {
	ActorID             string                          `json:"actorId"`
	Ordinal             int                             `json:"ordinal"`
	SpaceID             string                          `json:"spaceId"`
	DomainID            string                          `json:"domainId"`
	TerminalInvocations []CommonFolioTerminalInvocation `json:"terminalInvocations,omitempty"`
	LastError           string                          `json:"lastError,omitempty"`
	LastResponse        string                          `json:"lastResponse,omitempty"`
}

type CommonFolioTerminalInvocation struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	ClaimVersion uint64 `json:"claimVersion,omitempty"`
	UpdatedAt    string `json:"updatedAt,omitempty"`
}

type commonFolioInvocationList struct {
	Invocations []struct {
		ID           string `json:"id"`
		Status       string `json:"status"`
		ClaimVersion uint64 `json:"claim_version"`
		UpdatedAt    string `json:"updated_at"`
	} `json:"invocations"`
}

func executeCommonFolioAutomationEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, eventType string, target map[string]any) (CommonFolioAutomationResult, error) {
	writerGroup := firstNonEmpty(stringTarget(target, "writerGroup"), "graph-writers")
	limit := intTarget(target, "limit", 3)
	if limit <= 0 {
		limit = 1
	}
	assignments := commonFolioAssignments(resources, writerGroup, limit)
	result := CommonFolioAutomationResult{Operation: eventType, WriterGroup: writerGroup}
	if len(assignments) == 0 {
		return result, fmt.Errorf("%s found no provisioned graph writer assignments for group %q", eventType, writerGroup)
	}
	for i, assignment := range assignments {
		ordinal := intTarget(target, "ordinal", i)
		if ordinal < 0 {
			ordinal = i
		}
		item := CommonFolioAutomationAssignment{ActorID: assignment.ActorID, Ordinal: ordinal, SpaceID: assignment.SpaceID, DomainID: assignment.DomainID, PrincipalID: assignment.PrincipalID}
		switch eventType {
		case "commonfolio-automation-seed":
			stdout, stderr, err := seedCommonFolioAutomation(ctx, driver, environment, env.NodeRef{Ordinal: ordinal}, assignment, target)
			item.ProcedureID = commonFolioProcedureID(target)
			item.BindingID = commonFolioBindingID(target, assignment)
			item.Stdout, item.Stderr = bounded(stdout, 4096), bounded(stderr, 4096)
			result.Assignments = append(result.Assignments, item)
			if err != nil {
				return result, fmt.Errorf("seed CommonFolio automation for %s on ordinal %d: %w", assignment.ActorID, ordinal, err)
			}
		case "commonfolio-automation-diagnostics":
			stdout, stderr, err := commonFolioAutomationDiagnostics(ctx, driver, environment, env.NodeRef{Ordinal: ordinal}, assignment, target)
			item.Stdout, item.Stderr = bounded(stdout, 4096), bounded(stderr, 4096)
			result.Assignments = append(result.Assignments, item)
			if err != nil {
				return result, fmt.Errorf("collect CommonFolio automation diagnostics for %s on ordinal %d: %w", assignment.ActorID, ordinal, err)
			}
			result.Diagnostics = true
		default:
			return result, fmt.Errorf("unsupported CommonFolio automation event type %q", eventType)
		}
		result.Configured++
	}
	return result, nil
}

func executeCommonFolioAwaitTerminalEvent(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, resources *provision.ScenarioResources, target map[string]any) (CommonFolioTerminalFixtureResult, error) {
	writerGroup := firstNonEmpty(stringTarget(target, "writerGroup"), "graph-writers")
	limit := intTarget(target, "limit", 3)
	if limit <= 0 {
		limit = 1
	}
	minimum := intTarget(target, "minimumPerAssignment", 1)
	if minimum <= 0 {
		minimum = 1
	}
	timeout, err := targetDuration(target, "timeout")
	if err != nil {
		return CommonFolioTerminalFixtureResult{}, err
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	interval, err := targetDuration(target, "interval")
	if err != nil {
		return CommonFolioTerminalFixtureResult{}, err
	}
	if interval <= 0 {
		interval = time.Second
	}
	assignments := commonFolioAssignments(resources, writerGroup, limit)
	result := CommonFolioTerminalFixtureResult{WriterGroup: writerGroup, MinimumPerAssignment: minimum, Timeout: timeout.String(), Interval: interval.String()}
	if len(assignments) == 0 {
		return result, fmt.Errorf("commonfolio-automation-await-terminal found no provisioned graph writer assignments for group %q", writerGroup)
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	timeoutError := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("terminal CommonFolio automation fixture did not converge within %s", timeout)
	}
	for {
		result.Attempts++
		result.Assignments = result.Assignments[:0]
		ready := true
		for i, assignment := range assignments {
			ordinal := intTarget(target, "ordinal", i)
			if ordinal < 0 {
				ordinal = i
			}
			item := CommonFolioTerminalFixtureAssignment{ActorID: assignment.ActorID, Ordinal: ordinal, SpaceID: assignment.SpaceID, DomainID: assignment.DomainID}
			stdout, stderr, listErr := listCommonFolioInvocations(waitCtx, driver, environment, env.NodeRef{Ordinal: ordinal}, assignment, target)
			item.LastResponse = bounded(stdout, 8192)
			if listErr != nil {
				item.LastError = bounded(strings.TrimSpace(stderr+"\n"+listErr.Error()), 4096)
				ready = false
			} else {
				var list commonFolioInvocationList
				if err := json.Unmarshal([]byte(stdout), &list); err != nil {
					item.LastError = "decode invocation list: " + err.Error()
					ready = false
				} else {
					for _, invocation := range list.Invocations {
						if isTerminalAutomationStatus(invocation.Status) {
							item.TerminalInvocations = append(item.TerminalInvocations, CommonFolioTerminalInvocation{ID: invocation.ID, Status: invocation.Status, ClaimVersion: invocation.ClaimVersion, UpdatedAt: invocation.UpdatedAt})
						}
					}
					if len(item.TerminalInvocations) < minimum {
						ready = false
						item.LastError = fmt.Sprintf("found %d terminal invocation(s), want at least %d", len(item.TerminalInvocations), minimum)
					}
				}
			}
			result.Assignments = append(result.Assignments, item)
		}
		if waitCtx.Err() != nil {
			return result, timeoutError()
		}
		if ready {
			result.Ready = true
			return result, nil
		}
		if err := sleepContext(waitCtx, interval); err != nil {
			if waitCtx.Err() != nil {
				return result, timeoutError()
			}
			return result, err
		}
	}
}

func listCommonFolioInvocations(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, node env.NodeRef, assignment provision.ActorAssignment, target map[string]any) (string, string, error) {
	cmd := []string{
		"mycel",
		"--daemon-addr", firstNonEmpty(stringTarget(target, "daemonAddr"), "127.0.0.1:9091"),
		"--username", firstNonEmpty(stringTarget(target, "username"), provision.DefaultAdminUsername),
		"--password", firstNonEmpty(stringTarget(target, "password"), provision.DefaultAdminPassword),
		"--output", "json",
		"automation", "runs",
		"--domain-id", assignment.DomainID,
		"--automation", commonFolioBindingID(target, assignment),
		"--limit", fmt.Sprint(intTarget(target, "runLimit", 100)),
	}
	res, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: cmd})
	return res.Stdout, res.Stderr, err
}

func isTerminalAutomationStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "succeeded", "failed", "skipped", "cancelled", "canceled":
		return true
	default:
		return false
	}
}

func commonFolioAssignments(resources *provision.ScenarioResources, writerGroup string, limit int) []provision.ActorAssignment {
	if resources == nil {
		return nil
	}
	out := make([]provision.ActorAssignment, 0, limit)
	for _, assignment := range resources.Assignments {
		if assignment.GroupName != writerGroup || assignment.BehaviorType != "graph-transaction" || assignment.SpaceID == "" || assignment.DomainID == "" || assignment.PrincipalID == "" {
			continue
		}
		out = append(out, assignment)
		if len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ActorID < out[j].ActorID })
	return out
}

func seedCommonFolioAutomation(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, node env.NodeRef, assignment provision.ActorAssignment, target map[string]any) (string, string, error) {
	procedure, binding, err := commonFolioAutomationJSON(target, assignment)
	if err != nil {
		return "", "", err
	}
	procedurePath := "/tmp/mycel-lab-commonfolio-procedure-" + safePathToken(assignment.ActorID) + ".json"
	bindingPath := "/tmp/mycel-lab-commonfolio-binding-" + safePathToken(assignment.ActorID) + ".json"
	script := "cat > " + shellQuote(procedurePath) + " <<'JSON'\n" + procedure + "\nJSON\n" +
		"cat > " + shellQuote(bindingPath) + " <<'JSON'\n" + binding + "\nJSON\n" +
		strings.Join(commonFolioBaseCLI(target), " ") + " automation procedure put --domain-id " + shellQuote(assignment.DomainID) + " " + shellQuote(procedurePath) + "\n" +
		strings.Join(commonFolioBaseCLI(target), " ") + " automation binding put --domain-id " + shellQuote(assignment.DomainID) + " " + shellQuote(bindingPath) + "\n" +
		strings.Join(commonFolioBaseCLI(target), " ") + " automation runs --domain-id " + shellQuote(assignment.DomainID) + " --automation " + shellQuote(commonFolioBindingID(target, assignment)) + " --limit 20\n"
	res, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: []string{"sh", "-ec", script}})
	return res.Stdout, res.Stderr, err
}

func commonFolioAutomationDiagnostics(ctx context.Context, driver env.EnvironmentDriver, environment env.Environment, node env.NodeRef, assignment provision.ActorAssignment, target map[string]any) (string, string, error) {
	cmd := commonFolioBaseCLI(target)
	script := strings.Join(cmd, " ") + " automation procedure get --domain-id " + shellQuote(assignment.DomainID) + " " + shellQuote(commonFolioProcedureID(target)) + "\n" +
		strings.Join(cmd, " ") + " automation binding get --domain-id " + shellQuote(assignment.DomainID) + " " + shellQuote(commonFolioBindingID(target, assignment)) + "\n" +
		strings.Join(cmd, " ") + " automation runs --domain-id " + shellQuote(assignment.DomainID) + " --automation " + shellQuote(commonFolioBindingID(target, assignment)) + " --limit " + shellQuote(fmt.Sprint(intTarget(target, "runLimit", 50))) + "\n"
	res, err := driver.Exec(ctx, environment, node, env.ExecRequest{Command: []string{"sh", "-ec", script}})
	return res.Stdout, res.Stderr, err
}

func commonFolioAutomationJSON(target map[string]any, assignment provision.ActorAssignment) (string, string, error) {
	procedureID := commonFolioProcedureID(target)
	bindingID := commonFolioBindingID(target, assignment)
	procedureVersion := intTarget(target, "procedureVersion", 1)
	if procedureVersion <= 0 {
		procedureVersion = 1
	}
	procedurePrompt := firstNonEmpty(stringTarget(target, "procedurePrompt"), "Summarize the CommonFolio page entry and return concise text.")
	procedure := map[string]any{
		"id":      procedureID,
		"name":    firstNonEmpty(stringTarget(target, "procedureName"), "CommonFolio-style page summary"),
		"version": procedureVersion,
		"status":  "enabled",
		"input": map[string]any{
			"target": "changed",
			"mode":   "fields",
			"fields": []string{"payload.text", "properties.title", "properties.actor", "properties.transactionSeq"},
		},
		"inference": map[string]any{
			"operation": "chat",
			"profile":   firstNonEmpty(stringTarget(target, "inferenceProfile"), "commonfolio-page-summary"),
			"parameters": map[string]any{
				"responseFormat": "text",
				"maxInputTokens": 12000,
			},
		},
		"prompt": procedurePrompt,
		"output": map[string]any{
			"mode": "text",
			"actions": []map[string]any{{
				"update_node": map[string]any{
					"target": "changed",
					"set": map[string]string{
						"properties.summary": "$result.text",
					},
				},
			}},
		},
	}
	binding := map[string]any{
		"id":                bindingID,
		"name":              "CommonFolio-style entry trigger for " + assignment.ActorID,
		"version":           procedureVersion,
		"procedure_id":      procedureID,
		"procedure_version": procedureVersion,
		"status":            "enabled",
		"scope": map[string]any{
			"space_id":  assignment.SpaceID,
			"domain_id": assignment.DomainID,
		},
		"trigger": map[string]any{
			"type":   "graph_event",
			"events": []string{"node.created", "node.updated"},
			"labels": []string{"pkm.page_entry"},
		},
		"runtime": map[string]any{
			"actor_principal_id":        "automation",
			"owner_principal_id":        assignment.PrincipalID,
			"on_behalf_of_principal_id": assignment.PrincipalID,
			"inference_profile":         firstNonEmpty(stringTarget(target, "inferenceProfile"), "commonfolio-page-summary"),
			"event_origin_override":     "if_present",
		},
		"debounce": map[string]any{
			"duration":   firstNonEmpty(stringTarget(target, "debounce"), "2s"),
			"coalesceBy": "changed",
		},
		"idempotency": map[string]any{
			"scope":                 "target",
			"target":                "changed",
			"inputHashFields":       []string{"payload.text", "properties.title"},
			"skipIfOutputUnchanged": true,
		},
	}
	procedureJSON, err := json.MarshalIndent(procedure, "", "  ")
	if err != nil {
		return "", "", err
	}
	bindingJSON, err := json.MarshalIndent(binding, "", "  ")
	if err != nil {
		return "", "", err
	}
	return string(procedureJSON), string(bindingJSON), nil
}

func commonFolioProcedureID(target map[string]any) string {
	return firstNonEmpty(stringTarget(target, "procedureId"), "commonfolio.page-summary")
}

func commonFolioBindingID(target map[string]any, assignment provision.ActorAssignment) string {
	prefix := firstNonEmpty(stringTarget(target, "bindingPrefix"), "commonfolio.page-summary")
	return prefix + "." + safePathToken(assignment.ActorID)
}

func commonFolioBaseCLI(target map[string]any) []string {
	return []string{"mycel", "--daemon-addr", shellQuote(firstNonEmpty(stringTarget(target, "daemonAddr"), "127.0.0.1:9091")), "--username", shellQuote(firstNonEmpty(stringTarget(target, "username"), provision.DefaultAdminUsername)), "--password", shellQuote(firstNonEmpty(stringTarget(target, "password"), provision.DefaultAdminPassword)), "--output", "json"}
}

func safePathToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return b.String()
}
