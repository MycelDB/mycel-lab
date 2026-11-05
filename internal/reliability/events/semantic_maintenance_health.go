package events

import (
	"errors"
	"regexp"
	"strings"
	"time"

	adminv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/admin/v1"
)

// SemanticMaintenanceHealthV1 describes the node-background signal separately
// from the owner/scoped reliability certificate. Timers and run counters are
// diagnostics only, never proof of freshness or successful indexing.
type SemanticMaintenanceHealthV1 struct {
	SchemaVersion       int    `json:"schemaVersion"`
	Observed            bool   `json:"observed"`
	RPCCode             string `json:"rpcCode"`
	SignalScope         string `json:"signalScope"`
	Enabled             bool   `json:"enabled"`
	Degraded            bool   `json:"degraded"`
	CauseClass          string `json:"causeClass"`
	RecognizedTransient bool   `json:"recognizedTransient"`
	AnalyzerRuns        int32  `json:"analyzerRuns"`
	WorkerRuns          int32  `json:"workerRuns"`
	LastWorkerSuccessAt string `json:"lastWorkerSuccessAt,omitempty"`
	LastWorkerErrorAt   string `json:"lastWorkerErrorAt,omitempty"`
	ThrottleState       string `json:"throttleState"`
	MetadataValid       bool   `json:"metadataValid"`
}

var errSemanticNodeHealthPending = errors.New("semantic node background health has not cleared")

// Match only exact released ownership/startup messages, including a possible
// gRPC Unavailable wrapper. Never treat arbitrary Unavailable/provider/auth text
// or a substring match as authorization to wait. Joined reasons must ALL match.
var maintenanceNoLeader = regexp.MustCompile(`^semantic raft partition for space [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} has no leader$`)
var maintenanceGroupNoLeader = regexp.MustCompile(`^semantic raft partition group [0-9]{1,10} has no leader$`)
var maintenanceOwnerChanged = regexp.MustCompile(`^semantic raft partition leader changed to node [1-9][0-9]{0,19}$`)

func maintenanceCauseClass(degraded bool, reason string) (string, bool) {
	if !degraded {
		if reason != "" {
			return "inconsistent", false
		}
		return "clear", false
	}
	if len(reason) == 0 || len(reason) > 4096 {
		return "unclassified", false
	}
	lines := strings.Split(reason, "\n")
	if len(lines) > 16 {
		return "unclassified", false
	}
	class := ""
	for _, line := range lines {
		line = strings.TrimPrefix(line, "rpc error: code = Unavailable desc = ")
		current := ""
		switch {
		case maintenanceNoLeader.MatchString(line), maintenanceGroupNoLeader.MatchString(line):
			current = "raft-no-leader"
		case line == "semantic operation leadership changed", maintenanceOwnerChanged.MatchString(line):
			current = "raft-ownership-change"
		default:
			return "unclassified", false
		}
		if class == "" {
			class = current
		} else if class != current {
			class = "raft-ownership-transition"
		}
	}
	return class, true
}

func maintenanceTime(value string) (string, bool) {
	if value == "" {
		return "", true
	}
	if len(value) > 64 {
		return "", false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return "", false
	}
	return parsed.UTC().Format(time.RFC3339Nano), true
}

func captureMaintenanceHealth(r *adminv1.GetSemanticMaintenanceStatusResponse) *SemanticMaintenanceHealthV1 {
	e := &SemanticMaintenanceHealthV1{SchemaVersion: 1, Observed: r != nil, SignalScope: "node-background", RPCCode: "OK"}
	if r == nil {
		return e
	}
	e.Enabled, e.Degraded = r.GetEnabled(), r.GetDegraded()
	e.CauseClass, e.RecognizedTransient = maintenanceCauseClass(e.Degraded, r.GetDegradedReason())
	e.AnalyzerRuns, e.WorkerRuns = r.GetAnalyzerRuns(), r.GetWorkerRuns()
	var successOK, errorOK bool
	e.LastWorkerSuccessAt, successOK = maintenanceTime(r.GetLastWorkerSuccessAt())
	e.LastWorkerErrorAt, errorOK = maintenanceTime(r.GetLastWorkerErrorAt())
	switch r.GetThrottleState() {
	case "ok", "throttled":
		e.ThrottleState = r.GetThrottleState()
	default:
		e.ThrottleState = "unknown"
	}
	e.MetadataValid = successOK && errorOK && e.AnalyzerRuns >= 0 && e.WorkerRuns >= 0
	return e
}

func validateMaintenanceHealth(e *SemanticMaintenanceHealthV1) error {
	if !e.Observed {
		return errors.New("semantic node maintenance health is missing")
	}
	if !e.MetadataValid || e.ThrottleState != "ok" {
		return errors.New("semantic node maintenance health metadata is invalid or throttled")
	}
	if e.Degraded && (!e.RecognizedTransient || !e.Enabled) {
		return errors.New("semantic node maintenance health rejected: " + e.CauseClass)
	}
	if !e.Degraded && e.CauseClass != "clear" {
		return errors.New("semantic node maintenance health is contradictory")
	}
	return nil
}
