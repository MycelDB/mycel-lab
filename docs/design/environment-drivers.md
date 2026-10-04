# Environment driver abstraction

## Status

Proposed for [mycel-lab#1](https://github.com/MycelDB/mycel-lab/issues/1).

This design documents the first environment-driver abstraction for Mycel Lab. It
is intentionally scoped to the initial local drivers: `dry-run`, `k3d`, and
`compose`. Generic remote Kubernetes, cloud, and local multi-process drivers are
future work.

## Goals

- Keep scenario intent independent from the backend used to execute it.
- Make `dry-run`, disposable `k3d`, and Docker Compose execution use one common
  runner-facing environment contract.
- Let scenarios declare required environment capabilities so unsupported
  backends fail before creating or mutating resources.
- Preserve the existing `k3d` behavior while moving Kubernetes-specific logic
  behind the common interface.
- Add a Compose backend suitable for fast destructive local system validation.
- Keep CLI/API override semantics explicit so a run request can choose a backend
  without editing scenario YAML.

## Non-goals

- Generic remote Kubernetes driver.
- Cloud provider drivers.
- Local multi-process driver.
- Full backup/restore operation parity.
- Full raft disruption harness parity.
- REST control-plane implementation.
- UI implementation.

## Terminology

- **Scenario**: test intent and workload definition.
- **EnvironmentSpec**: scenario or run-request environment selection and
  backend-specific configuration.
- **Environment driver**: Go implementation that maps logical environment
  operations to backend mechanics.
- **Environment handle**: runtime state returned by a created driver instance,
  including node handles and endpoint discovery data.
- **Capability**: named operation or feature that a scenario can require and a
  driver can advertise.
- **Node handle**: logical Mycel daemon node plus backend-specific resource
  identity such as a Kubernetes pod or Compose service.

## Scenario model

Scenarios may declare a default environment:

```yaml
environment:
  driver: compose
  namespace: mycel-lab
  options:
    composeFile: ../mycel/tests/compose/cluster/compose.yml
    projectNamePrefix: mycel-lab
  capabilities:
    required:
      - per-node-endpoints
      - node-restart
      - rolling-restart
      - logs
```

or:

```yaml
environment:
  driver: k3d
  namespace: mycel-lab
  options:
    clusterNamePrefix: mycel-lab
  capabilities:
    required:
      - per-node-endpoints
      - node-exec
      - node-restart
      - rolling-restart
      - logs
```

`dry-run` remains valid and must not mutate external resources:

```yaml
environment:
  driver: dry-run
  capabilities:
    required:
      - planning
      - artifacts
```

The initial `EnvironmentSpec` should keep existing fields compatible and add a
bounded generic options map:

```go
type EnvironmentSpec struct {
    Driver        string                 `yaml:"driver" json:"driver"`
    Namespace     string                 `yaml:"namespace,omitempty" json:"namespace,omitempty"`
    KeepOnFailure bool                   `yaml:"keepOnFailure,omitempty" json:"keepOnFailure,omitempty"`
    Options       map[string]any         `yaml:"options,omitempty" json:"options,omitempty"`
    Capabilities  CapabilityRequirements `yaml:"capabilities,omitempty" json:"capabilities,omitempty"`
}

type CapabilityRequirements struct {
    Required []string `yaml:"required,omitempty" json:"required,omitempty"`
}
```

Known driver option keys should be normalized into typed driver config structs
at runtime. Unknown option keys should fail validation for the selected driver so
misspellings do not silently change test behavior.

## Environment selection precedence

Driver selection must be deterministic:

```text
run request / CLI override > scenario YAML environment > runner default
```

Initial CLI overrides can be minimal and still set the future REST shape:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/example.yaml \
  --environment-driver compose \
  --environment-option composeFile=../mycel/tests/compose/cluster/compose.yml
```

The runner should store both the scenario-declared environment and the resolved
effective environment in run artifacts.

## Driver capabilities

Capabilities are string constants owned by `internal/reliability/env`. Initial
capabilities:

| Capability | Meaning |
| --- | --- |
| `planning` | Driver can produce resolved environment state without external mutation. |
| `artifacts` | Driver can write environment state artifacts. |
| `per-node-endpoints` | Driver can provide one daemon endpoint per logical node. |
| `node-exec` | Driver can execute a command inside/on a logical node. |
| `node-restart` | Driver can restart one logical node. |
| `rolling-restart` | Driver can perform a rolling restart of all daemon nodes. |
| `logs` | Driver can collect node/environment logs. |
| `object-store-fixture` | Driver includes or can expose object-store fixture resources. |
| `volume-replacement` | Driver can replace a node's persistent volume/data volume. |

The first tranche should require only capabilities needed by an actual scenario.
Unsupported future capabilities must produce clear validation errors such as:

```text
scenario requires capability "volume-replacement", but driver "compose" supports: per-node-endpoints,node-restart,rolling-restart,logs
```

## Driver contract

The current `EnvironmentDriver` already has `Preflight`, `Create`, `Delete`, and
`CaptureState`. The new contract should expand that shape rather than replace it
with backend-specific calls elsewhere:

```go
type Driver interface {
    Name() string
    Capabilities() CapabilitySet
    Validate(EnvironmentSpec) error
    Preflight(context.Context, EnvironmentSpec) error
    Create(context.Context, spec.ResolvedScenario, EnvironmentSpec) (Environment, error)
    WaitReady(context.Context, Environment) error
    Nodes(context.Context, Environment) ([]Node, error)
    Endpoints(context.Context, Environment) ([]Endpoint, error)
    Exec(context.Context, Environment, NodeRef, ExecRequest) (ExecResult, error)
    RestartNode(context.Context, Environment, NodeRef) error
    RollingRestart(context.Context, Environment) error
    CaptureState(context.Context, Environment, *artifacts.Sink) error
    Delete(context.Context, Environment) error
}
```

Some methods may return `ErrUnsupportedCapability`; the runner should normally
catch that earlier through capability validation.

### Environment handle

```go
type Environment struct {
    Name      string            `json:"name"`
    Driver    string            `json:"driver"`
    Namespace string            `json:"namespace,omitempty"`
    Context   string            `json:"context,omitempty"`
    Metadata  map[string]string `json:"metadata,omitempty"`
}
```

Driver-specific state that must survive within a run should be represented by
portable metadata keys or by typed internal handles if not serialized. Artifacts
should include `environment/state.json` with enough detail for debugging.

### Node and endpoint handles

```go
type Node struct {
    Name     string            `json:"name"`
    Ordinal  int               `json:"ordinal"`
    Role     string            `json:"role,omitempty"`
    Resource string            `json:"resource"`
    Metadata map[string]string `json:"metadata,omitempty"`
}

type Endpoint struct {
    NodeName   string `json:"nodeName"`
    DaemonAddr string `json:"daemonAddr"`
    Scheme     string `json:"scheme,omitempty"`
}
```

Actors and provisioning should consume driver-discovered endpoints rather than
assuming Kubernetes port-forward addresses. `dry-run` can synthesize endpoints;
`k3d` can continue using port-forwards; `compose` can use host-published ports.

## Runner lifecycle

The runner should follow this backend-neutral lifecycle:

1. Resolve scenario, cluster profile, and effective environment spec.
2. Select driver by effective environment spec.
3. Validate driver options and required capabilities.
4. Run driver preflight.
5. Create or reset environment.
6. Wait for readiness.
7. Discover nodes and daemon endpoints.
8. Write resolved environment artifacts.
9. Provision MycelDB resources using discovered endpoints.
10. Start actors.
11. Execute phase events through driver operations.
12. Run final oracle checks using discovered endpoints.
13. Collect environment logs/state artifacts.
14. Clean up provisioned MycelDB resources.
15. Delete environment unless retained by policy.

Environment deletion should still be best-effort in `defer` cleanup. When
`keepOnFailure` or CLI `--keep-environment-on-failure` is active, deletion may
be skipped after environment creation and artifacts must record the retained
backend resource names.

## Event execution

Phase events should stop being hard-coded to Kubernetes. Event runtime can be
implemented as a thin adapter over driver methods:

| Event | Required capability | Driver method |
| --- | --- | --- |
| `pod-restart` / `node-restart` | `node-restart` | `RestartNode` |
| `pod-stop` | `node-restart` initially | delete/restart semantics mapped by driver |
| `rolling-restart` | `rolling-restart` | `RollingRestart` |

`pod-*` names may remain compatibility spellings for existing scenarios, but new
scenario docs should prefer logical `node-*` terminology where possible.

## Driver behavior

### `dry-run`

- Never creates, modifies, or deletes external resources.
- Supports planning, artifact writing, synthesized nodes, and synthesized
  endpoints.
- Does not support real `node-exec`, `node-restart`, `rolling-restart`, or
  external logs unless a test explicitly models them as dry-run events.
- Must stay the default when `--dry-run` is set, regardless of scenario default.

### `k3d`

- Current disposable local Kubernetes path.
- Creates a `k3d` cluster, imports local image if available, applies rendered
  Kubernetes manifests, waits for StatefulSet/pods readiness, and deletes the
  cluster on cleanup.
- Discovers nodes from pods owned by the `myceld` StatefulSet.
- Exposes endpoints through Kubernetes port-forwards, retaining the existing
  `--console-endpoints` behavior.
- Implements restart operations through `kubectl delete pod` and rolling restart
  through `kubectl rollout restart statefulset/myceld`.
- Captures Kubernetes resources, pod descriptions, pod logs, and driver state.

### `compose`

- Uses Docker Compose for fast local destructive validation.
- Supports a driver option for `composeFile`; if absent, use a repository-local
  MycelDB fixture path only when it can be resolved safely.
- Uses an isolated Compose project name to avoid colliding with developer
  clusters.
- Performs reset/up/wait lifecycle and deletes project resources on cleanup.
- Discovers nodes from configured service names or from Compose labels.
- Exposes endpoints through host-published gRPC ports.
- Implements node restart through `docker compose restart <service>`.
- Implements rolling restart by restarting daemon services sequentially and
  waiting for readiness after each one.
- Captures `docker compose ps`, service logs, and relevant fixture state.
- Reports `object-store-fixture` when the configured Compose stack includes the
  object-store/MinIO services needed by a scenario.

## Artifacts

Environment-related artifacts should be backend-neutral where possible:

```text
environment/state.json
environment/nodes.json
environment/endpoints.json
environment/capabilities.json
environment/logs/<node>.log
environment/backend-state.txt
```

Backend-specific details may use additional files:

```text
environment/kubernetes-resources.txt
environment/pods-describe.txt
environment/compose-ps.txt
environment/compose-config.yaml
```

## Compatibility and migration

- Existing scenarios with `environment.driver: k3d` continue to run.
- Existing `--dry-run` behavior continues to synthesize a dry-run environment.
- The current `kubernetes` alias can continue mapping to the `k3d` driver only
  as a temporary compatibility alias; a real remote `kubernetes` driver is out
  of scope for this issue and should not be implied by docs.
- Existing `pod-stop`/`pod-restart` event names can remain accepted, but design
  docs should describe logical node operations going forward.

## Failure handling

- Unsupported capabilities fail before create/reset.
- Missing required local tools fail in preflight.
- Failed create/reset attempts should clean up partial environments unless
  cleanup itself fails.
- Cleanup failures should be recorded in artifacts and events, but should not
  hide the original run failure.
- Destructive drivers require explicit confirmation unless the run is dry-run.

## Open questions

- Should Compose discovery rely on declared service names in `EnvironmentSpec`
  or inspect Compose labels generated from a template?
- Should `volume-replacement` be added to Compose immediately using Docker
  volumes, or deferred until backup/restore scenarios need it?
- Should `pod-*` event names be renamed to `node-*` with old names as aliases in
  the same tranche, or left for a separate cleanup?
