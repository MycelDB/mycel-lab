# Mycel Lab operations

Mycel Lab runs deterministic reliability scenarios for MycelDB clusters. The
current implementation supports local dry-run execution, catalog persistence in
Postgres, filesystem artifacts, baseline scenarios, metrics summaries, and run
listing/show/compare commands.

## Database setup

Catalog and run history commands require Postgres. Provide the database URL with
one of:

```sh
export MYCEL_LAB_DATABASE_URL='postgres://user:pass@localhost:5432/mycel_lab?sslmode=disable'
# or pass --database-url on each command
```

Apply migrations and check status:

```sh
mycel-lab db migrate
mycel-lab db status
```

Integration tests use `MYCEL_RELIABILITY_TEST_DATABASE_URL` or
`MYCEL_LAB_TEST_DATABASE_URL` and skip when neither is set.

## Import and export

Definitions are authored as YAML under `tests/reliability/` and imported into
Postgres for operational use:

```sh
mycel-lab import --dry-run tests/reliability/
mycel-lab import tests/reliability/
mycel-lab list scenario
mycel-lab export scenario one-node-graph-smoke --version latest > scenario.yaml
```

Definition lifecycle rules:

- new definitions create version `1`;
- unchanged imports are no-ops;
- changed latest definitions update in place until used by a run;
- after a definition is used by a run, changed imports create the next version;
- only unused definition versions can be deleted.

## Running scenarios

Run a scenario by file path:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/one-node-graph-smoke.yaml --dry-run
```

Run a baseline scenario by name from `tests/reliability/scenarios/`:

```sh
mycel-lab run scenario one-node-graph-smoke --confirm-destructive
```

Mycel Lab supports three initial environment drivers:

- `dry-run` — validate planning, rendering, phases, actors, and artifacts without external mutation;
- `k3d` — create a disposable local k3d/Kubernetes cluster;
- `compose` — create/reset a local Docker Compose project.

A run request can override the scenario default without editing YAML:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke.yaml \
  --dry-run \
  --environment-driver compose \
  --environment-option composeFiles=../mycel/tests/compose/cluster/compose.yml,tests/compose/ports.yml
```

Non-dry-run execution for `environment.driver: k3d` creates a disposable k3d
cluster, applies the rendered MycelDB Kubernetes manifests, waits for the
StatefulSet pods to become ready, runs phase events through the environment
driver, captures Kubernetes state/log artifacts, and deletes the cluster during
cleanup. The rendered daemons run in `MYCELD_MODE=mesh` with recognized raft
node addresses and per-pod raft local node IDs derived from StatefulSet
ordinals.

Use `--console-endpoints` to expose every daemon node as a stable local Mycel
Console endpoint for the duration of a run. For `k3d`, the lab renders one
Kubernetes service per pod (`myceld-0-client`, `myceld-1-client`, ...), starts
`kubectl port-forward` processes, prints the connection table, and writes
`environment/console-endpoints.json` into the run artifacts. For `compose`, the
driver uses the configured/published host ports. The default local ports start
at `19091`; override the base with `--console-port-base` where supported.

```sh
mycel-lab run scenario raft-3-node-short-outage \
  --confirm-destructive \
  --console-endpoints
```

For a three-node scenario, connect Mycel Console to:

```text
myceld-0  127.0.0.1:19091
myceld-1  127.0.0.1:19092
myceld-2  127.0.0.1:19093
```

Lab clusters use fixed local-only bootstrap credentials so each endpoint can be
monitored consistently:

```text
username: admin
password: admin-password
```

Required local tools for `k3d`:

```sh
k3d version
kubectl version --client=true
```

Required local tool for `compose`:

```sh
docker compose version
```

A Compose-backed smoke scenario is available for planning and local destructive
validation:

```sh
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke.yaml --dry-run
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke.yaml --confirm-destructive
```

Migrated system-integration validation suites are available for Compose and
k3d-backed local clusters:

```sh
mycel-lab run suite compose-cluster-validation --dry-run
mycel-lab run suite k3d-cluster-validation --dry-run

mycel-lab run suite compose-cluster-validation --confirm-destructive
mycel-lab run suite k3d-cluster-validation --confirm-destructive
```

These suites validate shared cluster identity/health, graph data-plane behavior,
and rolling restart recovery. Additional migrated suites cover disruption,
backup/restore, and soak/release-gate entrypoints:

```sh
mycel-lab run suite k3d-raft-disruption --dry-run
mycel-lab run suite k3d-raft-sensitive-gate --dry-run
mycel-lab run suite compose-user-backup-operations --dry-run
mycel-lab run suite compose-user-backup-restore --dry-run
mycel-lab run suite k3d-system-backup-restore --dry-run
mycel-lab run suite compose-cluster-soak --dry-run
mycel-lab run suite cluster-release-gate --dry-run
```

Destructive/operator variants use the same suite names with
`--confirm-destructive`. Restart-soak suites are native Mycel Lab k3d scenarios
with repeated rotating `node-restart` events. `compose-user-backup-operations`
is a native Compose smoke suite for `user-backup-export`,
`user-backup-validate`, and `user-backup-import` operations through the
environment driver. `compose-user-backup-restore` is the full native Compose
backup/restore suite with fixture creation, archive staging across a fresh reset,
restored-data checks, and safety assertions. `k3d-system-backup-restore` remains
a constrained `host-command` wrapper while k3d volume/PVC restore parity is being
implemented. The parity requirements for replacing the remaining wrapper are
tracked in the [native wrapper transition inventory](implementation/native-wrapper-transition-inventory.md).
The k3d cluster-validation suite intentionally does not yet include the legacy
one-PVC replacement/rejoin step; that remains blocked on a dedicated
volume-replacement capability.

## Running suites

Run a suite by file path or by name from `tests/reliability/suites/`:

```sh
mycel-lab run suite-file tests/reliability/suites/raft-reliability-baseline.yaml --dry-run
mycel-lab run suite raft-reliability-baseline --dry-run
```

Suites execute scenarios sequentially and honor `stopOnFailure`.

## Interpreting results

Each run writes a filesystem artifact directory containing:

- `resolved-scenario.json` — full resolved scenario snapshot;
- `events.jsonl` — run, phase, actor, and pod lifecycle events;
- `metrics.jsonl` — metric samples such as commits and latency placeholders;
- `manifests/myceld.yaml` — rendered Kubernetes manifests;
- `environment/state.json` — captured environment state;
- `environment/capabilities.json` — selected driver capabilities;
- `environment/nodes.json` — logical daemon nodes discovered by the driver;
- `environment/endpoints.json` — per-node daemon endpoints discovered by the driver;
- `environment/console-endpoints.json` — per-node Console endpoints when `--console-endpoints` is enabled;
- `result.json` — terminal run status and phase status;
- `summary.md` — human-readable summary.

Persisted runs can be inspected when run storage is enabled:

```sh
mycel-lab runs list
mycel-lab runs show <run-id>
mycel-lab runs compare <run-a> <run-b>
```

## Retaining environments for debugging

Use `--keep-environment-on-failure` with scenario execution to preserve the
environment after a failed run. Destructive environment operations require
`--confirm-destructive`; this prevents accidental mutation while preserving a
safe dry-run mode.

For `k3d`, the generated cluster name starts with `mycel-lab-`, and the
Kubernetes context is `k3d-<cluster-name>`. If a process is interrupted before
cleanup, inspect and remove clusters manually with:

```sh
k3d cluster list
k3d cluster delete <cluster-name>
```

For `compose`, the generated Compose project name starts with `mycel-lab-` by
default. If cleanup is interrupted, inspect and remove the project with:

```sh
docker compose -p <project-name> ps
docker compose -p <project-name> down --volumes --remove-orphans
```

## Safety notes

- Prefer `--dry-run` when validating YAML, profiles, suites, or generated
  manifests.
- Use disposable clusters and namespaces for destructive scenarios.
- Keep secrets in environment variables or local `.env` files; `.env*` files are
  ignored by git.
- Treat generated artifacts as local runtime output; `artifacts/` and `results/`
  are ignored by git.
