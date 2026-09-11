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

Current non-dry-run execution uses the local harness foundations and dry-run
cluster driver; real k3d/Kubernetes mutation remains behind the environment
driver boundary.

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

## Safety notes

- Prefer `--dry-run` when validating YAML, profiles, suites, or generated
  manifests.
- Use disposable clusters and namespaces for destructive scenarios.
- Keep secrets in environment variables or local `.env` files; `.env*` files are
  ignored by git.
- Treat generated artifacts as local runtime output; `artifacts/` and `results/`
  are ignored by git.
