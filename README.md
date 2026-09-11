# Mycel Lab

Mycel Lab is a deterministic reliability and benchmarking system for designing,
running, observing, and comparing long-running MycelDB cluster experiments.

## Documents

- [Structured reliability harness design](docs/design/reliability-harness.md)
- [Structured reliability harness implementation plan](docs/implementation/reliability-harness-implementation-plan.md)
- [Operations guide](docs/operations.md)

## Current commands

```sh
make test
make build-mycel-lab

mycel-lab import --dry-run tests/reliability/
mycel-lab run scenario-file tests/reliability/scenarios/example.yaml --dry-run
mycel-lab run suite-file tests/reliability/suites/raft-baseline.yaml --dry-run
mycel-lab run scenario-file tests/reliability/scenarios/actor-noop.yaml --confirm-destructive
mycel-lab run scenario-file tests/reliability/scenarios/graph-actor-smoke.yaml --confirm-destructive
mycel-lab run scenario-file tests/reliability/scenarios/graph-convergence-smoke.yaml --confirm-destructive
mycel-lab run scenario-file tests/reliability/scenarios/three-node-short-outage.yaml --confirm-destructive
mycel-lab run suite raft-reliability-baseline --dry-run
```

Catalog commands require Postgres via `--database-url`, `MYCEL_LAB_DATABASE_URL`,
`MYCEL_RELIABILITY_DATABASE_URL`, or `DATABASE_URL`:

```sh
mycel-lab db migrate
mycel-lab db status
mycel-lab import tests/reliability/
mycel-lab list scenario
mycel-lab export scenario example --version latest
```
