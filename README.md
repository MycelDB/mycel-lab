# Mycel Lab

Mycel Lab is a deterministic reliability and benchmarking system for designing,
running, observing, and comparing long-running MycelDB cluster experiments.

## Documents

- [Structured reliability harness design](docs/design/reliability-harness.md)
- [Structured reliability harness implementation plan](docs/implementation/reliability-harness-implementation-plan.md)

## Current commands

```sh
make test
make build-mycel-lab

mycel-lab import --dry-run tests/reliability/
mycel-lab run scenario-file tests/reliability/scenarios/example.yaml --dry-run
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
