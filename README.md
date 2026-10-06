# Mycel Lab

[![License](https://img.shields.io/github/license/MycelDB/mycel-lab)](LICENSE)

Mycel Lab is a deterministic reliability and benchmarking system for designing,
running, observing, and comparing long-running MycelDB cluster experiments.

## Documents

- [Structured reliability harness design](docs/design/reliability-harness.md)
- [Mycel Lab model](docs/design/model.md)
- [Environment driver abstraction design](docs/design/environment-drivers.md)
- [Structured reliability harness implementation plan](docs/implementation/reliability-harness-implementation-plan.md)
- [Environment driver abstraction implementation plan](docs/implementation/environment-driver-abstraction-plan.md)
- [MycelDB system integration migration plan](docs/implementation/system-integration-migration-plan.md)
- [MycelDB system integration migration inventory](docs/implementation/system-integration-migration-inventory.md)
- [Native wrapper transition inventory](docs/implementation/native-wrapper-transition-inventory.md)
- [Operations guide](docs/operations.md)

## Current commands

```sh
make test
make build-mycel-lab

mycel-lab import --dry-run tests/reliability/
mycel-lab run scenario-file tests/reliability/scenarios/example.yaml --dry-run
mycel-lab run suite-file tests/reliability/suites/raft-baseline.yaml --dry-run
# Non-dry runs require local k3d + kubectl and create disposable clusters.
mycel-lab run scenario-file tests/reliability/scenarios/actor-noop.yaml --confirm-destructive
mycel-lab run scenario-file tests/reliability/scenarios/graph-actor-smoke.yaml --confirm-destructive
mycel-lab run scenario-file tests/reliability/scenarios/graph-convergence-smoke.yaml --confirm-destructive
mycel-lab run scenario-file tests/reliability/scenarios/fifty-user-read-write.yaml --confirm-destructive --console-endpoints
mycel-lab run scenario-file tests/reliability/scenarios/three-node-short-outage.yaml --confirm-destructive
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke.yaml --dry-run
mycel-lab run suite compose-cluster-validation --dry-run
mycel-lab run suite k3d-cluster-validation --dry-run
mycel-lab run suite k3d-raft-disruption --dry-run
mycel-lab run suite compose-user-backup-operations --dry-run
mycel-lab run suite compose-user-backup-restore --dry-run
mycel-lab run suite k3d-system-backup-restore --dry-run
mycel-lab run suite k3d-raft-snapshot-pvc-rejoin --dry-run
mycel-lab run suite compose-cluster-soak --dry-run
mycel-lab run suite cluster-release-gate --dry-run
# Compose runs require Docker Compose and a local mycel image compatible with the fixture.
mycel-lab run scenario-file tests/reliability/scenarios/compose-smoke.yaml --confirm-destructive
mycel-lab run suite compose-cluster-validation --confirm-destructive
mycel-lab run suite k3d-cluster-validation --confirm-destructive
mycel-lab run suite k3d-raft-disruption --confirm-destructive
mycel-lab run suite k3d-raft-snapshot-pvc-rejoin --confirm-destructive
mycel-lab run suite compose-cluster-soak --confirm-destructive
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

## License

Mycel Lab is licensed under the [Apache License 2.0](LICENSE).
