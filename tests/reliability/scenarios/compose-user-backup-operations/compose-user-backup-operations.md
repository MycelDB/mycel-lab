# compose-user-backup-operations

## Purpose

Validates the user-backup command surface in a live Compose cluster: export a writer principal's data, validate the archive, and import it back through the daemon. This scenario is focused on operation semantics and archive handling, not full disaster recovery.

## What this test proves

- User backup export produces a usable archive from live graph data.
- Archive validation catches format/metadata problems before import.
- Import can replay the exported payload through the cluster without corrupting active state.

## Topology

```mermaid
flowchart LR
  actor0["graph-writers actors"] --> entry["Mycel endpoint"]
  entry --> svcA["myceld-a Compose service"]
  entry --> svcB["myceld-b Compose service"]
  entry --> svcC["myceld-c Compose service"]
  svcA <--> svcB
  svcB <--> svcC
  svcA <--> svcC
```

## Scenario phases

1. **write-source-fixture**. Duration: `2s`. This phase establishes baseline actor data before disruption or verification.
2. **export-validate-import**. Duration: `1s`. Event(s): `user-backup-export, user-backup-validate, user-backup-import`. This phase executes `user-backup-export, user-backup-validate, user-backup-import` while preserving workload/oracle evidence.
3. **post-import-stability**. Duration: `1s`. This phase runs the configured actors at the phase rate and records progress evidence.

## Actors

| Actor group | Profile | Count | Rate knobs | Role in this scenario |
|---|---|---:|---|---|
| `graph-writers` | `graph-committer` | `1` | `commitsPerSecond` = `4` | writes graph transactions |

## Tunable parameters

| Parameter | Current value/source | Why tune it |
|---|---|---|
| `seed` | `27272` | Change only when intentionally exploring a different deterministic schedule. |
| `environment.driver` | `compose` | Selects the infrastructure backend; changing it changes failure semantics. |
| `environment.namespace` | `mycel-lab` | Use a unique namespace/project when running concurrent destructive tests. |
| `clusterRef` | `raft-3-node` | Changes node count, raft placement, image, resources, and storage defaults. |
| `actorGroups[].count` | YAML actor group values | Increase for more concurrency pressure; decrease for faster local debugging. |
| `actorGroups[].rate` | YAML actor group rates | Increase to stress write/read paths; decrease to isolate environment failures. |
| `phases[].duration` | YAML phase durations | Lengthen to catch timing-sensitive bugs; shorten for smoke/debug loops. |
| `phases[].events[].target` | `user-backup-export, user-backup-import, user-backup-validate` targets | Adjust the disrupted node, restart cadence, backup path, snapshot options, or verification timeout. |

## Evidence and artifacts

- `result.json` is the first stop: it records phase status, duration, and terminal pass/fail state.
- `events.jsonl` shows disruption/backup/snapshot events and their structured payloads.
- `resolved-scenario.json` confirms the exact cluster profile, actor profiles, and overrides used for the run.
- `summary.md` gives a short human-readable run report suitable for PR comments.
- `environment/compose-config.yaml`, `environment/compose-ps.txt`, and service logs explain Compose-side failures.
- `oracle/` artifacts, when present, contain final consistency and cluster-health evidence.

## Common failure modes

- Docker Compose unavailable, stale projects, or port collisions prevent environment creation.
- Service logs or `environment/compose-config.yaml` disagree with the expected service/port mapping.
- Actor authentication or provisioning failures usually point to bootstrap credentials, principal setup, or endpoint routing.
- Graph correctness failures should be debugged from `actors/`, `events.jsonl`, and `oracle/` before rerunning.
- If a destructive run fails during cleanup, confirm disposable resources were removed before starting another run.

## When to run

Run after changes to user backup CLI/API behavior, archive compression, graph export/import, or Compose backup fixtures.

## Related files

- YAML: [`compose-user-backup-operations.yaml`](./compose-user-backup-operations.yaml)
- Suites referencing this scenario live under `tests/reliability/suites/`.
