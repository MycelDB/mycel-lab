# Changelog

All notable changes to Mycel Lab should be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Before `1.0.0`, scenario schemas, reliability suite behavior, and environment-driver behavior may still evolve, but operator-impacting and compatibility-affecting changes should be called out clearly.

## [Unreleased]

### Added

- Added SGT3 public-API semantic resource provisioning, reverse cleanup, finite semantic events, bounded maintenance convergence, and credential-free dry-run plans (#46).
- Added the compose-first SGT4 semantic graph-index contract scenario covering backfill, deterministic ranking/pagination/filtering, projection lifecycle, update/delete, transaction negatives, isolation, disabled rules, and rebuild (#46).
- Added SGT5 Level 3 provenance, analyzed/fresh frontier, bounded-capacity, record-identity, direct pagination, and minimum capability assertions (#46, MycelDB/mycel#238).
- Added the SGT0 proof/compatibility inventory and versioned corpus, semantic-evidence, and final-report schema drafts for provider-neutral semantic graph-index reliability issue #46.
- Added Draft 2020-12 schema compilation tests plus positive and contract-negative fixtures for isolated false-success conditions, operation shapes, artifact classification, and decoder/schema bounds.
- Added strict cataloged `ProviderProfile` definitions, scenario/CLI selection, redacted source-hashed resolution artifacts, typed finite budgets, enforcement-capability preflight, and credential-free deterministic/emulator profiles for SGT1 (#46).
- Added atomic concurrency-safe harness budget accounting, structured artifact/database/error redaction, and a bounded final secret scan whose findings fail the run (#46).
- Added independent `token-basis-v1` vectors and finite script goldens for the SGT2 daemon fixture prerequisite (#46, MycelDB/mycel#237).
- Added `k3d-commonfolio-procedure-replay-recovery`, an artifact-backed negative control for retained-volume replay of an older `procedure.create` over an updated automation procedure (MycelDB/mycel#236).

### Changed

- Definition import/export and catalog storage now include provider profiles; semantic scenarios may use `providerProfileRef` or `--provider-profile`, while required provider-owned limits fail unless their enforcement source is advertised.
- Documentation checks now parse JSON contracts and examples in addition to Markdown and reliability YAML.
- Strengthened the SGT0 contract with typed graph mutation operations, node/edge fixture shapes, explicit minimum-score and artifact-capture safety, Level 1-only baseline examples, bounded decoder annotations, Admin Maintenance lifecycle ownership, direct semantic filter prerequisites, and compose artifact topology.
- CommonFolio automation fixtures can now generate explicit procedure and binding versions and distinct procedure definitions for deterministic create/update replay scenarios.

## [v0.20.0] - 2026-10-20

### Added

- Added `k3d-commonfolio-stale-replay-recovery`, a deterministic retained-PVC reproduction and regression suite for stale automation invocation replay blocking replica admission and partition-group recovery (MycelDB/mycel#233).
- Added terminal CommonFolio automation fixture and authenticated all-replica Raft recovery events with failure diagnostics.

### Changed

- Added strict `--image` validation to override the daemon image in resolved scenarios and suites for exact A/B candidate validation, plus `harness-build.json` provenance in every run.
- Event-driven failures now preserve partial event diagnostics, cluster/oracle reports, all-pod logs, and best-effort previous-container logs.
- Declared final graph convergence/loss assertions now compare live node and edge counts and fail the run; phases can require acknowledged-write and successful-read progress.
- All-replica recovery now requires the exact system-plus-partition Raft group identity set, with missing, duplicate, and unexpected groups reported; terminal-fixture and recovery waits enforce overall context deadlines.
- Log absence assertions can select all matching pods, scan a complete time-bounded log window with bounded retained output, and inspect lines older than the captured artifact tail.
- Updated Lab to use `mycel-go-sdk v0.20.1`, the wire-compatible contract documentation correction for the v0.20 API/SDK line.

### Compatibility

- The stale-replay scenario defaults to daemon `v0.19.3` as a negative control; use `--image myceldb/mycel:v0.20.0` for the coordinated release regression.
- Best used with Mycel daemon `v0.20.0` and API/SDK `v0.20.1`; destructive suites remain explicit and require `--confirm-destructive`.

## [v0.19.2] - 2026-10-13

### Added

- Added `k3d-commonfolio-upgrade-replay`, a CommonFolio-style k3d reliability suite that provisions graph users/spaces/domains, installs page-summary-like automation procedures and bindings, runs graph/read workloads, restarts a pod, performs a StatefulSet image upgrade, and checks replay-blocker log patterns (#39, #40).
- Added CommonFolio automation seeding/diagnostics and log assertion events for scenario-level replay diagnostics (#40).
- Added a reusable StatefulSet image upgrade event with k3d local-image import support for testing locally built daemon images (#40).

### Compatibility

- Best used with Mycel daemon `v0.19.4` or later for the automation and graph replay/idempotency fixes validated by the CommonFolio upgrade replay scenario.

## [v0.19.1] - 2026-10-10

### Added

- Added `k3d-identity-raft-replay-recovery`, a focused k3d regression suite for identity Raft snapshot replay recovery after a duplicate-principal application-error tail entry (#36).

### Changed

- K3d runs now start daemon endpoint port-forwards whenever provisioning needs daemon endpoints, allowing system-event/noop scenarios to provision admin resources without requiring `--console-endpoints` (#36).

### Compatibility

- Best used with Mycel daemon `v0.19.2` or later for the identity Raft replay recovery fix validated by this regression suite.

## [v0.19.0] - 2026-10-09

### Added

- Added native environment-driver reliability suites for compose/k3d validation, restart soak, backup/restore, and snapshot/PVC rejoin workflows.
- Added scenario-specific documentation directories and intent guides for reliability suites.
- Added k3d raft snapshot PVC rejoin validation.

### Changed

- Updated Lab to use the coordinated `mycel-go-sdk v0.19.0` release.
- Migrated legacy shell-centric system integration flows into native Mycel Lab scenarios/suites.
- Updated k3d system backup restore validation to use the daemon offline restore CLI and asynchronous cluster backup API.
- Hardened release-gate preflight checks, failure artifact capture, k3d naming, actor reauthentication, and health convergence handling.

### Compatibility

- Best used with Mycel daemon/API/SDK `v0.19.0` for matching cluster backup, offline restore, and identity scoped access semantics.
- Destructive suites remain explicit and require `--confirm-destructive`; default suite/tooling usage remains dry-run safe.

## Release notes policy

For each release, add a dated section such as:

```md
## [v0.19.0] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
```

Include notes for scenario/schema changes, environment-driver behavior, destructive-run safety, backup/restore workflows, raft/cluster reliability workflows, dependency updates, and compatibility with daemon/API/SDK versions.
