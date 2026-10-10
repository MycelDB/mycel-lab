# Changelog

All notable changes to Mycel Lab should be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Before `1.0.0`, scenario schemas, reliability suite behavior, and environment-driver behavior may still evolve, but operator-impacting and compatibility-affecting changes should be called out clearly.

## [Unreleased]

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
