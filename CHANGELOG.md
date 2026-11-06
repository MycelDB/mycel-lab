# Changelog

All notable changes to Mycel Lab should be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Before `1.0.0`, scenario schemas, reliability suite behavior, and environment-driver behavior may still evolve, but operator-impacting and compatibility-affecting changes should be called out clearly.

## [Unreleased]

### Added

- Added strict hermetic release execution bundles and maintained protected-gate draft templates that bind clean source trees, harness/tool binaries (including exact Buildx and Compose plugins), registry and local image identities, native and side manifests, provenance evidence, selected configuration files, resource allocation, and outer/finalization bounds; `release-bundle build` captures a reproducible authoritative bundle, protected gates consume it directly with collision-resistant bounded K3d allocation names, and candidate preflight rejects mismatches and mutable/default provider-emulator identities before environment provisioning (#88).
- Added a controlled out-of-order semantic publication scenario: hold an older pinned backfill response until a newer maintenance record is searched, then require superseded skip and unchanged newer record identity. Added bounded local emulator gates and real connection-drop outcomes (#46).
- Added nine finite provider-failure cases for 429/Retry-After, 503, transport loss, malformed/truncated JSON, incorrect dimensions, refusal, deadline, and controlled cancellation. Failure/frontier evidence precedes recovery of the same work and generation; request sequences have no implicit successful fallback (#46).
- Added provider-neutral ordered stage contracts with exact per-stage profile/model/endpoint provenance, typed dataflow, bounded combined duration, and pre-secret/pre-deployment capability rejection. Native v0.21.4 semantic enrichment remains explicitly unsupported. Added a criterion-by-criterion acceptance audit separating harness implementation from runtime qualification (#46).
- Added same-space multi-domain semantic isolation with identical actor/principal/node identity, independent indexes, broad queries without rule/binding filters, exact hydration/provenance, peer-only updates, and retained restart. Added separate selected-model, configured-dimension, and vector-space migration checks requiring generation advancement and fail-closed search before each explicit rebuild. Alternate resources are tracked and cleaned after the owning rule, with isolated nested finalization snapshots (#46).
- Added a controlled semantic pending-work restart scenario and strict version-1 work checkpoints binding public work ID, scope, generation, graph revision range, and attempts. Post-restart preservation checks are read-only and cannot reconstruct missing work; normal maintenance resumes only after preservation is proved. Unknown capabilities, ambiguous/truncated lists, changed identity, false freshness, and replayed attempts fail closed (#46).
- Added a pre-candidate graph-commit restart qualification suite and policy requiring repeated fresh credential-free k3d clusters, one digest-pinned provenance-validated daemon identity, and zero unresolved ambiguous commits before assigning another candidate identity.
- Added SGT10 CI/release integration with credential-free pull-request checks, deterministic/emulator/live-profile dry-runs, protected exact-candidate live and cluster release-gate workflows, immutable action/image inputs, serialized destructive execution, fail-closed sanitized artifact uploads, and credential incident-response guidance (#46).

### Changed

- Canonical semantic scenarios, CI daemon-fixture source, and CI planning target immutable v0.21.4. CI and emulator builds use explicit read-only module resolution outside the workspace. The pending-restart tranche expanded the aggregate to four cases / 25 phases; the completion/failure/isolation/migration batch now defines 16 cases / 65 phases (contract: three cases; recovery: three; protocol: ten). Neither earlier v0.21.3 qualification nor the unexecuted four-case bundle qualifies this new scope. Fresh exact-source environment acceptance remains required (#46).

### Fixed

- Fixed a bounded #46 recovery-wait initialization gap exposed by the failed `dc97cd4` one-shot: reusable endpoint sessions formerly made a single 3-second session-creating Login immediately after restart, and a transient timeout aborted before the declared 60-second recovery poll began. A read-only TCP transport probe may wait within that same 60-second budget; each node then gets **one** explicitly bounded 10-second session initialization, with the existing 3-second read-only Raft/readiness polls, exact groups and stable-poll requirements intact. A timed-out Login is never retried because it may have already created an auth session. Suite-wide preflight rejects impossible per-node initialization budgets before provisioning; unit, fail-closed, ambiguous-login and fixture regressions cover all four affected scenarios. Source-only correction is not a runtime pass or transferred approval.
- Corrected the source-only #46 out-of-order and nine generated fault-matrix provider assertions after failed one-shot `8b13721`: the emulator's `attempt` is cumulative per input digest across work and search, not a work-retry counter. Exact bounded request totals now require the exact safe per-input request-count distribution, with a pre-provisioning consistency check against the per-input cap; public same-work attempt/generation/revision fences, gated ordering, outcome/status checks, and fail-closed cleanup remain unchanged. Historical runtime failure is preserved, the eleven unrun cases remain unqualified, and this source change authorizes no run.
- Back-merged main's hermetic release-bundle implementation into develop without dropping the semantic-suite work. The protected live workflow and bundle draft agree on `semantic-graph-index-live`, and both new semantic gate names retain mandatory bundle validation before provisioning; normal dry-runs remain planning only. This branch reconciliation transfers no prior candidate approval or runtime qualification.
- Fixed the Lab disabled-rule regression exposed by failed qualification `cbc4274`: the intentional negative-control wait now explicitly proves read-only disabled quiescence against captured generation/frontiers/commit and bracketed public rule state, rather than demanding indexing while disabled. Default indexed freshness, health/capacity/rebuild/scope fences, disabled-search rejection and re-enable catch-up remain strict. Quiescence explicitly reports no freshness proof; local regressions do not qualify runtime acceptance or authorize another run (#46).
- Semantic maintenance now distinguishes entry-node background error history from the current owner/scoped reliability certificate. Only exact bounded ownership/startup reasons may converge; node degradation must actually clear, with current same-generation freshness, no gaps and healthy capacity/rebuild state. Health-only waits are read-only; unknown/persistent errors and ambiguous mutations still fail. Safe cause/counter/timestamp evidence is preserved before rejection. Records the failed clean `574e690` restart separately; no runtime rerun or daemon/API change is implied (#46).
- Protocol-emulator connection drops now record their finite sanitized evidence before closing the socket, preventing EOF from racing observation capture. Both embedding and generation routes still produce a real transport error, not an HTTP 503; deterministic ordering checks and immediate-evidence regressions retain strict assertions (#46).
- Semantic pending/fault oracles now match v0.21.4's unresolved-gap `rebuild_required` flag without permitting actual required/running/failed rebuilds, unsafe capacity, false freshness, or inconsistent gap/flag evidence. Rejected status and timeout paths retain bounded safe public diagnostics before validation; unknown state text, provider/error bodies, and foreign identities are excluded. Completion and immutable work checkpoint fences remain strict. Records the failed one-shot 6615c17 qualification separately from these harness regressions (#46).
- Image provenance rejects the nonempty `invalid` build-command sentinel and unknown build identities. Semantic maintenance operation deadlines now bound RPCs as well as polling, reject nil success evidence, and retain only allowlisted diagnostic categories rather than raw error/degradation strings (#46 audit).
- Run finalization now independently bounds actor shutdown, provider-observation capture, cluster assertions, each live graph-oracle assignment, scenario cleanup, and explicit port-forward shutdown even when an operation ignores context cancellation; k3d forwarding remains active through scenario cleanup, late actor/store recording and abandoned artifact writers are disabled before terminal redaction, in-flight cleanup state is isolated without concurrent retries, and mandatory cleanup still produces terminal result and redaction artifacts.
- Graph actors now reconcile ambiguous commit responses with bounded retries of the same durable transaction identity and idempotency key instead of treating every EOF as definitively uncommitted; CI repeats a deterministic committed-response-loss regression 100 times.
- K3d readiness failures now retain bounded Kubernetes resources, pod descriptions, and namespace events before fail-closed image-provenance validation, so mandatory cleanup no longer erases the state needed to diagnose unready workloads.
- K3d environments now support an explicit node-pull policy for digest-pinned registry images instead of passing multiarch index references through k3d's host-local image import path, while defaulting to local import for private and development image compatibility.
- SGT9 cleanup now consumes bounded, complete structured inference-reference details from compatible daemons, proving and retaining immutable audit closures beyond the legacy 32-reference message limit without accepting truncated, malformed, unsupported, duplicate, or unrooted reference sets.
- Protected semantic and cluster release acceptance now preflight immutable, anonymously pullable multiarch MinIO and mc helper images for the native runner architecture before credentials or destructive execution, verify exact upstream source/version provenance, and pass only those preflighted digests into the harness.
- Protected cluster release acceptance now uses a native GitHub-hosted AMD64 runner, fails before checkout unless the kernel and Docker server report AMD64, installs checksum-pinned k3d, and verifies that the immutable multiarch candidate resolves to `linux/amd64` before destructive execution.
- Protected semantic live acceptance now uses a native GitHub-hosted ARM64 runner and fails before checkout or credential use unless both the execution host and Docker server report ARM64, preventing ARM64-only release candidates from failing on the former x64 runner.
- K3d environment creation now waits for the Kubernetes API `/readyz` endpoint before the single manifest apply, preventing transient post-create OpenAPI EOF failures without retrying mutations.
- Updated semantic-budget tests to clone protobuf messages without copying internal mutex state, allowing strict `go vet ./...` to pass in SGT10 CI.
- The SGT9 unauthorized-search control now captures daemon-owned provider-budget evidence through one reusable, directly authenticated administrative observer instead of reusing the semantic controller actor token; the negative actor remains access-token-only with zero login or refresh calls.
- Deferred live semantic-rule creation now proves the exact payload and scoped `semantic.manage` authorization through a bounded read-only validation RPC before the single create attempt. Only transient `Unauthenticated` validation failures retry; semantic mutations are never retried.
- Cross-daemon semantic budget reads now establish one bounded direct admin session on each visited peer instead of presenting a primary daemon's potentially node-local access token to another daemon.
- The SGT9 unauthorized-search control now verifies the pre-provisioned session issued on the negative actor's assigned daemon instead of creating a second Raft-backed login session, remains read-only during the probe, captures before/after daemon budget evidence on authentication and search failures when a scope is configured, explicitly requires that evidence in the live-provider scenario, and retains a safe stage discriminator with aggregate usage and gRPC-category evidence. Credential-free deterministic fixtures exercise the same node-local authorization path without fabricating an unavailable budget scope.
- Live semantic failure artifacts now retain allowlisted gRPC categories without persisting provider inputs, outputs, credentials, or raw error text.

## [v0.21.0] - 2026-10-27

### Added

- Added the manual SGT9 OpenAI-compatible live embedding suite with authenticated semantic-budget capability discovery before credential resolution, synthetic ranking invariants, immutable daemon-attempt reconciliation, explicit third-party endpoint policy, and live-artifact sanitization (#46).
- Added issue #50 provider-neutral procedure-inference acceptance contracts: a credential-free generation emulator, authoritative attempt/run/change polling assertions, completed control-barrier checks, clustered restart recovery, and system backup/restore scenarios.
- Added sanitized process-bound procedure-inference evidence validation and explicit separation between environment acceptance and daemon-only extreme capacity arithmetic.
- Added the SGT6 deterministic k3d semantic recovery suite for follower/leader/sequential restart, pending work, persisted indexes, interrupted rebuild, stale-completion fencing, replica outage, and per-node convergence (#46).
- Added SGT7 bounded credential-free OpenAI-compatible emulators for Compose and k3d, including scripted protocol failures, deadline cancellation, sanitized observation reconciliation, exact emulator image/script evidence, and fail-closed repair scenarios (#46).
- Added SGT8 deterministic advanced semantic matrix coverage for Unicode/long inputs, repeated-embedding tie ordering, complete pagination, concurrent churn, tombstones, finite high-cardinality convergence, and graph-authoritative k3d backup/restore with public rebuild (#46).
- Added fail-closed per-case SGT8 matrix artifacts binding required capabilities, proof level, profile/fixture hashes, negative controls, oracle text, evidence paths, and explicit `passed`, `failed`, or non-success `not-run` verdicts (#46).
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

- Cluster recovery fences can reuse one authenticated SDK session per daemon endpoint, keeping readiness and Raft-group polling read-only after bounded initial login; the Compose procedure-inference gate now records this inspection mode and allows 60 seconds for initial convergence without weakening exact-group, stable-poll, zero-apply-lag, or monotonic-progress assertions.
- Pinned Lab to the exact promoted Mycel Go SDK v0.21 source commit used by the coordinated release gates.
- Compose runs now carry `--image` through the effective environment passed to driver preflight and creation, apply it as a Compose-only `MYCEL_IMAGE` override to every lifecycle command, and require one nonempty resolved image ID for every configured daemon service before capture can succeed, binding semantic acceptance artifacts to an exact-head tag without leaking Compose options into k3d runs (#46).
- Actor-owned space and domain names now include actor-group identity, and scenario validation rejects actor-group names that normalize to the same resource scope, preventing groups that share one profile from collapsing onto the same resources; the deterministic semantic contract also uses a positive near-zero rule cooldown, an operation timeout longer than its maintenance deadline, node-bound incremental revision captures, and a forced initial backfill tied to the final pre-rule pinned snapshot revision so racing historical maintenance cannot retain an earlier same-source provenance revision (#46).
- The SGT4 rebuild phase now creates an eligible semantic record while rebuild state is running and asserts exact pinned-target baseline, delta-record, freshness, generation, and epoch provenance (#46).
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
