# Semantic suite validation checkpoint

Tracking issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

Implementation: [Semantic graph-index reliability suites plan](semantic-graph-index-reliability-suites-plan.md)

## Scope

This checkpoint distinguishes historical local candidate execution from current
harness validation and published-image qualification. Passing a dry-run is
planning evidence only. Neither this record nor the suite name authorizes
publication, issue closure, live-provider spend, or another destructive run.

## Current continuation target: v0.21.4

New work targets the released daemon index:

```text
docker.io/myceldb/mycel@sha256:0973cddd9c282abff40d8edeb2e8f7a801e58d40bfbab4c8507ce2d9daae2dd7
```

Tagged daemon source is `034ae41f78721b112a5b8284593b2652bab12c53`, with
API pin `1182464e29c2fb251a550df0a7e667a34195d1b9`. The canonical scenario
manifest/Compose defaults and semantic CI planning now select this exact image.

The new `compose-semantic-pending-restart` case captures versioned public work
identity/generation/revisions with pending queue and unresolved-gap evidence,
restarts retained data, and requires read-only preservation before normal public
maintenance resumes. It deliberately disables background scheduling and does not
claim an in-flight provider interruption or automatic scheduler recovery. A second
restart requires unchanged completed-work attempts and semantic record identity.
No post-boundary repair backfill, forced rebuild, reset, or reconstruction is
allowed. Unknown capabilities, truncated/ambiguous evidence, falsely fresh work,
changed identity/generation, and replayed attempts fail closed.

The aggregate is now four scenarios / 25 phases rather than three / 20. Its new
v0.21.4 environment-backed acceptance remains **not run** until fresh approval
and execution against an exact reviewed harness. Unit/fake-RPC and planning-only
checks validate the harness, not daemon restart behavior. No hosted-provider run
is part of this continuation.

### Implementation-only validation

On branch `issue/46-v0214-pending-restart`, the working-tree implementation passed:

```sh
GOWORK=off go test ./... -count=1
GOWORK=off go test -race ./... -count=1
GOWORK=off go test -race ./internal/reliability/events -run '^TestSemanticWork' -count=20
GOWORK=off go vet ./...
GOWORK=off go build -trimpath -buildvcs=true ./cmd/mycel-lab
make check-docs check-workflows
git diff --check
```

This includes authenticated fake-gRPC dispatch, read-only preservation, both
work and semantic generation fences, strict frontier/capacity evidence,
malformed/unknown checkpoint rejection, bounded RPCs/listing/polling, privacy-safe
RPC categories, and no retry of an ambiguous analysis mutation. Workflow policy
and all 36 workflow-script tests passed. These are harness regressions, not real
daemon restart or provider-protocol acceptance.

### Planning-only checkpoint

Retained local evidence:
`artifacts/dry-runs/issue46-v0214-pending-20261104T231515Z`.

| Suite | Dry-run scenarios | Planned phases |
| --- | --- | --- |
| `semantic-graph-index-release-gate` | 4 | 25 |
| `semantic-graph-index-recovery` | 2 | 10 |
| `semantic-graph-index-live` | 1 | 4 |

All seven terminal results declare `dryRun: true`; both manifest/Compose image
selections match the v0.21.4 index, and provider-request counts are zero. Artifact
safety passed for seven runs / 151 files. The executable records
`vcs.modified=true`: this is development-harness planning evidence, not a clean
committed candidate, environment-backed qualification, live-provider execution,
or approval for any of those operations. Earlier planning roots are retained.

### Superseding clean v0.21.3 checkpoint

A later clean-source run of Lab `9b42793cff87b86681064197745811ede030f334`
passed all three earlier aggregate scenarios / 20 phases against the immutable
v0.21.3 image. Harness SHA-256:
`91f575d44558cb6621fba09781e6ed613a756f1cd4bc282935cc59ef456a8b7a`.
All nine daemon observations were native ARM64 with clean-source provenance;
final oracles, cleanup/absence, and artifact safety passed. Evidence is retained
under `artifacts/destructive/semantic-graph-index-release-gate-clean-9b42793-20261104T181504Z-buildx`,
including `qualification-summary.md` and `qualification-verification.json`.

This completes the earlier clean-harness qualification follow-up **for that exact
old aggregate and daemon only**. It does not cover pending-work restart,
stale/out-of-order completion, the full protocol failure matrix, this new source,
or v0.21.4. Historical evidence below remains unaltered and must not be
transferred to a successor identity.

## Historical local candidate evidence

The retained local artifact root
`artifacts/destructive/semantic-graph-index-release-gate-20261104T155531Z-issue300-v6`
contains three passing non-dry runs:

| Scenario | Run ID | Result |
| --- | --- | --- |
| Contract | `compose-semantic-graph-contract-20261104-155531` | Passed |
| Recovery | `compose-semantic-graph-recovery-20261104-155556` | Passed |
| Protocol emulator | `compose-semantic-openai-emulator-20261104-155627` | Passed |

The preceding independent recovery rerun also passed:
`artifacts/destructive/semantic-graph-index-recovery-20261104T155452Z-issue300-v6-rerun/compose-semantic-graph-recovery-20261104-155452`.

All daemon containers in the aggregate recorded local image ID
`sha256:68ab5e1451c45e1c70bd22b252d4a3005626cda74ae3cbdb5927c27fc75c74c8`.
The image was `local/mycel:dev`, with dirty source and incomplete provenance,
not the subsequently published v0.21.3 image. These passes demonstrate the local
issue-300 recovery fix, not clean-source release qualification.

The artifact safety verifier was rerun successfully against both roots: three
aggregate runs / 110 files and one recovery run / 36 files, respectively. Artifact
roots are local, ignored evidence and are not included in the source commit.

The older `compose-semantic-graph-recovery-20261104-154035` attempt aborted before
any phase ran and lacks a recorded cause. It must not be mistaken for the latest
recovery result or assigned a cause unsupported by its artifacts.

## Historical harness validation (v0.21.3 tranche)

The resumed changes bound the complete semantic search operation, including RPCs,
pagination, and retry sleeps, by the configured search timeout or earlier parent
deadline. Retries remain opt-in and limited to transient transport/readiness
errors. Expected rejection and assertion failures are not retried. Regression
coverage includes restarting pagination without retaining partial results,
warnings, or stale page tokens. Emulator count assertions reject negative,
fractional, and out-of-range values instead of silently weakening expectations.

All validation commands below passed on the resumed working tree, including the
full race-enabled Go suite and all 36 workflow-policy regression tests:

```sh
go test ./... -count=1
go test -race ./...
go vet ./...
make check-docs check-workflows
git diff --check
```

The published v0.21.3 multiarch index was resolved from the registry as:

```text
docker.io/myceldb/mycel@sha256:9ace9d9cacca8ba3fe0e0cf40eaf281ebd32aca97d6269827f75494ba5ce8352
```

Its ARM64 manifest is
`sha256:d16f38862602c093bb0d4de2fd49ead7fbcc7f4fd35b46223483ddd647b76023`.
Both the three-scenario release-gate aggregate and live-provider **planning only**
passed dry-run resolution using the named immutable index. No hosted-provider
credential was read and no provider call was made. The artifact safety verifier
passed for all four dry runs / 85 files under:

```text
artifacts/dry-runs/semantic-graph-index-v0213-20261104T173322Z
```

## Approved published-image execution

After explicit operator approval, the credential-free aggregate ran against the
immutable v0.21.3 image above on native Linux ARM64. All three scenarios passed:

| Scenario | Run ID | Passed phases |
| --- | --- | --- |
| Contract | `compose-semantic-graph-contract-20261104-174028` | 11 |
| Recovery | `compose-semantic-graph-recovery-20261104-174046` | 5 |
| Protocol emulator | `compose-semantic-openai-emulator-20261104-174117` | 4 |

The retained root is:

```text
artifacts/destructive/semantic-graph-index-release-gate-v0213-20261104T174026Z
```

Each final oracle reports successful observations and required assertions. All
nine daemon-container image observations report the approved immutable index,
`linux/arm64`, completed clean-source provenance, and daemon revision
`b9a2dc7ad6e1774ebae64a5a8933c772d62e9f46`.

Every cleanup report confirms deletion and zero remaining clusters, containers,
networks, or volumes. A post-run Docker check also found no remaining semantic
suite containers or networks. Redaction reports are clean, and the independent
artifact safety verifier passed for all three runs / 112 retained files,
including the CLI log and supplemental source checkpoint. No hosted-provider
scenario ran, no hosted-provider credentials were read, and no hosted-provider
calls were made.

The harness executable SHA-256 shared by all three runs is:

```text
2488b33a9f456bb6da34b24df4d83327ab6c9a082842629a4800754592a430f5
```

It was built by `go run` with Go 1.26.5 from the uncommitted
`issue/46-semantic-suites` working tree based on `f39dc45`. Its build metadata
identifies a development module, not clean VCS provenance. The supplemental
`harness-source-checkpoint.json` records hashes of 284 source files immediately
after execution and before documentation-only result updates. This checkpoint
is not a substitute for a clean-source build. No runtime source or scenario
changed after execution.

The tool wrapper did not yet recognize the new suite name, so execution used the
Lab CLI directly with `--confirm-destructive`, the exact approved image, and a
fresh artifact root. No different canonical suite was substituted.

## Follow-up status

The clean v0.21.3 checkpoint above supersedes the original development-harness
qualification gap. The current expanded v0.21.4 aggregate still needs its own
reviewed-source approval and execution; no earlier pass transfers to it.

Controlled pending-work restart is now defined with harness regressions but lacks
fresh environment-backed acceptance. Stale/out-of-order provider completion,
automatic/in-flight provider recovery, broader provider composition, and the full
protocol error matrix remain actual implementation follow-ups. Nothing in this
record authorizes publication, issue closure, or another destructive/live run.
