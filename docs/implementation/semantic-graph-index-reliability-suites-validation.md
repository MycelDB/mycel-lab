# Semantic suite validation checkpoint

Tracking issue: [MycelDB/mycel-lab#46](https://github.com/MycelDB/mycel-lab/issues/46)

Implementation: [Semantic graph-index reliability suites plan](semantic-graph-index-reliability-suites-plan.md)

## Scope

This checkpoint distinguishes historical local candidate execution from current
harness validation and published-image qualification. Passing a dry-run is
planning evidence only. Neither this record nor the suite name authorizes
publication, issue closure, live-provider spend, or another destructive run.

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

## Current harness validation

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

## Remaining qualification and follow-up

The current development harness has now passed real execution against the
published v0.21.3 image. Clean-source harness qualification still requires an
exact committed build and fresh approval/execution under the release process;
this development pass must not be transferred to that later identity. Nothing
here authorizes publication or another destructive run.

Pending-work restart, stale/out-of-order provider completion, and the broader
protocol error matrix remain follow-up scope; the current recovery and emulator
scenarios must not be described as covering those cases.
