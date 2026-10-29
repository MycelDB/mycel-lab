# CI and protected reliability execution

Mycel Lab SGT10 separates credential-free pull-request validation from explicitly approved destructive or hosted-provider runs. Repository workflows are execution controls, not authorization: protected GitHub environments must require the maintainers designated by the release process.

## Workflow tiers

| Workflow | Trigger | Capability tier | Credentials | Mutation |
| --- | --- | --- | --- | --- |
| `CI` | pull request; pushes to `develop` and `main` | Tier 0 unit/contract and Tier 1/live planning | none | dry-run only |
| `Protected semantic live acceptance` | `workflow_dispatch` | Tier 2 manual live | `semantic-live` environment | disposable Compose resources and bounded provider calls |
| `Protected cluster release gate` | `workflow_dispatch` | release gate | none | disposable Compose and k3d resources |

Default CI runs unit, race, vet, documentation, workflow-policy, and build checks. It also dry-runs the deterministic semantic contract, protocol-emulator procedure control, recovery, backup/restore, and live-profile plans. Dry-run evidence is uploaded only after every retained run reports a clean redaction scan.

The live and release-gate workflows share one non-cancelling concurrency group. They cannot overlap or be silently cancelled after destructive execution begins.

## Required repository configuration

Configure these GitHub environments outside the workflow files:

- `semantic-live`
  - require explicit maintainer approval;
  - allow only protected branches or reviewed release branches;
  - store `OPENAI_API_KEY` as an environment secret;
  - prevent self-review where organization policy supports it.
- `release-gate`
  - require explicit maintainer approval;
  - allow only protected branches or reviewed release branches;
  - do not configure hosted-provider credentials.

The protected semantic workflow uses the GitHub-hosted `ubuntu-24.04-arm` runner so the release candidate's `linux/arm64` image executes natively. Before checkout or credential use, it verifies that both the runner kernel and Docker server report ARM64. Do not substitute an x64 runner or rely on ambient emulation for an ARM64-only candidate.

The release-gate workflow uses GitHub-hosted `ubuntu-24.04` to execute the candidate's `linux/amd64` image natively. Before checkout or action use, it verifies that both the runner kernel and Docker server report AMD64. It installs the exact k3d v5.9.0 AMD64 binary only after validating its pinned SHA-256 checksum, then verifies Docker, Compose, k3d, kubectl, and the selected daemon image's `linux/amd64` identity before destructive execution. Do not substitute ARM emulation or a self-hosted runner with an ARM64 Docker server.

Repository workflow permissions remain `contents: read`. Checkout credentials are not persisted. Every referenced action is pinned to a full commit SHA.

## Exact candidate inputs

Protected workflows accept:

- a full 40-character daemon commit SHA;
- a pullable daemon image reference containing `@sha256:<64 lowercase hex>`;
- an operator-supplied approval reason.

Before execution, the workflow verifies the checked-out daemon commit and the image's clean-source OCI labels. The semantic workflow selects and verifies the ARM64 image while the cluster release gate selects and verifies the AMD64 image from the same immutable multiarch index. A tag-only image, missing required platform, dirty build, revision mismatch, missing source label, or mutable daemon ref fails before the harness starts.

The live workflow additionally verifies the locked OpenAI-compatible provider-profile hash and these hard ceilings:

- 128 requests and operations;
- 20,000 input tokens;
- five minutes total provider time;
- USD 1.00 estimated cost.

The credential is scoped to the single harness step. It is never supplied to pull-request jobs, dry-runs, artifact validation, or artifact upload.

## Artifact upload policy

`verifyArtifactSafety.py` fails closed unless it finds at least one terminal `result.json` and every run has a successful `redaction-scan.json` with zero findings. It also rejects symlinks, non-regular files, files above the 64 MiB safety bound, and common API-key, bearer-token, or PEM patterns added after harness finalization. Only evidence that passes these checks can reach `actions/upload-artifact`. The checker writes a SHA-256 inventory into the upload root.

A product or infrastructure failure may still be uploaded when its redaction scan is clean. Evidence is not uploaded when execution ends before a terminal redaction report or when any sensitive finding exists. GitHub job logs remain subject to GitHub secret masking and must not intentionally print environment values, provider headers, or request bodies.

## Credential incident response

If a credential may have appeared in output or artifacts:

1. cancel queued jobs without deleting local evidence needed for diagnosis;
2. disable or rotate the environment secret immediately;
3. restrict or delete the affected GitHub artifact according to organization policy;
4. preserve non-sensitive run IDs, workflow identity, candidate commit/image digest, and timestamps;
5. inspect the redaction report and workflow logs without copying the suspected value;
6. file a security incident and correct the redactor or collection path through normal review;
7. do not retry the live run under the consumed approval;
8. require fresh candidate/gate approval when the correction changes execution identity.

## Promotion policy

Tier 2 live execution is manual and non-gating by default. Promoting it to scheduled or required status needs a measured reliability window, documented provider quota, explicit maintainer approval, and unchanged hard ceilings. A provider outage remains a failure or separately classified infrastructure result; it must never be rewritten as product success.

Stable release tags and publication are outside these workflows. Passing SGT10 or the cluster release gate does not itself authorize tagging, publication, issue closure, or Homebrew updates.
