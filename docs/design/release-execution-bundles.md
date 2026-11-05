# Hermetic release execution bundles

A release execution bundle is the single authoritative input to a release-candidate gate. It binds the candidate identity, harness binary and source tree, every runtime image, configuration files, tools, resource allocation, and execution/finalization bounds before any environment is created.

The v1 schema is [`tests/reliability/release-bundles/v1.schema.json`](../../tests/reliability/release-bundles/v1.schema.json). The Go decoder is strict: unknown fields, duplicate identities, incomplete provenance, paths outside `sourceRoot`, and mutable defaults such as `:dev` or `:latest` are rejected.

## Lifecycle

1. Build the daemon, provider emulator, helpers, and harness from clean exact revisions.
2. Resolve registry references to named `@sha256` digests and record every native platform manifest, config digest, and side-manifest digest. Local images must bind an exact Docker image/config ID and may not use a default tag.
3. Place the bundle above the clean source checkouts. Set `sourceRoot` to the relative workspace and enumerate each exact repository under `sources` with its path, revision, tree, and clean state. The harness identifies its source by name.
4. Hash every selected suite, scenario, cluster, actor profile, provider profile, workflow, runtime configuration, and provenance evidence file. Every file and provenance evidence path names its owning source tree. Bind each scenario's canonical resolved identity from the credential-free dry-run `resolved-scenario.sha256` artifact; this prevents an alternate profile search path from substituting different resolved cluster, actor, provider, phase, or assertion content.
5. Bind tool executable hashes and normalized (trimmed combined-output) version-command hashes, including the Docker Buildx plugin that produces registry evidence.
6. Copy a maintained draft template above the clean `mycel-lab` and `mycel` checkouts, then capture the exact running identities. For example:

   ```sh
   cp mycel-lab/tests/reliability/release-bundles/cluster-release-gate.draft.yaml.tmpl /candidate/release-bundle-draft.yaml
   (cd mycel-lab && go build -trimpath -buildvcs=true -o /candidate/bin/mycel-lab ./cmd/mycel-lab)
   /candidate/bin/mycel-lab release-bundle build \
     /candidate/release-bundle-draft.yaml /candidate/release-execution-bundle.yaml \
     --daemon-image 'docker.io/myceldb/mycel@sha256:<64-hex-digest>' \
     --candidate-id '<approved-candidate-id>'
   mycel-lab/bin/mycel-lab release-bundle validate /candidate/release-execution-bundle.yaml
   ```

   Build the harness outside every bound source checkout so its output cannot dirty a source identity. `build` accepts a lowercase DNS-label candidate ID of at most 32 characters and resolves clean Git revisions/trees, file hashes, driver-specific allocation names, harness/tool identities, anonymous OCI identities, and canonical resolved-scenario hashes. K3d names preserve a stable hash of the complete candidate/scenario allocation inside the length-limited prefix so truncation cannot collapse ordinary candidate identities. It never provisions an environment. The semantic protected workflow uses the adjacent `semantic-live-provider.draft.yaml.tmpl` template.
7. Pass only the bundle—not independent image, provider-profile, environment, port, or retention overrides—to each candidate run:

   ```sh
   ./bin/mycel-lab run suite cluster-release-gate \
     --release-bundle /candidate/release-execution-bundle.yaml \
     --confirm-destructive \
     --artifact-root /candidate/evidence/release-gate
   ```

Dry-runs use the same preflight. They resolve and inspect all images and tools but select the dry-run environment only after bundle validation succeeds.

## Fail-closed preflight

Before driver preflight or provisioning, Mycel Lab:

- verifies the bundle document and every bound file hash;
- verifies the running harness executable, embedded revision/toolchain, clean source tree, and Git tree;
- resolves every tool through `PATH`, verifies its executable and version-output hashes, binds both the Buildx evidence plugin and the Compose runtime plugin, and activates private verified executable snapshots for runtime;
- anonymously inspects each registry image through a temporary empty Docker configuration;
- verifies index, native platform manifest, config, side-manifest, and OCI source/revision/dirty identities;
- verifies local image IDs/config IDs;
- binds the selected scenario and suite source paths;
- snapshots every bound suite, scenario, profile, workflow, provenance, and runtime configuration file into a private read-only tree, then replaces scenario environment/image defaults with bundle allocation, snapshot paths, and immutable references (local provider emulators run by validated image ID, never by a mutable tag);
- rejects any CLI image, provider-profile, environment, console-port, keep-on-failure, or equivalent independent override;
- applies the bundle's outer timeout (at most six hours) and independently bounded finalization stages (at most 15 minutes each), including sensitive-host cleanup. Protected job timeouts reserve the sum of sequential scenario outer bounds, every finalization bound, and workflow overhead so hosted cancellation cannot preempt authoritative cleanup.

A failed preflight writes `release/execution-bundle-preflight.json` and `release/execution-bundle.json` when validation reached run artifact creation. `EnvironmentDriver.Preflight` and `EnvironmentDriver.Create` are never called.

## Image rules

Daemon and helper images must be anonymously pullable registry indexes containing exact native `linux/amd64` and `linux/arm64` manifests. All registry images use complete named digests and enumerate expected platform and side-manifest digests; unexpected manifests fail validation. Each helper also names the runtime environment variable that must resolve to its exact reference, preventing a preflighted helper from being replaced when the environment starts. Each platform config must carry:

- `org.opencontainers.image.source` equal to the bundle source;
- `org.opencontainers.image.revision` equal to the bundle revision;
- `io.myceldb.source.dirty=false`.

The daemon and provider-emulator identities additionally bind build method and build-context SHA-256; every image binds clean source/revision labels and a hashed provenance evidence file. Legacy immutable helper images may omit build-method/context labels, but if either is declared both must be exact and match every platform. Local provider-emulator images bind an exact `sha256:` image/config ID and exact platform; tags ending in `dev`, `latest`, `head`, `main`, `master`, `edge`, or `snapshot` are forbidden.

## Identity and retry semantics

The bundle's `metadata.candidateId` is evidence only for that exact identity. Validation and gate results do not transfer to another bundle or candidate. The preflight is evaluated once per loaded bundle. A preflight failure is terminal for that invocation and does not trigger provisioning, mutation, or an automatic retry.
