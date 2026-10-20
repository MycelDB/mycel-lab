# Provider profiles, budgets, and artifact safety

Status: SGT1 foundation for issue #46.

## Contract

A `ProviderProfile` is a versioned reliability definition:

```yaml
apiVersion: reliability.mycel.io/v1alpha1
kind: ProviderProfile
metadata:
  name: deterministic-token-basis-v1
mode: deterministic # deterministic | emulator | live
capabilities:
  embedding:
    connectorType: fake
    endpointKey: mycel-fixture
    modelKey: mycel/token-basis-v1-8
    dimensions: 8
    vectorSpaceKey: mycel/token-basis-v1/8
    fixtureStrategy: token-basis-v1
    batching: {mode: single, maxItems: 1}
budgets:
  enforcement: required # required | advisory
  maxOperations: 4096
  maxConcurrentOperations: 16
  requestTimeout: 5s
  totalTimeout: 10m
artifacts:
  captureBodies: false
  captureHeaders: false
```

Profiles live under `tests/reliability/provider-profiles/`. They are strict: unknown fields, unsupported versions, invalid dimensions, unbounded orchestration, unsupported declared capabilities, and inconsistent mode/secret combinations fail resolution. Profile documents are byte-bounded, fixed-depth typed structures.

Capability slots are independent. The v1alpha1 harness resolves embeddings. Generation, enrichment, structured-output, and routing slots are reserved but fail as unavailable when declared. Deterministic mode supports the daemon's explicit `fake`/`token-basis-v1` contract. Emulator and live embedding profiles currently support `openai-compatible`; Anthropic and OpenRouter are not inferred from provider names and remain unsupported until matching connectors and pipelines exist.

A scenario selects a profile with `providerProfileRef`. `--provider-profile <name-or-path>` overrides only that selection. Resolution records the selection source and SHA-256 of the exact profile bytes in `provider/provider-profile-resolved.json` and the resolved scenario.

## Credentials

Profiles contain only references such as:

```yaml
secretRef:
  env: OPENAI_API_KEY
```

Deterministic and emulator profiles reject secret references. Live execution resolves referenced environment variables only after non-secret profile and enforcement validation. Dry-runs never read credential values. Resolved JSON, catalog records, events, metrics, errors, and artifacts never include resolved values.

## Budgets and enforcement

Harness-owned limits are enforced by the concurrency-safe reservation ledger:

- total and concurrent orchestration operations;
- per-request and total deadlines.

Provider-owned limits are declared independently:

- requests and concurrent requests;
- retries per request;
- aggregate input/output tokens;
- optional estimated USD cost;
- capability-specific batch size.

A required limit resolves only when the responsible daemon/provider capability is advertised by trusted daemon/provider preflight. The bundled `openai-compatible-live` profile is an SGT1 contract fixture and intentionally cannot run until SGT1b adds that trusted discovery and enforcement path. Operators cannot assert enforcement with a CLI flag. A field is never labeled enforced merely because it exists. Advisory unavailable fields are recorded as `advisory`; required unavailable fields fail closed. SGT1b remains responsible for atomic daemon-side provider reservations and durable usage accounting.

Reservations are atomic. Each provider-profile run owns one ledger and total-deadline context. Scenario provisioning, each finite runtime event, and each actor graph operation reserve before work; later semantic admin/search operations must use the same gate as they are added. Exhaustion cancels scheduling, closes the ledger to new work, fails the run, and persists `provider/budget-final.json`. Releasing a reservation reduces only in-flight concurrency; cumulative use is never refunded. Each operation context uses the earlier of request and total deadlines.

## Redaction and final gate

Every runner artifact write passes through structured redaction. Sensitive map values, configured secrets embedded in map keys, CLI argument values, authorization/cookie values, API-key patterns, configured runtime secret values, and PEM material are replaced before persistence. Secret-bearing keys become `[REDACTED-KEY]`; a resulting key collision fails closed. The same sanitized payload is used for database persistence.

Finalization performs a bounded recursive artifact scan. `redaction-scan.json` contains only path, detector category, and byte offsets. Files with findings are removed and replaced by a fixed quarantine marker. If the tree cannot be completely scanned, the run artifact root is removed and recreated before the failure report is written. A finding, quarantine failure, unreadable artifact, symlink, or scan-limit violation fails the run. Provider body/header capture is disabled in this contract.

## Deterministic fixture

`tests/reliability/testdata/semantic/v1alpha1/fixtures/token-basis-v1-goldens.json` independently freezes vectors and finite scripts. The harness golden test duplicates only the published algorithm and imports no daemon internals. Daemon behavior is documented in `mycel/docs/design/admin/deterministic-inference-fixtures.md`.
