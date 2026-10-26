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
  requestTimeout: 45s
  totalTimeout: 10m
artifacts:
  captureBodies: false
  captureHeaders: false
```

Profiles live under `tests/reliability/provider-profiles/`. They are strict: unknown fields, unsupported versions, invalid dimensions, unbounded orchestration, unsupported declared capabilities, and inconsistent mode/secret combinations fail resolution. Profile documents are byte-bounded, fixed-depth typed structures.

Capability slots are independent. The v1alpha1 harness resolves exactly one capability required by the scenario: semantic scenarios select embedding, while procedure-inference scenarios select generation. Other declared slots fail closed rather than being ignored. Deterministic mode supports the daemon's explicit embedding `fake`/`token-basis-v1` contract. Emulator and live embedding or generation profiles support `openai-compatible`; Anthropic and OpenRouter are not inferred from provider names and remain unsupported until matching connectors and pipelines exist.

A scenario selects a profile with `providerProfileRef`. `--provider-profile <name-or-path>` overrides only that selection. Resolution records the selection source and SHA-256 of the exact profile bytes in `provider/provider-profile-resolved.json` and the resolved scenario.

## Credentials

Profiles contain only references such as:

```yaml
secretRef:
  env: OPENAI_API_KEY
```

Deterministic and emulator profiles reject secret references. Live execution first calls the authenticated, space-scoped daemon capability RPC and verifies every semantic budget invariant; only then does it resolve referenced environment variables. The runtime redactor is extended before the credential is submitted to the daemon. Dry-runs never read credential values. Resolved JSON, catalog records, events, metrics, errors, and artifacts never include resolved values or the secret reference structure.

## Local protocol emulator scripts

An emulator embedding or generation capability may include a bounded `emulatorScript` (`version: v1`). The schema accepts only a finite request ceiling and a finite digest-keyed outcome sequence. Inputs are selected by SHA-256; source text is never part of the profile. Supported OpenAI-compatible outcomes are success, HTTP error, malformed JSON, truncated JSON, refusal, and wrong dimensions, with bounded delay and `Retry-After` values. Safe request/model identity fields reject control characters and whitespace.

The dedicated `mycel-provider-emulator` command serves only `/healthz`, `/v1/embeddings`, and `/v1/chat/completions`. Compose gives it no host port; k3d uses an internal ClusterIP Service. The environment driver records the exact runtime image ID, script hash, bounded logs, and sanitized JSONL observations. An observation contains method, path, content length, capability, bounded model identities, sequence, attempt, batch count, body/input digests, outcome/status, cancellation, and authorization presence plus an allowlisted `Bearer`/`Basic`/`Other` scheme classification—never body text, vectors, generation output, authorization/arbitrary header values, or credentials. Generation observations also omit provider request IDs. Scenario assertions require contiguous sequence numbers, valid digests, declared batching, bounded attempts/counts, and exact or minimum safe protocol categories.

## Budgets and enforcement

Harness-owned limits are enforced by the concurrency-safe reservation ledger:

- total and concurrent orchestration operations;
- per-request and total deadlines.

Provider-owned limits are declared independently:

- requests and concurrent requests;
- aggregate retry attempts and retries per request;
- per-request and aggregate input tokens;
- request and total provider deadlines;
- versioned estimated USD cost in integer micros;
- capability-specific batch size.

A required limit resolves only when the responsible capability is returned by authenticated daemon discovery. The bundled `openai-compatible-live` profile is the manually runnable SGT9 direct-embedding profile; it remains `pending_authenticated_discovery` during dry-run and becomes `enforced` only after the live daemon proves daemon-owned Raft-replicated durable scopes, pre-provider reservations, retry/concurrency/selected-batch-mode/token/timeout/versioned-cost limits, conservative ambiguity, immutable attempt events, overage exhaustion, and bounded pages. The live endpoint is parsed and restricted to the explicitly approved `https://api.openai.com/v1` route (HTTPS, no userinfo, query, or fragment) before any credential lookup. Operators cannot assert enforcement with a CLI flag. Advisory unavailable fields are recorded as `advisory`; required unavailable fields fail closed before credential resolution.

Harness reservations remain atomic for orchestration. For semantic provider effects, the daemon creates an immutable route-pinned scope before an enabled budgeted rule is admitted, reserves before credential resolution and connector access, and appends a version-1 reservation plus at most one version-2 terminal event. The `semantic-provider-budget-reconcile` event reads a fixed high-water snapshot, rejects history gaps, checks event pairing, and reconciles bounded aggregate usage without retaining prompts, outputs, vectors, or provider request IDs. `provider/budget-final.json` continues to describe only `harness-originated-reservations`; daemon-originated calls are authoritative in the daemon scope evidence.

## Redaction and final gate

Every runner artifact write passes through structured redaction. Sensitive map values, configured secrets embedded in map keys, CLI argument values, authorization/cookie values, API-key patterns, configured runtime secret values, and PEM material are replaced before persistence. Secret-bearing keys become `[REDACTED-KEY]`; a resulting key collision fails closed. The same sanitized payload is used for database persistence.

Finalization performs a bounded recursive artifact scan. `redaction-scan.json` contains only path, detector category, and byte offsets. Files with findings are removed and replaced by a fixed quarantine marker. If the tree cannot be completely scanned, the run artifact root is removed and recreated before the failure report is written. A finding, quarantine failure, unreadable artifact, symlink, or scan-limit violation fails the run. Provider body/header capture is disabled in this contract.

## Deterministic fixture

`tests/reliability/testdata/semantic/v1alpha1/fixtures/token-basis-v1-goldens.json` independently freezes vectors and finite scripts. The harness golden test duplicates only the published algorithm and imports no daemon internals. Daemon behavior is documented in `mycel/docs/design/admin/deterministic-inference-fixtures.md`.
