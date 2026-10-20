# Semantic reliability contract testdata v1alpha1

This directory contains the SGT0 draft contracts for provider-neutral semantic graph-index reliability tests.

## Contents

- `schemas/semantic-corpus.schema.json` defines a synthetic graph corpus, mutation sequence, and query oracle.
- `schemas/semantic-evidence.schema.json` defines the common sanitized JSONL evidence envelope.
- `schemas/semantic-final-report.schema.json` defines the final gating semantic oracle report.
- `examples/` contains non-secret synthetic examples for each schema.
- `negative/` contains documents that must fail full schema validation, including isolated true-verdict violations for every success conditional plus malformed operations, unsafe classification, and a declared bound.

## Status

These schemas are design inputs for SGT3–SGT4 and are not accepted as environment-backed runtime evidence yet. SGT1 provider-profile resolution, typed budgets, structured redaction, and the bounded final artifact scan are implemented independently of these SGT3 evidence readers.

Go contract tests compile the schemas as JSON Schema Draft 2020-12, require all positive examples to validate, and require all negative fixtures to fail. `make check-docs` separately provides dependency-free JSON parsing and basic schema metadata checks.

Every persisted document carries an explicit `schemaVersion`. Once a schema version is consumed by a retained run, its shape and meaning are immutable; incompatible or additive shape changes receive a new version. Each schema also declares `x-mycel-decoder-limits`; runtime readers must enforce byte, nesting, and string limits during decoding rather than relying on post-decode schema checks alone.

## Safety

`fixtures/token-basis-v1-goldens.json` freezes SGT2 vectors and finite scripted outcomes independently of daemon internals. The examples are synthetic and safe for artifacts. In the corpus contract, `artifactCaptureSafe: true` is valid only with `dataClass: synthetic`; confidential or merely public data cannot opt into raw artifact capture. Production implementations must still apply structured redaction and the post-write artifact scan. Raw provider payloads, authorization metadata, credentials, TLS material, and graph values not explicitly classified artifact-safe must not be stored.
