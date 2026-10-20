# Semantic reliability contract testdata v1alpha1

This directory contains the SGT0 draft contracts for provider-neutral semantic graph-index reliability tests.

## Contents

- `schemas/semantic-corpus.schema.json` defines a synthetic graph corpus, mutation sequence, and query oracle.
- `schemas/semantic-evidence.schema.json` defines the common sanitized JSONL evidence envelope.
- `schemas/semantic-final-report.schema.json` defines the final gating semantic oracle report.
- `examples/` contains non-secret synthetic examples for each schema.

## Status

These schemas are design inputs for SGT1–SGT4. They are not accepted by the runtime yet. SGT1 freezes provider-profile resolution, and SGT3 freezes kind-specific evidence payloads before any environment-backed suite writes these artifacts.

Every persisted document carries an explicit `schemaVersion`. Once a schema version is consumed by a retained run, its shape and meaning are immutable; incompatible or additive shape changes receive a new version.

## Safety

The examples are synthetic and safe for artifacts. Production implementations must still apply structured redaction and the post-write artifact scan. Raw provider payloads, authorization metadata, credentials, TLS material, and graph values not explicitly classified artifact-safe must not be stored.
