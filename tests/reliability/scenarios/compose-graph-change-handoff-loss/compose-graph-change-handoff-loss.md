# Compose graph-change handoff loss

## Purpose

Reproduce the durability gap between a committed graph mutation and derived
consumers. The scenario makes the node-local graph-change notification path
unwritable, commits a semantic graph update, restores the path, and restarts
the three-node cluster.

## Expected invariant

The graph mutation must remain committed and semantic maintenance must expose
pending work for the committed revision. The authoritative replay restores the
raw dirty event, then the bounded assertion explicitly analyzes that event while
background maintenance remains disabled. Derived delivery is at-least-once and
must not depend on a best-effort node-local notification write.

## Acceptance behavior

The scenario must pass at `semantic-work-assert`: graph commit succeeds while
node-local notification delivery is unavailable, and restart replays the
authoritative graph-change journal to expose pending semantic work.

## Safety

The scenario uses an isolated Compose project, isolated host ports, a digest
pinned Mycel image, and only mutates `/data/mycel/graph-change-notification`.
Graph Raft data and semantic metadata are retained. Normal runner cleanup must
remove the Compose project and collected artifacts must pass redaction checks.
