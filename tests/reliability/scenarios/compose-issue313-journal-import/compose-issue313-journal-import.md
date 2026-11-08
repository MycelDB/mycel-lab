# compose-issue313-journal-import

This is the import-only #313 reproduction. It has one actor and no concurrent readers or writers. The actor follows the Commonfolio importer’s daemon API boundary: it reads the current node/edge snapshot, opens one read-write transaction, sends `ApplyGraphOperations` requests in bounded chunks within that transaction, and commits once.

The fixture uses the importer’s graph-native shape:

- `pkm.journal` roots with `journal_date` and `journal_day`;
- `pkm.journal_entry` nodes with deterministic Logseq-style metadata and bounded journal payloads;
- existing `pkm.page` targets;
- `contains` edges from journal roots to entries;
- deduplicated `references` edges from entries to pages with `ref_type`, `raw`, `target`, and `count` properties.

The default profile creates 64 existing pages during bootstrap, then imports 256 journal entries per transaction with two page references and 6 KiB of deterministic payload per entry. The scenario runs at 0.2 imports per second during a 50-second stress phase. All mutations remain one atomic transaction; ApplyGraphOperations chunking does not create intermediate commits. Actor events record only bounded counts, request protobuf size, latency, target mode, and redacted timing evidence. Daemon graph-write traces provide Raft/apply/storage/sink timings.

## Tunable profile dimensions

Edit `tests/reliability/actor-profiles/issue313-journal-import.yaml` for controlled comparisons:

- `entryPayloadBytes` and `journalPayloadBytes` — journal content size; the default 6 KiB entry payload produces a roughly 1.7 MiB operation request;
- `linksPerEntry` — reference fan-out;
- `pageTargetMode` — `existing-reused`, `existing-unique`, `same-transaction`, or `mixed`;
- `existingPageCount` and `sameTransactionPageCount` — target populations;
- `indexedProperties` — include or omit the importer’s indexed journal/page properties;
- `applyChunkOperations` — request chunking inside the same atomic transaction;
- actor `commitsPerSecond` and the `import-stress` duration — number of importer transactions.

The scenario deliberately does not use the importer’s client-side committed-chunk fallback. Splitting one logical import into multiple commits would not qualify the atomic large-transaction path under investigation.
