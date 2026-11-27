# Semantic search scaling benchmark

[Lab #138](https://github.com/MycelDB/mycel-lab/issues/138) tracks client-observed latency as graph data grows. This is a report-only experiment, not a reliability suite or release gate. A successful Lab exit means the run and cleanup completed; it does **not** mean search was fast enough or that every measurement was usable. Check the report status and its rows.

## Workloads

The `semantic-search-scaling` suite runs two isolated experiments using the standard three-daemon Raft Compose topology (including its idle object-store fixture) and the immutable Mycel v0.26.0 image. Each starts with a new graph, one domain, and one exact `mycel-file` search binding.

| Sweep | Graph nodes | Indexed vectors |
| --- | --- | --- |
| Indexed growth | 1k, 3k, 10k, 30k | Grows with graph nodes |
| Graph-only growth | 1k, 3k, 10k, 30k | Fixed at the first point (1k) |

The fixtures have no edges. Graph-only growth adds same-domain nodes whose label excludes them from the semantic rule. Lab verifies the acknowledged graph revision is fresh in the index, enumerates the graph, and reads the live physical index count outside the timed window. This self-projection emits one vector per indexed record. A setup error leaves the remaining size points in the report as not reached; Lab does not retry ambiguous graph mutations.

Both sweeps use the same four-query mix and local eight-dimensional token-basis fixture, no live provider, no filters, the first page of direct `SemanticSearch`, a minimum score of 0.5, and a limit of 10. Each query has 10 known top hits and matches about a quarter of the filler records. The selected index still scores all its vectors; the fixture keeps the number of *authorized matches* below v0.26.0's 10,000-candidate RPC bound so the 30k point measures search rather than that API limit. These synthetic scores, payloads, dimensions, and edge-free topology do not represent arbitrary production graphs.

Run one client and then eight clients, separately, against the same front-door daemon. Each has its own persistent connection. Login, graph/semantic setup, and eight untimed warm-up queries per participating client are outside the timed window. Each timed workload attempts 1,024 requests with a balanced query mix and one in-flight request per client, bounded by a 10-second request deadline and a three-minute workload budget. This is closed-loop throughput, not a demonstration that a cluster can sustain a particular arrival rate. Each size gets up to 12 minutes of setup. At larger sizes, seeding and indexing may exceed that budget or a measurement may not reach its attempt goal. Keep those rows visible, or review a smaller size ladder and run it again as a separate baseline.

## Planning and approved runs

Run from a Lab checkout next to the Mycel daemon checkout; see [operations](operations.md) for Compose prerequisites. The benchmark uses dedicated loopback ports `29491`–`29493`; a run creates and deletes scoped disposable Compose resources and must not overlap another run using those ports.

```sh
mycel-lab run suite semantic-search-scaling --dry-run
```

Dry-run validates the plans. It does not seed data, time RPCs, or produce a measurement report. After explicit approval for environment changes, use the same built Lab executable and machine for three **sequential, independent** suite runs, each with a separate artifact root. Do not run these as part of a release gate. For example, one approved repetition is:

```sh
mycel-lab run suite semantic-search-scaling --confirm-destructive --artifact-root artifacts/scaling/repeat-1
```

Use `repeat-2` and `repeat-3` for the other approved repetitions. The suite starts a separate cluster for each sweep and each repetition. Keep machine load, Docker context/resource settings, daemon image, and Compose environment fixed. A small smoke run can use copies of the scenario files with smaller `sizes` (e.g. `[40, 80]`) and 128 samples. For copies outside the repository, run from the Lab checkout with `mycel-lab run scenario-file <copy.yaml> --profile-dir tests/reliability --dry-run` to validate resolution first. Preserve any eventual smoke results separately from the baseline.

## Reports

Each scenario that enters the benchmark writes `benchmark/semantic-search-scaling.md` and `benchmark/semantic-search-scaling.json` under its run artifact directory. Inspect `result.json` and `environment/post-run-cleanup.json` before selecting reports for a comparison. The benchmark reports are independent of lifecycle verdicts such as a failed environment preflight or cleanup.

- **Measurement complete:** the point was verified, the attempt goal was reached, and there are enough correct results to show p95. There is no latency threshold. Incorrect order, incomplete pages, unexpected candidate counts, or wrong result provenance do not count as success and make the workload inconclusive.
- **Inconclusive:** setup/readiness could not be verified, the time budget ran out, responses were incorrect, or there were too few successful samples. Partial numbers remain visible but do not contribute to scaling ratios. A later size that didn't run is listed explicitly.
- p50/p95/p99 use nearest rank on **successful queries only**. p95 needs 100 successes and p99 needs 1,000. Report attempts, success throughput over wall time, errors, and timeouts alongside latency; losses can make successful-query percentiles look artificially low. Report artifacts do not contain query text, graph payloads, or credentials.

Compare repetitions of **one sweep at a time**. For example, the following emits Markdown to standard output (Bash or Zsh):

```sh
mycel-lab benchmark compare \
  artifacts/scaling/repeat-{1,2,3}/compose-semantic-search-scaling-indexed-*/benchmark/semantic-search-scaling.json
```

The comparison checks workload and environment identity, lists observed graph and index sizes, and shows the median and range of per-run p50/p95 values. Adjacent-point latency ratios are shown next to size ratios only when every repetition is usable at both sizes. Check the per-run errors and timeouts, especially near a budget limit. A graph-only vector size ratio of 1× controls for the amount of indexed data, not for all graph effects. These curves describe the measured range; do not use them to infer an asymptotic bound or to predict live-provider or hybrid search latency.
