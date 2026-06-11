# Global Indexing for Flink Streaming: Migrating from `flink_state` to Record Level Index

*Apache Hudi 1.2.0 ships Record Level Index (RLI) support for the Flink writer — the first scalable global index option for streaming upserts on Flink.*

## Why Flink needed a global index

For years, Hudi's Flink writer has offered two production-grade index choices, and neither one solves the problem of **global, cross-partition upserts at scale**:

- **Bucket index** is partition-local. Records hash to a fixed number of buckets *within a partition*, so the writer can route any incoming record to a file group without a lookup. Fast, predictable, and the default recommendation for partitioned Flink ingestion — but it cannot answer "does this key already exist in some *other* partition?" without scanning. If your update stream is partition-stable (e.g., a CDC feed where every change carries the original partition), bucket index is great. If it isn't, bucket index will happily write a second copy of the record in the new partition.

- **`flink_state` index** keeps the key-to-location mapping in Flink's keyed state (RocksDB-backed). It *is* global — every key the job has seen lives in state, regardless of partition — but it lives and dies with the Flink job. The state has to be rebuilt on a new pipeline, can't be shared across jobs, and grows linearly with the table's cardinality. At PB scale and hundreds of millions of keys, the state itself becomes the bottleneck: TaskManager memory pressure, slow recoveries, expensive savepoints, and tight coupling between the writer's lifecycle and the index's lifecycle.

Meanwhile, Spark writers have had **Record Level Index (RLI)** — a global, metadata-table-backed key index — for several releases. Tables created and indexed by Spark RLI couldn't be ingested by Flink without flipping the index type, which meant rebuilding state on the Flink side and losing engine portability.

### A concrete example: the orders table

Take a typical e-commerce `orders` table:

- **Partition column**: `datestr` (order creation date)
- **Record key**: `order_id`

Inserts naturally land in the latest partition. But updates — status changes, fulfillment events, returns — usually arrive *without* the original `datestr`. The upstream system knows the `order_id` changed; it doesn't always know which day's partition that order originally lived in.

Without a global index, the writer has two bad choices:

1. **Bucket index** — assume the update belongs to today's partition and write a duplicate. The original row in the older partition is now stale, and the table has two rows for the same `order_id`.
2. **`flink_state` index** — works correctly, but every `order_id` the table has ever seen must live in Flink state. For a table with hundreds of millions of historical orders, this is a multi-hundred-GB state blob attached to the Flink job, and rebuilding it on recovery (or on migrating to a new pipeline) takes hours.

This is the gap Flink RLI closes.

## What Flink RLI gives you

RLI stores the key-to-file-group mapping in the Hudi **metadata table (MDT)**, as a dedicated `record_index` partition. The mapping is:

- **Global** — partition-agnostic; an update for any `order_id` resolves to the correct file group regardless of partition.
- **Engine-agnostic** — Spark writes and reads RLI today; Flink now does too. A table indexed by Spark RLI can be picked up by a Flink streaming job, and vice versa, with no rebuild.
- **Decoupled from the writer's lifecycle** — the index is part of the table, not part of the Flink job's keyed state. New pipelines can attach immediately; savepoints stay small; recovery doesn't replay the index.
- **Shardable** — RLI is partitioned into N file groups (you choose), each holding a slice of the key space. Lookups and writes parallelize naturally.

## How it works under the hood

The Flink RLI pipeline introduces one new operator and reuses the rest of the existing topology:

```
Source → BucketAssign (RLI lookup) ──┬─→ DataWriter   ─→ Coordinator (commit)
                                     └─→ IndexWriter  ─┘
```

A few mechanics worth understanding:

**`BucketAssignFunction`** — for each incoming record, it looks up the key in the index backend. The lookup goes to an in-memory `RecordIndexCache` first, then falls through to the MDT `record_index` partition for misses. The function tags the record with its existing location (for updates) or a new bucket assignment (for inserts), and — critically — also **emits an index record** representing the new mapping.

**Co-flowing index records** — the index record is emitted in the same `processElement` call as the data record. Because Flink barriers flow with records, the data record and its index record are guaranteed to land in the same checkpoint. No skew between data and index commits.

**`GlobalRecordIndexPartitioner`** — index records are shuffled to the `IndexWriter` using the *same* hash function the MDT uses internally: `mapRecordKeyToFileGroupIndex(key, numFileGroups) % indexWriterParallelism`. This alignment matters: it ensures each `IndexWriter` subtask writes to a single MDT file group, avoiding an N×M write fan-out where N data writers would otherwise each write to M index file groups.

**`IndexWriter`** — buffers index records and flushes them to the MDT `record_index` partition on checkpoint. Write statuses go to the `StreamWriteOperatorCoordinator`.

**Coordinator commit** — on each checkpoint, the coordinator commits MDT (`record_index` + `files` partitions) and the data table atomically. From a reader's perspective, the index and the data move forward together.

**Async MDT compaction** — RLI generates a steady stream of log files in the metadata table. When RLI is on, the Flink job automatically schedules MDT compaction (configurable cadence via `metadata.compaction.delta_commits`), and the compaction runs through the existing Hudi compaction pipeline rather than competing for writer task slots.

### The cache

The lookup path lives or dies by the in-memory cache. `RecordIndexCache` is **checkpoint-aware**: it keeps a generation of entries per checkpoint, so uncommitted (in-flight) index updates from the current checkpoint are visible to subsequent lookups in the same checkpoint window, and committed generations are evicted as their checkpoint watermarks pass. The cache uses Hudi's `ExternalSpillableMap`, with a default heap budget of 256 MB per subtask, tunable via `index.rli.cache.size`.

For workloads with hot-key locality (e.g., recent orders being updated repeatedly), the cache absorbs most lookups. For workloads with random access across the full key space, more lookups fall through to MDT — which brings us to the benchmark.

## Migrating from `flink_state` to RLI

The migration is a config swap; there's no state to copy. The key changes:

```sql
-- Before: flink_state
'index.type' = 'FLINK_STATE'

-- After: global RLI
'index.type' = 'GLOBAL_RECORD_LEVEL_INDEX',
'metadata.enabled' = 'true',
'hoodie.metadata.record.index.enable' = 'true',
'hoodie.metadata.global.record.level.index.min.filegroup.count' = '100',

-- Compaction cadences (tune to your checkpoint interval)
'compaction.delta_commits' = '4',
'metadata.compaction.delta_commits' = '6',

-- Parallelism for the new IndexWriter operator
'index.write.tasks' = '25'
```

If the table already has an RLI partition (e.g., a Spark writer initialized it), the Flink job picks it up directly — no rebuild. If it doesn't, the first checkpoints initialize RLI from the existing data files.

The sizing knobs worth thinking about up front:

- **RLI shard count** (`hoodie.metadata.global.record.level.index.min.filegroup.count`) — pick enough shards that each one stays in the few-hundred-MB range. Underprovisioning here makes MDT compaction expensive later.
- **`index.write.tasks`** — index writer parallelism. The partitioner aligns subtasks to MDT file groups; setting this equal to (or a divisor of) the shard count gives the cleanest fan-out.
- **`index.rli.cache.size`** — heap budget for the in-memory cache. Raise this if your hit ratio is low and you're paying for MDT reads.
- **MDT compaction cadence** — `metadata.compaction.delta_commits` controls how often MDT compaction kicks in. Too infrequent and lookup latency degrades as log files accumulate; too frequent and compaction itself becomes a bottleneck.

## What we measured

We ran two benchmarks to validate that Flink RLI holds up at scale and meets streaming checkpoint SLAs. The goal was *not* to compare against bucket index — bucket index is a different (partition-local) solution and will always win a head-to-head on raw lookup cost. The goal was to establish that **global** indexing on Flink is operationally viable for production streaming ingestion at TB scale.

**Setup (both runs):** ~1B existing records, ~1TB on disk, 100 date partitions, 100 RLI shards (~500 MB/shard). Flink standalone cluster, 4 workers (64 vCPU / 256 GB each), HDFS storage, RocksDB state backend with incremental checkpoints.

### Run 1: baseline validation

We first validated at 20K records/sec with a 5-minute checkpoint interval over 50M incremental records — stable throughout, no source backpressure, all checkpoints comfortably within SLA. With the conservative path established, the more interesting question was how the system behaved under pressure.

### Run 2: aggressive

- 50K records/sec source rate, 100M total incremental records
- 3-minute checkpoint interval
- Same workload shape; reduced writer parallelism to increase per-task pressure

**Result:** stable. The 100M-record workload completed at the target throughput. Mild backpressure appeared at the higher source rate, but checkpoints continued to complete and the job remained healthy.

**With a 3-minute checkpoint SLA, average checkpoint E2E was 24.1s — well within budget.**

| Metric | Avg | P99 |
|--------|-----|-----|
| **Checkpoint interval (SLA)** | **180s** | **180s** |
| **Checkpoint E2E (actual)** | **24.1s** | — |
| Minibatch lookup latency | 3.35s | 8.94s |
| Data flush latency | 0.29s | 1.43s |
| Index flush latency | 2.33s | 4.84s |
| MDT compaction latency | 158.3s | 262.1s |

Lookup latency grew with the increased minibatch fan-out and reduced cache locality compared to Run 1, but the write path itself stayed well within the 3-minute checkpoint budget. The notable signal is in the last row: MDT compaction P99 is now within striking distance of the checkpoint interval.

### Takeaways from the benchmarks

A few observations worth calling out:

1. **Global RLI on Flink meets streaming SLAs at TB scale.** MDT-backed RLI sustained streaming ingestion against a 1B-record table at 50K records/sec under a 3-minute checkpoint SLA, without needing any additional local caching layer.

2. **Writers are not the bottleneck.** Data flush and index flush latencies stayed in the sub-second to low-seconds range. The workload is lookup-bound, not write-bound — and at the tested scale, neither is the constraint.

3. **MDT compaction is the dominant long-tail and the first scaling wall.** P99 MDT compaction latency reached ~260s under the aggressive run — close to the 3-minute checkpoint interval itself. As ingest throughput, RLI shard count, or checkpoint cadence get more aggressive, MDT compaction is the first thing that will need attention.

4. **Cache hit ratio tracks workload locality.** Under the aggressive run, the in-memory cache served ~21% of lookups. Workloads with stronger recent-key locality (e.g., the orders example, where most updates target the last few days' partitions) will see higher ratios and lower effective lookup latency.

## Operational signals to watch

Once the job is running, these metrics tell you whether RLI is healthy:

- **`lookupCacheHitRatio`** (per-minibatch) — high hit ratio means the in-memory cache is doing its job. Low hit ratio (random access) is a signal to raise `index.rli.cache.size` or to revisit your RLI shard count.
- **`localIndexLookupLatency` / `remoteIndexLookupLatency`** — split between cache-served and MDT-served lookups. If remote latency is growing, MDT may need more shards or more aggressive compaction.
- **MDT compaction latency** — if this approaches your checkpoint interval, dial up `metadata.compaction.delta_commits` cadence or scale `compaction.tasks`.
- **Checkpoint E2E latency** — the rolled-up health signal. Stable here means the pipeline is keeping up.

## What's next

A few directions on the roadmap:

- **Partition-level RLI** for very large fact tables where global indexing is more than you need but bucket index isn't expressive enough.
- **Secondary indexes** built on the same MDT-backed infrastructure, with Flink-side write paths that align with the RLI design.
- **MDT compaction tuning** — given the benchmarks point to compaction as the first scaling wall, expect ongoing work on metadata write amplification and record-index compaction strategies.

For users migrating Spark RLI tables to Flink streaming, or for anyone hitting the ceiling on `flink_state` index memory and savepoint costs, Flink RLI in 1.2.0 is now a production-ready path forward.

---

*Apache Hudi 1.2.0 release notes: [link]. Original design discussion: [GitHub #17452](https://github.com/apache/hudi/discussions/17452). RFC-102: RLI support for Flink streaming.*
