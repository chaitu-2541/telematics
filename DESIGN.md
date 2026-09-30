# Design choices and trade-offs

The pipeline uses Structured Streaming with availableNow. A single serverless
job generates two small files and runs Bronze, Silver, and Gold in sequence.
This gives a repeatable 15-minute production cadence without paying to keep
an idle continuous stream alive. The resulting latency is the schedule plus
processing time, not subsecond real time.

Bronze uses Auto Loader against the target's Unity Catalog volume. It records
raw typed fields, source file, ingest time, and rescued JSON. A persisted
schema location and an independent checkpoint mean a restart sees only new
files. Silver rejects empty truck IDs, unparsable times, and out-of-range
coordinates. Its SHA-256 key covers truck ID, UTC event time, latitude, and
longitude: identical pings collapse, while genuinely different locations at
the same time remain distinct. With no upstream event ID, this is an explicit
business assumption. Each micro-batch deduplicates its source before an
insert-only Delta MERGE handles duplicates across batches and retries.

Silver Change Data Feed supplies new inserts to Gold. The stream joins the
small Delta truck_details table as a static side, broadcast for this size.
Databricks can use the latest valid reference version for each micro-batch.
Past Gold rows preserve the attributes used when processed; a changed driver
does not silently rewrite history. A controlled re-enrichment/backfill would
be a separate versioned job if current attributes were required historically.
Gold MERGE on ping_id is idempotent.

One bundle deploys schemas and managed volumes as resources and the job as a
resource. Versioned DDL in that job creates all four tables. The target
variables isolate schemas, volume/checkpoint paths, job names, and schedules
inside the same workspace. Prod table structure and data only change when
the bundle is deployed and its job runs. Migration 002 checks for the new
Gold column before ALTER and backfills NULL values, so it is safe on retry.
Promote baseline-v1 to all targets, then migration 002 to dev, test, and prod
in order. For rollback, pause the schedule via a bundle change and deploy a
forward repair compatible with the added nullable column; do not drop it
immediately or replay an old schema against newer data.

At hundreds of thousands of trucks, move the generator to a real event source,
partition or cluster tables by query patterns, stop broadcasting a large
dimension, and choose a governed event ID from the source. Split migrations
from the 15-minute ingest run, monitor lag and bad-row counts, and use an
intentional reference change policy with versioned backfills. Checkpoint
locations and table names must remain stable between deploys.
