# Live walkthrough preparation

## What you built in one sentence

I deploy one serverless Databricks bundle to three isolated target schemas;
each scheduled run turns synthetic truck JSON into raw Bronze, deduplicated
Silver, and a Gold geofence table enriched with static truck details.

## Explain it from source to output

1. The bundle creates the target Unity Catalog schema, its managed landing
   volume, and one serverless notebook job. The job's versioned SQL provisions
   four Delta tables. Nothing in prod is manually created.
2. A deterministic seed inserts 20 truck details once. The generator writes
   two unique JSON files per job run, with 10 valid pings, one exact duplicate,
   and one invalid coordinate in each file.
3. Auto Loader reads new JSON files into append-only Bronze. Its schema
   location tracks input fields; unexpected values go to rescued data. The
   checkpoint tracks processed files.
4. Silver casts and validates the records. The hash of truck, time, and
   position is the deduplication key. A micro-batch dedup plus an insert-only
   MERGE protects against duplicates both within a batch and across restarts.
5. Gold streams Silver inserts using Change Data Feed. It joins the stream
   to the latest truck-details Delta snapshot, preserving the new ping even
   if the reference record is unexpectedly missing. The populated dimension
   should leave no missing driver in this exercise.
6. Gold stores the enriched attributes and a Chicago bounding-box flag.
   Its MERGE on ping_id tolerates repeated batches. The run ends with
   assertions and a sample query.

## Questions you are likely to get

**Why Structured Streaming rather than Lakeflow?**
I wanted direct control over the Silver and Gold MERGEs and visible
checkpoints while keeping the exercise small. Lakeflow could simplify
declarative expectations and lineage in a larger team project.

**Is this really real time?**
Auto Loader and the downstream stages use streaming reads. availableNow
processes new files and then stops; prod schedules that run every 15
minutes. It is near-real-time micro-batch processing with an explicit latency
trade-off to stay within Free Edition quota.

**Why does Bronze contain duplicates and bad coordinates?**
Bronze retains what was emitted. Silver is where the contract is enforced,
so data-quality failures can be measured against the raw record.

**What happens if the job fails after the Silver MERGE but before its
checkpoint advances?**
The same micro-batch may be retried. The MERGE inserts only keys not already
present, so it cannot create another Silver row for the same ping. Gold
likewise merges by ping_id.

**Why use Change Data Feed from Silver?**
Silver is written by MERGE. CDF makes its inserted rows an explicit
incremental input to Gold, instead of relying on a regular stream over
potentially changing Delta commits.

**What does a duplicate mean here?**
Same truck, same UTC event time, same latitude, same longitude. The brief
does not supply a ping event ID. If an upstream ID existed, that would be a
better key. A moved location at the same timestamp remains a different ping.

**What if the truck_details row changes?**
The next micro-batch sees the latest valid Delta version. Existing Gold
rows keep their original enrichment. Rebuilding history after a driver
change requires a deliberate backfill and a decision about historical
versus current attribution.

**How is prod deploy-only?**
The bundle owns schemas, volumes, and the job. CREATE TABLE and the ADD
COLUMNS migration are in source-controlled job code. I only run bundle
deploy and bundle run against prod; I use SQL there only to read evidence.

**What is the rollback plan for a column addition?**
Keep the nullable column for compatibility and deploy a forward repair,
possibly pausing the schedule first by deploying a bundle change. Dropping
the column immediately can break readers or lose data. A downgrade to the
original notebook alone is not a tested rollback.

**What would change for a large fleet?**
Use a real ingestion source, an upstream event ID, a better reference
update strategy, metrics and alerts, and tune file sizes and table layout.
Broadcasting the dimension only makes sense while it is small.

## The on-the-spot change

If asked to adjust the Chicago box, change the two coordinate ranges in
the Gold transformation and the migration backfill, commit the edit, then
validate, deploy, and run dev first. If asked to add a new column, add a
safe schema check and ALTER in create_tables, add its expression in Gold,
add a backfill for old rows, and repeat the dev → test → prod promotion.
Show the new column with a read-only SQL query after each run. Explain that
editing a transformation does not update old Gold rows unless a backfill is
included.
