# Telematics Databricks take-home

This bundle runs one small serverless job per target. A run provisions Delta
tables from versioned code, seeds 20 truck details, writes synthetic JSON pings,
then runs Auto Loader Bronze, deduplicating Silver, and enriched Gold in order.
The single scheduled job avoids keeping a stream alive between batches.

## 1. Prerequisites

1. Create a Databricks Free Edition workspace and install the current
   Databricks CLI on your own machine.
2. Sign in from your terminal. OAuth is convenient:

       databricks auth login --host https://YOUR-WORKSPACE-HOST --profile free
       databricks current-user me --profile free

   A personal access token is also possible. Never put it in this repository.
3. Select that profile for the following commands:

       export DATABRICKS_CONFIG_PROFILE=free

   In PowerShell, use:

       $env:DATABRICKS_CONFIG_PROFILE = "free"

4. Check which catalog allows you to create schemas:

       databricks catalogs list

   If main is not writable, set the same override for every target before
   running any bundle command:

       export BUNDLE_VAR_catalog=YOUR_WRITABLE_CATALOG

   In PowerShell:

       $env:BUNDLE_VAR_catalog = "YOUR_WRITABLE_CATALOG"

   The catalog itself is an existing workspace prerequisite; schemas,
   volumes, jobs, and tables are bundle managed.

## 2. Prove the initial version in dev, test, and prod

The supplied Git history has a tag named baseline-v1. Run these commands from
the folder containing databricks.yml:

       git checkout baseline-v1
       databricks bundle validate -t dev
       databricks bundle deploy -t dev
       databricks bundle run telematics -t dev
       databricks bundle deploy -t test
       databricks bundle run telematics -t test
       databricks bundle deploy -t prod
       databricks bundle run telematics -t prod

If a command fails, fix the cause and repeat the same deploy/run. Do not create
or alter any prod objects manually. Each run adds 24 Bronze records and 20
records each in Silver and Gold for that target: each
of two files contains 10 good rows, one duplicate, and one bad coordinate.
The job verifies the counts, unique Silver keys, valid coordinates, and the
presence of dimension attributes. Repeated runs generate new files, but
previous files are not copied again.

The three environments reside in one workspace and use distinct schema names,
volume names, job names, and schedules. Dev is paused, test runs hourly, and
prod runs every 15 minutes. They run sequentially when invoked by these
commands. No custom cluster is configured; the notebook job uses serverless.

## 3. Promote migration 002 through all targets

Go back to the final commit. It adds inside_chicago_geofence to Gold using an
idempotent schema check, backfills old rows, and calculates the flag for new
rows. Capture evidence after each target run:

       git checkout main
       databricks bundle validate -t dev
       databricks bundle deploy -t dev
       databricks bundle run telematics -t dev
       databricks bundle deploy -t test
       databricks bundle run telematics -t test
       databricks bundle deploy -t prod
       databricks bundle run telematics -t prod

For a real promotion, inspect the dev and test results before the next deploy.
No command above writes directly to a prod table; the prod job applies the
versioned migration after deployment. Keep BUNDLE_VAR_catalog set if you
needed a catalog override.

## 4. Read-only checks and evidence

Run these in a Databricks SQL editor, replacing main if you selected a
different catalog, and changing the schema suffix for each target:

       SELECT COUNT(*) AS bronze_count FROM main.telematics_prod.bronze_pings;
       SELECT COUNT(*) AS silver_count FROM main.telematics_prod.silver_pings;
       SELECT COUNT(*) AS gold_count FROM main.telematics_prod.gold_geofence_pings;

       SELECT truck_id, event_ts, driver, home_depot, region,
              inside_chicago_geofence
       FROM main.telematics_prod.gold_geofence_pings
       ORDER BY event_ts DESC LIMIT 10;

       DESCRIBE TABLE main.telematics_prod.gold_geofence_pings;

Capture the terminal deploy and run results for all targets, the workspace
job run and task output, the populated Gold query, and the Gold column before
and after migration 002. A 3–5 minute screen recording can show the same
sequence. The checks above only read prod; all creation and writes go through
the bundle deployment and job run.

## 5. Commit and submit

The archive includes Git history with baseline-v1 and the migration commit.
Create your public GitHub or GitLab repository, add it as origin, and push the
main branch and tag. Review your own account, workspace paths, and screenshots
before submitting. Submit the repository URL and the 3–5 minute recording or
screenshots. Never commit a token, a local CLI profile, or generated output.

## Troubleshooting

- Permission denied at bundle deploy: the catalog variable must point to a
  catalog where your user can CREATE SCHEMA. A bundle deployment creates the
  schema and managed volume before the job is run.
- No data in Gold: inspect the job run in order. Bronze, Silver, and Gold have
  independent checkpoints beneath the target volume. Do not delete them as a
  casual retry. Silver Change Data Feed is enabled at table creation.
- A source field changes type: Auto Loader keeps the unexpected value in
  _rescued_data. Add a reviewed table migration and transformation when that
  field should become a typed business column.
- A job fails after a MERGE: rerun it. Delta MERGE on ping_id and the persisted
  checkpoints make a repeated micro-batch safe.
- Quota: Free Edition has limited serverless capacity. Pause test and prod by
  changing pause_status for those targets in databricks.yml and deploying the
  change; their distinct cron schedules remain in source control.
