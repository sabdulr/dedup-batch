# NDJSON De-duplication Batch

A Spring Boot / Spring Batch application for de-duplicating newline-delimited JSON (NDJSON / JSONL) files.

## Overview

For a given `run-date`, the job:

1. validates that the required ready file exists:
   `<app.input-dir>/<run-date>/<run-date><ready-file-extension>`
2. renames the ready file to the running file:
   `<app.input-dir>/<run-date>/<run-date><running-file-extention>`
3. discovers today's raw input files under `app.input-dir/<run-date>/...`
4. discovers existing de-duplicated baseline files under `app.output-dir/<table>/<run-date>/...`
5. builds a RocksDB-backed index of uniqueness keys from the baseline output files
6. streams today's raw input files and removes records when:
   - the uniqueness key already exists in the baseline output files for that same run date, or
   - the same uniqueness key appears more than once within today's raw input files, including duplicates that occur within the same input file
7. processes one table at a time for stability and scale
8. uses file-level partitioning within a table
9. writes date-scoped temp output files
10. merges temp output files into final per-table outputs
11. renames the running file to the success file when the job finishes successfully
12. renames the running file to the fail file if the job fails after startup rename
13. writes a completion marker file under `output/<completed-file-folder>/` when the job finishes successfully
14. writes a per-run stats file under `logs/<run-date>/stats-<runId>.log`
15. writes a per-run extended stats file under `logs/<run-date>/stats-detailed-<runId>.log`
16. logs per-table and job-level dedupe summary statistics
17. exits after the batch job completes

Prior-day input files are never modified.

## Technology stack

- Java 21
- Spring Boot
- Spring Batch
- H2 for Spring Batch metadata
- RocksDB for disk-backed uniqueness key storage
- Gradle

## Application entry point

Main application class:

```text
com.sars.dedup.DedupApplication
```

The application is batch-only:
- it explicitly launches the Spring Batch job
- it logs startup / launch / completion / failure events
- it exits when the job finishes

## Input file naming

Expected raw input file format:

```text
<table>-<YYYY-MM-DD>-#.json
```

Examples:

```text
account-2026-03-11-1.json
award-2026-03-11-2.json
sars_PlasticHistory-2026-03-11-25.json
```

`#` is a positive sequence number (`1`, `2`, `3`, ...).

## Marker-file lifecycle

Before processing starts, this file must exist:

```text
<app.input-dir>/<run-date>/<run-date><ready-file-extension>
```

Default example:

```text
input/2026-03-11/2026-03-11.ready
```

When processing starts, the application renames it to:

```text
<app.input-dir>/<run-date>/<run-date><running-file-extention>
```

Default example:

```text
input/2026-03-11/2026-03-11.loading
```

When the batch job finishes successfully, the application renames the running file to:

```text
<app.input-dir>/<run-date>/<run-date><success-file-extension>
```

Default example:

```text
input/2026-03-11/2026-03-11.loaded
```

If the batch job fails after the startup rename, the application renames the running file to:

```text
<app.input-dir>/<run-date>/<run-date><fail-file-extension>
```

Default example:

```text
input/2026-03-11/2026-03-11.fail
```

These extensions are configurable in `application.yml`:

```yaml
app:
  ready-file-extension: .ready
  running-file-extention: .loading
  success-file-extension: .loaded
  fail-file-extension: .fail
```

Notes:
- `running-file-extention` is spelled with `extention` to match the current application property name
- if the ready file is missing, the application fails fast before rerun cleanup, RocksDB access, batch job launch, or output generation
- the success and fail renames only happen after the ready file has already been renamed to the running file

## Supported tables

The application processes a configurable set of tables.

The allowed list can be supplied through:
- `app.tables`
- `app.tables-file`

Resolution rule:
- if `app.tables-file` is provided, it is used
- otherwise `app.tables` is used

Blank lines are ignored. Lines starting with `#` are treated as comments. Matching is case-insensitive.

### Where to add or remove tables

You can manage the allowed table list in either of these places:

1. `app.tables` in `application.yml`
2. `app.tables-file` passed at runtime

If `app.tables-file` is present, it overrides `app.tables`.

#### Option A: edit `application.yml`

```yaml
app:
  tables:
    - account
    - accountbalance
    - accounthistory
    - award
    - sars_PlasticHistory
```

To add a table, add another entry to the list.
To remove a table, remove it from the list.

#### Option B: use a tables file

Example `tables.txt`:

```text
account
accountbalance
accounthistory
award
sars_PlasticHistory
```

To add a table, add a new line.
To remove a table, delete its line.

If a table is listed but there is no data for the run date, the application skips it and does not create empty temp or output folders.

## Input directory layout

Example raw input tree before the run starts:

```text
input/
  2026-03-11/
    2026-03-11.ready
    account/
      account-2026-03-11-1.json
      account-2026-03-11-2.json
    award/
      award-2026-03-11-1.json
    sars_PlasticHistory/
      sars_PlasticHistory-2026-03-11-1.json
```

Example marker state during processing:

```text
input/
  2026-03-11/
    2026-03-11.loading
    ...
```

Example marker state after success:

```text
input/
  2026-03-11/
    2026-03-11.loaded
    ...
```

The file discovery logic walks the input tree recursively and selects files based on the file name.

## Baseline de-duplication behavior

The application compares today's raw input files against already de-duplicated output files for the same run date.

### Raw input source

```text
<app.input-dir>/<run-date>/...
```

### Baseline comparison source

```text
<app.output-dir>/<table>/<run-date>/...
```

This means the dedupe baseline is not historical raw input files anymore.

The prior-index step builds its RocksDB index from files already published under:

```text
<app.output-dir>/<table>/<run-date>/MERGED=<table>-<run-date>.json
```

So the application performs:
- same-run-date dedupe against existing de-duplicated outputs
- same-run-date duplicate removal within the current raw input being processed

### Important implication for rerun

Because the existing output directories are now the dedupe baseline, rerun preparation must not delete:

```text
<app.output-dir>/<table>/<run-date>
```

before the job starts.

It is safe to clean:
- `work/temp-output/<run-date>`
- `work/rocksdb/<run-date>`

but the baseline output directories should be preserved.

## Uniqueness rule

Default uniqueness fields:

- `Source.Table`
- `ChangeData.ChangeDateTime`
- `Data._creationdate`

These are configurable globally and per table.

### How to change unique key combinations

The uniqueness key is configurable globally and per table.

#### Global uniqueness configuration

```yaml
app:
  uniqueness-fields:
    - Source.Table
    - ChangeData.ChangeDateTime
    - Data._creationdate
```

#### Per-table uniqueness overrides

```yaml
app:
  table-uniqueness-fields:
    account:
      - Source.Table
      - ChangeData.id
    award:
      - Source.Table
      - Data._creationdate
```

Notes:
- every configured field must exist in the JSON structure you want to de-duplicate on
- field names are case-sensitive with respect to the JSON payload
- changing the uniqueness key changes dedupe behavior for both baseline comparison and duplicate removal within the current run-date input files
- after changing uniqueness rules, prefer `rerun` for the affected business date rather than `restart`

## Output layout

For a run on `2026-03-11`, merged outputs are published to:

```text
output/<table>/2026-03-11/MERGED=<table>-2026-03-11.json
```

Examples:

```text
output/account/2026-03-11/MERGED=account-2026-03-11.json
output/award/2026-03-11/MERGED=award-2026-03-11.json
output/sars_PlasticHistory/2026-03-11/MERGED=sars_PlasticHistory-2026-03-11.json
```

The job writes temp files first, then publishes merged outputs. Output folders are only created for tables that actually produce merged output.

### Completion marker file

When the job completes successfully, it writes:

```text
<app.output-dir>/<completed-file-folder>/<run-date><completed-file-extension>
```

Default example:

```text
output/triggerFile/2026-03-11.trg
```

Both the completion marker directory and extension are configurable:

```yaml
app:
  completed-file-folder: triggerFile
  completed-file-extension: .trg
```

The completion file contains one line per table:

```text
<table>,<merged output file name>,<record count after de-duplication>
```

Example:

```text
account,MERGED=account-2026-03-11.json,3
award,MERGED=award-2026-03-11.json,0
sars_PlasticHistory,MERGED=sars_PlasticHistory-2026-03-11.json,0
```

This file is only written after the job finishes with `COMPLETED`.

### Empty-table behavior

If the configured table list contains tables with no input data for the run date, the application does not create empty temp or output folders for those tables.

That means:
- no empty `work/temp-output/<run-date>/<table>/` folder for tables with no written records
- no empty `output/<table>/<run-date>/` folder for tables with no merged output
- the completion marker should only contain tables that were successfully processed for that run date

## Stats logs

After a successful run, the application writes two per-run stats files under:

```text
./logs/<run-date>/
```

### Summary stats log

```text
./logs/<run-date>/stats-<runId>.log
```

Examples:

```text
./logs/2026-03-11/stats-default.log
./logs/2026-03-11/stats-20260314T220000.log
```

If no `runId` is supplied, the default run id used in the file name and file contents is:

```text
default
```

Example content:

```text
Run ID: default
Run Date: 2026-03-11
Data Processed Summary:
account:
    Total Records processed: 1000000
    Total Rejected: 50
    Total Duplicates: 1200
    Files: output/account/2026-03-11/MERGED=account-2026-03-11.json
    Duration: 2m 15.123s
------------------------------------------------------------------------
Grand Total: 1000000
Duration: 2m 20.456s
```

### Extended stats log

The application also writes a detailed extended stats log containing duplicate and reject details:

```text
./logs/<run-date>/stats-detailed-<runId>.log
```

Examples:

```text
./logs/2026-03-11/stats-detailed-default.log
./logs/2026-03-11/stats-detailed-20260314T220000.log
```

The extended log includes:
- duplicates across different input files
- duplicates within the same input file
- rejected rows due to invalid data or malformed JSON
- physical file line numbers from the source NDJSON files

Duplicate entries include both the current record location and the first-seen record location.

Example:

```text
Line 3 in input\2026-03-10\account\account-2026-03-10-1.json is a duplicate of line 1 in input\2026-03-10\account\account-2026-03-10-1.json
Line 25 in input\2026-03-10\account\account-2026-03-10-2.json is a duplicate of line 8 in input\2026-03-09\account\account-2026-03-09-4.json
Line 41 in input\2026-03-10\account\account-2026-03-10-4.json has been rejected due to invalid data/format
```

## Work directory layout

Work artifacts are date-scoped so the same business date can be rerun safely.

```text
work/
  batch-meta.mv.db
  temp-output/
    2026-03-11/
      ...
  rocksdb/
    2026-03-11/
      <table>/
        ...
```

Notes:
- `batch-meta.mv.db` is the Spring Batch metadata database and should normally be kept
- `temp-output/<run-date>` and `rocksdb/<run-date>` are safe to clean as part of a rerun for that same business date
- existing `output/<table>/<run-date>` directories may be needed as the dedupe baseline and should not be deleted before the run if baseline comparison is enabled

## Configuration

Stable defaults live in `application.yml`. Run-specific values should be passed as command-line arguments.

### Runtime arguments

Required at runtime:
- `--app.input-dir`
- `--app.output-dir`
- `--app.work-dir`
- `--app.run-date`

Optional:
- `--app.lookback-days`
- `--app.run-mode`
- `--app.run-id`
- `--app.rerun-id`
- `--app.tables[0]`, `--app.tables[1]`, ...
- `--app.tables-file`
- `--app.overwrite-output`

Use `runId` for log file naming and operational traceability. If your launcher still passes `rerunId`, keep it aligned with your current batch parameter mapping.

### Example `application.yml`

```yaml
spring:
  application:
    name: ndjson-dedupe-batch

  batch:
    job:
      enabled: false
    jdbc:
      initialize-schema: never
      platform: h2

  datasource:
     url: jdbc:h2:file:${BATCH_META_PATH:./work/dev/batch-meta};DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    username: sa
    password:
    driver-class-name: org.h2.Driver

logging:
  config: classpath:logback-spring.xml

app:
  run-mode: restart
  overwrite-output: true
  chunk-size: 1000
  grid-size: 8
  task-queue-capacity: 1000
  skip-malformed-json: true
  progress-log-interval: 100000

  ready-file-extension: .ready
  running-file-extention: .loading
  success-file-extension: .loaded
  fail-file-extension: .fail

  completed-file-folder: triggerFile
  completed-file-extension: .trg
```

## Restart vs rerun

### Restart

Use `restart` when a previous run for the same `run-date` failed and you want Spring Batch to continue that job instance.

Operationally, the input marker for that business date should reflect the failed state before you start again.

### Rerun

Use `rerun` when a previous run for the same `run-date` already completed, or when restart metadata is inconsistent and you want to process that date again from scratch.

Rerun should:
- delete `work/temp-output/<run-date>`
- delete `work/rocksdb/<run-date>`
- preserve `output/<table>/<run-date>` if those directories are being used as the dedupe baseline
- create a new Spring Batch job instance by adding `runId` or `rerunId`

Important:
- do not delete `work/batch-meta.mv.db` for a normal rerun
- do not delete `output/<table>/<run-date>` before the run if it is being used as the dedupe baseline
- open RocksDB lazily so rerun cleanup can happen before the database is opened

## Build and run scripts

### `build.sh`

```bash
./build.sh
```

### `start_dedup.sh`

Supported environment variables include:
- `SOFT_NOFILE_LIMIT`
- `JAVA_OPTS`
- `RUN_DATE`
- `INPUT_DIR`
- `OUTPUT_DIR`
- `WORK_DIR`
- `LOOKBACK_DAYS`
- `TABLES_FILE`
- `RUN_MODE`
- `RERUN_ID`
- `RUN_ID`
- `OVERWRITE_OUTPUT`
- `SPRING_BATCH_INIT_SCHEMA`
- `EXTRA_ARGS`

### Example `start_dedup.sh` command

```bash
RUN_DATE=2026-03-11 INPUT_DIR=/data/dedup/input OUTPUT_DIR=/data/dedup/output WORK_DIR=/data/dedup/work RUN_MODE=rerun RUN_ID=20260313T203500 SPRING_BATCH_INIT_SCHEMA=never ./start_dedup.sh
```

## Logging

Console logs are emitted during the run, and file logs can be written under:

```text
./logs/<run-date>/
```

At the end of a successful job, the application logs per-table and job-level rollups and writes:

```text
./logs/<run-date>/stats-<runId>.log
./logs/<run-date>/stats-detailed-<runId>.log
```

If no run id is supplied, the default file names are:

```text
./logs/<run-date>/stats-default.log
./logs/<run-date>/stats-detailed-default.log
```
