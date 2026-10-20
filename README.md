# Real-time pipeline lab: Amazon Kinesis → Databricks (Structured Streaming, Delta Lake, Unity Catalog)

A small, low-cost but production-shaped streaming pipeline: Kinesis Data Streams and Data Firehose on AWS,
Spark Structured Streaming and Delta Lake on Databricks serverless, CloudWatch monitoring and alerting,
and pytest unit tests.

```
 Producer (Docker, boto3, PutRecords + partial-failure retry)
        │
        ▼
 Kinesis Data Stream "lab-events"  (1 shard, provisioned, KMS-encrypted, 24 h retention)
        │                                   │
        ▼                                   ▼
 Data Firehose → S3 raw/yyyy/MM/dd/HH/      Databricks serverless job "kinesis-lab-medallion"
 (GZIP NDJSON archive, 7-day lifecycle)      setup → bronze → silver → reconcile → gold → optimize
        │                                     bronze_events      raw payload + Kinesis metadata (exactly-once)
        └──── batch reconcile / backfill ───► silver_events      typed, validated, de-duplicated (liquid clustering)
                                              silver_quarantine  failed data-quality rules
                                              gold_site_minute   1-min windows per site, touched windows recomputed from silver
                                              gold_device_status latest state per device (MERGE upsert)

 CloudWatch: alarms (throttling, iterator age, Firehose lag, S3 delivery) → SNS email,
             dashboard, and custom pipeline metrics published from Databricks (KinesisLab/Pipeline)
```

## What it demonstrates

| Area | Implementation |
|---|---|
| Kinesis Data Streams | Shard sizing, KMS encryption, `PutRecords` batching with retry of only the failed records, exponential backoff and jitter |
| Data Firehose | Kinesis-source delivery to S3 with time-based prefixes, GZIP, error prefix, delivery logging |
| Structured Streaming | Databricks Kinesis source with a Unity Catalog service credential, `availableNow` incremental runs, checkpoints in UC volumes |
| Delta Lake | Append-only bronze, `dropDuplicatesWithinWatermark`, `foreachBatch` + `MERGE` upserts, gold windows recomputed from silver (correct under late data and backfills), `OPTIMIZE`, liquid clustering |
| Streaming + batch | Batch job compares the Firehose archive with silver and backfills gaps with an insert-only `MERGE`; gold picks the backfill up in the same run |
| Data quality | Fixed schema, rule-based quarantine (`dq_reason`), duplicates and late events handled separately, `run.ps1 verify` consistency checks |
| Governance and IAM | No access keys: UC service credential, read-only storage credential and external location; least-privilege IAM roles with confused-deputy conditions; table tags; `GRANT SELECT` on gold only |
| Monitoring | CloudWatch alarms → SNS, dashboard, custom metrics (`RowsIngested`, `DataFreshnessSeconds`) |
| Testing | 20 pytest tests on local Spark 4.2.0 in Docker (transforms, gold aggregation, producer retry logic; no cloud calls) |
| Cost | About $0.015/hour (one shard) while running; `run.ps1 pause` deletes only the stream and Firehose, bringing idle cost to about $0.40/month (alarms) |

## Results

| Run | Outcome |
|---|---|
| End-to-end reconciliation | Bronze row count equals records sent, with unique sequence numbers. Every bronze row ends up in silver, in quarantine or removed as a duplicate. Gold windows sum to silver |
| Throttling test (asked for 3,000 records/s against a 1,000 records/s shard) | 17,500 records throttled and retried, 0 dropped; `kinesis-lab-write-throttled` alarm → SNS email, then back to OK |
| Data-loss drill (`.\run.ps1 job -SimulateLoss 50`) | 50 silver rows deleted; reconcile found 50 missing and backfilled exactly 50 from the S3 archive; `verify` passes afterwards |
| Pause / resume cycle | Stream and Firehose deleted and recreated; bronze checkpoint reset; the next 1,200 records landed in bronze exactly once, with no duplicates of earlier data |

## Lessons captured

| Symptom | Cause | Fix |
|---|---|---|
| Console Data viewer shows "No record found" at Trim horizon | A single `GetRecords` call can return nothing before reaching data | Keep reading with `NextShardIterator`, or start at a timestamp or sequence number |
| S3 objects contain `{...}{...}` run together | Firehose concatenates records as-is | Producer appends `\n`, so the files are NDJSON |
| External location validation: no LIST permission | Prefix was empty | Write at least one object first |
| `UNABLE_TO_INFER_SCHEMA` reading the archive | Firehose writes nested hourly folders | `recursiveFileLookup=true` |
| Duplicated rows after a failed run | Checkpoint was reset while the target table was kept | Reset the checkpoint and the table together, never just one |
| `foreachBatch` can't see a module imported in the notebook | On serverless (Spark Connect) the function runs in a separate process | Import the module inside the function |
| Streaming progress reported 0 rows for a run that wrote data | Spark Connect progress objects are not reliable for this | Count rows by `ingest_ts` from the table |
| `INVALID_NON_DETERMINISTIC_EXPRESSIONS` on `DELETE … rand()` | Delta DELETE needs a deterministic condition | Choose the IDs first, then delete |
| Throttle metric `Sum` far above the producer's count | That metric's `Sum` is not a record count | Alarm on > 0; count throttled records at the producer |
| Notebook header parse error after deploy | UTF-8 BOM added by Windows PowerShell 5.1 | Write rendered files as UTF-8 without BOM |
| Gold window count one higher than silver after the loss drill | The streaming aggregation's counts can't subtract deleted rows, and the backfilled row was counted a second time | Gold recomputes each touched window from silver instead of keeping running counts |
| `resume` built an invalid Kinesis ARN | PowerShell variable names are case-insensitive: `$r` (region flag) overwrote `$R` (region) | Distinct names; scripts stop on any failed AWS call |
| `pause` reported success while the stream was still listed | List calls include streams that are still deleting | Confirm each resource returns "not found" |

## Layout

| Path | Contents |
|---|---|
| `producer/` | Event generator (`normal`, `hot`, `burst` modes) and its Dockerfile |
| `databricks/` | `lab_transforms.py` (pure, tested), notebooks `00`–`06` (`06_verify.sql` = consistency checks), job spec, `deploy.ps1` |
| `infra/` | Step scripts `01`–`06`, `pause.ps1` / `resume.ps1`, IAM policy templates, dashboard, `teardown.ps1`, `_env.ps1` (settings and template rendering) |
| `tests/` | pytest suite and its Dockerfile (Java 17 + PySpark 4.2.0) |
| `run.ps1` | Entry point: `test`, `produce`, `deploy`, `job`, `verify`, `pause`, `resume`, `teardown` |

## Run it

Prerequisites: Windows PowerShell 5.1, AWS CLI v2, a recent Databricks CLI (one that has the `credentials` command), Docker,
and a Unity Catalog-enabled workspace (Free Edition works). The step scripts `01`, `03`, `04` and `06` have not yet been run
from a clean account; `run.ps1` commands, `pause` and `resume` have.
Account-specific values live in `lab.env` (gitignored). Committed files use `{{TOKEN}}` placeholders that are filled in at deploy time.

```powershell
Copy-Item lab.env.example lab.env      # fill in account ID, bucket, Databricks profile/user/external ID
.\infra\01_storage_stream.ps1          # S3 bucket and Kinesis stream (charging starts here)
.\infra\03_firehose.ps1                # Firehose to S3
.\run.ps1 produce -Duration 60         # put at least one object under raw/ before step 4
.\infra\04_databricks_access.ps1       # IAM role and Unity Catalog credentials
.\infra\06_monitoring.ps1              # SNS, alarms, dashboard
.\run.ps1 deploy                       # notebooks and job
.\run.ps1 job                          # run the medallion pipeline
.\run.ps1 verify                       # consistency checks (all should be true)
.\run.ps1 test                         # unit tests
.\run.ps1 produce -Rate 3000 -Duration 60 -Mode burst   # throttling test
.\run.ps1 job -SimulateLoss 50         # data-loss and backfill drill
.\run.ps1 pause                        # stop hourly charges, keep all data (resume with .\run.ps1 resume)
.\run.ps1 teardown -All                # remove everything
```
