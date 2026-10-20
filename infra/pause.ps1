# Pause the lab: delete only the hourly-billed resources (Firehose, then the Kinesis stream).
# Kept: S3 archive, Databricks tables/job/credentials, IAM roles, alarms, dashboard, SNS, log group.
# Kinesis has no stop/pause, so deleting the stream is the only way to stop the shard-hour charge.
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\_env.ps1"
$r = "--region=$AWS_REGION"

$exists = aws kinesis describe-stream-summary --stream-name $STREAM_NAME $r --query "StreamDescriptionSummary.StreamStatus" --output text 2>$null
if ($exists) {
    # Safety check: warn if the stream holds records bronze hasn't read yet (they would be lost)
    Write-Host "Stream status: $exists. Run '.\run.ps1 job' first if you produced data since the last job run."
}

Write-Host "== Deleting Firehose $FIREHOSE_NAME"
aws firehose delete-delivery-stream --delivery-stream-name $FIREHOSE_NAME $r 2>$null
for ($i = 0; $i -lt 30; $i++) {
    aws firehose describe-delivery-stream --delivery-stream-name $FIREHOSE_NAME $r --output text 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { break }
    Start-Sleep 5
}

Write-Host "== Deleting Kinesis stream $STREAM_NAME"
aws kinesis delete-stream --stream-name $STREAM_NAME $r 2>$null
aws kinesis wait stream-not-exists --stream-name $STREAM_NAME $r 2>$null

aws kinesis describe-stream-summary --stream-name $STREAM_NAME $r 2>$null | Out-Null; $streamGone = ($LASTEXITCODE -ne 0)
aws firehose describe-delivery-stream --delivery-stream-name $FIREHOSE_NAME $r 2>$null | Out-Null; $fhGone = ($LASTEXITCODE -ne 0)
if ($streamGone -and $fhGone) { Write-Host "Paused: stream and Firehose deleted, hourly charges stopped. Resume with .\run.ps1 resume" }
else { Write-Warning "Still present (may still be DELETING) - stream gone: $streamGone, Firehose gone: $fhGone. Re-run pause or check the console." }
