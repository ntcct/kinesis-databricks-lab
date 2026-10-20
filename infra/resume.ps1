# Resume after pause: recreate the Kinesis stream and Firehose with the same settings,
# and reset the Databricks bronze checkpoint (the new stream has new shards and sequence numbers).
# Only bronze's Kinesis checkpoint is reset; tables and downstream checkpoints are kept, so nothing is duplicated.
# Safe to re-run: existing resources are kept. Any failure stops the script with an error.
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\_env.ps1"
# Note: PowerShell variable names are case-insensitive, so don't reuse a name like $r/$R for two values
$Region = $AWS_REGION; $Acct = $AWS_ACCOUNT_ID; $RegArg = "--region=$Region"
$ROLE = "kinesis-lab-firehose-role"; $LG = "/aws/kinesisfirehose/$FIREHOSE_NAME"
function Check($what) { if ($LASTEXITCODE -ne 0) { throw "Failed: $what (exit $LASTEXITCODE)" } }
function Exists([scriptblock]$cmd) { $ErrorActionPreference = 'Continue'; & $cmd 2>&1 | Out-Null; return ($LASTEXITCODE -eq 0) }

if (-not (Exists { aws iam get-role --role-name $ROLE })) {
    throw "IAM role $ROLE not found. It is only removed by 'teardown -All'; run infra\03_firehose.ps1 for a full rebuild."
}

if (Exists { aws kinesis describe-stream-summary --stream-name $STREAM_NAME $RegArg }) {
    Write-Host "== Stream $STREAM_NAME already exists, keeping it (and its checkpoint)"
    $newStream = $false
} else {
    Write-Host "== Creating Kinesis stream $STREAM_NAME (1 shard, provisioned, KMS)"
    aws kinesis create-stream --stream-name $STREAM_NAME --shard-count 1 --stream-mode-details StreamMode=PROVISIONED --tags project=$PROJECT_TAG $RegArg; Check "create-stream"
    aws kinesis wait stream-exists --stream-name $STREAM_NAME $RegArg; Check "wait stream-exists"
    aws kinesis start-stream-encryption --stream-name $STREAM_NAME --encryption-type KMS --key-id alias/aws/kinesis $RegArg; Check "start-stream-encryption"
    aws kinesis wait stream-exists --stream-name $STREAM_NAME $RegArg; Check "wait stream-exists (encryption)"
    $newStream = $true
}

if (Exists { aws firehose describe-delivery-stream --delivery-stream-name $FIREHOSE_NAME $RegArg }) {
    Write-Host "== Firehose $FIREHOSE_NAME already exists, keeping it"
} else {
    Write-Host "== Creating Firehose $FIREHOSE_NAME"
    $cfg = @{
        RoleARN = "arn:aws:iam::${Acct}:role/$ROLE"; BucketARN = "arn:aws:s3:::$BUCKET_NAME"
        Prefix = 'raw/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/!{timestamp:HH}/'
        ErrorOutputPrefix = 'errors/!{firehose:error-output-type}/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/'
        BufferingHints = @{ SizeInMBs = 1; IntervalInSeconds = 60 }; CompressionFormat = "GZIP"
        CloudWatchLoggingOptions = @{ Enabled = $true; LogGroupName = $LG; LogStreamName = "S3Delivery" }
    } | ConvertTo-Json -Depth 4 -Compress
    [IO.File]::WriteAllText("$env:TEMP\lab-fh_s3.json", $cfg, (New-Object Text.UTF8Encoding($false)))
    aws firehose create-delivery-stream --delivery-stream-name $FIREHOSE_NAME --delivery-stream-type KinesisStreamAsSource `
        --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:${Region}:${Acct}:stream/$STREAM_NAME,RoleARN=arn:aws:iam::${Acct}:role/$ROLE" `
        --extended-s3-destination-configuration "file://$env:TEMP/lab-fh_s3.json" --tags Key=project,Value=$PROJECT_TAG $RegArg | Out-Null
    $rc = $LASTEXITCODE; Remove-Item "$env:TEMP\lab-fh_s3.json"; $LASTEXITCODE = $rc; Check "create-delivery-stream"
}
for ($i = 0; $i -lt 30; $i++) {
    $st = aws firehose describe-delivery-stream --delivery-stream-name $FIREHOSE_NAME $RegArg --query "DeliveryStreamDescription.DeliveryStreamStatus" --output text; Check "describe-delivery-stream"
    if ($st -ne 'CREATING') { break }
    Start-Sleep 10
}
if ($st -ne 'ACTIVE') { throw "Firehose status is $st, expected ACTIVE" }

if ($newStream) {
    Write-Host "== Resetting Databricks bronze checkpoint (new stream = new sequence numbers)"
    $ckpt = "dbfs:/Volumes/$DATABRICKS_CATALOG/$DATABRICKS_SCHEMA/checkpoints/bronze_events"
    if (Exists { databricks fs ls $ckpt -p $DATABRICKS_PROFILE }) {
        databricks fs rm -r $ckpt -p $DATABRICKS_PROFILE; Check "reset bronze checkpoint"
    }
}

$s = aws kinesis describe-stream-summary --stream-name $STREAM_NAME $RegArg --query "StreamDescriptionSummary.[StreamStatus,OpenShardCount,EncryptionType]" --output text
Write-Host "Resumed. Stream: $s | Firehose: $st. Next: .\run.ps1 produce, then .\run.ps1 job"
