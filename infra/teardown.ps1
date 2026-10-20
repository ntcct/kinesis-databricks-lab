# Tears down all billable AWS lab resources. Safe to re-run: missing resources are skipped.
#   .\infra\teardown.ps1              # stops hourly charges (stream, Firehose, alarms) and keeps S3 data and IAM
#   .\infra\teardown.ps1 -All         # also deletes the S3 bucket, IAM roles/users, SNS topic and dashboard
param([switch]$All)
$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\_env.ps1"
$r = "--region=$AWS_REGION"

Write-Host "== Firehose $FIREHOSE_NAME"
aws firehose delete-delivery-stream --delivery-stream-name $FIREHOSE_NAME $r 2>$null

Write-Host "== Kinesis stream $STREAM_NAME (shard-hours stop once it's deleted)"
aws kinesis delete-stream --stream-name $STREAM_NAME --enforce-consumer-deletion $r 2>$null

Write-Host "== CloudWatch alarms with prefix kinesis-lab-"
$alarms = aws cloudwatch describe-alarms --alarm-name-prefix "kinesis-lab-" --query "MetricAlarms[].AlarmName" --output text $r
if ($alarms) { aws cloudwatch delete-alarms --alarm-names ($alarms -split '\s+') $r }

if ($All) {
    Write-Host "== Databricks: job, notebooks, schema, external location, credentials"
    $dp = "-p=$DATABRICKS_PROFILE"
    foreach ($j in @(databricks jobs list --name kinesis-lab-medallion -o json $dp | ConvertFrom-Json)) { if ($j) { databricks jobs delete $j.job_id $dp } }
    databricks workspace delete "/Users/$DATABRICKS_USER/kinesis-lab" --recursive $dp 2>$null
    databricks schemas delete "$DATABRICKS_CATALOG.$DATABRICKS_SCHEMA" --force $dp 2>$null
    databricks external-locations delete kinesis_lab_raw --force $dp 2>$null
    databricks storage-credentials delete kinesis_lab_raw_cred --force $dp 2>$null
    databricks credentials delete-credential kinesis_lab_cred --force $dp 2>$null
    Write-Host "== Dashboard and SNS topic"
    aws cloudwatch delete-dashboards --dashboard-names kinesis-lab $r 2>$null
    $topic = "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:kinesis-lab-alerts"
    aws sns delete-topic --topic-arn $topic $r 2>$null
    aws logs delete-log-group --log-group-name "/aws/kinesisfirehose/$FIREHOSE_NAME" $r 2>$null

    Write-Host "== IAM roles"
    foreach ($role in @('kinesis-lab-firehose-role', 'kinesis-lab-databricks-role')) {
        foreach ($p in (aws iam list-role-policies --role-name $role --query "PolicyNames[]" --output text 2>$null) -split '\s+' | Where-Object { $_ }) {
            aws iam delete-role-policy --role-name $role --policy-name $p
        }
        aws iam delete-role --role-name $role 2>$null
    }
    Write-Host "== IAM users"
    foreach ($user in @('kinesis-lab-producer', 'kinesis-lab-databricks')) {
        foreach ($k in (aws iam list-access-keys --user-name $user --query "AccessKeyMetadata[].AccessKeyId" --output text 2>$null) -split '\s+' | Where-Object { $_ }) {
            aws iam delete-access-key --user-name $user --access-key-id $k
        }
        foreach ($p in (aws iam list-user-policies --user-name $user --query "PolicyNames[]" --output text 2>$null) -split '\s+' | Where-Object { $_ }) {
            aws iam delete-user-policy --user-name $user --policy-name $p
        }
        aws iam delete-user --user-name $user 2>$null
    }
    Write-Host "== S3 bucket $BUCKET_NAME (all objects; versioning is not enabled)"
    aws s3 rb "s3://$BUCKET_NAME" --force $r 2>$null
}
Write-Host "Done. Check: Kinesis, Firehose and CloudWatch Alarms consoles should be empty for this lab."
