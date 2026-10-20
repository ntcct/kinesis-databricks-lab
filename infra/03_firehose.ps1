# Step 3: least-privilege IAM role and Firehose (Kinesis stream -> S3, GZIP, hourly prefixes)
. "$PSScriptRoot\_env.ps1"
$R = $AWS_REGION; $A = $AWS_ACCOUNT_ID; $F = $FIREHOSE_NAME
$ROLE = "kinesis-lab-firehose-role"; $LG = "/aws/kinesisfirehose/$F"

aws iam create-role --role-name $ROLE --assume-role-policy-document "file://$(Render-ToTemp $PSScriptRoot\policies\firehose-trust.json)" --tags Key=project,Value=$PROJECT_TAG
aws iam put-role-policy --role-name $ROLE --policy-name firehose-least-privilege --policy-document "file://$(Render-ToTemp $PSScriptRoot\policies\firehose-permissions.json)"

aws logs create-log-group --log-group-name $LG --tags project=$PROJECT_TAG --region $R
aws logs put-retention-policy --log-group-name $LG --retention-in-days 7 --region $R
aws logs create-log-stream --log-group-name $LG --log-stream-name S3Delivery --region $R

Start-Sleep 10  # IAM propagation
$cfg = @{
  RoleARN = "arn:aws:iam::${A}:role/$ROLE"; BucketARN = "arn:aws:s3:::$BUCKET_NAME"
  Prefix = 'raw/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/!{timestamp:HH}/'
  ErrorOutputPrefix = 'errors/!{firehose:error-output-type}/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/'
  BufferingHints = @{ SizeInMBs = 1; IntervalInSeconds = 60 }; CompressionFormat = "GZIP"
  CloudWatchLoggingOptions = @{ Enabled = $true; LogGroupName = $LG; LogStreamName = "S3Delivery" }
} | ConvertTo-Json -Depth 4 -Compress
Set-Content "$env:TEMP\lab-fh_s3.json" $cfg
aws firehose create-delivery-stream --delivery-stream-name $F --delivery-stream-type KinesisStreamAsSource `
  --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:${R}:${A}:stream/$STREAM_NAME,RoleARN=arn:aws:iam::${A}:role/$ROLE" `
  --extended-s3-destination-configuration "file://$env:TEMP/lab-fh_s3.json" --tags Key=project,Value=$PROJECT_TAG --region $R
Get-ChildItem $env:TEMP -Filter "lab-*" | Remove-Item
aws firehose describe-delivery-stream --delivery-stream-name $F --query "DeliveryStreamDescription.DeliveryStreamStatus" --region $R
