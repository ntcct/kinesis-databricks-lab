# Step 6: SNS alerts, CloudWatch alarms, dashboard, and Databricks metric publishing permission
. "$PSScriptRoot\_env.ps1"
$R = $AWS_REGION; $S = $STREAM_NAME; $F = $FIREHOSE_NAME
# Unencrypted on purpose: CloudWatch alarms can't publish to a topic encrypted with the AWS-managed aws/sns key (a customer-managed key would be needed)
$TOPIC = aws sns create-topic --name kinesis-lab-alerts --tags Key=project,Value=$PROJECT_TAG --region $R --query TopicArn --output text
aws sns subscribe --topic-arn $TOPIC --protocol email --notification-endpoint $ALERT_EMAIL --region $R   # click Confirm in the email

function Alarm($name, $desc, $ns, $metric, $dim, $stat, $period, $thr, $op) {
  aws cloudwatch put-metric-alarm --alarm-name $name --alarm-description $desc --namespace $ns --metric-name $metric `
    --dimensions $dim --statistic $stat --period $period --evaluation-periods 1 --datapoints-to-alarm 1 `
    --threshold $thr --comparison-operator $op --treat-missing-data notBreaching `
    --alarm-actions $TOPIC --ok-actions $TOPIC --tags Key=project,Value=$PROJECT_TAG --region $R
}
Alarm kinesis-lab-write-throttled   "Shard write limit exceeded"              AWS/Kinesis  WriteProvisionedThroughputExceeded "Name=StreamName,Value=$S"         Sum     60  0      GreaterThanThreshold
Alarm kinesis-lab-iterator-age      "Consumer more than 10 min behind"        AWS/Kinesis  GetRecords.IteratorAgeMilliseconds "Name=StreamName,Value=$S"         Maximum 300 600000 GreaterThanThreshold
Alarm kinesis-lab-firehose-behind   "Firehose more than 5 min behind"         AWS/Firehose KinesisMillisBehindLatest          "Name=DeliveryStreamName,Value=$F" Maximum 300 300000 GreaterThanThreshold
Alarm kinesis-lab-firehose-delivery "Firehose S3 delivery success below 100%" AWS/Firehose DeliveryToS3.Success                "Name=DeliveryStreamName,Value=$F" Minimum 300 1      LessThanThreshold

aws cloudwatch put-dashboard --dashboard-name kinesis-lab --dashboard-body "file://$(Render-ToTemp $PSScriptRoot\dashboard.json)" --region $R

# Databricks may publish custom metrics, but only to its own namespace
$pol = '{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"cloudwatch:PutMetricData\",\"Resource\":\"*\",\"Condition\":{\"StringEquals\":{\"cloudwatch:namespace\":\"KinesisLab/Pipeline\"}}}]}'
aws iam put-role-policy --role-name kinesis-lab-databricks-role --policy-name publish-pipeline-metrics --policy-document $pol
Get-ChildItem $env:TEMP -Filter "lab-*" | Remove-Item
