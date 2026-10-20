# Step 1: S3 bucket and Kinesis data stream
. "$PSScriptRoot\_env.ps1"
$R = $AWS_REGION; $B = $BUCKET_NAME; $S = $STREAM_NAME

aws s3api create-bucket --bucket $B --region $R   # us-east-1; other regions need --create-bucket-configuration LocationConstraint=$R
aws s3api put-public-access-block --bucket $B --public-access-block-configuration `
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
aws s3api put-bucket-encryption --bucket $B --server-side-encryption-configuration `
  '{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"AES256\"},\"BucketKeyEnabled\":true}]}'
aws s3api put-bucket-lifecycle-configuration --bucket $B --lifecycle-configuration `
  '{\"Rules\":[{\"ID\":\"expire-raw-7d\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"raw/\"},\"Expiration\":{\"Days\":7}},{\"ID\":\"expire-errors-7d\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"errors/\"},\"Expiration\":{\"Days\":7}},{\"ID\":\"abort-mpu\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"\"},\"AbortIncompleteMultipartUpload\":{\"DaysAfterInitiation\":1}}]}'
aws s3api put-bucket-tagging --bucket $B --tagging "TagSet=[{Key=project,Value=$PROJECT_TAG}]"

aws kinesis create-stream --stream-name $S --shard-count 1 --stream-mode-details StreamMode=PROVISIONED --tags project=$PROJECT_TAG --region $R
aws kinesis wait stream-exists --stream-name $S --region $R
aws kinesis start-stream-encryption --stream-name $S --encryption-type KMS --key-id alias/aws/kinesis --region $R
aws kinesis describe-stream-summary --stream-name $S --region $R
