# Step 4: Databricks Unity Catalog access to AWS, with no access keys
# The role trusts the Databricks UC master role (with your Databricks account's external ID) and itself (self-assume).
. "$PSScriptRoot\_env.ps1"
$P = $DATABRICKS_PROFILE; $ROLE = "kinesis-lab-databricks-role"; $ARN = "arn:aws:iam::${AWS_ACCOUNT_ID}:role/$ROLE"
$BUCKET_RAW = "s3://$BUCKET_NAME/raw/"

# 1. IAM role. The self-reference in the trust policy is only valid once the role exists,
#    so create the role with the UC master role only, then add the self-reference
$trust = Render-Template "$PSScriptRoot\policies\databricks-trust.json"
Set-Content "$env:TEMP\lab-trust0.json" ($trust -replace ",\s*`"$([regex]::Escape($ARN))`"", '')
aws iam create-role --role-name $ROLE --assume-role-policy-document "file://$env:TEMP/lab-trust0.json" --tags Key=project,Value=$PROJECT_TAG
aws iam put-role-policy --role-name $ROLE --policy-name databricks-read-only --policy-document "file://$(Render-ToTemp $PSScriptRoot\policies\databricks-permissions.json)"
Start-Sleep 15  # IAM propagation, otherwise the self-reference is rejected as an invalid principal
aws iam update-assume-role-policy --role-name $ROLE --policy-document "file://$(Render-ToTemp $PSScriptRoot\policies\databricks-trust.json)"
Get-ChildItem $env:TEMP -Filter "lab-*" | Remove-Item

# 2. Unity Catalog: service credential (Kinesis), storage credential and external location (S3 archive), schema
databricks credentials create-credential --json "{\`"name\`":\`"kinesis_lab_cred\`",\`"purpose\`":\`"SERVICE\`",\`"aws_iam_role\`":{\`"role_arn\`":\`"$ARN\`"}}" -p $P
databricks storage-credentials create --json "{\`"name\`":\`"kinesis_lab_raw_cred\`",\`"read_only\`":true,\`"aws_iam_role\`":{\`"role_arn\`":\`"$ARN\`"}}" -p $P
databricks external-locations create kinesis_lab_raw $BUCKET_RAW kinesis_lab_raw_cred --read-only -p $P   # needs at least one object under raw/
databricks schemas create $DATABRICKS_SCHEMA $DATABRICKS_CATALOG -p $P
