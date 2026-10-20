# Render templates with lab.env values, upload notebooks + shared module, then create or update the job
. "$PSScriptRoot\..\infra\_env.ps1"
$P = $DATABRICKS_PROFILE
$Root = "/Users/$DATABRICKS_USER/kinesis-lab"
databricks workspace mkdirs $Root -p $P
# Shared module as a plain workspace FILE (importable); notebooks as NOTEBOOKs
databricks workspace import "$Root/lab_transforms.py" --file (Render-ToTemp "$PSScriptRoot\lab_transforms.py") --format AUTO --overwrite -p $P
foreach ($nb in Get-ChildItem $PSScriptRoot -File | Where-Object { $_.Name -match '^0\d_.*\.(py|sql)$' }) {
    $lang = if ($nb.Extension -eq '.sql') { 'SQL' } else { 'PYTHON' }
    databricks workspace import "$Root/$($nb.BaseName)" --file (Render-ToTemp $nb.FullName) --language $lang --format SOURCE --overwrite -p $P
}
$spec = Render-Template "$PSScriptRoot\job_medallion.json"
$existing = databricks jobs list --name kinesis-lab-medallion -p $P -o json | ConvertFrom-Json
if ($existing) {
    $id = @($existing)[0].job_id
    Set-Content "$env:TEMP\lab-reset.json" "{`"job_id`": $id, `"new_settings`": $spec}"
    databricks jobs reset --json "@$env:TEMP\lab-reset.json" -p $P
    "Updated job $id"
} else {
    Set-Content "$env:TEMP\lab-job.json" $spec
    databricks jobs create --json "@$env:TEMP\lab-job.json" -p $P
}
Get-ChildItem $env:TEMP -Filter "lab-*" | Remove-Item
