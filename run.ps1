# Single entry point for the lab.
#   .\run.ps1 test                       # pytest in Docker (local Spark 4.2.0, no cloud calls)
#   .\run.ps1 produce [-Rate 20] [-Duration 120] [-Mode normal|hot|burst]
#   .\run.ps1 deploy                     # render templates and upload notebooks and the job to Databricks
#   .\run.ps1 job [-SimulateLoss 0]      # run the medallion job and wait for it to finish
#   .\run.ps1 teardown [-All]            # delete everything billable; -All deletes everything
#   .\run.ps1 pause                      # delete only stream + Firehose (stops hourly charges), keep all data
#   .\run.ps1 resume                     # recreate stream + Firehose, reset bronze checkpoint
#   .\run.ps1 verify                     # consistency checks across bronze/silver/gold (fails if any is false)
param(
    [Parameter(Mandatory)][ValidateSet('test', 'produce', 'deploy', 'job', 'teardown', 'pause', 'resume', 'verify')][string]$Task,
    [int]$Rate = 20, [int]$Duration = 120, [ValidateSet('normal', 'hot', 'burst')][string]$Mode = 'normal',
    [int]$SimulateLoss = 0, [switch]$All
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
. .\infra\_env.ps1

switch ($Task) {
    'test' {
        docker build -q -t kinesis-lab-tests:1.0 -f tests/Dockerfile tests | Out-Null
        docker run --rm -v "${PWD}:/lab:ro" kinesis-lab-tests:1.0
    }
    'produce' {
        docker build -q -t kinesis-lab-producer:1.1 producer | Out-Null
        # AWS credentials are mounted read-only; nothing is baked into the image
        docker run --rm -v "$env:USERPROFILE\.aws:/home/app/.aws:ro" kinesis-lab-producer:1.1 `
            --stream $STREAM_NAME --region $AWS_REGION --rate $Rate --duration $Duration --mode $Mode
    }
    'deploy' { & .\databricks\deploy.ps1 }
    'job' {
        $id = @(databricks jobs list --name kinesis-lab-medallion -p $DATABRICKS_PROFILE -o json | ConvertFrom-Json)[0].job_id
        $body = "{`"job_id`": $id, `"notebook_params`": {`"simulate_loss`": `"$SimulateLoss`"}}"
        Set-Content "$env:TEMP\lab-runnow.json" $body
        databricks jobs run-now --json "@$env:TEMP\lab-runnow.json" -p $DATABRICKS_PROFILE -o json --timeout 40m |
            ConvertFrom-Json | ForEach-Object { $_.tasks | Select-Object task_key, @{n = 'result'; e = { $_.state.result_state } } }
        Remove-Item "$env:TEMP\lab-runnow.json"
    }
    'teardown' { if ($All) { & .\infra\teardown.ps1 -All } else { & .\infra\teardown.ps1 } }
    'pause' { & .\infra\pause.ps1 }
    'resume' { & .\infra\resume.ps1 }
    'verify' {
        # Runs databricks\06_verify.sql on the first SQL warehouse; fails if any check is false
        $wh = @(databricks warehouses list -p $DATABRICKS_PROFILE -o json | ConvertFrom-Json)[0].id
        # [IO.File] gives a plain string; Get-Content -Raw adds properties that ConvertTo-Json serialises
        $sql = [IO.File]::ReadAllText("$PSScriptRoot\databricks\06_verify.sql")
        $body = @{ warehouse_id = $wh; statement = $sql; wait_timeout = '50s' } | ConvertTo-Json
        Set-Content "$env:TEMP\lab-verify.json" $body
        $res = databricks api post /api/2.0/sql/statements --json "@$env:TEMP\lab-verify.json" -p $DATABRICKS_PROFILE | ConvertFrom-Json
        Remove-Item "$env:TEMP\lab-verify.json"
        if ($res.status.state -ne 'SUCCEEDED') { throw "Verify query $($res.status.state): $($res.status.error.message)" }
        $rows = $res.result.data_array | ForEach-Object { [pscustomobject]@{ check = $_[0]; ok = $_[1] } }
        $rows | Format-Table -AutoSize
        if ($rows | Where-Object { $_.ok -ne 'true' }) { throw "Verification failed" } else { "All checks passed" }
    }
}
