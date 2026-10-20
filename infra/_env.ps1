# Dot-source this from any script:  . "$PSScriptRoot\_env.ps1"
# Loads lab.env (gitignored, copy from lab.env.example) into variables, and defines Render-Template,
# which replaces {{KEY}} tokens in committed templates (policies, notebooks, job spec) with those values.
$LabRoot = Split-Path $PSScriptRoot -Parent
$envFile = Join-Path $LabRoot "lab.env"
if (-not (Test-Path $envFile)) { throw "Missing $envFile - copy lab.env.example to lab.env and fill it in." }
$LabEnv = [ordered]@{}
Get-Content $envFile | Where-Object { $_ -match '^\s*[A-Z_]+=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2
    $LabEnv[$k.Trim()] = $v.Trim()
    Set-Variable -Name $k.Trim() -Value $v.Trim() -Scope Script
}

function Render-Template([string]$Path) {
    $text = [IO.File]::ReadAllText($Path, [Text.Encoding]::UTF8)
    foreach ($k in $LabEnv.Keys) { $text = $text.Replace("{{$k}}", $LabEnv[$k]) }
    if ($text -match '\{\{([A-Z_]+)\}\}') { throw "Unresolved token {{$($Matches[1])}} in $Path" }
    return $text
}

# Render a template to a temp file and return its path (for CLI file:// arguments)
function Render-ToTemp([string]$Path) {
    $out = Join-Path $env:TEMP ("lab-" + [IO.Path]::GetFileName($Path))
    # UTF-8 without BOM: a BOM breaks the Databricks notebook header and some JSON parsers
    [IO.File]::WriteAllText($out, (Render-Template $Path), (New-Object Text.UTF8Encoding($false)))
    return $out
}
