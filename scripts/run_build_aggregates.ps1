param(
  [string]$BackendDir = (Get-Location).Path,
  [int]$Days = 7
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location $BackendDir

$logDir = Join-Path $BackendDir "logs"
if (!(Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}
$logFile = Join-Path $logDir "build_aggregates_task.log"
$ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

$venvActivate = Join-Path $BackendDir "env\\Scripts\\Activate.ps1"
if (Test-Path $venvActivate) {
  . $venvActivate
}

try {
  Add-Content -Path $logFile -Value "[$ts] START telemetry_build_aggregates days=$Days"

  $env:PYTHONUTF8 = "1"
  $env:PYTHONIOENCODING = "utf-8"

  $out = python manage.py telemetry_build_aggregates --days $Days 2>&1
  $out | Out-File -FilePath $logFile -Append -Encoding utf8

  $code = $LASTEXITCODE
  Add-Content -Path $logFile -Value "[$ts] END exit_code=$code"
  exit $code
} catch {
  Add-Content -Path $logFile -Value "[$ts] ERROR $($_.Exception.Message)"
  throw
}

