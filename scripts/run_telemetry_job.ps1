param(
  [string]$BackendDir = (Get-Location).Path,
  [int]$Limit = 1000,
  [int]$BatchSize = 1000,
  [int]$MergeBatchSize = 500,
  [int]$BackfillLastN = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location $BackendDir

$logDir = Join-Path $BackendDir "logs"
if (!(Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}
$logFile = Join-Path $logDir "telemetry_task.log"
$ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

$venvActivate = Join-Path $BackendDir "env\\Scripts\\Activate.ps1"
if (Test-Path $venvActivate) {
  . $venvActivate
}

try {
  Add-Content -Path $logFile -Value "[$ts] START telemetry_run limit=$Limit batch=$BatchSize merge_batch=$MergeBatchSize backfill=$BackfillLastN"

  # Forzar UTF-8 para evitar caracteres corruptos en logs
  $env:PYTHONUTF8 = "1"
  $env:PYTHONIOENCODING = "utf-8"

  # Evitar Tee-Object porque en Windows PowerShell suele escribir Unicode (UTF-16) al archivo.
  $out = python manage.py telemetry_run `
    --limit $Limit `
    --batch-size $BatchSize `
    --merge-batch-size $MergeBatchSize `
    --backfill-last-n $BackfillLastN 2>&1
  $out | Out-File -FilePath $logFile -Append -Encoding utf8

  $code = $LASTEXITCODE
  Add-Content -Path $logFile -Value "[$ts] END exit_code=$code"
  exit $code
} catch {
  Add-Content -Path $logFile -Value "[$ts] ERROR $($_.Exception.Message)"
  throw
}

