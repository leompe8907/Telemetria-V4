param(
  [string]$BackendDir = (Get-Location).Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location $BackendDir

$logDir = Join-Path $BackendDir "logs"
if (!(Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}
$logFile = Join-Path $logDir "ops_check_task.log"
$ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

$venvActivate = Join-Path $BackendDir "env\\Scripts\\Activate.ps1"
if (Test-Path $venvActivate) {
  . $venvActivate
}

try {
  Add-Content -Path $logFile -Value "[$ts] START telemetry_ops_check"

  # Forzar UTF-8 para evitar caracteres corruptos en logs
  $env:PYTHONUTF8 = "1"
  $env:PYTHONIOENCODING = "utf-8"

  # Evitar Tee-Object porque en Windows PowerShell suele escribir Unicode (UTF-16) al archivo.
  $out = python manage.py telemetry_ops_check 2>&1
  $out | Out-File -FilePath $logFile -Append -Encoding utf8

  $code = $LASTEXITCODE
  Add-Content -Path $logFile -Value "[$ts] END exit_code=$code"
  exit $code
} catch {
  Add-Content -Path $logFile -Value "[$ts] ERROR $($_.Exception.Message)"
  throw
}

