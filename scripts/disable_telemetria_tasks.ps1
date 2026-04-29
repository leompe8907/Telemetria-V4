param(
  [string[]]$TaskNames = @(
    "Telemetria-Run",
    "Telemetria-Ops-Check",
    "Telemetria-Build-Aggregates",
    "Telemetria-Integrity-Check"
  ),
  [switch]$StopRunningProcesses
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function _TaskExists([string]$Name) {
  try {
    $null = Get-ScheduledTask -TaskName $Name -ErrorAction Stop
    return $true
  } catch {
    return $false
  }
}

Write-Host "Deshabilitando tareas Telemetria..." -ForegroundColor Cyan

foreach ($name in $TaskNames) {
  if (!(_TaskExists $name)) {
    Write-Host " - SKIP (no existe): $name" -ForegroundColor Yellow
    continue
  }
  Disable-ScheduledTask -TaskName $name | Out-Null
  Write-Host " - Disabled: $name" -ForegroundColor Green
}

if ($StopRunningProcesses) {
  Write-Host "Deteniendo procesos PowerShell relacionados (best-effort)..." -ForegroundColor Cyan
  $patterns = @(
    "run_telemetry_job\.ps1",
    "run_ops_check\.ps1",
    "run_build_aggregates\.ps1",
    "run_integrity_check\.ps1",
    "Telemetria\\backend\\scripts"
  )
  $rx = ($patterns -join "|")

  $procs = Get-CimInstance Win32_Process |
    Where-Object { ($_.Name -in @("powershell.exe","pwsh.exe")) -and ($_.CommandLine -match $rx) }

  foreach ($p in $procs) {
    try {
      Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
      Write-Host " - Stopped PID=$($p.ProcessId)" -ForegroundColor Green
    } catch {
      Write-Host " - WARN no se pudo detener PID=$($p.ProcessId): $($_.Exception.Message)" -ForegroundColor Yellow
    }
  }
}

Write-Host "Estado actual:" -ForegroundColor Cyan
Get-ScheduledTask -TaskName $TaskNames -ErrorAction SilentlyContinue |
  Select-Object TaskName, State |
  Sort-Object TaskName |
  Format-Table -AutoSize

