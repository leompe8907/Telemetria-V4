param(
  [string[]]$TaskNames = @(
    "Telemetria-Run",
    "Telemetria-Ops-Check",
    "Telemetria-Build-Aggregates",
    "Telemetria-Integrity-Check"
  ),
  [switch]$RunOnceNow
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

Write-Host "Habilitando tareas Telemetria..." -ForegroundColor Cyan

foreach ($name in $TaskNames) {
  if (!(_TaskExists $name)) {
    Write-Host " - SKIP (no existe): $name" -ForegroundColor Yellow
    continue
  }
  Enable-ScheduledTask -TaskName $name | Out-Null
  Write-Host " - Enabled: $name" -ForegroundColor Green
}

if ($RunOnceNow) {
  Write-Host "Ejecutando una vez (best-effort)..." -ForegroundColor Cyan
  foreach ($name in $TaskNames) {
    if (!(_TaskExists $name)) { continue }
    try {
      Start-ScheduledTask -TaskName $name
      Write-Host " - Started: $name" -ForegroundColor Green
    } catch {
      Write-Host (" - WARN no se pudo iniciar {0}: {1}" -f $name, $_.Exception.Message) -ForegroundColor Yellow
    }
  }
}

Write-Host "Estado actual:" -ForegroundColor Cyan
Get-ScheduledTask -TaskName $TaskNames -ErrorAction SilentlyContinue |
  Select-Object TaskName, State |
  Sort-Object TaskName |
  Format-Table -AutoSize

