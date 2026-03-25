. (Join-Path $PSScriptRoot "_runtime.ps1")

$ErrorActionPreference = "Stop"

Ensure-RuntimeDir

$services = @(
    (Get-ServiceDefinition -Name "backend"),
    (Get-ServiceDefinition -Name "frontend")
)

$allReady = $true

foreach ($service in $services) {
    $process = Get-TrackedProcess -Service $service
    $healthy = $false

    if ($null -ne $process) {
        $healthy = Test-HttpReady -Uri $service.Url
    }

    if ($null -eq $process) {
        Write-Host "$($service.Name): stopped"
        $allReady = $false
        continue
    }

    if ($healthy) {
        Write-Host "$($service.Name): running (PID $($process.Id)) - $($service.Url)"
    } else {
        Write-Host "$($service.Name): running but not responding yet (PID $($process.Id))"
        Write-Host "Logs: $($service.LogFile)"
        $allReady = $false
    }
}

if (-not $allReady) {
    exit 1
}
