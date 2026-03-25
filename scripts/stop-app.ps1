. (Join-Path $PSScriptRoot "_runtime.ps1")

$ErrorActionPreference = "Stop"

Ensure-RuntimeDir

$frontend = Get-ServiceDefinition -Name "frontend"
$backend = Get-ServiceDefinition -Name "backend"

Stop-TrackedService -Service $frontend
Stop-TrackedService -Service $backend
