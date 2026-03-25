$ErrorActionPreference = "Stop"

$startScript = Join-Path $PSScriptRoot "start-app.ps1"

try {
    & $startScript -SkipFrontendBuild
} catch {
    Write-Host "Cached frontend build was unavailable. Rebuilding before startup..."
    & $startScript
}
