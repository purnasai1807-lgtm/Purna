$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "_runtime.ps1")

Ensure-RuntimeDir

$tunnel = @{
    Name = "Public Tunnel"
    PidFile = Join-Path $script:RuntimeDir "tunnel.pid"
    PublicUrlFile = Join-Path $script:RuntimeDir "tunnel-url.txt"
}

Stop-TrackedService -Service $tunnel
Remove-Item $tunnel.PublicUrlFile -ErrorAction SilentlyContinue
