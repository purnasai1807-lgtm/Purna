$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "_runtime.ps1")

Ensure-RuntimeDir

$tunnel = @{
    Name = "Public Tunnel"
    PidFile = Join-Path $script:RuntimeDir "tunnel.pid"
    LogFile = Join-Path $script:RuntimeDir "tunnel.log"
    ErrorFile = Join-Path $script:RuntimeDir "tunnel.err"
    PublicUrlFile = Join-Path $script:RuntimeDir "tunnel-url.txt"
    MetricsUrl = "http://127.0.0.1:20242/metrics"
}

function Get-TunnelUrl {
    param(
        [hashtable]$Tunnel
    )

    if (Test-Path $Tunnel.PublicUrlFile) {
        $url = (Get-Content $Tunnel.PublicUrlFile -Raw).Trim()
        if ($url) {
            return $url
        }
    }

    try {
        $metrics = Invoke-WebRequest -Uri $Tunnel.MetricsUrl -TimeoutSec 5 -UseBasicParsing
        $match = [regex]::Match(
            $metrics.Content,
            'userHostname="(?<url>https://[^"]+\.trycloudflare\.com)"'
        )
        if ($match.Success) {
            $url = $match.Groups["url"].Value
            Set-Content -Path $Tunnel.PublicUrlFile -Value $url
            return $url
        }
    } catch {
    }

    foreach ($path in @($Tunnel.LogFile, $Tunnel.ErrorFile)) {
        if (-not (Test-Path $path)) {
            continue
        }

        try {
            $match = [regex]::Match(
                (Get-Content $path -Raw),
                'https://[-a-z0-9]+\.trycloudflare\.com'
            )
            if ($match.Success) {
                Set-Content -Path $Tunnel.PublicUrlFile -Value $match.Value
                return $match.Value
            }
        } catch {
        }
    }

    return ""
}

$process = Get-TrackedProcess -Service $tunnel
if ($null -eq $process) {
    Write-Host "Public Tunnel: stopped"
    exit 1
}

$url = Get-TunnelUrl -Tunnel $tunnel

if ($url) {
    Write-Host "Public Tunnel: running (PID $($process.Id)) - $url"
} else {
    Write-Host "Public Tunnel: running (PID $($process.Id))"
    Write-Host "Logs: $($tunnel.LogFile)"
}
