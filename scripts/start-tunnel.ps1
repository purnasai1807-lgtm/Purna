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

function Resolve-CloudflaredExe {
    $preferredPaths = @(
        (Join-Path $script:ProjectRoot ".tools\cloudflared.exe"),
        "C:\Users\LIKHITHA\AppData\Local\Microsoft\WinGet\Packages\Cloudflare.cloudflared_Microsoft.Winget.Source_8wekyb3d8bbwe\cloudflared.exe"
    )

    foreach ($path in $preferredPaths) {
        if (Test-Path $path) {
            return (Resolve-Path $path).Path
        }
    }

    foreach ($commandName in @("cloudflared.exe", "cloudflared")) {
        $command = Get-Command $commandName -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($null -ne $command) {
            return $command.Source
        }
    }

    throw "cloudflared was not found."
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
            return $match.Groups["url"].Value
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
                return $match.Value
            }
        } catch {
        }
    }

    return $null
}

function Wait-ForTunnelReady {
    param(
        [int]$ProcessId,
        [hashtable]$Tunnel,
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
        if ($null -eq $process) {
            throw "Public tunnel exited before it became ready. Check $($Tunnel.LogFile) and $($Tunnel.ErrorFile)."
        }

        $url = Get-TunnelUrl -Tunnel $Tunnel
        if ($url) {
            Set-Content -Path $Tunnel.PublicUrlFile -Value $url
            return $url
        }

        Start-Sleep -Seconds 1
    }

    throw "Public tunnel did not become ready in time. Check $($Tunnel.LogFile) and $($Tunnel.ErrorFile)."
}

Stop-TrackedService -Service $tunnel -Quiet
Remove-Item $tunnel.PublicUrlFile -ErrorAction SilentlyContinue

$cloudflaredExe = Resolve-CloudflaredExe

$process = Start-Process `
    -FilePath $cloudflaredExe `
    -ArgumentList @("tunnel", "--no-autoupdate", "--metrics", "127.0.0.1:20242", "--url", "http://127.0.0.1:3000") `
    -WorkingDirectory $script:ProjectRoot `
    -RedirectStandardOutput $tunnel.LogFile `
    -RedirectStandardError $tunnel.ErrorFile `
    -PassThru

Set-Content -Path $tunnel.PidFile -Value $process.Id
$url = Wait-ForTunnelReady -ProcessId $process.Id -Tunnel $tunnel

Write-Host "Public tunnel is running:"
Write-Host $url
