param(
    [switch]$SkipFrontendBuild,
    [string]$FrontendHost = "0.0.0.0",
    [int]$FrontendPort = 3000,
    [string]$BackendHost = "127.0.0.1",
    [int]$BackendPort = 8000
)

. (Join-Path $PSScriptRoot "_runtime.ps1")

$ErrorActionPreference = "Stop"

Ensure-RuntimeDir

function Reset-FrontendBuildArtifacts {
    $generatedPaths = @(
        (Join-Path $script:FrontendDir ".next"),
        (Join-Path $script:FrontendDir "tsconfig.tsbuildinfo")
    )

    foreach ($path in $generatedPaths) {
        if (Test-Path $path) {
            Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction Stop
        }
    }
}

function Get-PrimaryIpv4Address {
    try {
        $address = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop |
            Where-Object {
                $_.IPAddress -notlike "127.*" -and
                $_.IPAddress -notlike "169.254.*"
            } |
            Sort-Object InterfaceMetric |
            Select-Object -First 1 -ExpandProperty IPAddress

        return $address
    } catch {
        return $null
    }
}

function Test-PortAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BindHost,
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    $listener = $null

    try {
        $ipAddress =
            if ($BindHost -eq "0.0.0.0") {
                [System.Net.IPAddress]::Any
            } else {
                [System.Net.IPAddress]::Parse($BindHost)
            }

        $listener = [System.Net.Sockets.TcpListener]::new($ipAddress, $Port)
        $listener.Start()
        return $true
    } catch {
        return $false
    } finally {
        if ($null -ne $listener) {
            $listener.Stop()
        }
    }
}

function Resolve-AvailablePort {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BindHost,
        [Parameter(Mandatory = $true)]
        [int]$PreferredPort,
        [int]$SearchWindow = 20
    )

    if (Test-PortAvailable -BindHost $BindHost -Port $PreferredPort) {
        return $PreferredPort
    }

    for ($candidatePort = $PreferredPort + 1; $candidatePort -le ($PreferredPort + $SearchWindow); $candidatePort += 1) {
        if (Test-PortAvailable -BindHost $BindHost -Port $candidatePort) {
            return $candidatePort
        }
    }

    throw "No available TCP port found for $BindHost starting at $PreferredPort."
}

$resolvedBackendPort = Resolve-AvailablePort -BindHost $BackendHost -PreferredPort $BackendPort
$resolvedFrontendPort = Resolve-AvailablePort -BindHost $FrontendHost -PreferredPort $FrontendPort
Set-RuntimeConfig -BackendHost $BackendHost -BackendPort $resolvedBackendPort -FrontendHost $FrontendHost -FrontendPort $resolvedFrontendPort

$backend = Get-ServiceDefinition -Name "backend"
$frontend = Get-ServiceDefinition -Name "frontend"

Stop-TrackedService -Service $frontend -Quiet
Stop-TrackedService -Service $backend -Quiet

$pythonRuntime = Resolve-PythonRuntime
$pythonExe = $pythonRuntime.PythonExe
$nodeExe = Resolve-NodeExe
$nextBin = Resolve-NextBin

if ($pythonRuntime.PythonPath) {
    if ($env:PYTHONPATH) {
        $env:PYTHONPATH = "$($pythonRuntime.PythonPath);$($env:PYTHONPATH)"
    } else {
        $env:PYTHONPATH = $pythonRuntime.PythonPath
    }
}

$env:NODE_ENV = "production"
$env:NEXT_PUBLIC_API_BASE_URL = "/api/proxy/api/v1"
$env:LOCAL_BACKEND_API_URL = "http://$BackendHost`:$resolvedBackendPort/api/v1"
$env:NEXT_PUBLIC_DIRECT_BACKEND_API_URL = "http://$BackendHost`:$resolvedBackendPort/api/v1"

try {
    if (-not $SkipFrontendBuild) {
        Write-Host "Building frontend for background run..."
        Reset-FrontendBuildArtifacts
        Push-Location $script:FrontendDir
        try {
            & $nodeExe $nextBin build
            if ($LASTEXITCODE -ne 0) {
                throw "Frontend build failed."
            }
        } finally {
            Pop-Location
        }
    }

    Write-Host "Starting backend..."
    $backendProcess = Start-Process `
        -FilePath $pythonExe `
        -ArgumentList @("-m", "uvicorn", "app.main:app", "--host", $BackendHost, "--port", "$resolvedBackendPort") `
        -WorkingDirectory $script:BackendDir `
        -RedirectStandardOutput $backend.LogFile `
        -RedirectStandardError $backend.ErrorFile `
        -PassThru
    Set-Content -Path $backend.PidFile -Value $backendProcess.Id
    Wait-ForServiceReady -ProcessId $backendProcess.Id -Service $backend -TimeoutSeconds 60

    Write-Host "Starting frontend..."
    $frontendProcess = Start-Process `
        -FilePath $nodeExe `
        -ArgumentList @("`"$nextBin`"", "start", "--hostname", $FrontendHost, "--port", "$resolvedFrontendPort") `
        -WorkingDirectory $script:FrontendDir `
        -RedirectStandardOutput $frontend.LogFile `
        -RedirectStandardError $frontend.ErrorFile `
        -PassThru
    Set-Content -Path $frontend.PidFile -Value $frontendProcess.Id
    Wait-ForServiceReady -ProcessId $frontendProcess.Id -Service $frontend -TimeoutSeconds 90
} catch {
    Stop-TrackedService -Service $frontend -Quiet
    Stop-TrackedService -Service $backend -Quiet
    throw
}

Write-Host ""
Write-Host "Auto Analytics AI is running in the background."
Write-Host "Frontend: http://127.0.0.1:$resolvedFrontendPort"
Write-Host "Backend:  http://127.0.0.1:$resolvedBackendPort/health"
$networkIp = Get-PrimaryIpv4Address
if ($networkIp) {
    Write-Host "Network:  http://$networkIp`:$resolvedFrontendPort"
}
Write-Host "Logs:     $script:RuntimeDir"
Write-Host ""
Write-Host "You can close VS Code and the app will keep running until this PC is turned off or you stop it."
