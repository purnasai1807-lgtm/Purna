param(
    [switch]$SkipFrontendBuild,
    [string]$FrontendHost = "0.0.0.0"
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
if (-not $env:NEXT_PUBLIC_API_BASE_URL) {
    $env:NEXT_PUBLIC_API_BASE_URL = "/api/proxy/api/v1"
}
if (-not $env:LOCAL_BACKEND_API_URL) {
    $env:LOCAL_BACKEND_API_URL = "http://127.0.0.1:8000/api/v1"
}
if (-not $env:NEXT_PUBLIC_DIRECT_BACKEND_API_URL) {
    $env:NEXT_PUBLIC_DIRECT_BACKEND_API_URL = "/api/proxy/api/v1"
}

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
        -ArgumentList @("-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000") `
        -WorkingDirectory $script:BackendDir `
        -RedirectStandardOutput $backend.LogFile `
        -RedirectStandardError $backend.ErrorFile `
        -PassThru
    Set-Content -Path $backend.PidFile -Value $backendProcess.Id
    Wait-ForServiceReady -ProcessId $backendProcess.Id -Service $backend -TimeoutSeconds 60

    Write-Host "Starting frontend..."
    $frontendProcess = Start-Process `
        -FilePath $nodeExe `
        -ArgumentList @("`"$nextBin`"", "start", "--hostname", $FrontendHost, "--port", "3000") `
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
Write-Host "Frontend: http://127.0.0.1:3000"
Write-Host "Backend:  http://127.0.0.1:8000/health"
$networkIp = Get-PrimaryIpv4Address
if ($networkIp) {
    Write-Host "Network:  http://$networkIp`:3000"
}
Write-Host "Logs:     $script:RuntimeDir"
Write-Host ""
Write-Host "You can close VS Code and the app will keep running until this PC is turned off or you stop it."
