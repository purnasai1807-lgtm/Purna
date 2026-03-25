Set-StrictMode -Version Latest

$script:ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$script:RuntimeDir = Join-Path $script:ProjectRoot ".runtime"
$script:BackendDir = Join-Path $script:ProjectRoot "backend"
$script:FrontendDir = Join-Path $script:ProjectRoot "frontend"

function Ensure-RuntimeDir {
    if (-not (Test-Path $script:RuntimeDir)) {
        New-Item -ItemType Directory -Path $script:RuntimeDir | Out-Null
    }
}

function Get-ServiceDefinition {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("backend", "frontend")]
        [string]$Name
    )

    switch ($Name) {
        "backend" {
            return @{
                Name = "Backend API"
                PidFile = Join-Path $script:RuntimeDir "backend.pid"
                LogFile = Join-Path $script:RuntimeDir "backend.log"
                ErrorFile = Join-Path $script:RuntimeDir "backend.err"
                Url = "http://127.0.0.1:8000/health"
            }
        }
        "frontend" {
            return @{
                Name = "Frontend"
                PidFile = Join-Path $script:RuntimeDir "frontend.pid"
                LogFile = Join-Path $script:RuntimeDir "frontend.log"
                ErrorFile = Join-Path $script:RuntimeDir "frontend.err"
                Url = "http://127.0.0.1:3000"
            }
        }
    }
}

function Get-TrackedProcess {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Service
    )

    if (-not (Test-Path $Service.PidFile)) {
        return $null
    }

    $rawPid = (Get-Content $Service.PidFile -Raw).Trim()
    if (-not $rawPid) {
        Remove-Item $Service.PidFile -ErrorAction SilentlyContinue
        return $null
    }

    try {
        return Get-Process -Id ([int]$rawPid) -ErrorAction Stop
    } catch {
        Remove-Item $Service.PidFile -ErrorAction SilentlyContinue
        return $null
    }
}

function Stop-TrackedService {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Service,
        [switch]$Quiet
    )

    $process = Get-TrackedProcess -Service $Service
    if ($null -eq $process) {
        if (-not $Quiet) {
            Write-Host "$($Service.Name) is not running."
        }
        return
    }

    Stop-Process -Id $process.Id -Force -ErrorAction Stop
    Remove-Item $Service.PidFile -ErrorAction SilentlyContinue

    if (-not $Quiet) {
        Write-Host "Stopped $($Service.Name) (PID $($process.Id))."
    }
}

function Resolve-PythonRuntime {
    $preferredPaths = @(
        (Join-Path $script:BackendDir ".venv\Scripts\python.exe")
    )

    $localPythonPackages = Join-Path $script:BackendDir ".packages"
    $pythonPathCandidates = @($null)

    if (Test-Path $localPythonPackages) {
        $pythonPathCandidates += (Resolve-Path $localPythonPackages).Path
    }

    foreach ($path in $preferredPaths) {
        if (Test-Path $path) {
            $resolvedPath = (Resolve-Path $path).Path
            foreach ($pythonPath in $pythonPathCandidates) {
                if (Test-PythonModules -PythonExe $resolvedPath -ModuleNames @("uvicorn", "fastapi", "sqlalchemy") -PythonPath $pythonPath) {
                    return @{
                        PythonExe = $resolvedPath
                        PythonPath = $pythonPath
                    }
                }
            }
        }
    }

    foreach ($commandName in @("python.exe", "python", "py.exe", "py")) {
        $command = Get-Command $commandName -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($null -ne $command) {
            foreach ($pythonPath in $pythonPathCandidates) {
                if (Test-PythonModules -PythonExe $command.Source -ModuleNames @("uvicorn", "fastapi", "sqlalchemy") -PythonPath $pythonPath) {
                    return @{
                        PythonExe = $command.Source
                        PythonPath = $pythonPath
                    }
                }
            }
        }
    }

    throw "No Python interpreter with the required backend packages was found."
}

function Resolve-NodeExe {
    $preferredPaths = @(
        (Join-Path $script:ProjectRoot ".tools\node-v22.22.1-win-x64\node.exe")
    )

    foreach ($path in $preferredPaths) {
        if (Test-Path $path) {
            return (Resolve-Path $path).Path
        }
    }

    foreach ($commandName in @("node.exe", "node")) {
        $command = Get-Command $commandName -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($null -ne $command) {
            return $command.Source
        }
    }

    throw "Node.js was not found. Install Node.js or restore .tools\node-v22.22.1-win-x64."
}

function Resolve-NextBin {
    $nextBin = Join-Path $script:FrontendDir "node_modules\next\dist\bin\next"
    if (Test-Path $nextBin) {
        return (Resolve-Path $nextBin).Path
    }

    throw "frontend\node_modules is missing. Run npm install in frontend first."
}

function Test-PythonModules {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe,
        [Parameter(Mandatory = $true)]
        [string[]]$ModuleNames,
        [string]$PythonPath
    )

    $importCommand = "import " + ($ModuleNames -join ", ")
    $originalPythonPath = $env:PYTHONPATH

    try {
        if ($PythonPath) {
            if ($originalPythonPath) {
                $env:PYTHONPATH = "$PythonPath;$originalPythonPath"
            } else {
                $env:PYTHONPATH = $PythonPath
            }
        } elseif ($null -eq $originalPythonPath) {
            Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
        }

        & $PythonExe -c $importCommand *> $null
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    } finally {
        if ($null -ne $originalPythonPath) {
            $env:PYTHONPATH = $originalPythonPath
        } else {
            Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
        }
    }
}

function Test-HttpReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Uri
    )

    try {
        $response = Invoke-WebRequest -Uri $Uri -TimeoutSec 5 -UseBasicParsing
        return $response.StatusCode -ge 200 -and $response.StatusCode -lt 500
    } catch {
        return $false
    }
}

function Wait-ForServiceReady {
    param(
        [Parameter(Mandatory = $true)]
        [int]$ProcessId,
        [Parameter(Mandatory = $true)]
        [hashtable]$Service,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
        if ($null -eq $process) {
            throw "$($Service.Name) exited before it became ready. Check $($Service.LogFile) and $($Service.ErrorFile)."
        }

        if (Test-HttpReady -Uri $Service.Url) {
            return
        }

        Start-Sleep -Seconds 1
    }

    throw "$($Service.Name) did not become ready in time. Check $($Service.LogFile) and $($Service.ErrorFile)."
}
