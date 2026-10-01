param(
    [int]$WaitSeconds = 180
)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = '1'
$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot 'infra/flink/compose.yaml'
$manifestFile = Join-Path $projectRoot '.runtime/flink/connectors/manifest.json'
$python = 'D:\PythonVenv\Scripts\python.exe'
$baseImage = 'flink:2.2.1-scala_2.12-java17'
$runtimeImage = 'sinket-flink:2.2.1-connectors'
$projectName = 'sinket-realtime'
$networkName = 'sinket-realtime'
$dorisNetwork = if ($env:DORIS_DOCKER_NETWORK) { $env:DORIS_DOCKER_NETWORK } else { 'dunnelean_doris' }

function Invoke-Docker {
    param([string[]]$DockerArgs)
    $result = & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker failed: docker $($DockerArgs -join ' ')" }
    return $result
}
function Read-DockerJson {
    param([string[]]$DockerArgs)
    return ((Invoke-Docker $DockerArgs) -join "`n" | ConvertFrom-Json)
}
function Assert-PortAvailable {
    param([int]$Port, [string]$Service, [object[]]$RunningContainers)
    $owned = $false
    foreach ($container in $RunningContainers) {
        foreach ($bindingProperty in $container.NetworkSettings.Ports.PSObject.Properties) {
            foreach ($binding in @($bindingProperty.Value)) {
                if ($null -eq $binding -or [int]$binding.HostPort -ne $Port) { continue }
                $labels = $container.Config.Labels
                if ($labels.'com.docker.compose.project' -ne $projectName -or $labels.'com.docker.compose.service' -ne $Service) {
                    throw "Port $Port is published by unrelated container $($container.Name). Stop or reconfigure that owner first."
                }
                if ($binding.HostIp -ne '127.0.0.1') { throw "Existing $Service exposes port $Port on $($binding.HostIp); expected 127.0.0.1." }
                $owned = $true
            }
        }
    }
    if ($owned) { return }
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)
    try { $listener.Start() }
    catch { throw "Port 127.0.0.1:$Port is occupied by a process outside $projectName. $($_.Exception.Message)" }
    finally { $listener.Stop() }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker CLI is required.' }
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw "Required Python environment is missing: $python" }
if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) { throw "Compose configuration is missing: $composeFile" }
if (-not (Test-Path -LiteralPath $manifestFile -PathType Leaf)) {
    throw 'Connector manifest is missing. Run scripts/build-flink.ps1 first.'
}
Invoke-Docker @('version', '--format', '{{.Server.Version}}') | Out-Null
Invoke-Docker @('compose', 'version', '--short') | Out-Null
Invoke-Docker @('image', 'inspect', $baseImage) | Out-Null
Invoke-Docker @('image', 'inspect', 'apache/kafka:4.3.1') | Out-Null
Invoke-Docker @('image', 'inspect', $runtimeImage) | Out-Null
Invoke-Docker @('compose', '-p', $projectName, '-f', $composeFile, 'config', '--quiet') | Out-Null

$manifest = Get-Content -LiteralPath $manifestFile -Raw -Encoding UTF8 | ConvertFrom-Json
if ($manifest.base_image -ne $baseImage -or $manifest.flink_version -ne '2.2.1' -or @($manifest.artifacts).Count -eq 0) {
    throw 'Connector manifest does not describe the required Flink 2.2.1 bundle.'
}
$artifactDirectory = Split-Path -Parent $manifestFile
foreach ($artifact in $manifest.artifacts) {
    if ([System.IO.Path]::GetFileName($artifact.filename) -ne $artifact.filename -or $artifact.sha256 -notmatch '^[0-9a-fA-F]{64}$') {
        throw 'Connector manifest contains an invalid filename or SHA256.'
    }
    $artifactFile = Join-Path $artifactDirectory $artifact.filename
    if (-not (Test-Path -LiteralPath $artifactFile -PathType Leaf)) { throw "Connector JAR is missing: $artifactFile" }
    $actualHash = (Get-FileHash -LiteralPath $artifactFile -Algorithm SHA256).Hash
    if ($actualHash -ne $artifact.sha256) { throw "Connector SHA256 mismatch: $($artifact.filename)" }
}
$imageManifest = (Invoke-Docker @('run', '--rm', '--network', 'none', '--entrypoint', 'cat', $runtimeImage, '/opt/flink/connectors-manifest.json')) -join "`n" | ConvertFrom-Json
if (($imageManifest.artifacts | ConvertTo-Json -Depth 20 -Compress) -ne ($manifest.artifacts | ConvertTo-Json -Depth 20 -Compress)) {
    throw 'The runtime image contains a different connector manifest. Rebuild the image before starting.'
}

$runningIds = @(Invoke-Docker @('ps', '-q'))
$runningContainers = @()
if ($runningIds.Count -gt 0) { $runningContainers = @(Read-DockerJson (@('inspect') + $runningIds)) }
Assert-PortAvailable 8081 'jobmanager' $runningContainers
Assert-PortAvailable 8083 'sql-gateway' $runningContainers
Assert-PortAvailable 19092 'kafka' $runningContainers
Invoke-Docker @('network', 'inspect', $dorisNetwork) | Out-Null
$mysql = @(Read-DockerJson @('inspect', 'dataworks-demo-mysql'))[0]
if (-not $mysql.State.Running) { throw 'Business MySQL container dataworks-demo-mysql is not running.' }

$networks = @(Invoke-Docker @('network', 'ls', '--format', '{{.Name}}'))
if ($networks -notcontains $networkName) { Invoke-Docker @('network', 'create', $networkName) | Out-Null }
$mysqlNetwork = $mysql.NetworkSettings.Networks.PSObject.Properties[$networkName]
if ($null -eq $mysqlNetwork) {
    Invoke-Docker @('network', 'connect', '--alias', 'mysql-rlt', $networkName, 'dataworks-demo-mysql') | Out-Null
} elseif (@($mysqlNetwork.Value.Aliases) -notcontains 'mysql-rlt') {
    throw 'MySQL is already on sinket-realtime without mysql-rlt alias. Preserve its current networks and add the alias before retrying.'
}

Write-Host 'Starting Flink 2.2.1 and Kafka 4.3.1...'
Invoke-Docker @('compose', '-p', $projectName, '-f', $composeFile, 'up', '-d', '--wait', '--wait-timeout', "$WaitSeconds") | Out-Host
& $python (Join-Path $PSScriptRoot 'check-flink.py') --timeout $WaitSeconds
if ($LASTEXITCODE -ne 0) { throw 'Flink runtime health checks failed. See .runtime/flink/health-report.json.' }
Write-Host 'Flink UI: http://127.0.0.1:8081; SQL Gateway: http://127.0.0.1:8083; Kafka: localhost:19092'
