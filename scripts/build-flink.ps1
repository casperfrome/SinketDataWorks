param(
    [string]$ImageTag = 'sinket-flink:2.2.1-connectors',
    [switch]$ForceDownload,
    [switch]$ConnectorsOnly
)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = 'D:\PythonVenv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw "Required Python environment was not found: $python" }
$buildArguments = @((Join-Path $PSScriptRoot 'build-flink-connectors.py'))
if ($ForceDownload) { $buildArguments += '--force-download' }
& $python @buildArguments
if ($LASTEXITCODE -ne 0) { throw 'Flink connector build or verification failed.' }
if (-not $ConnectorsOnly) {
    & docker build --file (Join-Path $projectRoot 'infra/flink/Dockerfile') --tag $ImageTag $projectRoot
    if ($LASTEXITCODE -ne 0) { throw 'Flink connector image build failed.' }
}
