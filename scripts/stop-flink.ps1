param()
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot 'infra/flink/compose.yaml'
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker CLI is required.' }
if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) { throw "Compose configuration is missing: $composeFile" }
& docker compose -p sinket-realtime -f $composeFile stop
if ($LASTEXITCODE -ne 0) { throw 'Stopping sinket-realtime failed.' }
Write-Host 'Flink and realtime Kafka stopped. Containers, volumes, networks and existing MySQL/Doris were preserved.'
