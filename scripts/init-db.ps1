param(
    [string]$ContainerId = $env:MYSQL_CONTAINER,
    [string]$Password = $env:DB_PASSWORD
)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
if ([string]::IsNullOrWhiteSpace($ContainerId)) {
    throw 'Specify -ContainerId or set MYSQL_CONTAINER to the metadata MySQL container name or ID.'
}
$projectRoot = Split-Path -Parent $PSScriptRoot
$localConfig = Join-Path $projectRoot 'backend/application-local.properties'
if ([string]::IsNullOrWhiteSpace($Password) -and (Test-Path -LiteralPath $localConfig)) {
    $passwordLine = Get-Content -LiteralPath $localConfig -Encoding UTF8 | Where-Object { $_ -match '^spring\.datasource\.password=' } | Select-Object -First 1
    if ($passwordLine) { $Password = $passwordLine.Substring($passwordLine.IndexOf('=') + 1) }
}
if ([string]::IsNullOrWhiteSpace($Password)) {
    throw 'Set DB_PASSWORD or create backend/application-local.properties with spring.datasource.password before initialization.'
}
$running = & docker inspect --format '{{.State.Running}}' $ContainerId
if ($LASTEXITCODE -ne 0 -or $running -ne 'true') { throw 'The specified MySQL container is not running.' }
& docker exec -e "MYSQL_PWD=$Password" $ContainerId mysql -u root --default-character-set=utf8mb4 -e 'CREATE DATABASE IF NOT EXISTS fake_dataworks_260927 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;'
if ($LASTEXITCODE -ne 0) { throw 'Database initialization failed.' }
Write-Host 'Database fake_dataworks_260927 is ready. Spring Boot applies schema migrations and initializes workspace identities.'
