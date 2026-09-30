param([string]$ContainerId=$env:MYSQL_CONTAINER)
$ErrorActionPreference='Stop'
$OutputEncoding=[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([string]::IsNullOrWhiteSpace($ContainerId)) {
    throw 'Specify -ContainerId or set MYSQL_CONTAINER to the metadata MySQL container name or ID.'
}
$projectRoot=Split-Path -Parent $PSScriptRoot
$localConfig=Join-Path $projectRoot 'backend/application-local.properties'
$passwordLine=if (Test-Path -LiteralPath $localConfig) {
    Get-Content -LiteralPath $localConfig -Encoding UTF8 | Where-Object { $_ -match '^spring\.datasource\.password=' } | Select-Object -First 1
} else { $null }
$backupDirectory=Join-Path $projectRoot '.runtime/backups'
New-Item -ItemType Directory -Force -Path $backupDirectory | Out-Null
$path=Join-Path $backupDirectory ('metadata-'+(Get-Date -Format 'yyyyMMdd-HHmmss')+'.sql')
$priorPassword=$env:MYSQL_PWD
try {
    $env:MYSQL_PWD=if ($env:DB_PASSWORD) {$env:DB_PASSWORD} elseif($passwordLine) {$passwordLine.Substring($passwordLine.IndexOf('=')+1)} else {throw 'Missing metadata password.'}
    $dump=& docker exec -e MYSQL_PWD $ContainerId mysqldump -uroot --single-transaction --no-tablespaces --set-gtid-purged=OFF --default-character-set=utf8mb4 fake_dataworks_260927
    if ($LASTEXITCODE -ne 0) {throw 'Metadata backup failed.'}
    [IO.File]::WriteAllLines($path,[string[]]$dump,[Text.UTF8Encoding]::new($false))
} finally {
    if ($null -eq $priorPassword) {Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue} else {$env:MYSQL_PWD=$priorPassword}
}
Write-Host "Metadata backup: $path"
