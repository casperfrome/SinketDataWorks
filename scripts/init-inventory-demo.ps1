param([string]$ContainerId='dataworks-demo-mysql',[string]$RootPassword=$env:DEMO_ROOT_PASSWORD)
$ErrorActionPreference='Stop'
$OutputEncoding=[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
$projectRoot=Split-Path -Parent $PSScriptRoot
$configPath=Join-Path $projectRoot 'backend/application-local.properties'
$config=[IO.File]::ReadAllText($configPath,[Text.Encoding]::UTF8)
if ([string]::IsNullOrWhiteSpace($RootPassword)) {
    # Only inspect the selected local demo container. Never print credentials.
    $containerInfo=(& docker inspect $ContainerId | ConvertFrom-Json)[0]
    $entry=$containerInfo.Config.Env | Where-Object { $_.StartsWith('MYSQL_ROOT_PASSWORD=') } | Select-Object -First 1
    if ($entry) { $RootPassword=$entry.Substring('MYSQL_ROOT_PASSWORD='.Length) }
}
if ([string]::IsNullOrWhiteSpace($RootPassword)) { throw 'Set DEMO_ROOT_PASSWORD in the local terminal.' }
$match=[regex]::Match($config,'(?m)^studio\.inventory\.password=(.*)\r?$')
$password=if ($match.Success) { $match.Groups[1].Value.Trim() } else {
    $bytes=New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    [Convert]::ToBase64String($bytes)
}
$sql=[IO.File]::ReadAllText((Join-Path $PSScriptRoot 'inventory-schema.sql'),[Text.Encoding]::UTF8)
$sql+="`n"+[IO.File]::ReadAllText((Join-Path $PSScriptRoot 'inventory-data.sql'),[Text.Encoding]::UTF8)
# Backfill provenance for the legacy all-layer atomic batches, without changing results.
foreach($table in @('dwd_inventory_ledger_di','dws_inventory_warehouse_di','ads_inventory_analysis_di')) {
    $sql+="`nINSERT IGNORE INTO etl_partition_publication SELECT '$table',r.business_date,r.build_id,JSON_OBJECT('dwd_inventory_ledger_di',r.build_id,'dws_inventory_warehouse_di',r.build_id,'ads_inventory_analysis_di',r.build_id),r.committed_at FROM etl_publish_receipt r WHERE EXISTS (SELECT 1 FROM $table t WHERE t.business_date=r.business_date) AND NOT EXISTS (SELECT 1 FROM etl_publish_receipt newer WHERE newer.business_date=r.business_date AND (newer.committed_at>r.committed_at OR (newer.committed_at=r.committed_at AND newer.build_id>r.build_id)));`n"
}
$escaped=$password.Replace("'","''")
$sql+="`nCREATE USER IF NOT EXISTS 'studio_inventory_etl'@'%' IDENTIFIED BY '$escaped';`nGRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, CREATE VIEW, SHOW VIEW, CREATE TEMPORARY TABLES, REFERENCES ON studio_inventory.* TO 'studio_inventory_etl'@'%';`n"
foreach($table in @('dwd_inventory_ledger_di','dws_inventory_warehouse_di','ads_inventory_analysis_di')) {
    $sql+="GRANT INSERT, DELETE ON studio_inventory.$table TO 'studio_inventory_etl'@'%';`n"
    $sql+="GRANT INSERT, DELETE ON studio_inventory.etl_stage_$table TO 'studio_inventory_etl'@'%';`n"
}
$sql+="GRANT INSERT ON studio_inventory.etl_publish_receipt TO 'studio_inventory_etl'@'%';`n"
$sql+="GRANT INSERT, UPDATE ON studio_inventory.etl_partition_publication TO 'studio_inventory_etl'@'%';`n"
$prior=$env:MYSQL_PWD
try {
    $env:MYSQL_PWD=$RootPassword
    $sql | & docker exec -i -e MYSQL_PWD $ContainerId mysql -uroot --default-character-set=utf8mb4
    if ($LASTEXITCODE -ne 0) { throw 'Inventory initialization failed.' }
    $env:MYSQL_PWD=$password
    & docker exec -e MYSQL_PWD $ContainerId mysql -ustudio_inventory_etl --batch --skip-column-names -e 'SELECT first_day FROM studio_inventory.inventory_demo_config;'
    if ($LASTEXITCODE -ne 0) { throw 'ETL credential verification failed; existing credentials were not overwritten.' }
    if (!$match.Success) { [IO.File]::AppendAllText($configPath,"`r`nstudio.inventory.password=$password`r`n",[Text.UTF8Encoding]::new($false)) }
} finally {
    if ($null -eq $prior) { Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue } else { $env:MYSQL_PWD=$prior }
}
Write-Host 'Inventory schema, append-only demo sources and limited ETL account are ready. Existing data was preserved.'
