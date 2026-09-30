param([string]$ContainerId='dataworks-demo-mysql',[string]$RootPassword=$env:DEMO_ROOT_PASSWORD)
$ErrorActionPreference='Stop'
$OutputEncoding=[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if ([string]::IsNullOrWhiteSpace($RootPassword)) {
    $info=(& docker inspect $ContainerId | ConvertFrom-Json)[0]
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the selected demo container.' }
    $entry=$info.Config.Env | Where-Object { $_.StartsWith('MYSQL_ROOT_PASSWORD=') } | Select-Object -First 1
    if ($entry) { $RootPassword=$entry.Substring('MYSQL_ROOT_PASSWORD='.Length) }
}
if ([string]::IsNullOrWhiteSpace($RootPassword)) { throw 'Set DEMO_ROOT_PASSWORD for the selected demo container.' }
# Only grants for the two existing demo identities. Does not initialize data or rotate passwords.
$permissions='SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, CREATE VIEW, SHOW VIEW, CREATE TEMPORARY TABLES, REFERENCES'
$sql="GRANT $permissions ON studio_demo.* TO 'studio_reader'@'%';`nGRANT $permissions ON studio_inventory.* TO 'studio_inventory_etl'@'%';"
$previous=$env:MYSQL_PWD
try {
    $env:MYSQL_PWD=$RootPassword
    $sql | & docker exec -i -e MYSQL_PWD $ContainerId mysql -uroot --default-character-set=utf8mb4
    if ($LASTEXITCODE -ne 0) { throw 'Demo authorization failed. Check that both demo accounts already exist.' }
} finally {
    if ($null -eq $previous) { Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue } else { $env:MYSQL_PWD=$previous }
}
Write-Host 'Both existing demo accounts now have read/write and DDL privileges on their own business database.'
