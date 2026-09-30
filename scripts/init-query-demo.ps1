param(
    [string]$ContainerId = $env:DEMO_MYSQL_CONTAINER,
    [string]$RootPassword = $env:DEMO_ROOT_PASSWORD
)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
if ([string]::IsNullOrWhiteSpace($ContainerId)) {
    throw 'Specify -ContainerId or set DEMO_MYSQL_CONTAINER to the business MySQL container name or ID.'
}
if ([string]::IsNullOrWhiteSpace($RootPassword)) { throw 'Set DEMO_ROOT_PASSWORD for initialization.' }
$projectRoot = Split-Path -Parent $PSScriptRoot
$configPath = Join-Path $projectRoot 'backend/application-local.properties'
$configText = if (Test-Path -LiteralPath $configPath) { [IO.File]::ReadAllText($configPath,[Text.Encoding]::UTF8) } else { '' }
function Read-LocalProperty([string]$Name) {
    $match = [regex]::Match($configText, '(?m)^' + [regex]::Escape($Name) + '=(.*)\r?$')
    if ($match.Success) { return $match.Groups[1].Value.Trim() }
    return ''
}
function New-Secret {
    $bytes = New-Object byte[] 32
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return [Convert]::ToBase64String($bytes)
}
$readerPassword = Read-LocalProperty 'studio.demo.password'
if (!$readerPassword) {
    $readerPassword = New-Secret
    $configText += "`r`nstudio.demo.password=$readerPassword`r`n"
}
if (!(Read-LocalProperty 'studio.datasource.encryption-key')) {
    $encryptionKey = New-Secret
    $configText += "`r`nstudio.datasource.encryption-key=$encryptionKey`r`n"
}
[IO.File]::WriteAllText($configPath,$configText,[Text.UTF8Encoding]::new($false))
$escapedPassword = $readerPassword.Replace("'", "''")
$sql = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'demo-data.sql'),[Text.Encoding]::UTF8)
$sql += "`nCREATE USER IF NOT EXISTS 'studio_reader'@'%' IDENTIFIED BY '$escapedPassword';`nGRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, CREATE VIEW, SHOW VIEW, CREATE TEMPORARY TABLES, REFERENCES ON studio_demo.* TO 'studio_reader'@'%';`n"
$priorPassword = $env:MYSQL_PWD
try {
    $env:MYSQL_PWD = $RootPassword
    $sql | & docker exec -i -e MYSQL_PWD $ContainerId mysql -uroot --default-character-set=utf8mb4
    if ($LASTEXITCODE -ne 0) { throw 'Business database initialization failed.' }
    $env:MYSQL_PWD = $readerPassword
    & docker exec -e MYSQL_PWD $ContainerId mysql -ustudio_reader --batch --skip-column-names -e 'SELECT COUNT(*) FROM studio_demo.orders;'
    if ($LASTEXITCODE -ne 0) { throw 'Reader verification failed; an existing reader may use a different password. No password was overwritten.' }
} finally {
    if ($null -eq $priorPassword) { Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue } else { $env:MYSQL_PWD=$priorPassword }
}
Write-Host 'studio_demo initialized. Reader credentials and encryption key are in the ignored local configuration.'
