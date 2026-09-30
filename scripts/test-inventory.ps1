param([string]$Container = 'dataworks-demo-mysql', [string]$Tests = '')
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$root = Split-Path -Parent $PSScriptRoot
$previousPassword = $env:INVENTORY_TEST_ROOT_PASSWORD
$previousJavaOptions = $env:JAVA_TOOL_OPTIONS
try {
    if (-not $env:INVENTORY_TEST_ROOT_PASSWORD) {
        $selectedContainer = (docker inspect $Container | ConvertFrom-Json)[0]
        if ($LASTEXITCODE -ne 0) { throw '库存演示 MySQL 容器不可用' }
        $secret = @($selectedContainer.Config.Env | Where-Object { $_.StartsWith('MYSQL_ROOT_PASSWORD=') })[0]
        if (-not $secret) { throw '请通过 INVENTORY_TEST_ROOT_PASSWORD 指定测试实例管理密码' }
        $env:INVENTORY_TEST_ROOT_PASSWORD = $secret.Substring('MYSQL_ROOT_PASSWORD='.Length)
    }
    $env:JAVA_TOOL_OPTIONS = "$previousJavaOptions -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8"
    Push-Location (Join-Path $root 'backend')
    try {
        $testArguments=@('-B','-ntp','test','-Dstudio.scheduler.enabled=false','-Dstudio.inventory.recover-on-start=false')
        if($Tests){$testArguments+="-Dtest=$Tests"}
        & mvn.cmd @testArguments
        if ($LASTEXITCODE -ne 0) { throw '后端测试失败' }
    } finally { Pop-Location }
} finally {
    $env:INVENTORY_TEST_ROOT_PASSWORD = $previousPassword
    $env:JAVA_TOOL_OPTIONS = $previousJavaOptions
}
