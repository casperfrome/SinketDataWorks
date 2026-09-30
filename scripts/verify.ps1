param(
    [string]$BaseUrl = 'http://127.0.0.1:8080',
    [string]$WorkspaceId = 'local-workspace',
    [string]$ReportPath
)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
Add-Type -AssemblyName System.Net.Http
$projectRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($ReportPath)) { $ReportPath = Join-Path $projectRoot '.runtime/api-verification.md' }
$BaseUrl = $BaseUrl.TrimEnd('/')
$runToken = (Get-Date -Format 'yyyyMMdd_HHmmss') + '_' + [Guid]::NewGuid().ToString('N').Substring(0, 8)
$rootName = '__api_verification_' + $runToken
$startedAt = Get-Date
$results = [System.Collections.Generic.List[object]]::new()
$createdIds = [System.Collections.Generic.List[string]]::new()
$rootIds = [System.Collections.Generic.List[string]]::new()
$failure = $null
$cleanupFailure = $null
$client = [System.Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(20)

function Invoke-Api {
    param([string]$Method, [string]$Path, $Body = $null, [int[]]$Expected = @(200), [System.Net.Http.HttpContent]$Content = $null)
    $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::new($Method), $BaseUrl + $Path)
    if ($null -ne $Content) { $request.Content = $Content }
    elseif ($null -ne $Body) {
        $json = ConvertTo-Json -InputObject $Body -Depth 100 -Compress
        $request.Content = [System.Net.Http.StringContent]::new($json, [System.Text.Encoding]::UTF8, 'application/json')
    }
    $response = $null
    try {
        $response = $client.SendAsync($request).GetAwaiter().GetResult()
        $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        $text = [System.Text.Encoding]::UTF8.GetString($bytes)
        $status = [int]$response.StatusCode
        if ($Expected -notcontains $status) { throw "$Method $Path returned HTTP $status (expected $($Expected -join '/')): $text" }
        $data = $null
        $mediaType = $response.Content.Headers.ContentType.MediaType
        if ($text -and ($mediaType -eq 'application/json' -or $mediaType.EndsWith('+json'))) { $data = ConvertFrom-Json -InputObject $text }
        return [PSCustomObject]@{ Status = $status; Data = $data; Text = $text; Bytes = $bytes }
    } finally {
        if ($null -ne $response) { $response.Dispose() }
        $request.Dispose()
    }
}
function Assert-That([bool]$Condition, [string]$Message) { if (-not $Condition) { throw $Message } }
function Test-Case([string]$Name, [scriptblock]$Action) {
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        & $Action | Out-Null
        $results.Add([PSCustomObject]@{ Name = $Name; Status = 'PASS'; Seconds = [math]::Round($watch.Elapsed.TotalSeconds, 2); Detail = '' })
        Write-Host "PASS  $Name"
    } catch {
        $results.Add([PSCustomObject]@{ Name = $Name; Status = 'FAIL'; Seconds = [math]::Round($watch.Elapsed.TotalSeconds, 2); Detail = $_.Exception.Message })
        Write-Host "FAIL  $Name"
        throw
    } finally { $watch.Stop() }
}
function New-TestObject([string]$Kind, [string]$Name, [string]$ParentId, [string]$NodeType = 'MaxCompute SQL', [hashtable]$Config = @{}) {
    $body = @{ workspaceId = $WorkspaceId; kind = $Kind; nodeType = $NodeType; name = $Name; description = '可重复 API 验收数据；脚本结束自动移至回收站'; parentId = $ParentId; content = '-- API smoke metadata'; config = $Config; owner = 'api_verifier'; tags = @('API验收', $runToken) }
    if ([string]::IsNullOrEmpty($ParentId)) { $body.parentId = $null }
    $object = (Invoke-Api 'POST' '/api/v1/objects' $body @(201)).Data
    $createdIds.Add($object.id)
    return $object
}
function Get-Object([string]$Id) { return (Invoke-Api 'GET' ('/api/v1/objects/' + $Id)).Data }
function Get-Run([string]$Id) { return @((Invoke-Api 'GET' ('/api/v1/runs?workspaceId=' + [Uri]::EscapeDataString($WorkspaceId))).Data | Where-Object { $_.id -eq $Id })[0] }
function Wait-Run([string]$Id, [string]$Status) {
    $deadline = [DateTime]::UtcNow.AddSeconds(25)
    while ([DateTime]::UtcNow -lt $deadline) {
        $run = Get-Run $Id
        if ($run.status -eq $Status) { return $run }
        if (@('SUCCESS', 'FAILED', 'CANCELLED') -contains $run.status) { throw "Run $Id reached $($run.status), expected $Status" }
        Start-Sleep -Milliseconds 250
    }
    throw "Run $Id did not reach $Status within 25 seconds"
}
function Upload-TestFile([string]$Id, [string]$Name, [string]$Text) {
    $content = [System.Net.Http.MultipartFormDataContent]::new()
    $part = [System.Net.Http.ByteArrayContent]::new([System.Text.Encoding]::UTF8.GetBytes($Text))
    $part.Headers.ContentType = [System.Net.Http.Headers.MediaTypeHeaderValue]::new('text/csv')
    $content.Add($part, 'file', $Name)
    return (Invoke-Api -Method 'POST' -Path ('/api/v1/objects/' + $Id + '/file') -Content $content).Data
}

try {
    Test-Case '服务健康、工作空间与独立验收目录' {
        Assert-That ((Invoke-Api 'GET' '/actuator/health').Data.status -eq 'UP') 'Backend health is not UP.'
        $spaces = (Invoke-Api 'GET' '/api/v1/workspaces').Data
        Assert-That (@($spaces | Where-Object { $_.id -eq $WorkspaceId }).Count -eq 1) "Workspace $WorkspaceId does not exist."
        $script:root = New-TestObject 'FOLDER' $rootName '' 'FOLDER'
        $rootIds.Add($script:root.id)
        $script:node = New-TestObject 'NODE' '版本与模拟运行' $script:root.id
        $script:folder = New-TestObject 'FOLDER' '父目录' $script:root.id 'FOLDER'
        $script:child = New-TestObject 'FOLDER' '子目录' $script:folder.id 'FOLDER'
    }
    Test-Case '保存内容与负责人、递增版本和版本快照' {
        $script:firstVersion = @((Invoke-Api 'GET' ('/api/v1/objects/' + $node.id + '/versions')).Data)[0]
        $script:node.content = 'THIS IS DELIBERATELY INVALID SQL; metadata only; 不应执行'
        $script:node.owner = '验收负责人'
        $script:node = (Invoke-Api 'PUT' ('/api/v1/objects/' + $node.id) $node).Data
        Assert-That ($node.version -eq 2) 'Save must advance version to 2.'
        $read = Get-Object $node.id
        Assert-That ($read.content -eq $node.content -and $read.owner -eq '验收负责人') 'Content or owner did not persist.'
        Assert-That (@((Invoke-Api 'GET' ('/api/v1/objects/' + $node.id + '/versions')).Data).Count -eq 2) 'Version snapshots are incomplete.'
    }
    Test-Case '乐观锁冲突保持服务端内容' {
        $stale = Get-Object $node.id
        $stale.version = 1
        $stale.content = 'stale content must not overwrite'
        $response = Invoke-Api 'PUT' ('/api/v1/objects/' + $node.id) $stale @(409)
        Assert-That ($response.Data.code -eq 'VERSION_CONFLICT') 'Expected VERSION_CONFLICT.'
        Assert-That ((Get-Object $node.id).content -eq $node.content) 'Conflict changed content.'
    }
    Test-Case '同目录重名与目录循环移动被拒绝' {
        $response = Invoke-Api 'POST' '/api/v1/objects' @{ workspaceId = $WorkspaceId; kind = 'NODE'; name = $node.name; parentId = $root.id } @(409)
        Assert-That ($response.Data.code -eq 'NAME_CONFLICT') 'Expected NAME_CONFLICT.'
        $cycle = Get-Object $folder.id
        $cycle.parentId = $child.id
        $response = Invoke-Api 'PUT' ('/api/v1/objects/' + $folder.id) $cycle @(400)
        Assert-That ($response.Data.code -eq 'DIRECTORY_CYCLE') 'Expected DIRECTORY_CYCLE.'
        Assert-That ((Get-Object $folder.id).parentId -eq $root.id) 'Failed move changed the directory.'
    }
    Test-Case '合法 DAG 保存；循环、重复连线、孤立边、自循环拒绝' {
        $nodes = @(@{ id = 'n1'; label = '上游'; nodeType = 'MaxCompute SQL'; x = 0; y = 0 }, @{ id = 'n2'; label = '下游'; nodeType = 'MaxCompute SQL'; x = 200; y = 0 })
        $script:workflow = New-TestObject 'WORKFLOW' 'DAG验收' $root.id '周期工作流' @{ graph = @{ nodes = $nodes; edges = @(@{ id = 'e1'; source = 'n1'; target = 'n2' }) } }
        $badEdges = @(
            @{ label = 'cycle'; edges = @(@{ id = 'e1'; source = 'n1'; target = 'n2' }, @{ id = 'e2'; source = 'n2'; target = 'n1' }) },
            @{ label = 'duplicate'; edges = @(@{ id = 'e1'; source = 'n1'; target = 'n2' }, @{ id = 'e2'; source = 'n1'; target = 'n2' }) },
            @{ label = 'orphan'; edges = @(@{ id = 'e1'; source = 'n1'; target = 'missing' }) },
            @{ label = 'self'; edges = @(@{ id = 'e1'; source = 'n1'; target = 'n1' }) }
        )
        foreach ($bad in $badEdges) {
            $candidate = Get-Object $workflow.id
            $candidate.config.graph.edges = $bad.edges
            $response = Invoke-Api 'PUT' ('/api/v1/objects/' + $workflow.id) $candidate @(400)
            Assert-That ($response.Data.code -eq 'INVALID_GRAPH') "Invalid graph $($bad.label) was not rejected."
        }
        Assert-That ((Get-Object $workflow.id).version -eq 1) 'Rejected graphs advanced the version.'
    }
    Test-Case '历史内容恢复生成新版本' {
        $restored = (Invoke-Api 'POST' ('/api/v1/objects/' + $node.id + '/versions/' + $firstVersion.id + '/restore') @{ version = $node.version }).Data
        Assert-That ($restored.version -eq 3 -and $restored.content -eq $firstVersion.content) 'Version restore did not create the expected snapshot.'
        $restored.content = 'THIS IS DELIBERATELY INVALID SQL; metadata only; 不应执行'
        $script:node = (Invoke-Api 'PUT' ('/api/v1/objects/' + $node.id) $restored).Data
    }
    Test-Case '模拟排队、成功、失败、取消；无效 SQL 不执行' {
        $success = (Invoke-Api 'POST' '/api/v1/runs' @{ objectId = $node.id; mode = 'API_VERIFY' } @(201)).Data
        $failed = (Invoke-Api 'POST' '/api/v1/runs' @{ objectId = $node.id; mode = 'API_VERIFY'; simulateFailure = $true } @(201)).Data
        $cancelled = (Invoke-Api 'POST' '/api/v1/runs' @{ objectId = $node.id; mode = 'API_VERIFY' } @(201)).Data
        Assert-That ($success.status -eq 'QUEUED' -and $success.simulation -eq $true) 'New run is not a queued simulation.'
        $stopped = (Invoke-Api 'POST' ('/api/v1/runs/' + $cancelled.id + '/stop')).Data
        Assert-That ($stopped.status -eq 'CANCELLED') 'Stop did not cancel the queued run.'
        $done = Wait-Run $success.id 'SUCCESS'
        $failedDone = Wait-Run $failed.id 'FAILED'
        Assert-That (@($done.rows).Count -gt 0 -and $done.simulation -eq $true) 'Simulation result is missing.'
        Assert-That (($done.logs -join ' ').Contains('未执行用户代码')) 'Simulation log does not explain non-execution.'
        Assert-That ((Get-Run $cancelled.id).status -eq 'CANCELLED') 'Cancelled run unexpectedly resumed.'
        Assert-That ((Get-Object $node.id).content -eq $node.content) 'Simulation modified source content.'
        Assert-That ((Invoke-Api 'GET' '/actuator/health').Data.status -eq 'UP') 'Service became unhealthy after simulation.'
    }
    Test-Case '本地发布记录可追溯保存与查询' {
        $record = (Invoke-Api 'POST' '/api/v1/records' @{ workspaceId = $WorkspaceId; objectId = $node.id; kind = 'RELEASE'; title = $rootName + ' 发布验收'; status = 'SUCCESS'; payload = @{ simulation = $true; version = $node.version; content = $node.content; config = $node.config; verificationToken = $runToken } } @(201)).Data
        $found = @((Invoke-Api 'GET' ('/api/v1/records?workspaceId=' + [Uri]::EscapeDataString($WorkspaceId) + '&kind=RELEASE')).Data | Where-Object { $_.id -eq $record.id })
        Assert-That ($found.Count -eq 1 -and $found[0].payload.content -eq $node.content) 'Release record did not persist the source snapshot.'
    }
    Test-Case '资源上传返回完整对象，下载与副本独立于后续替换' {
        $script:resource = New-TestObject 'RESOURCE' '上传资源' $root.id 'FILE'
        $first = Upload-TestFile $resource.id '../smoke-first.csv' "id,name`n1,验收一"
        Assert-That ($first.id -eq $resource.id -and $first.version -eq 2 -and $first.config.fileName -eq 'smoke-first.csv') 'Upload did not return a versioned StudioObject.'
        $copy = (Invoke-Api 'POST' ('/api/v1/objects/' + $resource.id + '/copy') @{ name = '上传资源副本' }).Data
        $createdIds.Add($copy.id)
        Assert-That ($copy.parentId -eq $root.id) 'Copy with omitted parentId did not preserve parent.'
        $second = Upload-TestFile $resource.id 'smoke-second.csv' "id,name`n2,验收二"
        Assert-That ($second.version -eq 3 -and $second.config.file.name -eq 'smoke-second.csv') 'Replacing upload did not update file metadata.'
        $originalText = (Invoke-Api 'GET' ('/api/v1/objects/' + $resource.id + '/file')).Text
        $copiedText = (Invoke-Api 'GET' ('/api/v1/objects/' + $copy.id + '/file')).Text
        Assert-That ($originalText -eq "id,name`n2,验收二" -and $copiedText -eq "id,name`n1,验收一") 'Resource replacement corrupted the original or copied download.'
    }
    Test-Case '递归软删除、父目录恢复约束与重名恢复事务' {
        $script:alreadyDeleted = New-TestObject 'NODE' '此前单独删除' $root.id
        Invoke-Api 'DELETE' ('/api/v1/objects/' + $alreadyDeleted.id) | Out-Null
        Invoke-Api 'DELETE' ('/api/v1/objects/' + $root.id) | Out-Null
        foreach ($id in $createdIds) { Assert-That ((Get-Object $id).deleted -eq $true) "Descendant $id was not soft deleted." }
        $response = Invoke-Api 'POST' ('/api/v1/objects/' + $node.id + '/restore') @{} @(409)
        Assert-That ($response.Data.code -eq 'PARENT_DELETED') 'Child restored under a deleted parent.'
        $script:conflictRoot = New-TestObject 'FOLDER' $rootName '' 'FOLDER'
        $rootIds.Add($conflictRoot.id)
        $response = Invoke-Api 'POST' ('/api/v1/objects/' + $root.id + '/restore') @{} @(409)
        Assert-That ($response.Data.code -eq 'NAME_CONFLICT') 'Restore did not report the active duplicate name.'
        Assert-That ((Get-Object $node.id).deleted -eq $true) 'Failed restore partially restored children.'
        $restored = (Invoke-Api 'POST' ('/api/v1/objects/' + $root.id + '/restore') @{ name = $rootName + '_restored' }).Data
        Assert-That (-not $restored.deleted -and -not (Get-Object $node.id).deleted -and -not (Get-Object $resource.id).deleted) 'Rename restore did not restore the deleted group.'
        Assert-That ((Get-Object $alreadyDeleted.id).deleted -eq $true) 'Restore resurrected a previously separately deleted child.'
    }
} catch { $failure = $_.Exception.Message }
finally {
    try {
        foreach ($id in $rootIds) {
            $item = Get-Object $id
            if (-not $item.deleted) { Invoke-Api 'DELETE' ('/api/v1/objects/' + $id) | Out-Null }
        }
        foreach ($id in $createdIds) {
            $item = Get-Object $id
            if (-not $item.deleted) { Invoke-Api 'DELETE' ('/api/v1/objects/' + $id) | Out-Null }
        }
        $active = @((Invoke-Api 'GET' ('/api/v1/objects?workspaceId=' + [Uri]::EscapeDataString($WorkspaceId))).Data)
        $leftovers = @($active | Where-Object { $createdIds.Contains($_.id) })
        Assert-That ($leftovers.Count -eq 0) 'Active verification objects remain after cleanup.'
        $results.Add([PSCustomObject]@{ Name = '仅软删除本次对象并确认无活动验收数据'; Status = 'PASS'; Seconds = 0; Detail = "$($createdIds.Count) objects moved to recycle bin" })
        Write-Host "PASS  Cleanup: $($createdIds.Count) owned objects are in the recycle bin."
    } catch {
        $cleanupFailure = $_.Exception.Message
        $results.Add([PSCustomObject]@{ Name = '清理本次验收数据'; Status = 'FAIL'; Seconds = 0; Detail = $cleanupFailure })
    }
    $finishedAt = Get-Date
    $status = if ($failure -or $cleanupFailure) { 'FAIL' } else { 'PASS' }
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add('# Data Studio API 验收记录')
    $lines.Add('')
    $lines.Add("- 结果：**$status**")
    $lines.Add("- 开始：$($startedAt.ToString('yyyy-MM-dd HH:mm:ss zzz'))")
    $lines.Add("- 完成：$($finishedAt.ToString('yyyy-MM-dd HH:mm:ss zzz'))")
    $lines.Add("- 服务：$BaseUrl")
    $lines.Add("- 工作空间：$WorkspaceId")
    $lines.Add("- 验收标识：$runToken")
    $lines.Add("- 创建对象数：$($createdIds.Count)")
    $lines.Add('')
    $lines.Add('| 检查 | 结果 | 秒 | 说明 |')
    $lines.Add('|---|---|---:|---|')
    foreach ($result in $results) {
        $detail = ($result.Detail -replace '\|', '\|' -replace '[\r\n]+', ' ')
        $lines.Add("| $($result.Name) | $($result.Status) | $($result.Seconds) | $detail |")
    }
    $lines.Add('')
    $lines.Add('## 数据与执行边界')
    $lines.Add('')
    $lines.Add('本脚本只通过当前运行服务的 API 操作独立 UUID 验收目录，不构建、不停止或重启服务，不修改已有用户对象。每次执行使用新名称，结束时仅软删除本次创建的对象，回收站、版本、模拟运行和发布记录保留用于追溯。资源验收文件也随回收站元数据保留。')
    $lines.Add('')
    $lines.Add('模拟验收保存故意无效的 SQL 文本，验证其仍返回标明本地模拟的成功结果、固定数据及“未执行用户代码”日志，同时数据库健康和原始内容不受影响；不提交可破坏数据库的 SQL。')
    $lines.Add('')
    $lines.Add('## 重复运行')
    $lines.Add('')
    $lines.Add('在项目根目录执行：')
    $lines.Add('')
    $lines.Add('```powershell')
    $lines.Add('powershell -ExecutionPolicy Bypass -File scripts/verify.ps1')
    $lines.Add('# 或使用 PowerShell 7: pwsh -File scripts/verify.ps1')
    $lines.Add('```')
    $lines.Add('')
    $lines.Add('可用 `-BaseUrl`、`-WorkspaceId`、`-ReportPath` 指定已运行的服务、现有工作空间和报告路径。报告会更新为最近一次执行结果；此处不修改全局偏好。')
    if ($failure) { $lines.Add(''); $lines.Add('执行失败：' + $failure) }
    if ($cleanupFailure) { $lines.Add(''); $lines.Add('清理失败：' + $cleanupFailure) }
    $reportDirectory = Split-Path -Parent ([System.IO.Path]::GetFullPath($ReportPath))
    [System.IO.Directory]::CreateDirectory($reportDirectory) | Out-Null
    [System.IO.File]::WriteAllLines([System.IO.Path]::GetFullPath($ReportPath), $lines, [System.Text.UTF8Encoding]::new($false))
    $client.Dispose()
    Write-Host "Report: $ReportPath"
}
if ($failure -or $cleanupFailure) { throw "API verification failed. See $ReportPath" }
