# 实时数据开发暂停交接（2026-10-01）

按用户出门、明天再恢复的指令暂停。功能验收尚未完成，不能视为交付通过。暂停时没有活动 Goal。

## 已保存的实现

- 独立实时后端、Flyway V11、目录/任务/草稿、不可变发布、持久化作业及异步操作、保存点、预览与浏览器旧数据幂等迁移。
- Kafka 加密 SASL、真实连接测试与 Topic 浏览；MySQL/Doris 宿主机和 Flink 执行地址分别配置；CDC 前置条件及绑定物理表结构检查。
- 后端受管 DDL、手写数据源引用与凭据注入、脚本拆分、Statement Set、EXPLAIN、100 行/30 秒 SELECT 预览与资源清理。
- 固定提交身份、幂等启动、数据库活动锁、未知结果对账、detached 作业、真实状态/指标/异常/节点日志/Checkpoint、Savepoint、恢复/升级/回滚。
- 前端真实接口接线、工作空间草稿保护、校验/计划/结果/日志面板、运维升级与回滚、取消未知操作、显式全新重启确认。
- 新验收脚本 `scripts/test-realtime.py`；README、后端接口、运行与实时开发文档已更新。

V11 已在本地元数据库应用，恢复工作时不要修改该迁移的 checksum。

## 测试与证据

前端最终类型检查、86 项测试及生产构建通过；浏览器实查涵盖 SQL 校验/计划、SELECT `+I`、预览清理、字段导入、深浅主题、950px 抽屉、空间隔离、草稿跨切换/重启保护、发布隔离、有限真实作业 FINISHED 和明确全新重启。仅剩新版后端的固定运行时长截图复核。

此前后端单元 195 项通过；此前集成测试已分批验证。暂停前的新完整 Maven 轮次被主动中止，**已完成 207 项，0 failure / 0 error，但整轮 BUILD FAILURE 是人为停止 Surefire 导致，不能标为全轮通过**。最新 RealtimeService 43、RealtimeRepositoryIntegration 8、Compiler 12、Gateway 7、Schema 9、Kafka 5 等已在这轮完成。SchedulingOperationsIntegrationTest 中途停止，其后测试及 package 未运行。

产品 API 最新完整验收仍为 FAILED（runId `20261001_171006_e78936`）：已通过数据源、保存冲突/旧数据迁移、Kafka JSON/CSV、并发启动及请求幂等、目标漂移防护、JDBC SELECT 预览、Kafka 预览超时/取消。CDC 三路增改删已核对，随后并发 CDC 预览因 server-id 超出连接器 Integer 范围失败。源码已修 signed 范围并有回归测试，**尚未重新打包及实测修复结果**。

证据保留于 `.runtime/realtime/`（均不上传凭据）：

- `acceptance-report.json`、`acceptance-report-before-cleanup-fix.json`、`acceptance-report-before-server-id-fix.json`：实际失败/历史报告，不改为通过。
- `backend-tests-at-pause.json`：本轮已完成测试的脱敏摘要，状态 INTERRUPTED_BY_PAUSE。
- `ui-regression-report.json` 与 11 张 `ui-*.png`：页面回归结果；旧后端的运行时长截图已注明。
- `restart-check.json`：后端停止期间原 CDC 作业仍 RUNNING 的证据。恢复后同 Job ID/阻止重复启动的完整检查未完成，为暂停已取消该作业。
- `pause-cleanup.json`：资源清理及中断集成测试遗留夹具列表。
- `paused-workspace-20261001.zip`、`wip-at-pause.diff`、`git-status-at-pause.txt`：源码/未跟踪新文件备份与 Git 状态。

## 明确未完成项

1. **阻塞修复尚未落盘**：`RealtimeService.restart()` 的 QUEUED 阶段目前直接 stop/cancel，再到 submit 校验。如果接收重启后、worker 首次执行前数据源或物理结构漂移，会先停止旧作业再失败。必须在停止旧作业前执行与 UPGRADE 相同级别的预检并持久化 PREFLIGHT_OK：`verifyDatasourceBindings → compile(restart_preflight) → verifyPreparedBindings → validateBoundSchemas → EXPLAIN`。PREFLIGHT_OK 后继续已有持久化停止触发；已终态 OLD_TERMINAL 直接 submit。增加“预检失败不停止旧作业”和“PREFLIGHT_OK 重启后继续而不重复预检”测试，并更新现有 running/fresh restart 的 Gateway 预检 stub。
2. 最新源码中生产/初始化会话清理重试、signed server-id、物理结构预检、最深 Gateway 错误摘要、真实固定运行时长已落盘，编译及部分测试通过，仍须最终完整测试/打包。
3. 重跑完整产品 API 验收：并发 CDC 预览隔离、Checkpoint TaskManager 恢复、Savepoint 同版恢复、默认有状态重启、兼容升级、不兼容升级、Doris 回滚及无遗留作业尚未完成最终轮次。
4. 补真实后端重启：运行一项本次独立 CDC 作业，停止并恢复后端，确认 Flink Job ID 不变、同任务新启动返回 409，随后取消。不要使用暂停时已取消的作业冒充该测试通过。
5. 新 jar 页面运行时长复核，脱敏最终报告、Git diff/凭据检查，验收通过后提交并推送 main。

## 暂停时的资源和服务

- 本次重启检查生产 job `c28b157d-dc0d-47f8-be12-4911e5ba9c33` / Flink `a423e61e07f3fbd54f0d8081b967773a` 已确认 CANCELED；恢复后的产品记录为 STOPPED。
- JobManager 当前 **0 活动作业**，数据库 **0 活动预览**。3 个明确验收空间内 16 个留存 Gateway operation/session 资源逐个确认关闭或已不存在。页面两项有限作业均 FINISHED，两个专属浏览器会话均关闭，没有验收轮询。
- 完整 Maven/Surefire 专属进程已停止（Surefire PID 33204，父 Maven PID 62056）；产品验收 Python 脚本没有运行。暂停时保留 1 个 `sched-it-*` 集成测试空间及其 `scheduler_it_*` 独立业务库/用户，详细 ID 见 `pause-cleanup.json`，明天按测试 cleanup 顺序定向清理，不删共享数据。
- 后端 8080 已恢复 **此前验证过的旧 jar** `.runtime/realtime/live-backend.jar`，PID **63984**（以 `live-backend.pid` 为准），健康检查 200。它尚不包含上述最新源码修复。为暂停期间避免自动测试调度，启动覆盖 `studio.scheduler.enabled=false`、`studio.inventory.recover-on-start=false`；明天最终恢复正常运行前确认配置。
- 前端原 Vite 5173 继续运行，HTTP 200。没有重启/停止共享前端。
- Flink JM 8081、Gateway 8083 HTTP 200；JM/Gateway/TM、实时 Kafka、既有 Kafka、两个 MySQL、Doris FE/BE Docker 容器全部保持运行。没有删除任何容器或卷，未重启共享设施。
- 业务测试表与 Topic 按原验收规则保留用于检查；恢复验收会创建新的独立名称。

## Git 与保存状态

工作目录 `D:\AllForCareer\fakeDataWorks_260927`，分支 main，HEAD 仍为 `80009e6 feat: add Flink runtime and verified streaming connectors`。改动均保留在工作区，新增源码/测试未跟踪文件也已备份；没有 reset、stash、丢弃或隐藏修改。

本次没有提交或推送 main：用户要求暂停，存在上列真实阻塞与失败验收，将当前状态作为“验收完成”推送不合适。仓库的完成后提交/推送 main 要求仍是恢复后的待办，**通过最终验收后必须执行**。

## 恢复步骤与具体命令

先阅读本交接、确认 Git 改动和已暂停的验收状态，修复第 1 项。Python 必须使用指定环境，PowerShell 显式 UTF-8。

```powershell
Set-Location 'D:\AllForCareer\fakeDataWorks_260927'
$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$env:PYTHONIOENCODING = 'utf-8'
git status --short
Get-Content -Encoding UTF8 '.runtime/realtime/pause-cleanup.json'
```

在停止当前自有后端前核对 PID/命令行，避免 Maven 集成测试与运行中的业务协调器相互干扰。仅停止本文件指定 jar 的 java 进程；Docker 和前端继续保留。

```powershell
$backendProcessId = [int](Get-Content -Encoding UTF8 '.runtime/realtime/live-backend.pid')
$backendProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$backendProcessId"
if ($backendProcess.Name -ne 'java.exe' -or $backendProcess.CommandLine -notlike '*live-backend.jar*') { throw '后端进程不匹配' }
Stop-Process -Id $backendProcessId
$acceptanceConfig = Get-Content -Encoding UTF8 '.runtime/flink/acceptance-local.json' | ConvertFrom-Json
$env:NODE_DEBUG_DORIS_DATABASE = $acceptanceConfig.database
$env:NODE_DEBUG_DORIS_USER = $acceptanceConfig.username
$env:NODE_DEBUG_DORIS_PASSWORD = $acceptanceConfig.dorisPassword
& './scripts/test-inventory.ps1'  # 自动从已知 MySQL 容器读取测试 root 密码，不输出凭据
if ($LASTEXITCODE -ne 0) { throw '测试未通过' }
mvn.cmd -B -ntp -f backend/pom.xml -DskipTests package
if ($LASTEXITCODE -ne 0) { throw '打包未通过' }
```

通过后复制新 jar，后台隐藏启动；保留 UTF-8。恢复验收阶段仍关闭离线调度与库存自动恢复，完成后再恢复正常设置。

```powershell
Copy-Item -LiteralPath 'backend/target/data-studio-0.0.1-SNAPSHOT.jar' -Destination '.runtime/realtime/live-backend.jar'
$backendJar = (Resolve-Path '.runtime/realtime/live-backend.jar').Path
$backendProcess = Start-Process -FilePath java -ArgumentList @('-Dfile.encoding=UTF-8','-Dstdout.encoding=UTF-8','-Dstderr.encoding=UTF-8','-jar',$backendJar,'--server.port=8080','--studio.scheduler.enabled=false','--studio.inventory.recover-on-start=false') -WorkingDirectory (Resolve-Path 'backend').Path -WindowStyle Hidden -RedirectStandardOutput (Join-Path (Get-Location) '.runtime/realtime/live-backend.log') -RedirectStandardError (Join-Path (Get-Location) '.runtime/realtime/live-backend-error.log') -PassThru
[IO.File]::WriteAllText((Join-Path (Get-Location) '.runtime/realtime/live-backend.pid'),[string]$backendProcess.Id,[Text.UTF8Encoding]::new($false))
Invoke-RestMethod 'http://127.0.0.1:8080/actuator/health'
& D:\PythonVenv\Scripts\python.exe './scripts/test-realtime.py' --backend-url 'http://127.0.0.1:8080'
```

完整产品验收会重启专属 TaskManager；先确认没有其他活动作业。保留当前失败报告副本后再运行，不能只改报告状态。随后补后端重启与页面运行时长检查；前端只有新增修改时再重跑：

```powershell
npm.cmd --prefix frontend run typecheck
npm.cmd --prefix frontend test
npm.cmd --prefix frontend run build
git diff --check
git fetch origin
# 完成验收、检查变更与凭据之后再执行：
git add README.md backend docs frontend scripts/test-realtime.py
git commit -m 'feat: complete realtime Flink development and operations'
git push origin main
```

不要将 `.runtime`、本地配置、明文凭据、未经脱敏的 Surefire XML properties 或连接器运行日志加入 Git。

## 2026-10-02 恢复完成

上述暂停时的未完成项已处理。运行中重启预检已修复；新增 V12 CDC 范围登记、响应体超时及远端协议异常恢复处理，V11 checksum 保持不变。后端最终 349 项单元与集成测试、打包通过；前端类型检查、86 项测试和构建通过。

完整产品 API 最终轮次的 runId 记录在[脱敏验收明细](realtime-acceptance-2026-10-02.json)：Kafka、CDC 三路增改删、预览隔离与清理、Checkpoint、Savepoint、默认有状态重启、真实聚合同拓扑升级、不兼容恢复与旧版回滚全部通过。另行完成真实后端停止和恢复：原 Job ID 保持不变、重复启动 409、原请求幂等。页面已补新作业真实时长的两次采样与截图。

最终检查为 0 活动 Flink 作业、0 活动预览、0 CDC 占用范围；后端健康 UP、前端 HTTP 200，后端已恢复正常配置。暂停、失败和中断的历史证据原样保留。完整结果见[验收报告](realtime-acceptance-2026-10-02.md)。
