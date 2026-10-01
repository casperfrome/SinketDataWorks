# SinketDataWorks 后端

Java 25、Spring Boot 4.1.1、Spring JDBC、Flyway、MySQL。入口为 `StudioApplication`，默认监听 `127.0.0.1:8080`。离线同步依赖 Dunnelean Java SDK 0.1.0，Java 包名和 Maven 产物名保留兼容。

## 本地运行

先按[项目启动说明](../README.md#启动)准备 MySQL、数据库、配置和 Java SDK。在本目录将 `application-local.properties.example` 复制为未提交的 `application-local.properties`，填写数据库连接及随机 32 字节 Base64 加密密钥；已有配置时保留原文件及原密钥。

未使用本地文件时，可通过 `DB_URL`、`DB_USER`、`DB_PASSWORD`、`STUDIO_ENCRYPTION_KEY` 环境变量配置；本地文件的显式属性优先。SDK 需要先安装到本机 Maven 仓库，见[SDK 安装说明](../docs/offline-sync.md#java-sdk-依赖)。

从项目根目录启动：

```powershell
Set-Location backend
mvn.cmd spring-boot:run
```

日志显示在终端，按 `Ctrl+C` 停止。保持从 `backend` 目录启动，使本地配置和 `backend/storage` 上传路径一致。前端单独启动，见主 README。

Flyway 维护 V1–V12 版本迁移；旧七表数据库需备份并显式建立 V1 基线，见[迁移说明](../docs/mysql-query.md)。`SeedData` 仅补充一个默认工作空间，不生成开发文件或连接。V8 将旧内置沙箱和同步验收空间退出用户列表，保留其数据并暂停自动计划；用户自建空间保持可见。默认数据库名为 `fake_dataworks_260927`，可通过连接 URL 指向自己的数据库；辅助初始化及备份脚本使用默认数据库名。

## 分层与接口

Controller 负责 HTTP/DTO 转换；Service 负责校验、事务和执行提供器；Repository 负责参数化 SQL；domain 定义开发对象，config 处理配置与幂等种子数据，exception 统一错误输出。

接口前缀 `/api/v1`，成功直接返回 JSON，失败返回 `{ "code": "...", "message": "..." }`。主要入口：

| 路径 | 方法 | 用途 |
|---|---|---|
| `/workspaces` | GET、POST | 默认与自建空间列表、创建空白空间 |
| `/objects?workspaceId=&deleted=false` | GET | 对象与回收站 |
| `/objects`、`/objects/{id}` | POST、GET、PUT、DELETE | 创建、读取、完整保存、软删除 |
| `/objects/{id}/copy`、`/restore` | POST | 递归复制、恢复本次一并删除的子对象 |
| `/objects/{id}/versions` | GET | 版本快照 |
| `/objects/{id}/versions/{versionId}/restore` | POST | 将历史内容保存为新版本，body 包含当前 `version` |
| `/objects/{id}/file` | POST、GET | multipart 字段 `file` 上传、下载；文件上限 20 MB |
| `/runs`、`/runs/{id}/stop` | GET/POST、POST | 真实 MySQL、Doris、数据集成、工作流及模拟运行和停止 |
| `/runs/parameters/prepare` | POST | 基于节点草稿准备调试参数；只读，不保存或执行 |
| `/records`、`/records/{id}` | GET/POST、PATCH | 发布、评审、检查、冒烟、治理、AI、代码管理记录 |
| `/preferences` | GET、PUT | 本地单用户偏好完整 JSON |
| `/actuator/health` | GET | 服务与数据库健康状态 |

`POST /workspaces` 接受 `{ "code": "finance_analysis", "name": "财务分析" }`，返回 201 和新空间。`code` 为唯一名称，须为 3–64 位小写字母、数字或下划线，且以字母开头；`name` 为 1–100 字符的显示名。重名返回 409 `WORKSPACE_CODE_CONFLICT`，格式错误返回 400。新空间没有文件、连接或运行记录，环境为本地。`GET /workspaces` 只返回 `DEFAULT` 和 `USER` 空间；验收空间使用 `TEST`，旧内置沙箱使用 `ARCHIVED`，均保留底层数据。

`PUT /objects/{id}` 接受完整对象，需要当前 `version` 和 `parentId`（null 表示根目录）。`owner` 可修改，须为 1–100 个字符且不含控制字符。同目录重名、版本冲突及恢复冲突返回 409；跨工作空间父目录、循环目录和非法 DAG 返回 400。DAG 使用迭代拓扑排序，拒绝重复起终点连线。删除同时软删除当前未删除的后代；恢复不会复活此前单独删除的子对象。

`LocalSimulationProvider` 将运行快照保存到 MySQL，状态依次为 `QUEUED → RUNNING → SUCCESS/FAILED`，可取消。服务重启把中断任务标记为失败并追加说明。模拟结果明确标为本地模拟。`RunService` 根据节点显式配置分派到模拟、共享 SQL、同步或工作流通道；共享 SQL 保留 `MysqlExecutionProvider` 类名，支持 `MYSQL`、`DORIS` provider。MySQL/Doris 脚本使用独立业务连接、逐语句提交和业务库边界校验，不复用元数据库。Shell、Python 仍不执行。

资源文件存放在运行目录的 `storage`，使用生成的 UUID 文件名。元数据库保存文件索引和对象 `config.file`/`config.fileName`。上传在事务中增加对象版本并返回完整对象，回滚时清理新文件。复制资源共享不可变文件；重新上传时保留仍被副本引用的原文件。没有上传文件但有文本内容的资源可直接下载其 UTF-8 内容。

## 验证

`mvn.cmd -B -ntp "-Dtest=*Test,!*IntegrationTest" test` 执行不连接数据库的单元测试。完整 `mvn.cmd -B -ntp test` 还包含真实 MySQL 集成测试，应先配置独立测试元数据库和业务连接。集成测试使用自己的工作空间及夹具，结束后仅清理其自身的数据。覆盖重名、跨空间、目录循环、版本冲突与恢复、递归复制、回收站事务、模拟成功/失败/取消，以及真实 SQL、发布和调度；模拟节点不会执行用户代码。

集成测试关闭启动恢复任务的行为，不会修改其它工作空间的运行状态。普通后端启动时会按设计恢复数据库中遗留的未完成任务。

## MySQL 真实查询

数据源 CRUD、凭据 AES-GCM 加密、真实元数据、结果分页、执行快照与取消机制详见 [接口和使用说明](../docs/mysql-query.md)。

`dw_datasource` 保存数据源及密文，`dw_run_result` 保存有上限的结果集。运行状态采用条件更新，终态不可被异步回写覆盖。密码加密密钥和演示账号密码由初始化脚本写入被忽略的 `application-local.properties`；请随数据备份原密钥。

真实工作流在预检及事务完成后按依赖推进；不可变发布包通过专用接口创建和运行，不使用通用操作记录。V3 新增发布表及父子运行关系。协议、版本语义及限额见 [真实工作流说明](../docs/workflow-execution.md)。

## MySQL ↔ Doris 同步

`SyncExecutionService` 通过 Java SDK 提交、查询和取消 Dunnelean 作业，独立 Rust 服务负责 Arrow 数据传输。基础工作台不要求运行同步服务；离线同步需另外准备 Dunnelean、MySQL 和 Doris。连接、发布、调度、目标占用与恢复规则见[离线批量同步](../docs/offline-sync.md)。

## Doris SQL 与分区同步

`DORIS` provider 复用 SQL 执行器、不可变发布、调度和工作流。Doris 方言通过明确识别的语法与当前业务库边界校验；支持查询、SHOW/DESC、DML 及 OLAP 表/分区 DDL。跨 schema、catalog 前缀、外部表函数及未识别语法在执行前拒绝，SHOW/DESC 分类为查询，DDL 保留逐语句提交状态。详细范围见 [Doris SQL](../docs/doris-query.md)。同步元数据新增分区类型、表达式、列和物理分区范围；配置仍保存在 JSON，未新增 Flyway 迁移。固定分区覆盖在目标锁后解析并冻结提交范围；来源字段路由做覆盖须选择已有物理分区。初始化订单样例为零数据、零分区的 AUTO 表，`partition.retention_count=400` 管理历史分区数量。参数/字段分区赋值和覆盖规则见 [分区同步](../docs/offline-sync.md#分区读写与订单示例)。

## 实时开发 API

V11 新增独立实时元数据表，V12 持久化 MySQL CDC server-id 占用范围并按执行端点加锁，保护跨任务和预览并发。实时对象不写入离线运行、任务计划或 Cron 表。所有接口使用 `/api/v1` 前缀，并在请求体或查询参数中携带 `workspaceId`。

| 路径 | 行为 |
| --- | --- |
| `/realtime/state` | GET 空间任务、目录、发布、作业和控制操作 |
| `/realtime/tasks`、`/tasks/{id}` | POST 创建，PUT 保存（`expectedRevision`），DELETE 删除 |
| `/realtime/tasks/{id}/copy`、`/draft` | 复制任务；PUT/DELETE 保存、丢弃草稿 |
| `/realtime/folders`、`/folders/{id}` | 创建、修改、删除目录 |
| `/realtime/tasks/{id}/releases` | POST 校验并保存不可变发布，含任务快照及预期版本 |
| `/realtime/validate`、`/plan`、`/preview` | POST 草稿 SQL 校验、计划及 SELECT 预览 |
| `/realtime/previews/{id}`、`/{id}/cancel` | GET 分页结果（`token`），POST 取消预览 |
| `/realtime/jobs`、`/jobs/{id}` | POST 从 `releaseId` 启动，可提供 `savepointId`；GET 真实详情 |
| `/realtime/jobs/{id}/{action}` | POST `cancel`、`stop`、`savepoint`、`restart`、`upgrade`；升级传 `targetReleaseId` |
| `/realtime/operations/{id}`、`/{id}/rollback` | GET 操作进度，POST 从失败升级保留的保存点恢复旧发布 |
| `/realtime/import` | POST 幂等导入旧浏览器内容，保留真实对象并排除模拟作业 |
| `/datasources/{id}/topics` | GET Kafka Topic 元数据 |
| `/datasources/{id}/tables/{table}/cdc-metadata` | GET MySQL CDC 条件及表结构检查 |

发布、校验、计划、预览和作业控制返回 `202 {operationId, jobId?, previewId?}`；轮询操作获取 `status`、`phase`、`result` 或脱敏错误。状态为 `RUNNING / SUCCESS / FAILED / RECOVERING`，结果未知时继续对账。写操作可传 `requestId` 保证幂等，数据库活动锁防止同一任务重复生产运行。

生产作业使用独立部署身份、固定 Job ID 和 detached 提交；恢复参数使用 `execution.state-recovery.path`、`NO_CLAIM`、严格状态匹配。预览默认 100 行/30 秒，单独管理查询作业与清理。详细 SQL、凭据和升级边界见[实时开发说明](../docs/realtime-development.md)。

普通单元及集成测试使用 `studio.realtime.polling-enabled=false`，避免后台协调其他空间的真实记录。产品验收使用 `D:\PythonVenv\Scripts\python.exe scripts/test-realtime.py --backend-url http://127.0.0.1:8080`，报告为 `.runtime/realtime/acceptance-report.json`。
