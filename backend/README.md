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

Flyway 维护 V1–V8 版本迁移；旧七表数据库需备份并显式建立 V1 基线，见[迁移说明](../docs/mysql-query.md)。`SeedData` 仅补充一个默认工作空间，不生成开发文件或连接。V8 将旧内置沙箱和同步验收空间退出用户列表，保留其数据并暂停自动计划；用户自建空间保持可见。默认数据库名为 `fake_dataworks_260927`，可通过连接 URL 指向自己的数据库；辅助初始化及备份脚本使用默认数据库名。

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
| `/runs`、`/runs/{id}/stop` | GET/POST、POST | 真实 MySQL、工作流及模拟运行和停止 |
| `/records`、`/records/{id}` | GET/POST、PATCH | 发布、评审、检查、冒烟、治理、AI、代码管理记录 |
| `/preferences` | GET、PUT | 本地单用户偏好完整 JSON |
| `/actuator/health` | GET | 服务与数据库健康状态 |

`POST /workspaces` 接受 `{ "code": "finance_analysis", "name": "财务分析" }`，返回 201 和新空间。`code` 为唯一名称，须为 3–64 位小写字母、数字或下划线，且以字母开头；`name` 为 1–100 字符的显示名。重名返回 409 `WORKSPACE_CODE_CONFLICT`，格式错误返回 400。新空间没有文件、连接或运行记录，环境为本地。`GET /workspaces` 只返回 `DEFAULT` 和 `USER` 空间；验收空间使用 `TEST`，旧内置沙箱使用 `ARCHIVED`，均保留底层数据。

`PUT /objects/{id}` 接受完整对象，需要当前 `version` 和 `parentId`（null 表示根目录）。`owner` 可修改，须为 1–100 个字符且不含控制字符。同目录重名、版本冲突及恢复冲突返回 409；跨工作空间父目录、循环目录和非法 DAG 返回 400。DAG 使用迭代拓扑排序，拒绝重复起终点连线。删除同时软删除当前未删除的后代；恢复不会复活此前单独删除的子对象。

`LocalSimulationProvider` 将运行快照保存到 MySQL，状态依次为 `QUEUED → RUNNING → SUCCESS/FAILED`，可取消。服务重启把中断任务标记为失败并追加说明。模拟结果明确标为本地模拟。`RunService` 根据节点显式配置分派到 `LocalSimulationProvider` 、`MysqlExecutionProvider` 或 `WorkflowService`；MySQL 脚本使用独立业务连接、逐语句提交和业务库边界校验，不复用元数据库。Shell、Python 仍不执行。

资源文件存放在运行目录的 `storage`，使用生成的 UUID 文件名。元数据库保存文件索引和对象 `config.file`/`config.fileName`。上传在事务中增加对象版本并返回完整对象，回滚时清理新文件。复制资源共享不可变文件；重新上传时保留仍被副本引用的原文件。没有上传文件但有文本内容的资源可直接下载其 UTF-8 内容。

## 验证

`mvn.cmd -B -ntp "-Dtest=*Test,!*IntegrationTest" test` 执行不连接数据库的单元测试。完整 `mvn.cmd -B -ntp test` 还包含真实 MySQL 集成测试，应先配置独立测试元数据库和业务连接。每个集成测试创建独立 `test-<UUID>` 工作空间，结束后仅清理其自身的数据。集成测试覆盖重名、跨空间、目录循环、版本冲突与恢复、递归复制、回收站事务、模拟成功/失败/取消以及用户 SQL 不被执行。

集成测试关闭启动恢复任务的行为，不会修改其它工作空间的运行状态。普通后端启动时会按设计恢复数据库中遗留的未完成任务。

## MySQL 真实查询

数据源 CRUD、凭据 AES-GCM 加密、真实元数据、结果分页、执行快照与取消机制详见 [接口和使用说明](../docs/mysql-query.md)。

`dw_datasource` 保存数据源及密文，`dw_run_result` 保存有上限的结果集。运行状态采用条件更新，终态不可被异步回写覆盖。密码加密密钥和演示账号密码由初始化脚本写入被忽略的 `application-local.properties`；请随数据备份原密钥。

真实工作流在预检及事务完成后按依赖推进；不可变发布包通过专用接口创建和运行，不使用通用操作记录。V3 新增发布表及父子运行关系。协议、版本语义及限额见 [真实工作流说明](../docs/workflow-execution.md)。

## MySQL ↔ Doris 同步

`SyncExecutionService` 通过 Java SDK 提交、查询和取消 Dunnelean 作业，独立 Rust 服务负责 Arrow 数据传输。基础工作台不要求运行同步服务；离线同步需另外准备 Dunnelean、MySQL 和 Doris。连接、发布、调度、目标占用与恢复规则见[离线批量同步](../docs/offline-sync.md)。
