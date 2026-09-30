# SinketDataWorks 架构

项目将工作台交互、开发元数据和运行适配分开。浏览器负责编辑草稿；Spring Boot 负责校验、事务和持久化；执行协调器根据显式配置选择本地模拟、真实 MySQL SQL 脚本或 Dunnelean 离线同步。同步的数据通路、持久控制与目标占用规则见 [离线同步](offline-sync.md)。

## 分层与目录

```text
React 工作台（127.0.0.1:5173，可配置端口）
  ├─ Monaco：代码 / 版本差异 / Notebook 代码单元格
  ├─ React Flow：工作流画布
  ├─ 节点注册表：98 个类型 → 编辑器 / 配置表单
  └─ api.ts：JSON REST / 资源上传下载
              │ Vite /api 代理
Spring Boot（127.0.0.1:8080）
  StudioController
      ↓
  ObjectService / WorkspaceService / RecordService / FileService
      ↓                         ↓
  StudioRepository          ExecutionProvider
      ↓                         ↓
  JdbcTemplate          LocalSimulationProvider
      ↓                         ↓
  MySQL  ←────────────── 运行记录与内容快照

  FileService → 本地 storage 目录（文件字节）
```

后端是单 Maven 模块，包根为 `com.fake.dataworks`：

| 层 / 包 | 职责 |
| --- | --- |
| `controller` | `/api/v1` 路由、请求和响应适配、文件下载 |
| `dto`、`domain` | 请求结构和开发对象模型 |
| `service` | 工作空间校验、对象增删改、目录关系、版本、记录、文件、模拟执行 |
| `repository` | JdbcTemplate SQL、JSON 列编码、事务内数据库操作 |
| `config` | Gson JSON 编码和幂等工作空间身份初始化 |
| `exception` | 业务错误码、HTTP 状态和统一错误响应 |

服务层持有写操作的事务边界。对象写入使用工作空间锁、版本条件和唯一键保护；业务逻辑不放在 Controller。没有 ORM 实体自动建表，表结构由 Flyway 的 V1–V7 迁移维护。

## 前端状态和编辑数据

`App.tsx` 组织工作空间、目录、多标签、草稿、设置、运行面板和辅助弹窗。`StudioEditor` 根据对象及节点类型选择代码、Notebook、工作流或配置界面；`DatasourceView` 和 `RecycleView` 实现数据源及回收站页面；`Inspector` 编辑右侧配置和历史版本。

节点注册表包含类型分组、语言、编辑器类型及配置字段。兼容旧类型别名，使种子对象和后来创建的对象都可打开。未知类型有通用代码编辑器回退；这只是编辑能力，不表示具有相应引擎。

核心开发对象采用 `StudioObject`：

| 字段 | 用途 |
| --- | --- |
| `id`、`workspaceId`、`parentId` | 工作空间和目录归属 |
| `kind`、`nodeType` | 对象大类与具体节点类型 |
| `name`、`description`、`owner`、`tags` | 用户可维护的元数据 |
| `content` | SQL、脚本、DDL、函数或组件内容 |
| `config` | 引擎表单、运行、调度、工作流、Notebook、表字段等结构化配置 |
| `version`、`updatedAt` | 乐观版本检查和修改信息 |
| `favorite`、`deleted` | 收藏和软删除 |

`config.run` 保存计算资源、资源组、参数、优先级和运行方式；`config.schedule` 保存调度类型、周期、Cron、生效日期、重试和依赖。`config.graph` 保存节点位置与连线，`config.cells` 保存 Notebook 单元格及模拟输出；表目录使用 `columns` 和 `sampleRows`，`rows` 可以是行数统计。

编辑操作先写当前会话草稿。保存请求携带提交时版本，成功后更新服务器对象；如果用户在请求期间继续输入，只确认已提交内容，保留后续编辑。收藏等元数据更新只合并对应字段，不用服务器内容覆盖未保存代码、工作流或 Notebook。删除后根据实际存活对象集合清理标签与草稿。布局、主题、打开的标签和编辑器偏好通过偏好接口持久化，未保存代码不写入偏好。

工作流新增连线时前端立即检查目标存在、自连线、重复边和循环；后端保存时再次校验整个图，使用拓扑算法避免深链递归溢出。真实工作流以 config.graph 中的边驱动手动执行；旧 config.schedule 仅用于模拟对象；真实计划由独立 ScheduleService 与 dw_workflow_schedule 驱动，不读取旧配置。

## 持久化与一致性

默认数据库为 `fake_dataworks_260927`，使用 InnoDB 和 `utf8mb4`。核心元数据表如下；任务、调度、库存和同步的扩展表由后续迁移维护，另有 Flyway 迁移历史表：

| 表 | 保存内容 |
| --- | --- |
| `dw_workspace` | 工作空间名称、代码、地域 |
| `dw_object` | 目录、节点、工作流、Notebook、表、组件、资源、函数、个人环境；内容和 JSON 配置 |
| `dw_version` | 每次保存的名称、内容、配置快照和版本号 |
| `dw_run` | 真实 / 模拟运行状态、日志、执行方式及运行时对象快照；工作流父子运行关系 |
| `dw_workflow_release` | 不可变工作流发布包、发布号、完整图及所有子节点快照 |
| `dw_record` | 发布、评审、检查、冒烟、治理、AI、Git 快照记录 |
| `dw_preference` | 本地单用户偏好 |
| `dw_file` | 资源原始文件名、存储文件名、大小、类型及所属对象 |
| `dw_datasource` | 按空间隔离的数据源连接配置和加密密码 |
| `dw_run_result` | MySQL 有上限的持久化结果集 |

同工作空间、同父目录下的存活对象不允许重名。服务层拒绝跨工作空间父目录和把目录移动到自身或后代。更新需要当前版本，过期保存返回 `VERSION_CONFLICT`，不覆盖已保存数据。恢复旧版本通过普通更新生成新版本，旧快照继续保留。

删除是软删除；目录递归删除记录同一删除批次。恢复只恢复该次随目录删除的子对象，不复活此前独立删除的子项。恢复前检查同名冲突，用户可改名重试；不提供永久删除入口。

上传文件以随机名称存放在本地目录，MySQL 保存索引。上传在事务中更新索引、对象配置和版本；失败会清理新文件。复制资源沿用文件引用，替换时只清理没有其他对象引用的旧文件。默认脚本从 `backend` 作为工作目录启动，因此文件位于 `backend/storage`。种子资源没有上传文件时，可从其文本内容下载。

**版本恢复针对名称、代码和配置，不是上传文件字节的历史归档。** 文件下载始终读取当前文件索引；需要完整文件历史时应另增不可变文件版本模型。

初始化采用幂等建库、Flyway 版本迁移和按固定 ID 补充工作空间身份。旧七表数据库需备份、核对并显式建立 V1 基线，再运行 V2。新库从 V1 开始；禁止 Flyway clean。

## REST 接口

统一前缀 `/api/v1`，成功直接返回 JSON 对象或数组。错误响应为 `{ "code": "...", "message": "..." }`，搭配相应 HTTP 状态。浏览器 API 封装将连接失败与业务错误显示给用户。

| 路由 | 方法和行为 |
| --- | --- |
| `/workspaces` | `GET` 工作空间列表 |
| `/objects?workspaceId=…&deleted=…` | `GET` 存活对象或回收站 |
| `/objects` | `POST` 创建对象 |
| `/objects/{id}` | `GET` 查询，`PUT` 携版本保存，`DELETE` 软删除 |
| `/objects/{id}/copy` | `POST` 复制，可指定名称和父目录 |
| `/objects/{id}/restore` | `POST` 恢复，可指定新名称 |
| `/objects/{id}/versions` | `GET` 历史版本 |
| `/objects/{id}/versions/{versionId}/restore` | `POST` 携当前版本恢复历史快照 |
| `/objects/{id}/file` | `POST multipart/form-data` 上传，`GET` 下载 |
| `/runs?workspaceId=…` | `GET` 运行历史 |
| `/runs` | `POST` 创建运行，真实 MySQL 需要 `expectedVersion`；模拟保留 `simulateFailure` |
| `/runs/{id}/stop` | `POST` 停止排队或运行中的任务 |
| `/records?workspaceId=…&kind=…` | `GET` 操作记录，可按类型筛选 |
| `/records`、`/records/{id}` | `POST` 创建记录，`PATCH` 更新状态或载荷 |
| `/preferences` | `GET` / `PUT` 单用户偏好 |

`/actuator/health` 提供就绪检查，位于上述前缀之外。接口当前无登录、权限或租户认证，服务默认只监听本机。

## 运行适配与模拟边界

`ExecutionProvider` 提供两个替换点：

```java
Map<String, Object> start(StudioObject snapshot, String mode, boolean simulateFailure);
Map<String, Object> stop(String runId);
```

`LocalSimulationProvider` 使用单线程定时执行器推进 `QUEUED → RUNNING → SUCCESS / FAILED`，允许变为 `CANCELLED`。创建时保存代码与配置快照，成功结果为固定演示表格，失败由显式模拟参数触发；它不解析或执行编辑器中的代码。重启时把上次未结束的模拟运行标为失败并追加中断日志，避免历史长期停留在运行中。

`RunService` 是执行入口；`provider=MYSQL` 的 MySQL 节点交给 `MysqlExecutionProvider`，显式 WORKFLOW 交给 WorkflowService，其他节点继续模拟。业务连接、加密凭据、语法校验、执行取消、结果上限与分页详见 [MySQL 真实查询](mysql-query.md)。执行时固定快照，按记录中的 provider 停止任务；数据库条件更新保护终态。

其余模拟边界：

- Notebook 输出由浏览器生成固定说明，随保存写入配置，不启动 Python 内核。
- AI 使用本地字段和模板生成 SQL，不请求模型服务。
- 代码管理把内容和配置放在 `GIT` 类型记录中，没有 Git 对象数据库、分支合并或远程同步。
- 真实工作流发布使用独立不可变发布表，按发布 ID 运行；旧模拟发布、评审、检查、冒烟和治理保留本地记录。定时调度和云资源配置不会执行云操作。
- 未观察到的云端管理详情按官方功能结构与统一视觉实现；独立产品只有范围说明入口。

## 运行维护和验证入口

日常运行使用两个 PowerShell 终端：在 `backend` 目录执行 `mvn.cmd spring-boot:run`，在 `frontend` 目录执行 `npm.cmd run dev -- --port 5173 --strictPort`。首次准备时单独初始化数据库、安装前端依赖，具体命令见[项目启动说明](../README.md#启动)。前后端仅监听本机，默认端口分别为 5173 和 8080，端口占用时启动报错。日志直接显示在终端，按 `Ctrl+C` 停止对应服务；MySQL 独立运行。后端以 `backend` 为工作目录，沿用本地配置、加密密钥和 `backend/storage` 上传目录。

前端 `npm run typecheck`、`npm run build` 检查类型与生产构建；`node --test tests/*.test.ts` 测试图规则及保存期间的草稿处理。后端 `mvn test` 包含图校验和真实 MySQL 集成测试，集成测试使用随机工作空间隔离并清理自身数据、文件，存储路径为 `backend/target/test-storage`。这些检查验证本地实现，不代表与云端全部页面或执行语义等价。

## 真实工作流协调与发布

`WorkflowService` 校验全部引用和父子版本，`MysqlExecutionProvider.prepare` 只准备查询；父子快照在同一事务提交之后才进入协调器。每流程串行，独立流程共享已有查询池。WAITING 子节点依赖成功后入队，失败后代 SKIPPED，其他分支继续。取消与调度共用流程锁，状态落库仍使用条件更新；启动恢复处理 WAITING/QUEUED/RUNNING，不重放。

发布包保存在只追加的 `dw_workflow_release`，与可修改的 `dw_record` 分开。发布运行从包内恢复完整图和子对象，不解析当前开发对象；数据源目标身份必须匹配，密码在运行提交时读取并只存内存。发布号 R 与开发对象 V 分离。前端使用只读运行画布，状态不写回编辑草稿。详见 [工作流协议与边界](workflow-execution.md)。


## 库存与调度扩展（V4）

`InventoryExecutionService` 接受显式 MATERIALIZE 节点，PreparedStatement 绑定业务日期/UTC 截止时间/批次。阶段事务写批次暂存，最终事务一起替换三层当天正式结果并插入提交凭据。MySQL 命名锁限制同库落表并发，最终写入与持锁连接相同；恢复取得原锁后检查凭据，状态未知时阻止新落表。

`ScheduleService` 持久化计划及唯一触发实例，使用 Spring CronExpression 按时区生成未来时间。计划绑定不可变 R 包；父子运行与触发实例关联在元数据库事务提交后才启动。重试冻结原上下文、创建新运行；漏跑和重叠有可查询记录。

后端保留旧工作流发布与调度兼容能力；当前工作台通过右侧调度配置及数据源浏览访问相关功能，独立调度管理及库存分析页面已移除。详情与 SQL 口径见 [库存日结与调度](inventory-scheduling.md)。


## 任务调度（V5）

TaskRepository 持久化独立任务发布、任务计划和逐期实例。TaskScheduleService 先生成全部到期实例，再解析同业务日期的最近上游期次；依赖就绪才调用 TaskService。任务服务固定发布代码与上游运行，元数据库事务提交后启动真实执行。InventoryExecutionService 的单任务批次只发布一个目标分区，旧 workflow bundle schema 1/2 保留原语义，schema 3 逐任务提交。

业务库 etl_partition_publication 与正式分区在同一事务更新，记录数据来源；暂存批次保留，供重跑与下钻读取。前端 TaskScheduleEditor 在任务右侧调度配置使用，调度草稿由 App 持有，与代码草稿共同参与标签关闭保护。
