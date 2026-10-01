# 实时数据开发与运维

实时模块使用 Spring Boot / MySQL 保存目录、任务、发布、作业和控制操作，通过 SQL Gateway 提交 Flink SQL，通过 JobManager REST 管理真实作业。实时对象与离线运行、Cron 调度独立。

## 环境与连接

先按 [Flink 运行环境](flink-runtime.md) 准备集群和连接器。后端默认 `studio.flink.gateway-url=http://127.0.0.1:8083`、`studio.flink.jobmanager-url=http://127.0.0.1:8081`。

数据源中的宿主机地址用于元数据与连接测试，“Flink 执行地址”用于容器内读写：

| 数据源 | 宿主机连接 | 本项目默认 Flink 执行地址 |
| --- | --- | --- |
| MySQL | `127.0.0.1:3307` | `mysql-rlt:3306` |
| Kafka | `localhost:19092` | `kafka_rlt_4_3_1:9092` |
| Doris SQL | `127.0.0.1:9030` | `fe:9030` |
| Doris FE HTTP | `127.0.0.1:8030` | `fe:8030` |

默认映射只用于已有本地设施，其他连接须填写真实可达的执行地址。Doris 还需容器可达的 BE HTTP 地址，当前环境为 `172.30.41.3:8040`。

Kafka 使用真实 AdminClient 测试连接和浏览 Topic，支持 PLAINTEXT / SASL_PLAINTEXT / SASL_SSL，以及 PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512。密码由后端 AES-GCM 加密保存，公共接口仅返回 `passwordSet`。离线编辑器只选择对应类型的数据库连接。MySQL CDC 需开启 ROW/FULL Binlog、具备复制权限，字段和目标表通过真实元数据检查。

发布和提交前检查绑定字段、类型、非空限制及主键：CDC 主键须匹配来源表；JDBC Upsert 须完整匹配主键或唯一键；Doris 模型及 Unique Key 须与物理表一致，同步删除要求 Unique Merge-on-Write。物理表变化后应重新导入字段并校验。

## SQL 编辑与调试

Source / Sink 面板支持 Kafka、MySQL CDC、MySQL JDBC 和 Doris，字段配置支持主键、可空性及事件时间 Watermark。也可直接编写 Flink 表、视图和处理 SQL。

绑定配置是受管 DDL 的配置来源。“插入 DDL”使用 `@realtime-binding:<id>:begin/end` 标记维护 SQL 块。后端检查预览与配置是否一致，执行时替换为带运行地址与凭据的 DDL；未插入的绑定生成前置 DDL。配置变化或手改受管块后需通过差异面板确认更新。同名手写表冲突、缺失/重复/嵌套标记会给出错误。

手写连接表可在 WITH 中使用 `'studio.datasource-id' = '<数据源 ID>'` 引用注册数据源。后端移除该工作台选项，并注入连接地址与凭据后提交 Flink。不要在 SQL 中填写密码或 JAAS。集群目标、提交身份及状态恢复配置由平台固定。

表定义需使用 CREATE TABLE；修改字段或连接配置后重新发布。CREATE TABLE AS 查询会提交额外作业，ALTER TABLE 可绕开发布绑定，因此当前不支持这两种语句。

“SQL 校验”和“执行计划”建立独立会话并执行 Flink EXPLAIN，不启动正式写入。连续多路 INSERT 归并为一个 `EXECUTE STATEMENT SET`，也支持显式 Statement Set。一项任务只提交一个生产作业，多个独立写入段需调整脚本。

“结果预览”执行选中的单条 SELECT，默认最多 **100 行或 30 秒**，展示 `+I / -U / +U / -D` 行类型。预览使用任务 DDL 上下文，但不执行 INSERT；Kafka 使用独立调试消费组，CDC 使用独立 server-id。达到限制、结束或停止后清理真实查询作业、operation 和 session。Gateway operation 的 FINISHED 不代表流式查询已结束。

CDC 的 `server-id` 可留空自动分配；显式范围须在连接器支持的 `1..2147483647` 内，至少覆盖并行度。预览会覆盖显式配置，使用独立范围。同一脚本检查范围重叠，V12 注册表按实际 MySQL 执行地址在所有任务、工作空间及预览之间加锁登记；冲突返回 `CDC_SERVER_ID_IN_USE`。未知提交继续保留范围，确认远端终结且 Gateway 清理完成后才释放。

## 保存、发布与作业控制

保存内容在 MySQL 中持久化，版本号防止旧请求覆盖新内容。未保存草稿与标签状态按工作空间保存在浏览器，慢网络保存不会覆盖请求期间的新编辑。

发布固定 SQL、绑定、运行参数和公开数据源配置。新草稿或新发布不改变已运行作业。运行前检查数据源是否漂移；目标、账号等公开配置变更后需重新发布，密码从同一数据源的当前加密凭据读取。

默认并行度为 1，Checkpoint 间隔 60 秒，固定失败重启 3 次、间隔 10 秒。Flink Checkpoint 的 EXACTLY_ONCE 不代表所有外部 Sink 的统一交付语义；JDBC 主键 Upsert、Kafka 普通追加和 Doris 2PC 分别按连接器实际能力运行。

实时运维支持启动、取消、运行中 Savepoint、保存状态后停止、重启和恢复。生产作业 detached 提交，捕获 Job ID 后释放 Gateway 会话；关闭浏览器或重启业务后端不主动取消生产作业。提交或控制状态未知时先对账，确认前不重复提交、不释放任务活动锁。

Gateway 会话清理失败会保留句柄并后台重试。后端重启后按持久化部署身份和 Job ID 对账；初始化尚未提交的旧会话也须完成清理后才能重新建立会话。

Gateway 单次 HTTP 请求的 20 秒超时覆盖响应头及响应体，JSON 响应最多 8 MiB，文本日志最多 2 MiB，避免慢响应或超大响应长期占用预览和运维工作线程。

运行中重启默认保存状态并停止，再从该保存点恢复同一发布版本。已结束作业优先使用同版本最近保存点；没有保存点时，必须明确确认全新重启。

运行中重启先检查发布连接配置、物理表结构和 Flink 执行计划，预检成功后才停止旧作业。预检阶段会持久化，后端重启后继续已有控制操作。

## Savepoint、升级与回滚

保存点使用真实 `file:///opt/flink/state/savepoints/...` 路径。恢复使用 `execution.state-recovery.path`、`NO_CLAIM` 和严格状态匹配，核对 JobManager 的保存点恢复信息。

跨发布升级先校验新版本，再保存状态并停止旧作业，从保存点启动新版本。有状态升级保持物理 Source/Sink、字段类型和主键身份一致，SQL 执行计划仍可能不兼容。失败时保存点保留，可恢复旧发布；恢复旧发布前需确认新作业终结。回滚恢复计算状态，不能撤销已提交的外部数据。

Doris 各次部署、升级和回滚使用独立 Label 前缀，同一提交重试复用前缀。保存点存在不保证历史 Doris 事务仍在保留期内，缺失或过期状态会显示真实错误。

## 运维与存储边界

作业状态、速率、反压、异常、Checkpoint 和 Savepoint 来自真实 Flink REST；缺失指标显示“不可用”。日志标明共享节点来源并脱敏后返回。

旧 `sinket-realtime:v1:<workspaceId>` 数据一次性幂等导入，原始数据保留备份；模拟作业和 `mock://` 保存点不进入正式运行记录。已遗失的 SASL 会话密码需重新填写。

备份需保存元数据库、原加密密钥及独立的 Flink 状态卷。单机集群没有 JobManager HA。清除浏览器存储影响未保存草稿和页面状态，已保存任务、发布及运维历史保留在后端。

## 验证

```powershell
npm.cmd --prefix frontend run typecheck
npm.cmd --prefix frontend test
npm.cmd --prefix frontend run build
mvn.cmd -B -ntp -f backend/pom.xml "-Dtest=*Test,!*IntegrationTest" test
& D:\PythonVenv\Scripts\python.exe ./scripts/test-realtime.py
```

产品验收报告为 `.runtime/realtime/acceptance-report.json`。验收使用独立表和 Topic，结束时清理本次作业，保留测试数据及运行容器；恢复测试会重启专用 TaskManager，发现其他活动作业时拒绝开始。
