# 本地库存日结与调度

> 当前工作台采用任务级调度和逐任务独立提交，统一「调度运维」入口见 [任务调度与上游依赖](task-scheduling.md)。本页库存口径继续有效；三层原子发布说明适用于旧工作流发布版本。新安装不会生成库存示例节点，独立库存分析页面已移除。

本版为本机单用户演示。业务库 `studio_inventory` 与元数据库分开，保留原 `studio_demo` 订单示例和业务账号。库存 SELECT 在独立业务连接执行，最终发布三层正式结果；浏览器关闭后，后端继续调度。

## 初始化与启动

1. 启动元数据库和业务 MySQL 实例（示例容器名为 `dataworks-demo-mysql`），确认 `backend/application-local.properties` 可用。
2. 升级前运行 `scripts/backup-metadata.ps1 -ContainerId 'your-mysql-container'`，另行备份上传目录 `backend/storage` 和原本地配置中的加密密钥。已有加密凭据必须使用原密钥。
3. 在项目根目录运行 `./scripts/init-inventory-demo.ps1 -ContainerId 'your-business-mysql-container'`。脚本读取选定业务容器的本地管理凭据，创建专用账号、业务表、暂存表和样例源数据，并将库存账号密码写入被忽略的本地配置。可用 `DEMO_ROOT_PASSWORD` 指定管理密码；不会打印密码。
4. 从 `backend` 目录启动后端 `mvn.cmd spring-boot:run`。Flyway 维护元数据库的调度及落表配置结构；初始化脚本仅准备业务表、账号与源数据，不创建工作台节点或数据源。通过“数据源”页面添加库存业务库连接。
5. 启动前端 `npm.cmd run dev -- --port 5173 --strictPort`，访问 `http://127.0.0.1:5173`。

初始化可重复执行，不清空已有数据，不重置用户编辑的节点。样例第一天在首次初始化时固定为当日减两天，保存在 `inventory_demo_config.first_day`。初始采集时间使用 UTC；业务日期使用 DATE。专用账号具有对应业务库的读写及表结构权限。已有安装可执行 `scripts/grant-demo-readwrite.ps1 -ContainerId 'your-business-mysql-container'` 补齐授权；元数据库保持隔离。

## 当前工作台使用

1. 从“数据源”浏览 `studio_inventory` 的真实表与字段，新建 MySQL SQL 节点绑定该连接，查询业务数据。
2. 已有库存节点和工作流升级后可从回收站恢复。运行后在编辑器结果面板及工作流节点详情查看日志、行数与结果。
3. 当前任务计划在右侧“调度配置”维护，需发布并应用版本；修改开发 SQL 不改变已发布版本。启用与暂停计划、上游依赖规则见[任务调度](task-scheduling.md)。

## 库存口径

| 层 | 业务表 | 粒度及规则 |
|---|---|---|
| ODS | `ods_inventory_opening/inbound/outbound/transfer/adjustment` | 单据行的追加版本；`record_id` 标识采集记录 |
| 维度 | `dim_warehouse`、`dim_sku` | 仓库名称/区域，SKU 名称/件/固定标准成本/安全库存 |
| DWD | `dwd_inventory_ledger_di` | 业务日期＋稳定行键；每仓 SKU 期初余额行与当天流水 |
| DWS | `dws_inventory_warehouse_di` | 业务日期＋仓库；期初、入/出库、调入/出、调整、期末及金额 |
| ADS | `ads_inventory_analysis_di` | 同粒度，补充区域、SKU 数、库存异常、低库存占比 |

先限制 `ingested_at <= :source_cutoff`，再按来源类型＋单据号＋行号选取最高 revision，按采集时间、采集 ID 稳定打破平局，最后过滤 `status != VALID`。因此撤销不会使旧有效版本重新出现，重复采集不会重复计入。

调拨分为转出负数与转入正数，固定成本下全仓数量与金额净额均为零。盘点调整可正可负。期初由有效初始余额及业务日前有效流水求和，不读取前一日正式结果。新增仓 SKU 当日期初为零；没有当日流水仍保留余额行。初始余额是起始基线，同一仓 SKU 若采集修订，应使用原单据行的更高 revision。

数量统一为“件”，金额使用 DECIMAL × SKU 固定标准成本。期末 = 期初 + 入库 − 出库 + 调入 − 调出 + 调整。负库存保留，单独统计；低库存定义为 `0 <= 库存 < 安全库存`，与负库存分别统计。SKU 数为在截至该日有效日账中出现的仓 SKU 数，不展开所有仓与 SKU 的笛卡尔积。样例 SKU 标准成本视作固定维度，不提供成本历史版本。

期初余额行使用 `row_kind=OPENING_SNAPSHOT`，来源 `opening_and_prior_flows`，表示历史有效记录的聚合；流水行保留来源表及单据。缺失仓库/SKU、空数量、非调整类负数、无效调拨阻止发布。负的计算结果不阻止发布。

| 日期 | 仓 | 期末件数 | 期末金额 |
|---|---|---:|---:|
| 第一天 | W1 | 155 | 1900.00 |
| 第一天 | W2 | 33 | 480.00 |
| 第一天 | W3 | 4 | 30.00 |
| 第二天 | W1 | 162 | 1960.00 |
| 第二天 | W2 | 26 | 410.00 |
| 第二天 | W3 | -2 | -15.00 |

第三天无流水，余额与第二天相同。

## SELECT 与原子发布

在节点“运行配置”选择 MySQL / 库存落表、业务数据源及预建目标表。库存流程本版要求同一数据源的 DWD → DWS → ADS 三节点。编辑单条 SELECT/WITH，输出列须符合目标表结构，后端包装 INSERT；拒绝用户 DDL/DML、多语句、文件/会话操作及其他数据库访问。普通 SQL 模式支持多语句读写和表结构操作，见 [SQL 执行说明](mysql-query.md)。

| 参数 | 绑定类型 | 含义 |
|---|---|---|
| `:bizdate` | LocalDate | 指定业务日期 |
| `:source_cutoff` | UTC LocalDateTime | 固定的 ODS 采集截止时间 |
| `:build_id` | String | 本次运行批次 ID |

参数只在 SQL 值位置进行 PreparedStatement 绑定，不替换字符串中的同名文本，不用于标识符。DWS/ADS 模板读取 `etl_stage_*` 并按同一 `:build_id`、`:bizdate` 筛选。编辑 SQL 时应保留此筛选；结果业务日期必须与运行一致。结果预览最多 1000 行，实际落表不受预览行数限制。

每个节点独立提交本批次暂存。所有节点成功后，持有业务库锁的连接在一个事务内删除指定日期的三层正式行、插入本批次结果，并写入 `etl_publish_receipt`。提交成功才标记 PUBLISHED；重跑替换当天，不累加，不改变其他日期。

同一 MySQL 业务库通过命名锁限制一个落表流程，开发调试、发布运行、定时调度、重跑共用。取消和提交互斥；已确认提交不能被改成取消。提交应答丢失时检查凭据，状态未知则继续核实，禁止冲突写入，不立即重试。重启时先取得原提交锁再读凭据：有凭据恢复成功，无凭据标为中断失败；连接不可用则 RECOVERING，恢复后继续检查。暂存表保留历史批次便于排查，本地演示版暂未自动清理。

## 调度语义

- 一个工作流一个计划，绑定明确 R 版本；保存配置采用版本号校验。
- Spring 六字段 Cron：秒、分、时、日、月、周，秒固定 0，最小一分钟。支持分钟/小时/日/周/月表单及高级 Cron，例如每分钟 `0 * * * * *`、每日 02:00 `0 0 2 * * *`。月中不存在的日期跳过；有夏令时的时区按本地日历匹配，秋季重复时间使用不同 UTC 时点，春季不存在时间跳过。
- 时区、起止日期按计划时区解释，起止日期包含当日。业务日期为计划本地日期加偏移（-365 至 0）。新启用从下一匹配点开始。
- 每秒共用一次调度扫描。计划＋UTC 时点＋补数批次唯一，重复扫描去重。上一轮任务或同库落表仍在运行时保留等待资源的实例。
- 服务启动时尚未生成的到期期次逐条记为 SKIPPED / MISSED_INTERVAL，不自动补跑；正常扫描延迟继续等待执行。暂停停止生成并撤回未开始的周期尝试，已经开始的运行继续收尾；恢复原实例，显式补数和重跑可执行。
- 自动重试默认关闭；可设置 1–10 次、1–30 整分钟间隔。仅明确临时连接、死锁或数据库连接/内存资源不足可重试；执行池、工作流容量及落表锁暂满时继续等待资源，不消耗失败重试次数。权限/语法错误、超时、取消、服务中断不重试。重试创建新运行，固定原发布版本、日期与截止时间。历史保留每次尝试的运行链接。
- 支持任务和整体工作流的业务日期范围补数，任务依赖支持最近一期及全天完成屏障。保持本机单进程，不支持任意跨周期区间或外部资源组。最新 R 重跑和原输入复现的区别见 [调度说明](task-scheduling.md)。

## 接口与测试

统一前缀 `/api/v1`：

| 方法/接口 | 用途 |
|---|---|
| GET `/workflow-schedules?workspaceId=&workflowId=` | 计划列表 |
| POST `/workflows/{id}/schedule` | 创建计划，默认暂停 |
| PUT `/workflow-schedules/{id}` | 更新，需要 `expectedVersion` |
| POST `/workflow-schedules/preview` | 预览未来五次时间和业务日期 |
| GET `/workflow-schedules/{id}/triggers?page=&pageSize=&status=` | 实例分页 |
| POST `/schedule-triggers/{id}/rerun`、`/runs/{id}/rerun` | 原版本、原上下文重跑 |
| POST `/runs`、`/workflow-releases/{id}/runs` | 可选 `businessDate`，原无参数调用兼容 |
| GET `/inventory?workspaceId=&dataSourceId=&businessDate=&warehouseId=` | 正式 ADS、可选日期、仓库 |
| GET `/inventory/ledger?...&skuId=&page=` | DWD 日账与 SKU 余额 |

运行返回 `triggerType`、`scheduledAt`、`attempt`、`businessDate`、`sourceCutoffAt`、`buildId`、`publicationStatus`；节点另含 `writtenRows`、`targetTable`。密码不进入快照、日志或 API。

在项目根目录运行 `./scripts/test-inventory.ps1 -Container 'your-business-mysql-container'` 跑全部后端测试，包括独立临时业务库上的库存集成测试。脚本仅从指定本地业务容器获取测试管理员凭据，也可用环境变量 `INVENTORY_TEST_ROOT_PASSWORD` 提供。测试结束删除自己创建的随机库/账号，不清理演示库。普通 `mvn.cmd test` 没有该环境变量时会跳过库存集成类；调度后台轮询在测试进程中默认禁用。测试时关闭正在运行的后端，避免单用户恢复逻辑读取测试中间状态。

前端执行 `npm.cmd test`、`npm.cmd run typecheck`、`npm.cmd run build`。
