# 任务调度与上游依赖

真实 MySQL、Doris、离线同步任务各自拥有发布版本、调度计划和上游依赖。数据开发右侧负责配置；左侧「调度运维」集中查看任务、实例和执行尝试，包括尚未创建运行的等待、暂停和漏跑实例。参数规则见 [调度参数](schedule-parameters.md)。

新建 MySQL 节点后绑定数据源，保存 SQL 与调度参数，发布当前任务，在面板顶部选择执行版本；在调度时间设置周期、时区、日期偏移，在调度依赖添加上游，最后应用到调度。后台自动调度只需要后端与数据库运行。

已有库存节点恢复后可继续执行；系统不再自动创建演示 SQL、计划或连接。旧工作流发布版本仍保持原执行语义。默认每天北京时间 02:00、业务日期偏移 -1。生效计划决定周期时间，所选发布版本决定代码及调度参数；开发参数未发布时面板提示差异。

## 依赖匹配与失败处理

`LATEST`（历史配置默认）匹配同一业务日期、计划时间不晚于下游的最近一期，比较实际 UTC 时点。各任务时区、Cron、有效期和业务日期偏移独立生效。失败期次不能由更早的成功期次代替。

`ALL_DAY` 让每日任务等待上游同业务日期的全部期次，包括尚未到达的未来期次。预期时点在创建实例时固定；夏令时按实际本地日历枚举，秋季可有 25 个小时或 1,500 个分钟期次。未来期次与尚未生成的到期期次等待；明确失败、取消或漏跑阻断；修复对应上游实例后，未执行的下游自动重新判断。

全天依赖只作为完成屏障，SQL 自行读取当天正式分区。它不提供 `:upstream_<alias>_build_id` 单批次参数，也不合并单值批次血缘；引用全天别名的单值批次参数会在执行前报错。使用全天屏障读取正式表时，正式结果以业务库实际数据为准。

例如 DWD 每小时执行，DWS 每日 02:05 执行，DWS 使用同业务日期 02:00 的 DWD。若这个实例失败，不会使用 01:00 的成功实例。预览显示未来五次执行及其对应上游。

| 状态 | 含义 |
|---|---|
| 等待上游 | 对应上游尚在执行、等待资源或等待自动重试 |
| 等待资源 | 等待本任务执行资格、业务库锁或执行池资源 |
| 依赖阻断 | 上游最终失败、取消、漏跑，或没有对应期次实例 |
| 已暂停 | 周期任务已暂停，此实例尚未开始执行 |
| 已跳过 | 启动恢复时逐期记录的停机漏跑 |

上游依赖形成环、自身依赖、重复任务和重复参数别名均不能保存。暂停上游不会自动改变其他任务。普通手动运行不会顶替调度期次。同一任务跨调试、独立调度、补数与工作流子节点最多一个实际运行；等待依赖的实例不占执行资格，后续期次保留。

一个任务定义可以产生多个逻辑实例，每个实例保留多次实际运行尝试。周期实例固定已应用的 R、日期、计划时点、时区、参数、截止时间和依赖期次；真正提交前固定成功上游的运行 ID。每轮生成最多 200 条，持久化游标继续下一轮；正常扫描延迟继续排队，重启时尚未生成的停机期次逐条记录 `SKIPPED / MISSED_INTERVAL`，由用户补数。

运维入口默认「使用最新发布版本重跑」：保留业务日期、计划时点、时区，接受操作时选择最新 R、当前已应用依赖，刷新参数、成功上游及截止时间；之后排队和自动重试保持这次输入。「原输入复现」从所选历史尝试复制完整上下文。两种操作都在同一实例追加历史。旧重跑接口继续沿用原版本与输入语义。

自动重试默认关闭，只适用于安全的临时连接/死锁失败，沿用该次固定版本和输入。普通写入 SQL、离线同步、超时、取消和提交不确定不自动重放；资源队列拥塞继续等待，不消耗失败重试次数。

暂停停止生成新周期实例，将未执行的周期实例置为 `PAUSED`；已排队但未开始的尝试条件撤回，恢复后沿原上下文重新提交。同步已经提交远端的运行继续收尾。运行中的任务继续完成，显式重跑和补数可在暂停期间执行。停止实例则发起取消并等待实际执行收尾；提交结果未知继续保留核实信息。

## 范围补数

在任务行点击「补数」，选择业务日期范围、根任务的小时/分钟时点范围及是否包含受影响下游。预览展示实例、R、参数、依赖和需修复的上游，提交前校验计划未变化。工作流支持整体补数，内部 DAG 继续由原协调器执行。

每次请求携带 UUID `requestId`，它同时作为 `batchKey`；重复请求返回同一批。单次最多 5,000 个实例，整批固定版本及截止时间；同批选中的依赖只绑定本批实例，其他依赖只绑定周期实例，不搜索其他补数批次。补数与周期历史相互独立，复用同一实例队列，无独立补数执行引擎。

## SQL 和提交语义

- `:bizdate` 是 DATE，`:source_cutoff` 是 UTC 数据截止时间，`:build_id` 是当前任务批次。
- `:upstream_dwd_build_id`、`:upstream_dws_build_id` 对应依赖别名。参数采用 PreparedStatement 值绑定，不拼接标识符。
- DWS 示例读取 `etl_stage_dwd_inventory_ledger_di`，按 `build_id=:upstream_dwd_build_id AND business_date=:bizdate` 筛选；ADS 同理读取固定 DWS 批次。编辑 SQL 时须保留输入批次与日期筛选。
- 每个任务先生成不可变暂存批次，再在同一事务中替换自己的正式日期分区、更新发布来源索引、写提交凭据。没有数据时也能提交空分区。
- 下游失败不撤回成功的上游。每张正式表独立保持其最后成功结果，界面显示各层批次，避免误认为三层始终同步。
- 取消与最终提交互斥；提交应答不确定时进入恢复核实，禁止冲突落表。提交凭据确认成功后恢复为成功，无凭据则标记中断。
- 历史暂存批次暂不自动清理，因为历史重跑与 ADS 日账下钻需要它们。

旧工作流 R 版本保持原三层原子提交并串行执行；普通工作流最多两个就绪节点并行，新库存节点独立提交任务。启动时准备的连接、SQL、同步配置及依赖别名固定，后续编辑不会改变尚未开始的子节点。手动画布连线控制该次手动编排，任务自动调度读取已应用计划中的依赖。

## 标签页快捷操作

右键任一编辑器标签可关闭当前、其他、左侧、右侧、全部或已保存标签。操作以右键目标为准，不会因右键改变当前编辑文件。保留当前选中项；若被关闭，优先选择最近的右侧标签，再选择左侧。

未保存的 SQL、工作流和任务调度表单统一列在一次确认中。可保存并关闭、丢弃并关闭或取消。保存失败保留标签；已成功保存的部分保留保存结果。浏览器刷新前同样提示未保存内容。

## 接口与验证

统一前缀 `/api/v1`：

| 接口 | 用途 |
|---|---|
| GET/POST `/tasks/{id}/releases` | 任务版本列表与发布 |
| GET `/task-releases/{id}` | 不可变代码快照与数据源绑定 |
| POST `/task-releases/{id}/runs` | 已发布任务指定日期重算 |
| GET `/task-schedules?workspaceId=&taskId=` | 任务计划 |
| POST `/tasks/{id}/schedule`、PUT `/task-schedules/{id}` | 保存计划；更新需要 `expectedVersion` |
| POST `/task-schedules/preview` | 五次时间和对应上游期次 |
| GET `/task-schedules/{id}/triggers` | 分页实例与尝试记录 |
| POST `/task-triggers/{id}/rerun` | 重跑指定期次，返回触发记录 |
| POST `/runs/{id}/rerun` | 固定任务版本与输入重跑，返回新运行 |

统一运维前缀 `/api/v1/scheduling`：

| 接口 | 用途 |
|---|---|
| GET `/tasks` | 空间内独立任务与工作流、生效 R 和最新 R |
| GET `/instances`、`/instances/{id}` | 服务端分页筛选与实例详情、依赖、尝试历史 |
| PUT `/tasks/{scheduleId}/state` | `{kind,enabled,expectedVersion}` 暂停/恢复 |
| POST `/instances/{id}/rerun` | `{mode: LATEST或ORIGINAL,attemptRunId?}`；默认 LATEST |
| POST `/instances/{id}/stop` | 停止等待或运行中的实例 |
| POST `/backfills/preview`、`/backfills` | 范围预览及携带 `previewToken,requestId` 的幂等创建 |
| GET `/backfills/{batchKey}` | 实例及按状态聚合的进度 |

筛选支持 `workspaceId,kind,scheduleId,objectId,businessDateFrom,businessDateTo,status,source,batchKey,search,page,pageSize`。详情和实例操作也支持 `/instances/{kind}/{id}` 形式。

V10 仅扩展原触发表的日期、批次和运行列及查询索引，唯一键为 `(schedule_id,scheduled_at,batch_key)`，空批次是周期实例；任务、队列与执行尝试继续复用原表。旧发布包保持原 `schemaVersion`。

模型参考 [DataWorks 任务与实例](https://help.aliyun.com/zh/dataworks/user-guide/node-o-and-m)、[复杂调度依赖](https://help.aliyun.com/zh/dataworks/user-guide/principles-and-examples-for-scheduling-configuration-in-complex-dependency-scenarios) 和 [补数据](https://help.aliyun.com/zh/dataworks/user-guide/backfill-data-for-an-auto-triggered-node-and-view-data-backfill-instances-of-the-node/)。本项目保留本机单进程实现，只采用最近一期与全天两种匹配规则。

运行返回 `releaseKind=TASK`、`upstreamRuns`、`lineage`、业务日期、批次及提交状态。运行结果继续在编辑器结果面板查看。

测试：停止常规后端后运行 `./scripts/test-inventory.ps1 -Container 'your-business-mysql-container'`；前端运行 `npm.cmd test`、`npm.cmd run typecheck` 和 `npm.cmd run build`。测试使用随机业务库和工作空间，结束清理自身数据；普通 `mvn test` 未设置测试业务库管理员凭据会跳过库存集成类。

本轮隔离数据库、回归测试与浏览器流程记录见 [轻量调度验收](scheduling-verification.md)。
