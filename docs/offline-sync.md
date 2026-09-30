# MySQL ↔ Doris 离线批量同步

工作台通过 Dunnelean Java SDK 0.1.0 控制独立 Rust 服务。基础工作台构建需要 SDK，只有使用离线同步时才需要启动 Rust 服务和 Doris。Java 保存配置、发布版本、调度与状态；Dunnelean 通过 Arrow 批次直接连接数据库，业务数据不经过浏览器或 Java 结果预览。同步服务独立部署，工作台通过 SDK 的 HTTP 控制接口管理作业。

## Java SDK 依赖

后端依赖 `io.github.casperfrome:dunnelean-java-sdk:0.1.0`。SDK 要求 Java 17+，项目使用的 Java 25 可直接运行。该版本尚未发布到 Maven Central，每台开发或构建机器都需先安装到自己的本机 Maven 仓库。

SDK 源码固定为官方提交 [`7053dc9da70aa0d5f9874017ce2162a7f010522d`](https://github.com/casperfrome/Dunnelean/tree/7053dc9da70aa0d5f9874017ce2162a7f010522d)，完整说明见[官方 Java SDK 文档](https://github.com/casperfrome/Dunnelean/blob/7053dc9da70aa0d5f9874017ce2162a7f010522d/docs/java-sdk.md)。在工作台项目根目录执行以下命令，使用相邻目录的独立 checkout：

```powershell
git clone https://github.com/casperfrome/Dunnelean.git ../Dunnelean
git -C ../Dunnelean checkout --detach 7053dc9da70aa0d5f9874017ce2162a7f010522d
mvn.cmd -B -ntp -f ../Dunnelean/sdk/java/pom.xml install
```

已有对应版本 SDK 时可跳过安装。SDK 封装 HTTP 控制接口，安装 JAR 后仍需另行启动 Rust 服务；环境变量凭据和证书路径由 Rust 服务进程读取。

## 启动与连接

先准备自己的 MySQL、Doris 和 Dunnelean，再按主 README 启动后端与前端。以下 Rust 构建命令从工作台项目根目录执行；需另外安装 Rust 工具链，完整引擎配置参见 [Dunnelean README](https://github.com/casperfrome/Dunnelean/blob/7053dc9da70aa0d5f9874017ce2162a7f010522d/README.md)。不要重复启动占用同一端口或状态库的进程。

```powershell
Set-Location ../Dunnelean
cargo build --release --locked
.\target\release\dunnelean.exe serve --config dunnelean.toml
```

后端 `studio.sync.service-url` 默认 `http://127.0.0.1:9876`，可用环境变量 `DUNNELEAN_URL` 覆盖，仅允许本机 HTTP 地址。`GET /healthz` 必须包含 `state_store_id`；旧版本引擎需要一起升级。Dunnelean 的 `var/dunnelean.sqlite` 必须持久保留，与工作台元数据库一起备份，不能在未完成任务期间更换。

Doris 端口取决于自己的部署，以下是常见的本地映射示例：

| 角色 | 服务 | 示例宿主机端口 |
| --- | --- | --- |
| FE | SQL、HTTP、Flight | `9030`、`8030`、`8070` |
| BE | HTTP、Flight | `8040`、`8050` |

在“数据源”新增 MySQL 和 Doris 连接，使用有相应业务库读写权限的专用账号。密码加密保存在工作台元数据库，发布快照和运行响应不包含明文密码。Doris 的 SQL 地址用于元数据，额外端点用于传输；本地连接示例如下（端口需与实际部署一致）：

```json
{
  "feHttpUrls": ["http://127.0.0.1:8030"],
  "beHttpUrls": ["http://127.0.0.1:8040"],
  "flightUri": "grpc://127.0.0.1:8070",
  "httpEndpointMap": {},
  "flightEndpointMap": {}
}
```

若 Doris 返回浏览器或引擎无法访问的容器内部端点，需在 `httpEndpointMap` / `flightEndpointMap` 中，将实际返回的端点映射到引擎可访问的宿主机地址；也可配置每个可直达 BE 的地址。不要直接照抄其他环境的容器 IP。数据源类型保存后不能切换，请为另一种数据库新增连接。MySQL SQL 节点只能选择 MySQL 数据源。

## 配置与执行

1. 新建“离线同步”节点；新建“数据集成”节点也使用真实同步编辑器。旧模拟节点保留原有执行方式。
2. 选择来源、目标数据源及已有表，方向可以是 MySQL→Doris 或 Doris→MySQL。
3. 选择读取字段并配置映射，或留空使用同名字段。预检会检查真实字段、类型、键和表模型。
4. 选择写入方式、批量大小、并行度和超时，点击“校验连接与字段映射”，保存后运行。
5. 可独立发布任务并应用调度，也可加入真实 MySQL/同步混合工作流，按依赖运行、发布和调度。

| 写入方式 | 语义 |
| --- | --- |
| 追加 | 向 MySQL InnoDB 或 Doris Duplicate Key 表插入；不会删除已有行 |
| 主键更新 | MySQL 需选择匹配的主键/唯一索引；Doris 需 Unique Key Merge-on-Write 表 |
| 清空后覆盖 | 全部预检通过后执行 TRUNCATE，再按目标表模型分批写入；空来源也清空目标 |

默认每批最多 10,000 行、16 MiB，写入并行度 1，超时 3,600 秒。实际批次可能因 Flight 分片或字节上限小于所设行数。目标表必须预先存在；首版不自动建表，不支持 CDC、增量位点、跨库事务、冲突合并或整表原子替换。

“双向”表示两个方向都能配置独立批量任务。它不会自动持续互相回写。按批次提交，停止或失败不会撤销 TRUNCATE 和已提交批次；覆盖中途失败时目标可能只剩部分数据。系统不会自动重试整次写入任务，需核实后由用户主动重跑。

## 筛选条件与参数

填写 WHERE 条件本身，留空读取全表，例如：

```sql
business_date = '${day}' AND id > 0
```

在右侧“调度配置”定义 `day`，可使用项目现有的日期表达式及参数预览。发布版本固定条件、字段映射和参数定义。MySQL 通过预编译参数绑定；Doris 通过 UTF-8 Base64 常量传入参数值，不拼接未经处理的文本。沿用 `:bizdate`、`:source_cutoff`、`:build_id` 与已配置上游批次参数；`:source_cutoff` 按原 JDBC 约定转换为 UTC 微秒 DATETIME。连接会话时区固定为 `+08:00`。

只提供表/字段/条件配置，不提供自定义 SELECT 或任意前后置 SQL。发布包固定数据源的类型、连接地址、库、账号及端点；连接身份变化后需重新发布。密码轮换不要求重新发布。

## 停止与恢复

同一个物理目标（数据库类型、解析后的主机、端口、库、表）只有一个活动同步写入；其他任务等待。占用存于元数据库，不随 Java 进程退出而丢失。这个锁只协调经本工作台提交的同步任务，不能阻止数据库外部写入。

工作台运行 ID 同时作为 Dunnelean 的 `request_id`。提交响应丢失时查询同一 ID，必要时使用原始配置再次请求，不生成第二次写入。停止调用按 request ID 的取消接口；即使取消先于提交到达，持久取消记录也会拒绝迟到提交。

Java 重启时，已提交的任务进入核实状态并请求停止远端，未提交的任务终止。含同步节点的工作流终止剩余下游，不自动恢复 DAG。只有确认远端终态且没有未知提交，才释放目标占用。

以下情况保持 `RECOVERING`：远端失联、引擎状态库身份改变、请求记录丢失、批次提交结果未知。运行详情提供批次凭据及“人工核实结果”：检查远端和数据库在途操作已停止，核对目标数据后填写说明，将运行结束为失败并解除占用。该操作不会重放数据；远端明确仍在运行时拒绝解除。状态库不可达时，用户的人工核实是最终依据。

## API 与持久化

- 数据源继续使用 `/api/v1/datasources`，新增 `type`、`options`。
- `GET /api/v1/datasources/{id}/tables/{table}/sync-metadata` 返回字段、目标模型和 MySQL 唯一键。
- `POST /api/v1/sync/validate` 接收完整节点草稿；只预检，不执行 TRUNCATE 或写入。
- 运行、停止、发布和调度复用现有任务/工作流接口。
- `GET /api/v1/runs/{id}/sync/batches` 查看批次凭据。
- `POST /api/v1/runs/{id}/sync/resolve` 接收 `{"note":"核实说明"}`，保留审计记录。

节点使用 `config.run.provider=SYNC` 和 `config.sync`。V7 增加数据源类型/端点、`dw_sync_execution` 控制记录和 `dw_sync_target_lock` 目标占用。升级前备份元数据库；V7 不清空已有开发对象。

## 验证

在 `backend` 目录运行单元测试和 SDK HTTP 契约检查：

```powershell
mvn.cmd -B -ntp "-Dtest=*Test,!*IntegrationTest" test
```

真实同步验收脚本位于 `scripts/test-sync.py`、`scripts/test-sync-faults.py` 和 `scripts/test-sync-finalize.py`。它们创建独立 `sync_accept_*` 测试库，验证多个批次、类型精度、字段映射、写入方式、发布、调度、取消及恢复；报告和当前夹具身份保存到被忽略的 `.runtime`。

这些脚本用于专用本地验收环境，默认元数据库端口 `3306`、业务 MySQL 端口 `3307`（容器名 `dataworks-demo-mysql`）、Doris SQL 端口 `9030`，并从本地配置及选定容器读取凭据。需安装 `pymysql`、`requests`、`psutil`，连接自己的验收实例时先核对并调整脚本中的环境设置。使用专用 Python 环境执行脚本，不要对日常业务库运行。

故障脚本会重启 `.runtime/sync-backend.pid` 指向的验收后端，要求进程运行名含 `data-studio-sync` 的 JAR。验收前应构建当前后端，将 `backend/target/data-studio-0.0.1-SNAPSHOT.jar` 复制到 `.runtime/data-studio-sync.jar`，从 `backend` 目录启动专用验收后端并记录正确 PID；不要填写日常后端或其他进程的 PID。Dunnelean 引擎应已运行并保持原状态库。

脚本默认保留自己的示例供查看，确认不再需要后，可给 `test-sync.py` 添加 `--cleanup`，只清理 `.runtime/sync-live-fixture.json` 所指向的当前夹具。单元测试通过不代表真实数据库同步验收通过。
