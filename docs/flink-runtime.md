# Flink 2.2.1 本地实时运行环境

本环境包含 Flink、SQL Gateway、Kafka，以及 MySQL CDC、MySQL JDBC、Doris 和 Kafka 连接器。实时开发工作台通过后端向本集群提交真实作业；操作与存储说明见[实时开发](realtime-development.md)。

## 环境与连接地址

Docker Compose 项目名为 `sinket-realtime`，配置为 `infra/flink/compose.yaml`。三个 Flink 服务共用 `sinket-flink:2.2.1-connectors`，基础镜像固定为 `flink:2.2.1-scala_2.12-java17`。新 Kafka 使用 `apache/kafka:4.3.1`，单节点 KRaft，有独立的数据卷。

| 服务 | 容器 | 宿主机地址 | Docker 网络地址 |
| --- | --- | --- | --- |
| Flink JobManager / UI | `flink-rlt-jobmanager` | `http://127.0.0.1:8081` | `jobmanager:8081` |
| Flink TaskManager | `flink-rlt-taskmanager` | 无公开端口 | `taskmanager` |
| SQL Gateway REST | `flink-rlt-sql-gateway` | `http://127.0.0.1:8083` | `sql-gateway:8083` |
| 实时 Kafka | `kafka_rlt_4_3_1` | `localhost:19092` | `kafka_rlt_4_3_1:9092` |
| 已有业务 MySQL | `dataworks-demo-mysql` | `127.0.0.1:3307` | `mysql-rlt:3306` |
| 已有 Doris FE | `dunnelean-fe-1` | SQL `127.0.0.1:9030`；HTTP `127.0.0.1:8030` | SQL `fe:9030`；HTTP `fe:8030` |
| 已有 Doris BE | `dunnelean-be-1` | `127.0.0.1:8040` | `172.30.41.3:8040`（本机实际通告地址） |

Kafka 内外监听分别通告容器域名和 `localhost`。Flink SQL 中必须使用内部 Kafka 地址；宿主机客户端使用 `localhost:19092`。容器中填写 `localhost:19092` 会连接容器自身，不能作为 Flink 的 Kafka 地址。

启动脚本幂等创建 `sinket-realtime` 网络，并将已有 MySQL 加入该网络、设置 `mysql-rlt` 别名；保留 MySQL 已有网络。三个 Flink 服务同时加入已有 `dunnelean_doris` 网络，访问 FE 和 FE 返回的 BE 地址。该 Doris 网络与现有数据库需事先运行，脚本不重建数据库；其他已有 Doris 网络可通过环境变量 `DORIS_DOCKER_NETWORK` 指定。

默认 JobManager 进程内存 1 GiB，TaskManager 2 GiB、4 个 Slot；默认并行度 1，Checkpoint 间隔 60 秒、模式 `EXACTLY_ONCE`，失败固定重启 3 次、间隔 10 秒。容器内存上限分别为 JobManager 1536 MiB、TaskManager 2560 MiB、SQL Gateway 1536 MiB、Kafka 1536 MiB；Kafka JVM 堆为 512 MiB。

Checkpoint 和 Savepoint 分别写入 `file:///opt/flink/state/checkpoints`、`file:///opt/flink/state/savepoints`，三个 Flink 服务挂载同一个命名卷 `sinket-realtime_flink-state`。Kafka 数据使用 `sinket-realtime_kafka-data`。停止与再次启动保留两个卷。这个单机集群不提供 JobManager 高可用；Checkpoint / Savepoint 用于作业恢复。

## 连接器构建

| 能力 | 固定 Maven 制品 |
| --- | --- |
| Kafka / Upsert Kafka | `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2` |
| MySQL CDC | `org.apache.flink:flink-sql-connector-mysql-cdc:3.6.0-2.2` |
| MySQL JDBC | `org.apache.flink:flink-connector-jdbc-core:4.1.0-2.2`、`org.apache.flink:flink-connector-jdbc-mysql:4.1.0-2.2` |
| JDBC OpenLineage | `io.openlineage:openlineage-java:1.32.0`、`io.openlineage:openlineage-sql-java:1.32.0` |
| Doris | `org.apache.doris:flink-doris-connector-2.2:26.3.0` |
| MySQL 驱动 | `com.mysql:mysql-connector-j:26.7.0` |
| MySQL 驱动 Protobuf | `com.google.protobuf:protobuf-java:4.31.1` |

连接器来自正式 Maven 二进制制品。JDBC 的两个模块和完整 OpenLineage 运行依赖通过 Maven Shade 构建一个 bundle，合并 SPI；Jackson、Apache HTTP / Commons 等第三方包隔离到项目命名空间，保留 OpenLineage SQL JNI 类名和 Linux 原生资源。独立 MySQL 驱动供 JDBC 与 CDC 共用，避免重复驱动。

Doris 正式 fat JAR 内嵌的 SLF4J API 与 Flink 日志栈重复，构建脚本只对这部分日志类与相关元数据做去重，生成 `-cleaned.jar`。清单保留原始与最终 SHA256 和删除记录，可追溯这个正式二进制的重打包过程；不编译 Doris 连接器源码。Flink 本体、Scala 与日志依赖由基础镜像提供，JSON / CSV 直接复用基础镜像已有 JAR。SQL Gateway 的官方 JAR加入同一个镜像的 `/opt/flink/lib`。

构建会检查下载来源校验和、重复类、SPI、JNI 资源、驱动数量，以及不应进入 bundle 的 Flink / 日志依赖。生成的正式安装目录为 `.runtime/flink/connectors/`，其 `manifest.json` 包含固定版本、下载 URL、原始制品与安装 JAR 的 SHA256；镜像内副本为 `/opt/flink/connectors-manifest.json`。JAR 与构建日志均被 Git 忽略。

在项目根目录运行，要求 Docker、JDK 和 `mvn.cmd` 可用：

```powershell
# Python 构建固定使用 D:\PythonVenv\Scripts\python.exe。
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ./scripts/build-flink.ps1
# 仅构建并检查连接器，暂不构建 Docker 镜像：
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ./scripts/build-flink.ps1 -ConnectorsOnly
```

默认复用并重新核验本地下载缓存；需要重新下载时加 `-ForceDownload`。重复构建不会向 Docker 挂载宿主机 Maven 仓库或任意未列入清单的 JAR。

## 启动、检查与停止

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ./scripts/start-flink.ps1
& D:\PythonVenv\Scripts\python.exe ./scripts/check-flink.py
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ./scripts/stop-flink.ps1
```

启动前检查 Docker、Compose、基础和运行镜像、连接器清单与 SHA256，以及 8081、8083、19092 端口。重复启动允许本 Compose 项目的对应服务继续占用端口；发现无关容器或进程占用则报错，不停止端口原有服务。镜像清单与本地清单不一致时先重新构建。

启动使用 `docker compose up -d --wait`。随后健康检查确认 Flink / Gateway 版本为 2.2.1，一个 TaskManager 注册 4 个 Slot，所有 Flink 容器安装 JAR 的 SHA256 一致、JSON / CSV 存在，共享状态卷可写，并通过 Gateway 临时会话执行 `SELECT 1`。Kafka 内外监听通过官方 `kafka-broker-api-versions.sh` 验证；宿主机另发真实 Kafka ApiVersions / Metadata 请求，确认通告为 `localhost:19092`，并非只检查 TCP 端口。

健康报告为 `.runtime/flink/health-report.json`。`check-flink.py` 可用 `--timeout`、`--flink-url`、`--gateway-url`、`--manifest`、`--report` 覆盖检查参数。`start-flink.ps1 -WaitSeconds 180` 可调整启动等待时间。停止脚本仅 `compose stop`，保留容器、卷、网络、已有数据库及验收数据。

查看日志和作业：

```powershell
docker compose -p sinket-realtime -f ./infra/flink/compose.yaml logs --tail 100 jobmanager taskmanager sql-gateway kafka
Invoke-RestMethod http://127.0.0.1:8081/jobs/overview
```

## 真实链路验收

在独立环境已健康、没有其他活动 Flink 作业时执行：

```powershell
# 验收脚本需要现有 Python 环境中的 requests 与 pymysql。
& D:\PythonVenv\Scripts\python.exe ./scripts/test-flink.py
```

验收会创建专用数据库 `studio_realtime_test`、账号 `studio_rlt_test`，每次使用独立表名和 Kafka Topic。MySQL 管理密码默认从 `dataworks-demo-mysql` 的容器配置读取，也可设置 `FLINK_MYSQL_ADMIN_USER`、`FLINK_MYSQL_ADMIN_PASSWORD`；Doris 默认使用 root 的本地配置，可设置 `FLINK_DORIS_ADMIN_USER`、`FLINK_DORIS_ADMIN_PASSWORD`。凭据写入 `.runtime/flink/acceptance-local.json`，渲染后的真实 SQL（含测试凭据）保存在同目录，均不提交到 Git。报告会脱敏。

MySQL CDC 要求 `log_bin=ON`、`binlog_format=ROW`、`binlog_row_image=FULL`，验收检查这些变量并使用专用账号的复制与测试库权限。脚本读取服务器实际时区配置 CDC / JDBC；当前业务 MySQL 为 UTC，不修改数据库全局时区。MySQL 9.7.2 的兼容性以本机真实验收报告为准。Doris FE 会返回 BE 的 Stream Load 地址，必要时通过 `--doris-be` 指定实际容器可达的 HTTP 地址；脚本也支持 `--mysql-host`、`--mysql-port`、`--doris-host`、`--doris-port`、`--gateway-url`、`--flink-url` 与 `--timeout`。

验收覆盖 SQL Gateway 会话、DDL、执行计划、真实作业提交及这些链路：

- Kafka JSON / CSV 读写，Upsert Kafka 的更新、删除和源端物化。
- MySQL JDBC 追加写、读取、主键更新与删除。
- MySQL CDC 初始快照、binlog 增改删；同一流写入 JDBC、Doris Unique Key 表与 Upsert Kafka，核对真实数据。
- 产生成功 Checkpoint 后重启 TaskManager，确认作业恢复和后续数据继续处理。
- 生成 Savepoint，停止原作业，从相同 SQL / 算子结构恢复，并核对恢复后的增改删。

恢复验收会重启此集群的 TaskManager；发现非本次活动作业时拒绝开始。脚本结束时停止本次测试作业并关闭会话，容器继续运行，专用测试库 / Topic 保留用于复核。重复运行创建新的表与 Topic，不覆盖旧验收数据。最终机器可读报告为 `.runtime/flink/acceptance-report.json`；任何必需验收或清理失败都返回非零退出码并标记失败，不能将“容器启动”视为整体验收通过。

2026-10-01 本机实测：Flink 2.2.1 / Java 17、Kafka 4.3.1、MySQL 9.7.2、Doris 4.1.4，全部 9 项验收通过。TaskManager 从 Checkpoint 5 恢复，同 SQL 新作业从 Savepoint 恢复，恢复后继续插入和删除；JDBC、Doris 与 Upsert Kafka 回放的最终数据一致，删除键 2、3 的 tombstone 已验证。7 个测试作业均已终结，5 个会话已关闭。完整报告和每轮历史结果保存在本地 `.runtime/flink/`，不包含在 Git 提交中。

可运行 SQL 示例见 `infra/flink/examples/`，用实际测试库、Topic 和本地账号替换模板占位符后，通过 SQL Gateway 或镜像内 SQL Client 执行。渲染后的完整本次 SQL 见 `.runtime/flink/acceptance-*.sql`。

产品 API 验收使用 `scripts/test-realtime.py`，报告为 `.runtime/realtime/acceptance-report.json`。测试连接、发布、预览、真实多路写入及状态恢复；运行前须完成其他活动作业，恢复测试会重启专用 TaskManager。

版本与接口依据：[Flink 官方下载](https://flink.apache.org/downloads/)、[Flink 2.2 SQL Gateway REST](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql-gateway/rest/)、[Doris Flink 连接器](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/overview/)、[MySQL Connector/J 兼容说明](https://dev.mysql.com/doc/connector-j/en/connector-j-versions.html)。
