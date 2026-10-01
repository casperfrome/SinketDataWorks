# Flink SQL 示例

`kafka-json.sql`、`kafka-csv.sql` 可直接通过 SQL Client 或 SQL Gateway 执行。先创建注释中要求的 Topic，再发送示例数据。每份文件使用独立会话；INSERT 是持续作业，结束后要取消作业。

```powershell
docker exec -it flink-rlt-sql-gateway /opt/flink/bin/sql-client.sh gateway --endpoint http://127.0.0.1:8083
```

进入客户端后逐条执行 SQL，或将 SQL 文件复制到容器后使用 `-f` 执行。容器中的客户端连接地址为容器内 `127.0.0.1:8083`；宿主机后端调用 SQL Gateway 地址为 `http://127.0.0.1:8083`。

`mysql-cdc-multi-sink.sql.template` 演示一个 CDC 源同时写入 MySQL JDBC、Doris、Upsert Kafka；`jdbc-append.sql.template` 演示没有主键的追加写入。替换所有 `{{...}}`，准备专属物理表和 Topic 后执行。密码可从验收生成的 `.runtime/flink/acceptance-local.json` 读取，渲染后的 SQL 只存放在 `.runtime/flink/`。

物理表必须与模板中的字段一致。MySQL CDC 源和 JDBC 主键目标是 `(id INT PRIMARY KEY, payload VARCHAR(255) NOT NULL)`；JDBC 追加目标不设置主键；Doris 目标使用 `UNIQUE KEY(id)`、`replication_num=1` 和 `enable_unique_key_merge_on_write=true`。CDC 的 server-id 范围和 Doris label-prefix 必须为这次作业独立生成。当前业务 MySQL 使用 UTC，模板据此配置时区；验收会读取实际服务器偏移，CDC 的 `server-time-zone` 必须与服务器一致。

完整真实验收执行：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' scripts/test-flink.py
```

验收自动创建独立表与 Topic，执行格式读写、CDC 快照和增改删、JDBC 追加和读取、Doris 两阶段提交、Upsert Kafka tombstone、Checkpoint 与 Savepoint 恢复，最后取消本次测试作业并关闭会话。它会重启 TaskManager，因此会拒绝在集群还有其他运行作业时开始。专属表和 Topic 保留用于复核，容器继续运行。

验收报告为 `.runtime/flink/acceptance-report.json`。`.runtime/flink/acceptance-<runID>.sql` 是包含凭据的分会话执行记录，并不是整份可直接重跑的脚本；恢复段还需要对应的 REST stop/savepoint 操作。恢复使用 Flink 2.2.1 的 `execution.state-recovery.path`，必须保持原 SQL 拓扑与连接器版本。
