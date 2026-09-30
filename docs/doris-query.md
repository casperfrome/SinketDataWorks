# Doris SQL 真实执行

新建 Doris 节点，选择 Doris schema，保存后运行。MySQL/Doris 节点分别只绑定匹配类型的连接。schema 选项来自当前工作空间已注册连接的数据库名，旁边显示类型和连接名称以区分同名数据库；需要访问另一数据库时，先注册对应连接。

```json
{"run":{"provider":"DORIS","dataSourceId":"Doris数据源ID","timeoutSeconds":30}}
```

执行使用共享 JDBC SQL 通道，支持多语句预检、调度参数、结果分页、停止、逐语句提交状态和恢复。会话固定北京时间、节点查询/写入超时，并设置 `group_commit=off_mode`，成功响应用于记录该语句的提交状态。写入任务不自动重试；发布、调度与工作流可组合 MySQL、Doris、数据集成节点。仅执行 Doris SQL 时需要 Doris，无需启动 Dunnelean 服务。

支持 `SELECT/WITH`、本库 `INSERT/UPDATE/DELETE`、`SHOW CREATE TABLE`、`SHOW PARTITIONS`、`DESC/DESCRIBE`，以及本库 OLAP `CREATE/ALTER/DROP/TRUNCATE TABLE`，包括 AUTO 建表、常规字段/属性修改和增加/删除/清空分区。

整个脚本执行前完成语法及业务库边界校验。表名使用 `表名` 或 `当前schema.表名`；跨库引用、`catalog.schema.表名`、外部表定义、外部或系统表函数、账号权限 SQL、任意 LOAD、事务和用户会话 SET 会被拒绝。支持范围为明确识别的 Doris 语法子集，未识别的形式会在运行前报错。SHOW/DESC 显示查询结果；DDL 显示逐语句状态。异步 schema change 成功表示服务器已接受操作，可再次查询元数据确认进度。

```sql
SHOW CREATE TABLE ods_orders_di;
SHOW PARTITIONS FROM ods_orders_di;
DESC ods_orders_di;
SELECT ds, COUNT(*) AS order_count
FROM ods_orders_di
GROUP BY ds
ORDER BY ds;
```

初始化的 `test_ods.ods_orders_di` 为 DATE `ds` 每日 AUTO RANGE 空表，初始数据行数和物理分区数均为 0，`SHOW PARTITIONS` 返回空结果；分区在首次非空写入时创建。`partition.retention_count=400` 按历史分区数量保留，当前及未来分区另行保留。准备方法见[订单分区示例](offline-sync.md#分区读写与订单示例)。

每条 SQL 独立提交，后续失败不撤回已成功语句。停止、超时或断连期间的写入可能显示提交未知，不能解释为已回滚。API 复用原 SQL 运行、结果、发布和调度接口，provider 为 `DORIS`。

真实验收在独立 `partition_accept_*` 库运行，覆盖 AUTO DDL、DML、分区操作、查询/SHOW/DESC、停止、超时、跨库拒绝、发布和混合工作流，不向用户订单表放入测试数据。
