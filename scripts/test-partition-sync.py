"""Real partition sync and Doris SQL acceptance in a dedicated random test space.

Requires the local backend, Dunnelean, MySQL and Doris. Never inserts into test_ods.
Keeps its isolated fixture and writes a secret-free report under .runtime.
"""
import datetime as dt
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import secrets
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def load_script(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sync = load_script("sync_acceptance", "test-sync.py")
init = load_script("orders_initializer", "init-doris-orders.py")
call, execute, wait = sync.call, sync.execute, sync.wait


def main():
    key = uuid.uuid4().hex[:8]
    database, user, workspace = "partition_accept_" + key, "part_" + key, "partition-accept-" + key
    password = secrets.token_urlsafe(24)
    metadata, mysql, doris = sync.admins()
    report = dict(database=database, workspaceId=workspace, checks=[], runs={}, objects={})
    path = ROOT / ".runtime/partition-acceptance.json"

    def passed(label):
        report["checks"].append(label)
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("PASS " + label, flush=True)

    def node(name, engine, content="", config=None):
        value = call("POST", "/objects", dict(workspaceId=workspace, parentId=None,
            kind="NODE", nodeType=engine, name=name, content=content, config=config or {}), 201)
        report["objects"][name] = value["id"]
        return value

    def save(value, **patch):
        value["config"]["sync"].update(patch)
        return call("PUT", "/objects/" + value["id"], value)

    def run(value, label, success=True):
        result = wait(call("POST", "/runs", dict(objectId=value["id"],
            expectedVersion=value["version"], businessDate="2026-09-29"), expected=(200, 201, 202)))
        assert result["status"] == ("SUCCESS" if success else "FAILED"), result
        report["runs"][label] = result["id"]
        return result

    try:
        for connection in (mysql, doris):
            execute(connection, f"CREATE DATABASE `{database}`")
        execute(mysql, f"CREATE USER '{user}'@'%%' IDENTIFIED BY %s", (password,))
        execute(mysql, f"GRANT ALL ON `{database}`.* TO '{user}'@'%'")
        execute(doris, f"CREATE USER '{user}' IDENTIFIED BY %s", (password,))
        execute(doris, f"GRANT SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV ON `{database}`.* TO '{user}'")
        execute(metadata, "INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(%s,%s,%s,%s,'TEST')",
                (workspace, "分区同步验收 " + key, key, "本地"))
        columns = "id BIGINT NOT NULL,user_id BIGINT NOT NULL,total_amount DECIMAL(18,2) NOT NULL,status VARCHAR(20) NOT NULL,ordered_at DATETIME NOT NULL"
        execute(mysql, f"CREATE TABLE `{database}`.orders ({columns},PRIMARY KEY(id)) ENGINE=InnoDB")
        execute(mysql, f"CREATE TABLE `{database}`.returned_orders (ds DATE NOT NULL,{columns},PRIMARY KEY(id)) ENGINE=InnoDB")
        rows = [(1, 101, Decimal("10.12"), "PAID", dt.datetime(2026, 9, 28, 9, 10)),
                (2, 102, Decimal("20.23"), "PENDING", dt.datetime(2026, 9, 29, 10, 20)),
                (3, 103, Decimal("30.34"), "PAID", dt.datetime(2026, 9, 29, 11, 30))]
        with mysql.cursor() as cursor:
            cursor.executemany(f"INSERT INTO `{database}`.orders VALUES(%s,%s,%s,%s,%s)", rows)
        for table in ("auto_orders", "auto_routes"):
            execute(doris, f"CREATE TABLE `{database}`.`{table}` (ds DATE NOT NULL,{columns}) AUTO PARTITION BY RANGE(date_trunc(ds,'day')) () PROPERTIES('partition.retention_count'='400','replication_num'='1')")
        execute(doris, f"CREATE TABLE `{database}`.auto_multi (ds DATE NOT NULL,zone VARCHAR(10) NOT NULL,{columns}) AUTO PARTITION BY LIST(zone,ds) () PROPERTIES('replication_num'='1')")
        common = dict(workspaceId=workspace, host="127.0.0.1", database=database, username=user, password=password)
        ms = call("POST", "/datasources", dict(common, type="MYSQL", name="分区验收 MySQL", port=3307), 201)
        ds = call("POST", "/datasources", dict(common, type="DORIS", name="分区验收 Doris", port=9030, options=init.local_doris_options()), 201)
        report.update(mysqlId=ms["id"], dorisId=ds["id"])
        fields = ["id", "user_id", "total_amount", "status", "ordered_at"]
        forward = node("参数日期写入", "数据集成", config=dict(run=dict(provider="SYNC"),
            sync=dict(sourceDataSourceId=ms["id"], targetDataSourceId=ds["id"], sourceTable="orders",
                      targetTable="auto_orders", columns=fields,
                      mapping=[dict(source=c, target=c) for c in fields],
                      targetPartitionAssignments=[dict(target="ds", mode="value", value="${bizdate}")],
                      writeMode="append", batchRows=2, parallelism=1, timeoutSeconds=120),
            schedule=dict(parameters=[dict(name="bizdate", value="$bizdate", source="MANUAL")])))
        info = call("GET", f"/datasources/{ds['id']}/tables/auto_orders/sync-metadata")
        assert info["partition"]["automatic"] and info["partition"]["type"] == "RANGE", info
        assert info["partition"]["columns"][0]["name"] == "ds" and not info["partition"]["partitions"], info
        assert call("POST", "/sync/validate", forward)["valid"]
        assert not execute(doris, f"SHOW PARTITIONS FROM `{database}`.auto_orders")
        passed("Empty AUTO DATE table recognized; validation creates no data or partitions")
        first = run(forward, "fixed-date")
        assert first["writtenRows"] == 3 and first["committedBatches"] >= 2, first
        assert first["resolvedTargetPartitions"], first
        assert first["resolvedPartitionAssignments"] == [dict(target="ds", value="2026-09-29")], first
        assert execute(doris, f"SELECT DISTINCT ds FROM `{database}`.auto_orders")[0][0] == dt.date(2026, 9, 29)
        forward = save(forward, targetPartitionAssignments=[dict(target="ds", mode="value", value="2026-09-28")])
        run(forward, "other-date")
        forward = save(forward, writeMode="overwrite", where="id <= 2",
                       targetPartitionAssignments=[dict(target="ds", mode="value", value="${bizdate}")])
        result = run(forward, "partition-overwrite")
        assert execute(doris, f"SELECT ds,COUNT(*) FROM `{database}`.auto_orders GROUP BY ds ORDER BY ds") == ((dt.date(2026, 9, 28), 3), (dt.date(2026, 9, 29), 2))
        assert result["resolvedTargetPartitions"] and result["clearStatus"] == "CLEARED", result
        passed("Basic-date parameter normalization, multiple batches and overwrite preserves other partitions")
        partition_count = len(execute(doris, f"SHOW PARTITIONS FROM `{database}`.auto_orders"))
        empty = save(forward, where="id < 0", targetPartitionAssignments=[dict(target="ds", mode="value", value="2026-09-30")])
        empty_result = run(empty, "new-empty-date")
        assert empty_result["clearStatus"] == "SKIPPED_NO_PARTITION", empty_result
        assert len(execute(doris, f"SHOW PARTITIONS FROM `{database}`.auto_orders")) == partition_count
        routed = save(empty, targetTable="auto_routes", writeMode="append", where="",
                      targetPartitionAssignments=[dict(target="ds", mode="column", source="ordered_at")])
        run(routed, "field-date")
        assert execute(doris, f"SELECT ds,COUNT(*) FROM `{database}`.auto_routes GROUP BY ds ORDER BY ds") == ((dt.date(2026, 9, 28), 1), (dt.date(2026, 9, 29), 2))
        broken = save(routed, writeMode="overwrite")
        call("POST", "/sync/validate", broken, expected=400)
        call("POST", "/runs", dict(objectId=broken["id"], expectedVersion=broken["version"]), expected=400)
        assert execute(doris, f"SELECT COUNT(*) FROM `{database}`.auto_routes")[0][0] == 3
        passed("Missing AUTO partition skips clearing; date-field routing works and unbounded routed overwrite is rejected")
        # Validate on both the editor and execution paths before any truncate/write.
        invalid = broken
        for value in ("2026-02-30", "${missing_date}"):
            invalid = save(invalid, writeMode="overwrite", targetPartitionAssignments=[dict(target="ds", mode="value", value=value)])
            call("POST", "/sync/validate", invalid, expected=400)
            call("POST", "/runs", dict(objectId=invalid["id"], expectedVersion=invalid["version"]), expected=400)
            assert execute(doris, f"SELECT COUNT(*) FROM `{database}`.auto_routes")[0][0] == 3
        route_info = call("GET", f"/datasources/{ds['id']}/tables/auto_routes/sync-metadata")
        route_partition = next(p["name"] for p in route_info["partition"]["partitions"] if p.get("lower") == "2026-09-29")
        bounded = save(invalid, where="id = 2", targetPartitions=[route_partition],
                       targetPartitionAssignments=[dict(target="ds", mode="column", source="ordered_at")])
        bounded_run = run(bounded, "bounded-route-overwrite")
        assert bounded_run["resolvedTargetPartitions"] == [route_partition] and bounded_run["clearStatus"] == "CLEARED"
        assert execute(doris, f"SELECT ds,COUNT(*) FROM `{database}`.auto_routes GROUP BY ds ORDER BY ds") == ((dt.date(2026, 9, 28), 1), (dt.date(2026, 9, 29), 1))
        passed("Invalid date/missing parameters fail before writing; explicit physical partition bounds routed overwrite")
        info = call("GET", f"/datasources/{ds['id']}/tables/auto_orders/sync-metadata")
        selected = next(p["name"] for p in info["partition"]["partitions"] if p.get("lower") == "2026-09-29")
        reverse = node("分区及数据联合读取", "数据集成", config=dict(run=dict(provider="SYNC"),
            sync=dict(sourceDataSourceId=ds["id"], targetDataSourceId=ms["id"], sourceTable="auto_orders",
                      targetTable="returned_orders", sourcePartitionFilter=dict(partitions=[selected], where="ds = '${bizdate}'"),
                      where="id <= 2", writeMode="overwrite", batchRows=2, timeoutSeconds=120),
            schedule=dict(parameters=[dict(name="bizdate", value="2026-09-29", source="MANUAL")])))
        run(reverse, "partition-read")
        actual = execute(mysql, f"SELECT ds,id,user_id,total_amount,status,ordered_at FROM `{database}`.returned_orders ORDER BY id")
        assert actual == tuple((dt.date(2026, 9, 29),) + row for row in rows[:2]), actual
        passed("Doris physical partition + partition predicate + data filter roundtrip preserves orders exactly")
        # Partition expression order intentionally differs from the table's column order.
        multi = node("复合分区参数写入", "数据集成", config=dict(run=dict(provider="SYNC"),
            sync=dict(sourceDataSourceId=ms["id"], targetDataSourceId=ds["id"], sourceTable="orders",
                      targetTable="auto_multi", columns=fields, mapping=[dict(source=c, target=c) for c in fields],
                      targetPartitionAssignments=[dict(target="ds", mode="value", value="${bizdate}"), dict(target="zone", mode="value", value="${zone}")],
                      writeMode="append", batchRows=2, timeoutSeconds=120),
            schedule=dict(parameters=[dict(name="bizdate", value="$bizdate", source="MANUAL"), dict(name="zone", value="east", source="MANUAL")])))
        multi_info = call("GET", f"/datasources/{ds['id']}/tables/auto_multi/sync-metadata")
        assert [c["name"] for c in multi_info["partition"]["columns"]] == ["zone", "ds"]
        assert run(multi, "multi-date")["resolvedTargetPartitions"]
        multi = save(multi, targetPartitionAssignments=[dict(target="ds", mode="value", value="${bizdate}"), dict(target="zone", mode="value", value="west")])
        run(multi, "multi-other-value")
        multi = save(multi, writeMode="overwrite", where="id <= 2",
                     targetPartitionAssignments=[dict(target="ds", mode="value", value="${bizdate}"), dict(target="zone", mode="value", value="${zone}")])
        multi_run = run(multi, "multi-overwrite")
        assert multi_run["clearStatus"] == "CLEARED" and len(multi_run["resolvedTargetPartitions"]) == 1
        assert execute(doris, f"SELECT zone,COUNT(*) FROM `{database}`.auto_multi GROUP BY zone ORDER BY zone") == (("east", 2), ("west", 3))
        passed("Composite AUTO LIST uses expression key order; parameter routing and scoped overwrite preserve other tuples")
        sql = node("Doris真实SQL", "Doris", config=dict(run=dict(provider="DORIS", dataSourceId=ds["id"], timeoutSeconds=30)), content="SELECT COUNT(*) AS n FROM auto_orders; SHOW CREATE TABLE auto_orders; SHOW PARTITIONS FROM auto_orders; DESC auto_orders;")
        sql_run = run(sql, "doris-query")
        assert sql_run["provider"] == "DORIS" and not sql_run.get("simulation"), sql_run
        sql["content"] = "CREATE TABLE sql_auto(ds DATE NOT NULL,id BIGINT NOT NULL) AUTO PARTITION BY RANGE(date_trunc(ds,'day')) () PROPERTIES('partition.retention_count'='400','replication_num'='1'); INSERT INTO sql_auto VALUES('2026-09-29',1); SELECT * FROM sql_auto; ALTER TABLE sql_auto SET ('partition.retention_count'='399');"
        sql = call("PUT", "/objects/" + sql["id"], sql)
        run(sql, "doris-ddl-dml")
        physical = execute(doris, f"SHOW PARTITIONS FROM `{database}`.sql_auto")[0][1]
        sql["content"] = f"TRUNCATE TABLE sql_auto PARTITION (`{physical}`); DROP TABLE sql_auto;"
        sql = call("PUT", "/objects/" + sql["id"], sql)
        run(sql, "doris-partition-ddl")
        sql["content"] = "INSERT INTO auto_orders VALUES('2026-09-29',999,999,1,'PAID','2026-09-29 00:00:00'); SELECT * FROM dunnelean_test.target_orders;"
        sql = call("PUT", "/objects/" + sql["id"], sql)
        call("POST", "/runs", dict(objectId=sql["id"], expectedVersion=sql["version"]), expected=400)
        assert execute(doris, f"SELECT COUNT(*) FROM `{database}`.auto_orders WHERE id=999")[0][0] == 0
        passed("Real Doris SQL handles query/metadata/AUTO DDL/DML/partition DDL and rejects cross-schema scripts before writing")
        sql["content"] = "SELECT COUNT(*) AS n FROM auto_orders;"
        sql = call("PUT", "/objects/" + sql["id"], sql)
        release = call("POST", f"/tasks/{sql['id']}/releases", dict(expectedVersion=sql["version"]), expected=(200, 201))
        sql["content"] = "SELECT 999 AS changed_draft;"
        sql = call("PUT", "/objects/" + sql["id"], sql)
        published = wait(call("POST", f"/task-releases/{release['id']}/runs", dict(businessDate="2026-09-29"), expected=(200, 201, 202)))
        assert published["status"] == "SUCCESS" and published["provider"] == "DORIS", published
        assert call("GET", f"/runs/{published['id']}/results")["rows"] == [["5"]]
        schedule = dict(taskId=sql["id"], releaseId=release["id"], cron="0 * * * * *", cycle="minute", timezone="Asia/Shanghai", startDate="2026-01-01", endDate="2027-12-31", businessDateOffset=0, retries=0, retryIntervalSeconds=60, enabled=False, dependencies=[])
        plan = call("POST", f"/tasks/{sql['id']}/schedule", schedule, expected=(200, 201))
        assert len(call("POST", "/task-schedules/preview", schedule)) == 5
        report["scheduleId"] = plan["id"]
        report["scheduleEvidence"] = sync.scheduled_release(plan, schedule, workspace)
        scheduled = call("GET", "/runs/" + report["scheduleEvidence"]["runId"])
        assert scheduled["provider"] == "DORIS" and scheduled["releaseId"] == release["id"]
        assert call("GET", f"/runs/{scheduled['id']}/results")["rows"] == [["5"]]
        passed("Doris immutable task release, real minute schedule and disabled schedule cleanup")
        mysql_sql = node("MySQL来源检查", "MySQL", config=dict(run=dict(provider="MYSQL", dataSourceId=ms["id"], timeoutSeconds=30)), content="SELECT COUNT(*) FROM orders;")
        forward = save(bounded, targetTable="auto_orders", targetPartitions=[], writeMode="overwrite", where="id <= 2",
                       targetPartitionAssignments=[dict(target="ds", mode="value", value="${bizdate}")])
        objects = (mysql_sql, forward, sql)
        workflow = call("POST", "/objects", dict(workspaceId=workspace, parentId=None, kind="WORKFLOW", nodeType="工作流", name="MySQL同步Doris检查", content="", config=dict(run=dict(provider="WORKFLOW"), graph=dict(nodes=[dict(id=str(i), objectId=o["id"], label=o["name"], nodeType=o["nodeType"], x=80+i*240, y=120) for i,o in enumerate(objects)], edges=[dict(id="e1", source="0", target="1"), dict(id="e2", source="1", target="2")]))), 201)
        versions = {o["id"]: o["version"] for o in objects}
        result = wait(call("POST", "/runs", dict(objectId=workflow["id"], expectedVersion=workflow["version"], expectedNodeVersions=versions, businessDate="2026-09-29"), expected=(200, 201, 202)))
        assert result["status"] == "SUCCESS", result
        wf_release = call("POST", f"/workflows/{workflow['id']}/releases", dict(expectedVersion=workflow["version"], expectedNodeVersions=versions), expected=(200, 201))
        result = wait(call("POST", f"/workflow-releases/{wf_release['id']}/runs", dict(businessDate="2026-09-29"), expected=(200, 201, 202)))
        assert result["status"] == "SUCCESS", result
        passed("Mixed MySQL → partition sync → Doris SQL workflow and workflow release")
        sql["content"] = "SELECT SLEEP(5);"
        sql["config"]["run"]["timeoutSeconds"] = 1
        sql = call("PUT", "/objects/" + sql["id"], sql)
        timeout = run(sql, "doris-timeout", success=False)
        assert timeout["errorCode"] == "QUERY_TIMEOUT", timeout
        sql["content"] = "SELECT SLEEP(20);"
        sql["config"]["run"]["timeoutSeconds"] = 30
        sql = call("PUT", "/objects/" + sql["id"], sql)
        active = call("POST", "/runs", dict(objectId=sql["id"], expectedVersion=sql["version"]), expected=(200, 201, 202))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if call("GET", "/runs/" + active["id"])["status"] == "RUNNING":
                break
            time.sleep(.1)
        time.sleep(.5)
        call("POST", f"/runs/{active['id']}/stop", {})
        cancelled = wait(active)
        assert cancelled["status"] == "CANCELLED", cancelled
        passed("Doris SQL server timeout and cancellation")
        assert execute(doris, "SELECT COUNT(*) FROM test_ods.ods_orders_di")[0][0] == 0
        assert not execute(doris, "SHOW PARTITIONS FROM test_ods.ods_orders_di")
        passed("User example remains empty with zero physical partitions")
        print(json.dumps(dict(checks=len(report["checks"]), database=database, workspaceId=workspace), ensure_ascii=False), flush=True)
    finally:
        for connection in (metadata, mysql, doris):
            connection.close()


if __name__ == "__main__":
    main()
