"""Create the empty orders ODS table and an unpublished sync example.

Run with D:\\PythonVenv\\Scripts\\python.exe. Credentials are stored by the
backend's encrypted datasource API and are never printed or written by this script.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess

import pymysql
import requests

ROOT = Path(__file__).resolve().parents[1]
DATABASE = "test_ods"
TABLE = "ods_orders_di"
USER = "test_ods_etl"


def api(base, method, path, body=None):
    response = requests.request(method, base.rstrip("/") + "/api/v1" + path,
                                json=body, timeout=90)
    if not response.ok:
        # Failed requests may contain submitted credentials; do not print bodies.
        error = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        raise RuntimeError(f"{method} {path}: HTTP {response.status_code} {error.get('code', '')}")
    return response.json() if response.content else None


def local_doris_options():
    options = dict(feHttpUrls=["http://127.0.0.1:8030"],
                   beHttpUrls=["http://127.0.0.1:8040"],
                   flightUri="grpc://127.0.0.1:8070",
                   flightEndpointMap={}, httpEndpointMap={})
    for name, sql_port in (("dunnelean-fe-1", 8070), ("dunnelean-be-1", 8050)):
        try:
            container = json.loads(subprocess.check_output(["docker", "inspect", name], encoding="utf-8"))[0]
            for network in container["NetworkSettings"]["Networks"].values():
                address = network.get("IPAddress")
                if address:
                    options["flightEndpointMap"][f"grpc+tcp://{address}:{sql_port}"] = f"grpc://127.0.0.1:{sql_port}"
                    if sql_port == 8050:
                        options["httpEndpointMap"][f"http://{address}:8040"] = "http://127.0.0.1:8040"
        except (subprocess.CalledProcessError, KeyError, IndexError):
            pass
    return options


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--workspace-id", default="local-workspace")
    args = parser.parse_args()
    sources = api(args.base_url, "GET", f"/datasources?workspaceId={args.workspace_id}")
    mysql = next((s for s in sources if s["type"] == "MYSQL" and s["database"] == "studio_demo"), None)
    if mysql is None:
        raise RuntimeError("Register the studio_demo MySQL business datasource first")
    doris = next((s for s in sources if s["type"] == "DORIS" and s["database"] == DATABASE), None)
    if doris:
        api(args.base_url, "POST", f"/datasources/{doris['id']}/test", {})
    password = None if doris else os.environ.get("TEST_ODS_PASSWORD", secrets.token_urlsafe(32))
    with pymysql.connect(host="127.0.0.1", port=9030,
                         user=os.environ.get("DORIS_ADMIN_USER", "root"),
                         password=os.environ.get("DORIS_ADMIN_PASSWORD", ""),
                         charset="utf8mb4", autocommit=True) as connection:
        with connection.cursor() as cursor:
            for statement in (ROOT / "scripts/doris-orders-schema.sql").read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    cursor.execute(statement)
            cursor.execute(f"SELECT COUNT(*) FROM `{DATABASE}`.`{TABLE}`")
            if cursor.fetchone()[0] != 0:
                raise RuntimeError("The example table contains data; nothing was cleared")
            cursor.execute(f"SHOW PARTITIONS FROM `{DATABASE}`.`{TABLE}`")
            if cursor.fetchall():
                raise RuntimeError("The example table already has partitions; nothing was cleared")
            cursor.execute(f"SHOW CREATE TABLE `{DATABASE}`.`{TABLE}`")
            ddl = cursor.fetchone()[1]
            if "AUTO PARTITION BY RANGE" not in ddl.upper() or '"partition.retention_count" = "400"' not in ddl:
                raise RuntimeError("Existing table definition differs; nothing was replaced")
            if not doris:
                cursor.execute("CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s", (USER, password))
                cursor.execute(f"GRANT SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV ON `{DATABASE}`.* TO %s", (USER,))
    if not doris:
        doris = api(args.base_url, "POST", "/datasources", dict(
            workspaceId=args.workspace_id, name=DATABASE, type="DORIS", host="127.0.0.1",
            port=9030, database=DATABASE, username=USER, password=password,
            options=local_doris_options()))
        api(args.base_url, "POST", f"/datasources/{doris['id']}/test", {})
    objects = api(args.base_url, "GET", f"/objects?workspaceId={args.workspace_id}&deleted=false")
    name = "orders_to_ods_orders_di"
    existing = next((o for o in objects if o["name"] == name and o["parentId"] is None), None)
    if existing is None:
        columns = ["id", "user_id", "total_amount", "status", "ordered_at"]
        existing = api(args.base_url, "POST", "/objects", dict(
            workspaceId=args.workspace_id, parentId=None, kind="NODE", nodeType="数据集成",
            name=name, description="订单业务库按业务日期同步至 Doris AUTO 日分区表；示例尚未运行。",
            content="", owner="developer", tags=["订单", "分区同步"], config=dict(
                run=dict(provider="SYNC"),
                sync=dict(sourceDataSourceId=mysql["id"], targetDataSourceId=doris["id"],
                          sourceTable="orders", targetTable=TABLE, columns=columns,
                          mapping=[dict(source=column, target=column) for column in columns],
                          targetPartitionAssignments=[dict(target="ds", mode="value", value="${bizdate}")],
                          writeMode="append", batchRows=10000, parallelism=1, timeoutSeconds=3600),
                schedule=dict(parameters=[dict(name="bizdate", value="$bizdate", source="MANUAL")]))))
    print(json.dumps(dict(database=DATABASE, table=TABLE, rows=0, partitions=0,
                          datasourceId=doris["id"], nodeId=existing["id"],
                          nodeName=existing["name"]), ensure_ascii=False))


if __name__ == "__main__":
    main()
