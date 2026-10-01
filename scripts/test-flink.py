"""Real Flink connector and state recovery acceptance.

Run with D:\\PythonVenv\\Scripts\\python.exe scripts/test-flink.py.
Creates only run-specific tables/topics in studio_realtime_test. Credentials,
rendered SQL, and the machine-readable report stay in Git-ignored .runtime/flink.
All jobs submitted by this invocation are canceled and its sessions are closed.
Containers and fixture data remain available for inspection after acceptance.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
import uuid

import pymysql
import requests


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".runtime" / "flink"
LOCAL = RUNTIME / "acceptance-local.json"
REPORT = RUNTIME / "acceptance-report.json"
DATABASE = "studio_realtime_test"
USER = "studio_rlt_test"
TERMINAL = {"FINISHED", "CANCELED", "FAILED", "SUSPENDED"}


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def execute(connection, statement, params=None):
    with connection.cursor() as cursor:
        cursor.execute(statement, params)
        return cursor.fetchall()


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.run_id = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:6]
        self.prefix = "rlt_" + self.run_id
        self.http = requests.Session()
        self.sessions = []
        self.jobs = []
        self.connections = []
        self.secrets = []
        self.report = dict(runId=self.run_id, startedAt=self.now(), status="RUNNING", checks=[], jobs=[], fixtures={}, endpoints=dict(gateway=args.gateway_url, flink=args.flink_url, kafka="localhost:19092"))
        RUNTIME.mkdir(parents=True, exist_ok=True)
        self.sql_path = RUNTIME / ("acceptance-" + self.run_id + ".sql")
        self.sql_path.write_text("-- Local acceptance statement trace; contains credentials. Do not commit.\n-- Each session is independent; restore also requires the REST stop/savepoint operation.\n", encoding="utf-8")

    @staticmethod
    def now():
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat()

    def redact(self, value):
        text = str(value)
        for secret in sorted((x for x in self.secrets if x), key=len, reverse=True):
            text = text.replace(secret, "<redacted>")
            text = text.replace(sql_literal(secret), "'<redacted>'")
        return re.sub(r"(?i)('password'\s*=\s*)'[^']*'", r"\1'<redacted>'", text)

    def progress(self, message):
        print(self.redact(message), flush=True)

    def save_report(self):
        # Sanitize the entire serialization; diagnostic exceptions can contain DDL.
        content = self.redact(json.dumps(self.report, ensure_ascii=False, indent=2))
        REPORT.write_text(content, encoding="utf-8")
        REPORT.with_name("acceptance-report-" + self.run_id + ".json").write_text(content, encoding="utf-8")

    def check(self, name, function):
        record = dict(name=name, status="RUNNING", startedAt=self.now())
        self.report["checks"].append(record)
        self.save_report()
        self.progress("[RUN] " + name)
        started = time.monotonic()
        try:
            result = function()
            record.update(status="PASSED", evidence=result or {})
            self.progress("[PASS] " + name)
            return result
        except Exception as error:
            record.update(status="FAILED", error=self.redact(error))
            self.progress("[FAIL] " + name + ": " + self.redact(error))
            raise
        finally:
            record.update(finishedAt=self.now(), seconds=round(time.monotonic() - started, 2))
            self.save_report()

    def request(self, base, method, path, body=None, expected=(200, 201, 202, 204)):
        response = self.http.request(method, base.rstrip("/") + path, json=body, timeout=45)
        if response.status_code not in expected:
            raise RuntimeError(self.redact(f"{method} {path}: HTTP {response.status_code}: {response.text[:12000]}"))
        return response.json() if response.content else {}

    def gateway(self, method, path, body=None):
        return self.request(self.args.gateway_url, method, path, body)

    def rest(self, method, path, body=None):
        return self.request(self.args.flink_url, method, path, body)

    def docker(self, *command, input_text=None, timeout=60, allow_timeout=False):
        result = subprocess.run(["docker", *command], input=input_text, text=True, encoding="utf-8", errors="replace", capture_output=True, timeout=timeout)
        if result.returncode and not (allow_timeout and "TimeoutException" in result.stderr):
            raise RuntimeError(self.redact(f"Docker {command[:3]} failed: {result.stderr[-12000:]} {result.stdout[-12000:]}"))
        return result.stdout

    def wait(self, predicate, description, timeout=None):
        deadline = time.monotonic() + (timeout or self.args.timeout)
        last = None
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            last = result
            time.sleep(1)
        raise AssertionError(f"Timed out waiting for {description}; last={last}")

    def open_session(self, extra=None):
        properties = {
            "execution.target": "remote",
            "rest.address": "jobmanager",
            "rest.port": "8081",
            "parallelism.default": "1",
            "execution.runtime-mode": "STREAMING",
            "execution.checkpointing.interval": "5 s",
            "execution.checkpointing.timeout": "90 s",
            "execution.checkpointing.dir": "file:///opt/flink/state/checkpoints",
            "execution.checkpointing.savepoint-dir": "file:///opt/flink/state/savepoints",
        }
        properties.update(extra or {})
        session = self.gateway("POST", "/v1/sessions", dict(sessionName=self.prefix, properties=properties))["sessionHandle"]
        self.sessions.append(session)
        with self.sql_path.open("a", encoding="utf-8") as output:
            output.write("\n-- NEW SQL GATEWAY SESSION: " + session + "\n")
        return session

    def statement(self, session, statement, name="SQL", expect_job=False):
        with self.sql_path.open("a", encoding="utf-8") as output:
            output.write("\n-- " + name + "\n" + statement.rstrip(";\n") + ";\n")
        operation = self.gateway("POST", f"/v1/sessions/{session}/statements", dict(statement=statement, executionTimeout=0))["operationHandle"]
        path = f"/v1/sessions/{session}/operations/{operation}"
        deadline = time.monotonic() + self.args.timeout
        pages = []
        token_path = path + "/result/0?rowFormat=JSON"
        while time.monotonic() < deadline:
            status = self.gateway("GET", path + "/status")["status"]
            if status in {"ERROR", "CANCELED", "CLOSED"}:
                # The result endpoint supplies the useful exception when status=ERROR.
                self.gateway("GET", token_path)
                raise RuntimeError(name + " operation status: " + status)
            if status in {"FINISHED", "RUNNING"}:
                result = self.gateway("GET", token_path)
                if result.get("resultType") == "NOT_READY":
                    time.sleep(0.5)
                    continue
                pages.append(result)
                job = result.get("jobID") or result.get("jobId")
                if expect_job and not job:
                    # INSERT result also contains the job ID as a scalar result row.
                    candidate = re.search(r'"([a-f0-9]{32})"', json.dumps(result))
                    job = candidate.group(1) if candidate else None
                if job:
                    if job not in self.jobs:
                        self.jobs.append(job)
                        self.report["jobs"].append(dict(id=job, name=name))
                    return job if expect_job else pages
                if result.get("resultType") == "EOS" or not result.get("nextResultUri"):
                    if expect_job:
                        raise AssertionError(name + " returned no Flink job ID")
                    return pages
                token_path = result["nextResultUri"]
                if not token_path.startswith("/v1/"):
                    token_path = "/v1" + token_path
            time.sleep(0.5)
        raise AssertionError(name + " SQL Gateway result timed out")

    def job_state(self, job):
        result = self.rest("GET", "/jobs/" + job)
        state = result["state"]
        if state == "FAILED":
            details = self.rest("GET", f"/jobs/{job}/exceptions")
            entries = details.get("exceptionHistory", {}).get("entries", [])
            trace = details.get("root-exception") or (entries[0].get("stacktrace", "") if entries else json.dumps(details))
            causes = [line.strip() for line in trace.splitlines() if line.strip().startswith("Caused by:")]
            message = " | ".join(causes[-3:]) if causes else trace[:1200]
            raise AssertionError(self.redact(f"Job {job} failed: {message}"))
        return state

    def wait_job(self, job, state="RUNNING"):
        return self.wait(lambda: self.job_state(job) == state, f"job {job} {state}")

    def checkpoints(self, job):
        self.job_state(job)
        return self.rest("GET", f"/jobs/{job}/checkpoints")

    def wait_checkpoint(self, job):
        return self.wait(lambda: self.checkpoints(job).get("latest", {}).get("completed"), "completed checkpoint")

    def cancel(self, job):
        state = self.rest("GET", "/jobs/" + job)["state"]
        if state not in TERMINAL:
            self.rest("PATCH", f"/jobs/{job}?mode=cancel")
            self.wait(lambda: self.rest("GET", "/jobs/" + job)["state"] in TERMINAL, "job cancellation", timeout=90)

    def create_topic(self, topic):
        self.docker("exec", self.args.kafka_container, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1", "--config", "cleanup.policy=delete")
        self.report["fixtures"].setdefault("kafkaTopics", []).append(topic)

    def produce(self, topic, values):
        self.docker("exec", "-i", self.args.kafka_container, "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic, input_text="\n".join(values) + "\n")

    def consume(self, topic, count=None, keyed=False):
        command = ["exec", self.args.kafka_container, "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic, "--from-beginning", "--timeout-ms", "5000"]
        if count:
            command.extend(["--max-messages", str(count)])
        if keyed:
            command.extend(["--formatter-property", "print.key=true", "--formatter-property", "key.separator=\t", "--formatter-property", "null.literal=<tombstone>"])
        return self.docker(*command, timeout=30, allow_timeout=True).splitlines()

    def preflight(self):
        overview = self.rest("GET", "/overview")
        assert overview.get("flink-version") == "2.2.1", "Acceptance requires Flink 2.2.1"
        assert overview.get("taskmanagers", 0) >= 1 and overview.get("slots-total", 0) >= 4, "TaskManager/slots not ready"
        info = self.gateway("GET", "/v1/info")
        assert info.get("version") == "2.2.1", "Gateway and cluster versions differ"
        active = self.rest("GET", "/jobs/overview").get("jobs", [])
        external = [job for job in active if job.get("state") not in TERMINAL]
        assert not external, "Recovery test restarts TaskManager; finish other running jobs first"
        config = self.rest("GET", "/jobmanager/config")
        config_values = {item["key"]: item["value"] for item in config}
        self.report["cluster"] = dict(version=overview.get("flink-version"), gatewayVersion=info.get("version"), taskmanagers=overview["taskmanagers"], slots=overview["slots-total"], checkpointInterval=config_values.get("execution.checkpointing.interval"))
        return self.report["cluster"]

    def fixtures(self):
        local = json.loads(LOCAL.read_text(encoding="utf-8")) if LOCAL.exists() else dict(database=DATABASE, username=USER, mysqlPassword=secrets.token_urlsafe(28), dorisPassword=secrets.token_urlsafe(28))
        assert local.get("database") == DATABASE and local.get("username") == USER, "Unexpected local acceptance configuration"
        LOCAL.write_text(json.dumps(local, ensure_ascii=False, indent=2), encoding="utf-8")
        self.local = local
        self.secrets.extend([local["mysqlPassword"], local["dorisPassword"]])
        mysql_password = os.environ.get("FLINK_MYSQL_ADMIN_PASSWORD")
        if mysql_password is None:
            inspect = json.loads(self.docker("inspect", self.args.mysql_container))[0]
            env = dict(item.split("=", 1) for item in inspect["Config"]["Env"] if "=" in item)
            mysql_password = env.get("MYSQL_ROOT_PASSWORD", "")
        doris_password = os.environ.get("FLINK_DORIS_ADMIN_PASSWORD", os.environ.get("DORIS_ADMIN_PASSWORD", ""))
        self.secrets.extend([mysql_password, doris_password])
        self.mysql = pymysql.connect(host=self.args.mysql_host, port=self.args.mysql_port, user=os.environ.get("FLINK_MYSQL_ADMIN_USER", "root"), password=mysql_password, charset="utf8mb4", autocommit=True)
        self.connections.append(self.mysql)
        self.doris = pymysql.connect(host=self.args.doris_host, port=self.args.doris_port, user=os.environ.get("FLINK_DORIS_ADMIN_USER", os.environ.get("DORIS_ADMIN_USER", "root")), password=doris_password, charset="utf8mb4", autocommit=True)
        self.connections.append(self.doris)
        variables = dict(execute(self.mysql, "SHOW VARIABLES WHERE Variable_name IN ('version','log_bin','binlog_format','binlog_row_image','server_id')"))
        assert variables["log_bin"] == "ON" and variables["binlog_format"] == "ROW" and variables["binlog_row_image"] == "FULL", "MySQL CDC needs binary logging with ROW/FULL"
        offset = int(execute(self.mysql, "SELECT TIMESTAMPDIFF(SECOND,UTC_TIMESTAMP(),NOW())")[0][0])
        self.mysql_timezone = "UTC" if offset == 0 else f"{'+' if offset >= 0 else '-'}{abs(offset) // 3600:02d}:{abs(offset) % 3600 // 60:02d}"
        variables["cdc_server_time_zone"] = self.mysql_timezone
        execute(self.mysql, f"CREATE DATABASE IF NOT EXISTS `{DATABASE}` CHARACTER SET utf8mb4")
        execute(self.doris, f"CREATE DATABASE IF NOT EXISTS `{DATABASE}`")
        execute(self.mysql, "CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s", (USER, local["mysqlPassword"]))
        execute(self.mysql, "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP ON `studio_realtime_test`.* TO %s@'%%'", (USER,))
        execute(self.mysql, "GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SHOW DATABASES ON *.* TO %s@'%%'", (USER,))
        execute(self.doris, "CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s", (USER, local["dorisPassword"]))
        execute(self.doris, "GRANT SELECT_PRIV, LOAD_PRIV, CREATE_PRIV ON `studio_realtime_test`.* TO %s", (USER,))
        self.source = self.prefix + "_source"
        self.jdbc_target = self.prefix + "_jdbc"
        self.append_target = self.prefix + "_append"
        self.upsert_read_target = self.prefix + "_upsert_read"
        self.doris_target = self.prefix + "_doris"
        for table in [self.source, self.jdbc_target, self.upsert_read_target]:
            execute(self.mysql, f"CREATE TABLE `{DATABASE}`.`{table}` (id INT NOT NULL PRIMARY KEY, payload VARCHAR(255) NOT NULL)")
        execute(self.mysql, f"CREATE TABLE `{DATABASE}`.`{self.append_target}` (id INT NOT NULL, payload VARCHAR(255) NOT NULL)")
        execute(self.mysql, f"INSERT INTO `{DATABASE}`.`{self.source}` VALUES (1,'initial_one'),(2,'initial_two')")
        execute(self.doris, f"CREATE TABLE `{DATABASE}`.`{self.doris_target}` (id INT NOT NULL, payload VARCHAR(255) NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1','enable_unique_key_merge_on_write'='true')")
        self.report["fixtures"].update(database=DATABASE, mysqlTables=[self.source, self.jdbc_target, self.append_target, self.upsert_read_target], dorisTables=[self.doris_target], localCredentials=str(LOCAL.relative_to(ROOT)), renderedSql=str(self.sql_path.relative_to(ROOT)))
        return dict(mysql=variables, database=DATABASE, mysqlTables=len(self.report["fixtures"]["mysqlTables"]), dorisTables=1)

    def jdbc_ddl(self, name, physical, primary_key=True):
        key = ", PRIMARY KEY(id) NOT ENFORCED" if primary_key else ""
        return f"""CREATE TABLE {name} (id INT, payload STRING{key}) WITH (
 'connector'='jdbc', 'url'='jdbc:mysql://mysql-rlt:3306/{DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&connectionTimeZone={self.mysql_timezone.replace('+', '%2B')}',
 'table-name'={sql_literal(physical)}, 'username'={sql_literal(USER)}, 'password'={sql_literal(self.local['mysqlPassword'])},
 'driver'='com.mysql.cj.jdbc.Driver', 'sink.buffer-flush.max-rows'='1', 'sink.buffer-flush.interval'='1 s', 'sink.max-retries'='3')"""

    def kafka_ddl(self, name, topic, fmt, source=False):
        options = ", 'scan.startup.mode'='earliest-offset', 'scan.bounded.mode'='latest-offset', 'properties.group.id'=" + sql_literal(topic + "_reader") if source else ""
        return f"CREATE TABLE {name} (id INT, payload STRING) WITH ('connector'='kafka', 'topic'={sql_literal(topic)}, 'properties.bootstrap.servers'='kafka_rlt_4_3_1:9092', 'format'={sql_literal(fmt)}{options})"

    def kafka_formats(self):
        evidence = {}
        for fmt in ("json", "csv"):
            session = self.open_session()
            source_topic = self.prefix + "_" + fmt + "_input"
            sink_topic = self.prefix + "_" + fmt + "_output"
            self.create_topic(source_topic)
            self.create_topic(sink_topic)
            expected = [(11, fmt + "_one"), (12, fmt + "_two")]
            values = [json.dumps(dict(id=key, payload=value), separators=(",", ":")) if fmt == "json" else f"{key},{value}" for key, value in expected]
            self.produce(source_topic, values)
            self.statement(session, self.kafka_ddl("format_source", source_topic, fmt, True), fmt + " source DDL")
            self.statement(session, self.kafka_ddl("format_sink", sink_topic, fmt), fmt + " sink DDL")
            self.statement(session, "EXPLAIN INSERT INTO format_sink SELECT id,payload FROM format_source", fmt + " EXPLAIN")
            job = self.statement(session, "INSERT INTO format_sink SELECT id,payload FROM format_source", "Kafka " + fmt + " source/sink", True)
            self.wait_job(job, "FINISHED")
            actual = self.consume(sink_topic, 2)
            if fmt == "json":
                decoded = sorted((int(row["id"]), row["payload"]) for row in map(json.loads, actual))
            else:
                decoded = sorted((int(row.split(",", 1)[0]), row.split(",", 1)[1]) for row in actual)
            assert decoded == expected, fmt + " source/sink rows differ"
            evidence[fmt] = dict(jobId=job, rows=len(decoded), sourceTopic=source_topic, sinkTopic=sink_topic)
        return evidence

    def jdbc_append(self):
        session = self.open_session()
        self.statement(session, self.jdbc_ddl("append_sink", self.append_target, False), "JDBC append DDL")
        job = self.statement(session, "INSERT INTO append_sink VALUES (101,'append_one'),(101,'append_duplicate')", "JDBC append", True)
        self.wait_job(job, "FINISHED")
        rows = execute(self.mysql, f"SELECT id,payload FROM `{DATABASE}`.`{self.append_target}` ORDER BY payload")
        assert rows == ((101, "append_duplicate"), (101, "append_one")), "JDBC append did not retain duplicate IDs"
        # Reading the same JDBC connector exercises its bounded source runtime too.
        topic = self.prefix + "_jdbc_read"
        self.create_topic(topic)
        self.statement(session, self.kafka_ddl("jdbc_read_sink", topic, "json"), "JDBC source Kafka sink DDL")
        job_read = self.statement(session, "INSERT INTO jdbc_read_sink SELECT id,payload FROM append_sink", "JDBC source", True)
        self.wait_job(job_read, "FINISHED")
        actual = sorted((int(row["id"]), row["payload"]) for row in map(json.loads, self.consume(topic, 2)))
        assert actual == list(rows), "JDBC source rows differ"
        return dict(appendJobId=job, sourceJobId=job_read, rows=len(rows), duplicateKeysRetained=True)

    def cdc_start(self):
        self.cdc_session = self.open_session()
        self.upsert_topic = self.prefix + "_upsert"
        self.create_topic(self.upsert_topic)
        server_id = 100000 + secrets.randbelow(100000000)
        self.statement(self.cdc_session, f"""CREATE TABLE cdc_source (id INT, payload STRING, PRIMARY KEY(id) NOT ENFORCED) WITH (
 'connector'='mysql-cdc','hostname'='mysql-rlt','port'='3306','username'={sql_literal(USER)},'password'={sql_literal(self.local['mysqlPassword'])},
 'database-name'={sql_literal(DATABASE)},'table-name'={sql_literal(self.source)},'server-id'='{server_id}-{server_id + 8}',
 'server-time-zone'={sql_literal(self.mysql_timezone)},'scan.startup.mode'='initial','scan.incremental.snapshot.enabled'='true')""", "MySQL CDC DDL")
        self.statement(self.cdc_session, self.jdbc_ddl("jdbc_sink", self.jdbc_target), "JDBC upsert DDL")
        self.statement(self.cdc_session, f"""CREATE TABLE doris_sink (id INT, payload STRING, PRIMARY KEY(id) NOT ENFORCED) WITH (
 'connector'='doris','fenodes'='fe:8030','benodes'={sql_literal(self.args.doris_be)},'jdbc-url'='jdbc:mysql://fe:9030',
 'table.identifier'={sql_literal(DATABASE + '.' + self.doris_target)},'username'={sql_literal(USER)},'password'={sql_literal(self.local['dorisPassword'])},
 'sink.label-prefix'={sql_literal(self.prefix)},'sink.enable-delete'='true','sink.enable-2pc'='true',
 'sink.properties.format'='json','sink.properties.read_json_by_line'='true')""", "Doris delete/2PC DDL")
        self.upsert_ddl = f"""CREATE TABLE upsert_sink (id INT, payload STRING, PRIMARY KEY(id) NOT ENFORCED) WITH (
 'connector'='upsert-kafka','topic'={sql_literal(self.upsert_topic)},'properties.bootstrap.servers'='kafka_rlt_4_3_1:9092',
 'key.format'='json','value.format'='json','sink.buffer-flush.max-rows'='0')"""
        self.statement(self.cdc_session, self.upsert_ddl, "Upsert Kafka DDL")
        self.cdc_sql = "EXECUTE STATEMENT SET BEGIN\nINSERT INTO jdbc_sink SELECT id,payload FROM cdc_source;\nINSERT INTO doris_sink SELECT id,payload FROM cdc_source;\nINSERT INTO upsert_sink SELECT id,payload FROM cdc_source;\nEND"
        self.statement(self.cdc_session, "EXPLAIN " + self.cdc_sql.removeprefix("EXECUTE "), "CDC Statement Set EXPLAIN")
        self.cdc_job = self.statement(self.cdc_session, self.cdc_sql, "MySQL CDC to JDBC/Doris/Upsert Kafka", True)
        self.wait_job(self.cdc_job)
        self.wait_sinks({1: "initial_one", 2: "initial_two"})
        checkpoint = self.wait_checkpoint(self.cdc_job)
        return dict(jobId=self.cdc_job, snapshotRows=2, completedCheckpointId=checkpoint["id"], mysqlVersion=self.report["checks"][1]["evidence"]["mysql"]["version"], dorisTwoPhaseCommit=True)

    def wait_sinks(self, expected):
        def matches():
            self.job_state(self.cdc_job)
            mysql = dict(execute(self.mysql, f"SELECT id,payload FROM `{DATABASE}`.`{self.jdbc_target}`"))
            doris = dict(execute(self.doris, f"SELECT id,payload FROM `{DATABASE}`.`{self.doris_target}`"))
            return dict(mysql=mysql, doris=doris) if mysql == expected and doris == expected else None
        return self.wait(matches, "matching JDBC and Doris rows")

    def cdc_changes(self):
        execute(self.mysql, f"INSERT INTO `{DATABASE}`.`{self.source}` VALUES (3,'insert_three')")
        self.wait_sinks({1: "initial_one", 2: "initial_two", 3: "insert_three"})
        execute(self.mysql, f"UPDATE `{DATABASE}`.`{self.source}` SET payload='updated_one' WHERE id=1")
        self.wait_sinks({1: "updated_one", 2: "initial_two", 3: "insert_three"})
        execute(self.mysql, f"DELETE FROM `{DATABASE}`.`{self.source}` WHERE id=2")
        self.expected = {1: "updated_one", 3: "insert_three"}
        self.wait_sinks(self.expected)
        kafka_state, tombstones, count = self.upsert_state()
        assert kafka_state == self.expected and 2 in tombstones, "Upsert Kafka update/delete mismatch"
        return dict(finalRows=self.expected.copy(), kafkaMessages=count, kafkaTombstoneKeys=tombstones, verifiedStages=["insert", "update", "delete"])

    def upsert_state(self):
        state, tombstones = {}, []
        messages = self.consume(self.upsert_topic, keyed=True)
        for line in messages:
            key, value = line.split("\t", 1)
            key_id = int(json.loads(key)["id"])
            if value == "<tombstone>":
                state.pop(key_id, None)
                tombstones.append(key_id)
            else:
                state[key_id] = json.loads(value)["payload"]
        return state, tombstones, len(messages)

    def checkpoint_recovery(self):
        before = self.wait_checkpoint(self.cdc_job)
        old_attempts = self.rest("GET", f"/jobs/{self.cdc_job}").get("timestamps", {})
        self.docker("restart", self.args.taskmanager_container, timeout=60)
        self.wait_job(self.cdc_job)
        def restored():
            value = self.checkpoints(self.cdc_job).get("latest", {}).get("restored")
            return value if value and value.get("id", -1) >= before["id"] else None
        recovery = self.wait(restored, "checkpoint restored metadata")
        self.wait_sinks(self.expected)
        execute(self.mysql, f"INSERT INTO `{DATABASE}`.`{self.source}` VALUES (4,'after_checkpoint')")
        self.expected[4] = "after_checkpoint"
        self.wait_sinks(self.expected)
        return dict(jobId=self.cdc_job, checkpointBeforeRestart=before["id"], restored=recovery, finalRows=self.expected.copy(), oldTimestamps=old_attempts)

    def savepoint_recovery(self):
        old_job = self.cdc_job
        trigger = self.rest("POST", f"/jobs/{old_job}/stop", {"targetDirectory": "file:///opt/flink/state/savepoints", "drain": False})["request-id"]
        def savepoint():
            result = self.rest("GET", f"/jobs/{old_job}/savepoints/{trigger}")
            if result["status"]["id"] != "COMPLETED":
                return None
            operation = result.get("operation", {})
            assert not operation.get("failure-cause"), self.redact(operation.get("failure-cause"))
            return operation.get("location")
        location = self.wait(savepoint, "stop-with-savepoint")
        self.wait_job(old_job, "FINISHED")
        self.statement(self.cdc_session, "SET 'execution.state-recovery.path'=" + sql_literal(location), "Restore savepoint configuration")
        self.cdc_job = self.statement(self.cdc_session, self.cdc_sql, "Same SQL restored from savepoint", True)
        assert self.cdc_job != old_job, "Restore did not create a new job"
        self.wait_job(self.cdc_job)
        restored = self.wait(lambda: self.checkpoints(self.cdc_job).get("latest", {}).get("restored"), "savepoint restored metadata")
        assert restored.get("is_savepoint") and restored.get("external_path", "").rstrip("/") == location.rstrip("/"), "Job did not restore the requested savepoint"
        self.wait_sinks(self.expected)
        execute(self.mysql, f"INSERT INTO `{DATABASE}`.`{self.source}` VALUES (5,'after_savepoint')")
        self.expected[5] = "after_savepoint"
        self.wait_sinks(self.expected)
        execute(self.mysql, f"DELETE FROM `{DATABASE}`.`{self.source}` WHERE id=3")
        del self.expected[3]
        self.wait_sinks(self.expected)
        state, tombstones, count = self.upsert_state()
        assert state == self.expected and 3 in tombstones, "Upsert Kafka differs after savepoint recovery"
        return dict(previousJobId=old_job, restoredJobId=self.cdc_job, savepoint=location, restored=restored, finalRows=self.expected.copy(), kafkaMessages=count, kafkaTombstoneKeys=tombstones)

    def upsert_source(self):
        session = self.open_session()
        # Same Upsert Kafka DDL works as a source; earliest replay materializes deletes.
        self.statement(session, self.upsert_ddl.replace("upsert_sink", "upsert_source", 1), "Upsert Kafka source DDL")
        self.statement(session, self.jdbc_ddl("upsert_read_sink", self.upsert_read_target), "Upsert Kafka readback JDBC DDL")
        self.statement(session, "EXPLAIN INSERT INTO upsert_read_sink SELECT id,payload FROM upsert_source", "Upsert Kafka source EXPLAIN")
        job = self.statement(session, "INSERT INTO upsert_read_sink SELECT id,payload FROM upsert_source", "Upsert Kafka source readback", True)
        self.wait_job(job)
        def matches():
            self.job_state(job)
            rows = dict(execute(self.mysql, f"SELECT id,payload FROM `{DATABASE}`.`{self.upsert_read_target}`"))
            return rows == self.expected
        self.wait(matches, "Upsert Kafka source final materialized rows")
        self.cancel(job)
        return dict(jobId=job, finalRows=self.expected.copy(), replayedTombstones=True)

    def cleanup(self):
        errors = []
        for job in self.jobs:
            try:
                self.cancel(job)
            except Exception as error:
                errors.append(self.redact(f"job {job}: {error}"))
        for session in self.sessions:
            try:
                self.gateway("DELETE", "/v1/sessions/" + session)
            except Exception as error:
                errors.append(self.redact(f"session {session}: {error}"))
        for connection in self.connections:
            connection.close()
        self.report["cleanup"] = dict(status="FAILED" if errors else "PASSED", jobsCanceledOrTerminal=len(self.jobs), sessionsClosed=len(self.sessions) - len([x for x in errors if x.startswith("session")]), errors=errors, containersKeptRunning=True, fixturesKept=True)
        if errors:
            raise RuntimeError("Acceptance cleanup failed: " + "; ".join(errors))

    def run(self):
        try:
            self.check("cluster_and_gateway", self.preflight)
            self.check("isolated_database_fixtures", self.fixtures)
            self.check("kafka_json_csv_source_sink", self.kafka_formats)
            self.check("jdbc_append_and_source", self.jdbc_append)
            self.check("cdc_snapshot_to_jdbc_doris_upsert_kafka", self.cdc_start)
            self.check("cdc_binlog_insert_update_delete", self.cdc_changes)
            self.check("taskmanager_checkpoint_recovery", self.checkpoint_recovery)
            self.check("same_sql_savepoint_restore", self.savepoint_recovery)
            self.check("upsert_kafka_source_materialization", self.upsert_source)
            self.report["status"] = "PASSED"
        except (Exception, KeyboardInterrupt) as error:
            self.report.update(status="FAILED", error=self.redact(error))
            if self.jobs:
                self.report["jobDiagnostics"] = {}
                for job in self.jobs:
                    try:
                        self.report["jobDiagnostics"][job] = self.rest("GET", f"/jobs/{job}/exceptions")
                    except Exception:
                        pass
        finally:
            try:
                self.cleanup()
            except Exception as error:
                self.report.update(status="FAILED", cleanupError=self.redact(error))
            self.report["finishedAt"] = self.now()
            self.save_report()
        self.progress(f"Acceptance {self.report['status']}; report: {REPORT}")
        return 0 if self.report["status"] == "PASSED" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8083")
    parser.add_argument("--flink-url", default="http://127.0.0.1:8081")
    parser.add_argument("--mysql-host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=3307)
    parser.add_argument("--mysql-container", default="dataworks-demo-mysql")
    parser.add_argument("--doris-host", default="127.0.0.1")
    parser.add_argument("--doris-port", type=int, default=9030)
    parser.add_argument("--doris-be", default="172.30.41.3:8040")
    parser.add_argument("--kafka-container", default="kafka_rlt_4_3_1")
    parser.add_argument("--taskmanager-container", default="flink-rlt-taskmanager")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    return Acceptance(args).run()


if __name__ == "__main__":
    sys.exit(main())
