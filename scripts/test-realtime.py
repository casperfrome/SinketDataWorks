"""Verify realtime features through the product API and real Flink sinks.

Use D:\\PythonVenv\\Scripts\\python.exe. Fixtures own unique table/topic names.
Reports are redacted and written under .runtime/realtime; submitted jobs are
cleaned up, while fixtures and services remain available for inspection.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import importlib.util
import json
from pathlib import Path
import sys
import time
import uuid
from urllib.parse import urlsplit

spec = importlib.util.spec_from_file_location("flink_acceptance", Path(__file__).with_name("test-flink.py"))
flink = importlib.util.module_from_spec(spec)
spec.loader.exec_module(flink)


class RealtimeAcceptance(flink.Acceptance):
    def __init__(self, args):
        super().__init__(args)
        self.backend = args.backend_url.rstrip("/") + "/api/v1"
        self.report["endpoints"]["backend"] = args.backend_url.rstrip("/")
        self.workspace = None
        self.product_jobs = []
        self.realtime_report = flink.ROOT / ".runtime" / "realtime" / "acceptance-report.json"
        self.realtime_report.parent.mkdir(parents=True, exist_ok=True)

    def save_report(self):
        content = self.redact(json.dumps(self.report, ensure_ascii=False, indent=2))
        self.realtime_report.write_text(content, encoding="utf-8")
        self.realtime_report.with_name("acceptance-report-" + self.run_id + ".json").write_text(content, encoding="utf-8")

    def api(self, method, path, body=None, expected=(200, 201, 202, 204)):
        response = self.http.request(method, self.backend + path, json=body, timeout=45)
        if response.status_code not in expected:
            raise AssertionError(self.redact(f"API {method} {path}: {response.status_code}: {response.text[:6000]}"))
        # Only registered datasource credentials reach the product. Fixture
        # administrator passwords can be short numbers that occur in metrics.
        product_secrets = [self.local[key] for key in ("mysqlPassword", "dorisPassword")] if hasattr(self, "local") else []
        assert not any(secret in response.text for secret in product_secrets if secret), f"Product API leaked a datasource credential: {method} {path}"
        return response.json() if response.content else None

    def operation(self, response, allow_failed=False):
        identifier = response["operationId"]
        result = self.wait(lambda: self.finished_operation(identifier), "operation " + identifier)
        if not allow_failed:
            assert result["status"] in ("SUCCEEDED", "SUCCESS", "COMPLETED"), self.redact(json.dumps(result))
        return result

    def finished_operation(self, identifier):
        operation = self.api("GET", f"/realtime/operations/{identifier}?workspaceId={self.workspace}")
        return operation if operation["status"] in ("SUCCEEDED", "SUCCESS", "COMPLETED", "FAILED", "CANCELED", "CANCELLED") else None

    @staticmethod
    def fields(primary=False):
        return [dict(id="id", name="id", type="INT", nullable=False, primaryKey=primary), dict(id="payload", name="payload", type="STRING", nullable=False, primaryKey=False)]

    def binding(self, role, connector, source, name, **options):
        binding = dict(id=uuid.uuid4().hex, role=role, connector=connector, datasourceId=source["id"], tableName=name,
                       physicalTable="", topic="", fields=self.fields(connector in ("MYSQL_CDC", "MYSQL_JDBC", "DORIS")),
                       format="json", consumerGroup=self.prefix + "_" + name, startupMode="earliest-offset", writeMode="append",
                       cdcStartupMode="initial", serverId="", timezone=self.mysql_timezone, eventTimeField="", watermarkSeconds=0,
                       dorisModel="UNIQUE", syncDeletes=False, labelPrefix=self.prefix + "_" + name, feHttpUrls="http://fe:8030")
        binding.update(options)
        return binding

    def task(self, name, bindings, sql):
        return self.api("POST", "/realtime/tasks", dict(workspaceId=self.workspace, name=name, description="真实 API 验收", folderId=None,
                        sql=sql, bindings=bindings, runtime=dict(parallelism=1, checkpointSeconds=5, restartAttempts=3, restartDelaySeconds=10)))

    def release(self, task):
        response = self.api("POST", f"/realtime/tasks/{task['id']}/releases", dict(workspaceId=self.workspace, task=task, revision=task["revision"], note="真实验收", requestId=uuid.uuid4().hex))
        operation = self.operation(response)
        result = operation.get("result", {})
        if result.get("id") and result.get("releaseNo"):
            return result
        if result.get("release"):
            return result["release"]
        if result.get("releaseId"):
            return next(r for r in self.state()["releases"] if r["id"] == result["releaseId"])
        return max((r for r in self.state()["releases"] if r["taskId"] == task["id"]), key=lambda r: r["releaseNo"])

    def state(self):
        return self.api("GET", "/realtime/state?workspaceId=" + self.workspace)

    def job(self, identifier):
        return self.api("GET", f"/realtime/jobs/{identifier}?workspaceId={self.workspace}")

    def start(self, release, race=False, **extra):
        body = dict(workspaceId=self.workspace, releaseId=release["id"], requestId=uuid.uuid4().hex, **extra)
        if race:
            second = dict(body, requestId=uuid.uuid4().hex)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                replies = list(pool.map(lambda request: (request, self.api("POST", "/realtime/jobs", request, expected=(202,409))), (body, second)))
            accepted = [(request, reply) for request, reply in replies if reply.get("operationId")]
            assert len(accepted) == 1, "Concurrent starts must acquire only one production job"
            body, response = accepted[0]
            assert self.api("POST", "/realtime/jobs", body)["operationId"] == response["operationId"], "Retried start must reuse the operation"
        else:
            response = self.api("POST", "/realtime/jobs", body)
        operation = self.operation(response)
        result = operation.get("result", {})
        identifier = result.get("jobId") or result.get("job", {}).get("id") or operation.get("jobId")
        if not identifier:
            identifier = next(j["id"] for j in self.state()["jobs"] if j["releaseId"] == release["id"] and j["status"] in ("STARTING", "RUNNING"))
        job = self.wait(lambda: self.running_job(identifier), "product job RUNNING")
        self.product_jobs.append(identifier)
        if job.get("flinkJobId") not in self.jobs:
            self.jobs.append(job["flinkJobId"])
        return job

    def running_job(self, identifier):
        job = self.job(identifier)
        assert job["status"] not in ("FAILED", "LOST"), self.redact(json.dumps(job))
        return job if job["status"] == "RUNNING" else None

    def control(self, job, action, **options):
        return self.operation(self.api("POST", f"/realtime/jobs/{job['id']}/{action}", dict(workspaceId=self.workspace, requestId=uuid.uuid4().hex, **options)))

    def setup_product(self):
        workspace = self.api("POST", "/workspaces", dict(name="实时验收 " + self.run_id, code="rt_" + uuid.uuid4().hex[:12], region="local"))
        self.workspace = workspace["id"]
        self.kafka_source = self.api("POST", "/datasources", dict(workspaceId=self.workspace, type="KAFKA", name="Kafka", bootstrapServers="localhost:19092", flinkBootstrapServers="kafka_rlt_4_3_1:9092", securityProtocol="PLAINTEXT", saslMechanism="PLAIN", username=""))
        self.mysql_source = self.api("POST", "/datasources", dict(workspaceId=self.workspace, type="MYSQL", name="MySQL CDC/JDBC", host="127.0.0.1", port=3307, database=flink.DATABASE, username=flink.USER, password=self.local["mysqlPassword"], options=dict(flinkHost="mysql-rlt", flinkPort=3306)))
        self.doris_source = self.api("POST", "/datasources", dict(workspaceId=self.workspace, type="DORIS", name="Doris", host="127.0.0.1", port=9030, database=flink.DATABASE, username=flink.USER, password=self.local["dorisPassword"], options=dict(flinkHost="fe", flinkPort=9030, flinkFeHttpUrls=["http://fe:8030"], flinkBeHttpUrls=["http://" + self.args.doris_be])))
        for source in (self.kafka_source, self.mysql_source, self.doris_source):
            assert self.api("POST", f"/datasources/{source['id']}/test", {})["success"]
        assert self.api("GET", f"/datasources/{self.mysql_source['id']}/tables/{self.source}/cdc-metadata")["valid"]
        return dict(workspaceId=self.workspace, dataSources=3)

    def kafka_product(self):
        evidence = {}
        for fmt in ("json", "csv"):
            incoming, outgoing = self.prefix + "_api_" + fmt + "_in", self.prefix + "_api_" + fmt + "_out"
            self.create_topic(incoming);self.create_topic(outgoing)
            bindings = [self.binding("SOURCE", "KAFKA", self.kafka_source, "src", topic=incoming, format=fmt), self.binding("SINK", "KAFKA", self.kafka_source, "dst", topic=outgoing, format=fmt)]
            task = self.task("Kafka " + fmt, bindings, "INSERT INTO dst SELECT id,payload FROM src;")
            assert self.operation(self.api("POST", "/realtime/validate", dict(workspaceId=self.workspace, task=task)))["result"]["valid"]
            assert self.operation(self.api("POST", "/realtime/plan", dict(workspaceId=self.workspace, task=task)))["result"]["plan"]
            release = self.release(task)
            if fmt == "json":
                changed = dict(self.kafka_source, flinkBootstrapServers="changed-target:9092")
                self.api("PUT", f"/datasources/{self.kafka_source['id']}", changed)
                self.api("POST", "/realtime/jobs", dict(workspaceId=self.workspace, releaseId=release["id"], requestId=uuid.uuid4().hex), expected=(409,))
                self.api("PUT", f"/datasources/{self.kafka_source['id']}", self.kafka_source)
            job = self.start(release, race=fmt == "json")
            values = [json.dumps(dict(id=11, payload="one")), json.dumps(dict(id=12, payload="two"))] if fmt == "json" else ["11,one", "12,two"]
            self.produce(incoming, values)
            actual = self.consume(outgoing, 2)
            assert len(actual) == 2, "Kafka output row count differs"
            if fmt == "json":assert sorted((x["id"], x["payload"]) for x in map(json.loads, actual)) == [(11, "one"), (12, "two")]
            else:assert sorted(actual) == ["11,one", "12,two"]
            self.control(job, "cancel")
            evidence[fmt] = dict(jobId=job["flinkJobId"], rows=2)
        assert any(t["name"].startswith(self.prefix) for t in self.api("GET", f"/datasources/{self.kafka_source['id']}/topics"))
        return evidence

    def metadata_rules(self):
        task = self.task("元数据规则", [], "SELECT 1;")
        original = dict(task)
        task["description"] = "new revision"
        saved = self.api("PUT", f"/realtime/tasks/{task['id']}", task)
        assert saved["revision"] == original["revision"] + 1
        self.api("PUT", f"/realtime/tasks/{task['id']}", original, expected=(409,))
        bad = dict(saved, sql="INSERT INTO missing_sink SELECT 1;")
        self.api("POST", "/realtime/preview", dict(workspaceId=self.workspace, task=bad, query=bad["sql"]), expected=(400,))
        secret = dict(saved, sql="CREATE TABLE leak (id INT) WITH ('connector'='jdbc','password'='inline-secret');")
        self.api("PUT", f"/realtime/tasks/{task['id']}", secret, expected=(400,))
        mismatched_fields = self.fields(True)
        mismatched_fields[0]["type"] = "STRING"
        mismatched = self.task("物理目标结构预检", [self.binding("SINK", "MYSQL_JDBC", self.mysql_source, "bad_sink", physicalTable=self.jdbc_target, writeMode="upsert", fields=mismatched_fields)], "INSERT INTO bad_sink VALUES ('1','test');")
        rejected = self.operation(self.api("POST", f"/realtime/tasks/{mismatched['id']}/releases", dict(workspaceId=self.workspace, task=mismatched, revision=mismatched["revision"], requestId=uuid.uuid4().hex)), allow_failed=True)
        assert rejected["status"] == "FAILED" and rejected["errorCode"] == "TARGET_SCHEMA_MISMATCH", "Publication must verify the real target structure"
        self.api("DELETE", f"/realtime/tasks/{mismatched['id']}?workspaceId={self.workspace}")
        folder = self.api("POST", "/realtime/folders", dict(workspaceId=self.workspace, name="规则目录"))
        copy = self.api("POST", f"/realtime/tasks/{task['id']}/copy", dict(workspaceId=self.workspace, name="规则副本"))
        self.api("DELETE", f"/realtime/tasks/{copy['id']}?workspaceId={self.workspace}")
        self.api("DELETE", f"/realtime/folders/{folder['id']}?workspaceId={self.workspace}")
        legacy_id = "legacy_" + uuid.uuid4().hex
        legacy_task = dict(saved, id=legacy_id, name="旧浏览器导入", revision=1)
        state = dict(schemaVersion=1, tasks=[legacy_task], folders=[], drafts={}, releases=[], jobs=[dict(id="mockjob", taskId=legacy_id, status="RUNNING")], kafkaSources=[], openTabs=[legacy_id], activeId=legacy_id, selectedJobId="mockjob", view="development")
        body = dict(workspaceId=self.workspace, importId="import_" + uuid.uuid4().hex, state=state)
        imported = self.api("POST", "/realtime/import", body)
        again = self.api("POST", "/realtime/import", body)
        assert sum(t["id"] == legacy_id for t in again["tasks"]) == 1
        assert not any(j["id"] == "mockjob" for j in imported["jobs"])
        return dict(conflictProtected=True, inlineCredentialsRejected=True, previewWriteRejected=True, physicalSchemaVerified=True, legacyImportIdempotent=True)

    def preview_product(self):
        # A bounded connector query proves changelog metadata and cleanup without requiring new input.
        sql = f"CREATE TABLE preview_src (id INT, payload STRING, PRIMARY KEY(id) NOT ENFORCED) WITH ('connector'='jdbc','table-name'='{self.source}','studio.datasource-id'='{self.mysql_source['id']}'); SELECT id,payload FROM preview_src;"
        task = self.task("SELECT 预览", [], sql)
        result = self.operation(self.api("POST", "/realtime/preview", dict(workspaceId=self.workspace, task=task, query="SELECT id,payload FROM preview_src", maxRows=100, timeoutSeconds=30)))
        assert result["status"] in ("SUCCEEDED", "SUCCESS", "COMPLETED")
        preview = self.api("GET", f"/realtime/previews/{result['result']['previewId']}?workspaceId={self.workspace}")
        assert len(preview["rows"]) == 2, "Preview should return the two real source rows"
        assert preview["columns"] and all(row.get("kind") or row.get("rowKind") for row in preview["rows"]), "Preview should expose changelog kinds"
        incoming = self.prefix + "_debug";self.create_topic(incoming)
        self.produce(incoming, [json.dumps(dict(id=1, payload="debug"))])
        stream = self.task("流式预览超时", [self.binding("SOURCE", "KAFKA", self.kafka_source, "src", topic=incoming)], "SELECT id,payload FROM src;")
        timed = self.operation(self.api("POST", "/realtime/preview", dict(workspaceId=self.workspace, task=stream, query=stream["sql"])))
        timed_preview = self.api("GET", f"/realtime/previews/{timed['result']['previewId']}?workspaceId={self.workspace}")
        assert len(timed_preview["rows"]) == 1 and timed_preview["cleanupStatus"] == "COMPLETED"
        pending = self.api("POST", "/realtime/preview", dict(workspaceId=self.workspace, task=stream, query=stream["sql"]))
        preview_path = f"/realtime/previews/{pending['previewId']}?workspaceId={self.workspace}"
        self.wait(lambda: self.api("GET", preview_path).get("rows"), "stream preview first row")
        self.api("POST", f"/realtime/previews/{pending['previewId']}/cancel", dict(workspaceId=self.workspace))
        cancelled = self.operation(pending, allow_failed=True)
        assert cancelled["status"] == "SUCCESS"
        cancelled_preview = self.api("GET", preview_path)
        assert cancelled_preview["status"] == "CANCELLED" and cancelled_preview["cleanupStatus"] == "COMPLETED"
        return dict(operationId=result["id"], rows=len(preview["rows"]), streamingTimeout=True, streamingCancel=True)

    def cdc_product(self):
        self.upsert_topic = self.prefix + "_api_upsert";self.create_topic(self.upsert_topic)
        bindings = [self.binding("SOURCE", "MYSQL_CDC", self.mysql_source, "cdc_source", physicalTable=self.source),
                    self.binding("SINK", "MYSQL_JDBC", self.mysql_source, "jdbc_sink", physicalTable=self.jdbc_target, writeMode="upsert"),
                    self.binding("SINK", "DORIS", self.doris_source, "doris_sink", physicalTable=self.doris_target, syncDeletes=True),
                    self.binding("SINK", "KAFKA", self.kafka_source, "upsert_sink", topic=self.upsert_topic, writeMode="upsert", fields=self.fields(True))]
        sql = "INSERT INTO jdbc_sink SELECT id,payload FROM cdc_source; INSERT INTO doris_sink SELECT id,payload FROM cdc_source; INSERT INTO upsert_sink SELECT id,payload FROM cdc_source;"
        self.cdc_task = self.task("CDC 多路输出", bindings, sql)
        self.cdc_release = self.release(self.cdc_task)
        self.cdc_product_job = self.start(self.cdc_release)
        self.cdc_job = self.cdc_product_job["flinkJobId"]
        self.wait_sinks({1: "initial_one", 2: "initial_two"})
        self.cdc_changes()
        self.wait_checkpoint(self.cdc_job)
        debug = self.api("POST", "/realtime/preview", dict(workspaceId=self.workspace, task=self.cdc_task, query="SELECT id,payload FROM cdc_source"))
        self.operation(debug)
        debug_rows = self.api("GET", f"/realtime/previews/{debug['previewId']}?workspaceId={self.workspace}")
        assert len(debug_rows["rows"]) == len(self.expected) and debug_rows["cleanupStatus"] == "COMPLETED"
        assert self.job(self.cdc_product_job["id"])["status"] == "RUNNING", "CDC preview disturbed the production job"
        self.wait_sinks(self.expected)
        return dict(jobId=self.cdc_job, finalRows=self.expected.copy(), statementSet=True, concurrentCdcPreview=True, automaticCdcServerId=True)

    def cdc_server_id_isolation(self):
        # A second task must never disconnect a running replication reader.
        sql = "CREATE TABLE range_sink (id INT, payload STRING) WITH ('connector'='blackhole'); INSERT INTO range_sink SELECT id,payload FROM range_source;"
        tasks = [self.task("CDC 范围隔离 " + name, [self.binding("SOURCE", "MYSQL_CDC", self.mysql_source, "range_source", physicalTable=self.source, serverId="9001-9256")], sql) for name in ("A", "B")]
        releases = [self.release(task) for task in tasks]
        first = self.start(releases[0])
        pending = self.api("POST", "/realtime/jobs", dict(workspaceId=self.workspace, releaseId=releases[1]["id"], requestId=uuid.uuid4().hex))
        if pending.get("jobId"):
            self.product_jobs.append(pending["jobId"])
        rejected = self.operation(pending, allow_failed=True)
        assert rejected["status"] == "FAILED" and rejected["errorCode"] == "CDC_SERVER_ID_IN_USE"
        assert self.job(first["id"])["status"] == "RUNNING", "Conflicting CDC start disturbed the original reader"
        assert self.job(self.cdc_product_job["id"])["status"] == "RUNNING"
        self.control(first, "cancel")
        second = self.start(releases[1])
        self.control(second, "cancel")
        for task in tasks:
            self.api("DELETE", f"/realtime/tasks/{task['id']}?workspaceId={self.workspace}")
        return dict(overlapRejected=True, originalReaderUnaffected=True, terminalRangeReusable=True)

    def recovery_product(self):
        self.wait(lambda: not any(job["jid"] not in self.jobs and job["state"] not in ("FAILED", "CANCELED", "FINISHED")
                                 for job in self.rest("GET", "/jobs/overview")["jobs"]), "exclusive TaskManager recovery window")
        self.checkpoint_recovery()
        old = self.cdc_product_job
        self.control(old, "stop")
        points = self.job(old["id"])["savepoints"]
        assert points, "No real savepoint"
        point_uri = urlsplit(points[-1]["path"])
        assert point_uri.scheme == "file" and not point_uri.netloc and point_uri.path.startswith("/opt/flink/state/savepoints/savepoint-"), "Invalid real savepoint URI"
        self.cdc_product_job = self.start(self.cdc_release, savepointId=points[-1]["id"])
        self.cdc_job = self.cdc_product_job["flinkJobId"]
        restored = self.wait(lambda: self.checkpoints(self.cdc_job).get("latest", {}).get("restored"), "restored metadata")
        assert restored["is_savepoint"]
        flink.execute(self.mysql, f"INSERT INTO `{flink.DATABASE}`.`{self.source}` VALUES (6,'api_restored')")
        self.expected[6] = "api_restored";self.wait_sinks(self.expected)
        restarted = self.control(self.cdc_product_job, "restart")
        self.cdc_product_job = self.job(restarted["result"]["jobId"])
        self.product_jobs.append(self.cdc_product_job["id"])
        self.cdc_job = self.cdc_product_job["flinkJobId"];self.jobs.append(self.cdc_job)
        assert self.checkpoints(self.cdc_job)["latest"]["restored"]["is_savepoint"], "Default restart must preserve state"
        self.wait_sinks(self.expected)
        return dict(jobId=self.cdc_job, savepoint=points[-1]["path"], defaultRestartRestoresState=True, finalRows=self.expected.copy())

    def upgrade_product(self):
        self.control(self.cdc_product_job, "cancel")
        # A primary-key aggregate is optimized away. Start a separate genuine
        # aggregation job, then remove its populated state to test strict restore.
        aggregate_sql = " ".join(f"INSERT INTO {sink} SELECT MOD(id,1000000),MAX(payload) FROM cdc_source GROUP BY MOD(id,1000000);" for sink in ("jdbc_sink", "doris_sink", "upsert_sink"))
        task = self.task("CDC 状态升级", self.cdc_task["bindings"], aggregate_sql)
        aggregate_release = self.release(task)
        self.cdc_product_job = self.start(aggregate_release)
        self.cdc_job = self.cdc_product_job["flinkJobId"]
        vertices = self.rest("GET", f"/jobs/{self.cdc_job}")["vertices"]
        aggregates = [vertex["id"] for vertex in vertices if "GroupAggregate" in vertex["name"]]
        assert aggregates, "Upgrade fixture must retain a real stateful aggregate"
        self.wait_sinks(self.expected)
        checkpoint = self.wait_checkpoint(self.cdc_job)
        checkpoint_detail = self.rest("GET", f"/jobs/{self.cdc_job}/checkpoints/details/{checkpoint['id']}")
        assert any(checkpoint_detail["tasks"][vertex].get("state_size", 0) > 0 for vertex in aggregates), "Aggregate checkpoint must contain state"
        task = next(t for t in self.state()["tasks"] if t["id"] == task["id"])
        task["description"] = "Compatible release upgrade"
        task["runtime"]["restartDelaySeconds"] = 2
        task = self.api("PUT", f"/realtime/tasks/{task['id']}", task)
        compatible = self.release(task)
        upgraded = self.control(self.cdc_product_job, "upgrade", targetReleaseId=compatible["id"])
        identifier = upgraded["result"]["jobId"]
        self.product_jobs.append(identifier)
        self.cdc_product_job = self.job(identifier)
        self.cdc_job = self.cdc_product_job["flinkJobId"];self.jobs.append(self.cdc_job)
        assert self.checkpoints(self.cdc_job)["latest"]["restored"]["is_savepoint"]
        flink.execute(self.mysql, f"INSERT INTO `{flink.DATABASE}`.`{self.source}` VALUES (7,'upgraded')")
        self.expected[7] = "upgraded";self.wait_sinks(self.expected)
        self.wait_checkpoint(self.cdc_job)
        task = next(t for t in self.state()["tasks"] if t["id"] == task["id"])
        task["sql"] = " ".join(f"INSERT INTO {sink} SELECT id,payload FROM cdc_source;" for sink in ("jdbc_sink", "doris_sink", "upsert_sink"))
        task = self.api("PUT", f"/realtime/tasks/{task['id']}", task)
        incompatible = self.release(task)
        failed = self.operation(self.api("POST", f"/realtime/jobs/{identifier}/upgrade", dict(workspaceId=self.workspace, targetReleaseId=incompatible["id"], requestId=uuid.uuid4().hex)), allow_failed=True)
        assert failed["status"] == "FAILED" and failed.get("rollbackAvailable"), "Incompatible topology must retain rollback"
        failed_job = self.job(failed["jobId"])
        self.product_jobs.append(failed_job["id"])
        if failed_job.get("flinkJobId"):self.jobs.append(failed_job["flinkJobId"])
        restored = self.operation(self.api("POST", f"/realtime/operations/{failed['id']}/rollback", dict(workspaceId=self.workspace, requestId=uuid.uuid4().hex)))
        rollback_job = self.job(restored["result"]["jobId"]);self.product_jobs.append(rollback_job["id"])
        self.cdc_product_job = rollback_job;self.cdc_job = rollback_job["flinkJobId"];self.jobs.append(self.cdc_job)
        assert rollback_job["releaseId"] == compatible["id"]
        flink.execute(self.mysql, f"INSERT INTO `{flink.DATABASE}`.`{self.source}` VALUES (8,'rolled_back')")
        self.expected[8] = "rolled_back";self.wait_sinks(self.expected)
        return dict(upgradeJobId=identifier, failedOperation=failed["id"], rollbackJobId=rollback_job["id"], aggregateOperators=len(aggregates), aggregateStateVerified=True, finalRows=self.expected.copy())

    def cleanup(self):
        errors = []
        if self.workspace:
            # A replacement can be accepted before an assertion fails. Discover
            # every job in this invocation's isolated workspace before cleanup.
            try:
                for job in self.state()["jobs"]:
                    if job["id"] not in self.product_jobs:self.product_jobs.append(job["id"])
                    if job.get("flinkJobId") and job["flinkJobId"] not in self.jobs:self.jobs.append(job["flinkJobId"])
            except Exception as error:errors.append(self.redact(error))
        for identifier in self.product_jobs:
            try:
                job = self.job(identifier)
                if job["status"] not in ("STOPPED", "FAILED", "FINISHED", "CANCELLED"):
                    self.operation(self.api("POST", f"/realtime/jobs/{identifier}/cancel", dict(workspaceId=self.workspace, requestId=uuid.uuid4().hex)))
            except Exception as error:errors.append(self.redact(error))
        super().cleanup()
        self.report["cleanup"]["productCancelErrors"] = errors
        active = [j for j in self.rest("GET", "/jobs/overview")["jobs"] if j["jid"] in self.jobs and j["state"] not in ("FAILED", "CANCELED", "FINISHED")]
        assert not active, "Acceptance left active remote jobs"

    def run(self):
        try:
            self.check("cluster_and_gateway", self.preflight)
            self.check("isolated_database_fixtures", self.fixtures)
            self.check("product_datasources", self.setup_product)
            self.check("product_metadata_rules", self.metadata_rules)
            self.check("product_kafka_json_csv", self.kafka_product)
            self.check("product_select_preview", self.preview_product)
            self.check("product_cdc_multi_sink_changes", self.cdc_product)
            self.check("product_cdc_server_id_isolation", self.cdc_server_id_isolation)
            self.check("product_checkpoint_and_savepoint_restore", self.recovery_product)
            self.check("product_upgrade_failure_and_rollback", self.upgrade_product)
            self.report["status"] = "PASSED"
        except (Exception, KeyboardInterrupt) as error:
            self.report.update(status="FAILED", error=self.redact(error))
        finally:
            try:self.cleanup()
            except Exception as error:self.report.update(status="FAILED", cleanupError=self.redact(error))
            self.report["finishedAt"] = self.now();self.save_report()
        self.progress(f"Product acceptance {self.report['status']}; report: {self.realtime_report}")
        return 0 if self.report["status"] == "PASSED" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name, default in dict(backend_url="http://127.0.0.1:8080", gateway_url="http://127.0.0.1:8083", flink_url="http://127.0.0.1:8081", mysql_host="127.0.0.1", mysql_container="dataworks-demo-mysql", doris_host="127.0.0.1", doris_be="172.30.41.3:8040", kafka_container="kafka_rlt_4_3_1", taskmanager_container="flink-rlt-taskmanager").items():
        parser.add_argument("--" + name.replace("_", "-"), default=default)
    parser.add_argument("--mysql-port", type=int, default=3307);parser.add_argument("--doris-port", type=int, default=9030);parser.add_argument("--timeout", type=int, default=180)
    return RealtimeAcceptance(parser.parse_args()).run()


if __name__ == "__main__":sys.exit(main())
