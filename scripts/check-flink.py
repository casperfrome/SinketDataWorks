"""Check the independent Flink runtime without writing external business data.

Run with D:\\PythonVenv\\Scripts\\python.exe. Uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTAINERS = {
    "jobmanager": "flink-rlt-jobmanager",
    "taskmanager": "flink-rlt-taskmanager",
    "sql-gateway": "flink-rlt-sql-gateway",
    "kafka": "kafka_rlt_4_3_1",
}
FLINK_VERSION = "2.2.1"
BASE_IMAGE = "flink:2.2.1-scala_2.12-java17"
DORIS_NETWORK = os.environ.get("DORIS_DOCKER_NETWORK") or "dunnelean_doris"


def docker(*args: str, timeout: float = 60) -> str:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=timeout, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"docker {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def request(url: str, method: str = "GET", body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} returned HTTP {exc.code}: {detail}") from exc


def retry(action, timeout: float):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            return action()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
            last_error = exc
            time.sleep(min(1.0, max(0, deadline - time.monotonic())))
    raise RuntimeError(f"Timed out after {timeout}s: {last_error}")


def check_containers() -> dict:
    containers = json.loads(docker("inspect", *CONTAINERS.values()))
    found = {container["Name"].lstrip("/"): container for container in containers}
    images = set()
    volumes = set()
    for service, name in CONTAINERS.items():
        container = found[name]
        labels = container["Config"].get("Labels") or {}
        if labels.get("com.docker.compose.project") != "sinket-realtime" or labels.get("com.docker.compose.service") != service:
            raise RuntimeError(f"{name} does not belong to the expected Compose project/service")
        state = container["State"]
        if not state["Running"] or state.get("Health", {}).get("Status", "healthy") != "healthy":
            raise RuntimeError(f"{name} is not running and healthy")
        if "sinket-realtime" not in container["NetworkSettings"]["Networks"]:
            raise RuntimeError(f"{name} is missing the realtime network")
        if service != "kafka":
            images.add(container["Image"])
            if DORIS_NETWORK not in container["NetworkSettings"]["Networks"]:
                raise RuntimeError(f"{name} is missing the Doris network")
            shared = [mount for mount in container["Mounts"] if mount["Destination"] == "/opt/flink/state"]
            if len(shared) != 1 or not shared[0]["RW"] or shared[0]["Type"] != "volume":
                raise RuntimeError(f"{name} does not have the expected writable state volume")
            volumes.add(shared[0]["Name"])
            docker("exec", name, "test", "-w", "/opt/flink/state")
    if len(images) != 1 or len(volumes) != 1:
        raise RuntimeError("Flink services must share one image and one state volume")
    mysql = json.loads(docker("inspect", "dataworks-demo-mysql"))[0]
    network = mysql["NetworkSettings"]["Networks"].get("sinket-realtime", {})
    if not mysql["State"]["Running"] or "mysql-rlt" not in (network.get("Aliases") or []):
        raise RuntimeError("Business MySQL is not running with the mysql-rlt realtime alias")
    expected_ports = {"jobmanager": ("8081/tcp", "8081"), "sql-gateway": ("8083/tcp", "8083"), "kafka": ("19092/tcp", "19092")}
    for service, (internal, external) in expected_ports.items():
        bindings = found[CONTAINERS[service]]["NetworkSettings"]["Ports"].get(internal) or []
        if bindings != [{"HostIp": "127.0.0.1", "HostPort": external}]:
            raise RuntimeError(f"{service} port mapping is not restricted to 127.0.0.1:{external}")
    return {"containers": list(found), "image": next(iter(images)), "state_volume": next(iter(volumes))}


def check_jars(manifest_path: Path) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if manifest.get("base_image") != BASE_IMAGE or manifest.get("flink_version") != FLINK_VERSION:
        raise RuntimeError("Connector manifest does not match the required base image/version")
    artifacts = manifest.get("artifacts") or []
    if not artifacts:
        raise RuntimeError("Connector manifest has no artifacts")
    for artifact in artifacts:
        filename, expected_hash = artifact["filename"], artifact["sha256"]
        if Path(filename).name != filename or not filename.endswith(".jar"):
            raise RuntimeError("Invalid connector manifest filename")
        actual_hash = hashlib.sha256((manifest_path.parent / filename).read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            raise RuntimeError(f"Local connector SHA256 mismatch: {filename}")
    hashes = {}
    for service in ("jobmanager", "taskmanager", "sql-gateway"):
        name = CONTAINERS[service]
        image_manifest = json.loads(docker("exec", name, "cat", "/opt/flink/connectors-manifest.json"))
        if image_manifest != manifest:
            raise RuntimeError(f"{name} connector manifest differs from the local manifest")
        files = [f"/opt/flink/lib/{artifact['filename']}" for artifact in artifacts]
        output = docker("exec", name, "sha256sum", *files)
        actual = {Path(line.split(None, 1)[1].strip()).name: line.split(None, 1)[0] for line in output.splitlines()}
        for artifact in artifacts:
            if actual.get(artifact["filename"]) != artifact["sha256"]:
                raise RuntimeError(f"{name} connector SHA256 mismatch: {artifact['filename']}")
        libraries = docker("exec", name, "find", "/opt/flink/lib", "-maxdepth", "1", "-name", "*.jar").splitlines()
        if not any(Path(jar).name.startswith("flink-json-") for jar in libraries) or not any(Path(jar).name.startswith("flink-csv-") for jar in libraries):
            raise RuntimeError(f"{name} is missing the base image JSON/CSV formats")
        hashes[service] = actual
    return {"jar_count": len(artifacts), "container_sha256": hashes, "base_formats": ["json", "csv"]}


def check_flink(url: str) -> dict:
    overview = request(url + "/overview")
    managers = request(url + "/taskmanagers").get("taskmanagers", [])
    if overview.get("flink-version") != FLINK_VERSION:
        raise RuntimeError(f"Unexpected Flink version: {overview.get('flink-version')}")
    if len(managers) != 1 or managers[0].get("slotsNumber") != 4:
        raise RuntimeError(f"Expected one registered TaskManager with 4 slots: {managers}")
    return {"version": FLINK_VERSION, "taskmanagers": len(managers), "slots": 4, "running_jobs": overview.get("jobs-running", 0)}


def check_gateway(url: str, timeout: float) -> dict:
    info = request(url + "/v1/info")
    if info.get("version") != FLINK_VERSION:
        raise RuntimeError(f"Unexpected SQL Gateway version: {info.get('version')}")
    session = request(url + "/v1/sessions", "POST", {"sessionName": "runtime-health", "properties": {"execution.runtime-mode": "batch"}})["sessionHandle"]
    session_url = url + "/v1/sessions/" + session
    try:
        # Flink 2.2 Gateway only supports zero for executionTimeout. Bound
        # HTTP requests and result polling locally instead.
        operation = request(session_url + "/statements", "POST", {"statement": "SELECT 1", "executionTimeout": 0})["operationHandle"]
        operation_url = session_url + "/operations/" + operation
        deadline = time.monotonic() + timeout
        status = ""
        while time.monotonic() < deadline:
            status = request(operation_url + "/status").get("status", "")
            if status == "FINISHED":
                break
            if status in {"ERROR", "CANCELED", "CLOSED"}:
                raise RuntimeError(f"SQL Gateway SELECT 1 reached {status}")
            time.sleep(0.5)
        if status != "FINISHED":
            raise RuntimeError(f"SQL Gateway SELECT 1 did not finish within {timeout}s")
        next_url = operation_url + "/result/0?rowFormat=JSON"
        rows = []
        while next_url and time.monotonic() < deadline:
            result = request(next_url)
            rows.extend(row["fields"] for row in result.get("results", {}).get("data", []))
            if result.get("resultType") == "EOS" or rows:
                break
            next_uri = result.get("nextResultUri")
            next_url = urllib.parse.urljoin(url + "/", next_uri) if next_uri else next_url
            time.sleep(0.5)
        if rows != [[1]]:
            raise RuntimeError(f"SQL Gateway SELECT 1 returned unexpected rows: {rows}")
        return {"version": info["version"], "select_1": rows}
    finally:
        request(session_url, "DELETE")


def receive_exact(sock: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise RuntimeError("Kafka closed the connection before a complete response")
        result.extend(chunk)
    return bytes(result)


def kafka_request(host: str, port: int, api_key: int, version: int, payload: bytes, correlation: int) -> bytes:
    client = b"sinket-runtime-health"
    data = struct.pack(">hhih", api_key, version, correlation, len(client)) + client + payload
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.settimeout(10)
        sock.sendall(struct.pack(">i", len(data)) + data)
        size = struct.unpack(">i", receive_exact(sock, 4))[0]
        if size < 4 or size > 8 * 1024 * 1024:
            raise RuntimeError(f"Invalid Kafka response length: {size}")
        response = receive_exact(sock, size)
    if struct.unpack_from(">i", response)[0] != correlation:
        raise RuntimeError("Kafka response correlation ID mismatch")
    return response[4:]


def check_kafka() -> dict:
    for address in ("kafka_rlt_4_3_1:9092", "localhost:19092"):
        output = docker("exec", CONTAINERS["kafka"], "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", address)
        if "Produce" not in output or "Metadata" not in output:
            raise RuntimeError(f"Kafka broker API discovery failed for {address}")
    versions = kafka_request("127.0.0.1", 19092, 18, 0, b"", 101)
    error, count = struct.unpack_from(">hi", versions)
    if error != 0 or count < 1:
        raise RuntimeError(f"Host Kafka ApiVersions response is invalid: error={error}, count={count}")
    supported = {struct.unpack_from(">hhh", versions, 6 + index * 6)[0] for index in range(count)}
    if not {0, 3, 18}.issubset(supported):
        raise RuntimeError("Host Kafka listener is missing Produce/Metadata/ApiVersions APIs")
    metadata = kafka_request("127.0.0.1", 19092, 3, 0, struct.pack(">i", 0), 102)
    broker_count = struct.unpack_from(">i", metadata)[0]
    if broker_count != 1:
        raise RuntimeError(f"Expected one Kafka broker, received {broker_count}")
    broker_id, host_size = struct.unpack_from(">ih", metadata, 4)
    advertised_host = metadata[10:10 + host_size].decode("utf-8")
    advertised_port = struct.unpack_from(">i", metadata, 10 + host_size)[0]
    if (advertised_host, advertised_port) != ("localhost", 19092):
        raise RuntimeError(f"Incorrect Kafka host advertisement: {advertised_host}:{advertised_port}")
    return {"internal": "kafka_rlt_4_3_1:9092", "external": "localhost:19092", "broker_id": broker_id, "host_api_count": count}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--flink-url", default="http://127.0.0.1:8081")
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8083")
    parser.add_argument("--manifest", type=Path, default=ROOT / ".runtime/flink/connectors/manifest.json")
    parser.add_argument("--report", type=Path, default=ROOT / ".runtime/flink/health-report.json")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    report = {"status": "PASS", "checked_at": dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(), "checks": []}
    checks = [
        ("containers_networks_state", lambda: retry(check_containers, args.timeout)),
        ("connector_sha256_and_formats", lambda: check_jars(args.manifest)),
        ("flink_taskmanager_slots", lambda: retry(lambda: check_flink(args.flink_url.rstrip("/")), args.timeout)),
        ("sql_gateway_select_1", lambda: retry(lambda: check_gateway(args.gateway_url.rstrip("/"), args.timeout), args.timeout)),
        ("kafka_internal_and_host_metadata", lambda: retry(check_kafka, args.timeout)),
    ]
    for name, action in checks:
        started = time.monotonic()
        try:
            detail = action()
            report["checks"].append({"name": name, "status": "PASS", "detail": detail, "seconds": round(time.monotonic() - started, 2)})
            print(f"PASS  {name}", flush=True)
        except Exception as exc:
            report["status"] = "FAIL"
            report["checks"].append({"name": name, "status": "FAIL", "detail": str(exc), "seconds": round(time.monotonic() - started, 2)})
            print(f"FAIL  {name}: {exc}", file=sys.stderr, flush=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Health report: {args.report}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
