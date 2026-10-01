"""Build the pinned Flink connector distribution; no external Python packages needed."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import zipfile

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / ".runtime" / "flink" / "connectors"
DOWNLOADS = ROOT / ".runtime" / "flink" / "downloads"
BUILD = ROOT / ".runtime" / "flink" / "build"
POM = ROOT / "infra" / "flink" / "connectors" / "pom.xml"
LOCK = POM.parent / "dependencies.lock.json"
CENTRAL = "https://repo.maven.apache.org/maven2"
BASE_IMAGE = "flink:2.2.1-scala_2.12-java17"
PINNED = (
    ("org.apache.flink", "flink-sql-connector-kafka", "5.0.0-2.2"),
    ("org.apache.flink", "flink-sql-connector-mysql-cdc", "3.6.0-2.2"),
    ("org.apache.doris", "flink-doris-connector-2.2", "26.3.0"),
    ("com.mysql", "mysql-connector-j", "26.7.0"),
    ("com.google.protobuf", "protobuf-java", "4.31.1"),
)
FACTORIES = {
    "org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory",
    "org.apache.flink.streaming.connectors.kafka.table.UpsertKafkaDynamicTableFactory",
    "org.apache.flink.cdc.connectors.mysql.table.MySqlTableSourceFactory",
    "org.apache.flink.connector.jdbc.core.table.JdbcDynamicTableFactory",
    "org.apache.doris.flink.table.DorisDynamicTableFactory",
}
RELOCATED = (
    "com/fasterxml/jackson/", "org/apache/hc/", "org/apache/commons/",
    "org/yaml/snakeyaml/", "io/micrometer/", "org/HdrHistogram/", "org/LatencyUtils/",
)
DORIS_REMOVED_PREFIXES = (
    "org/slf4j/", "org/apache/logging/log4j/", "ch/qos/logback/",
    "META-INF/maven/org.slf4j/", "META-INF/maven/org.apache.logging.log4j/",
    "META-INF/maven/ch.qos.logback/",
)
DORIS_REMOVED_SERVICES = {
    "META-INF/services/org.slf4j.spi.SLF4JServiceProvider",
    "META-INF/services/org.apache.logging.log4j.spi.Provider",
    "META-INF/services/org.apache.logging.log4j.util.PropertySource",
}


def digest(path: Path, algorithm: str = "sha256") -> str:
    checksum = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def artifact_url(group: str, artifact: str, version: str, extension: str = "jar") -> str:
    return f"{CENTRAL}/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.{extension}"


def remote_checksum(url: str) -> tuple[str, str]:
    # Maven Central publishes SHA1 for older releases and SHA256 for newer ones.
    for algorithm in ("sha256", "sha1"):
        try:
            with urlopen(f"{url}.{algorithm}", timeout=45) as response:
                checksum = response.read().decode("ascii").strip().split()[0].lower()
            if re.fullmatch(r"[0-9a-f]{" + str(hashlib.new(algorithm).digest_size * 2) + r"}", checksum):
                return algorithm, checksum
            raise RuntimeError(f"Invalid {algorithm} checksum published at {url}")
        except HTTPError as error:
            if error.code != 404:
                raise
    raise RuntimeError(f"No official repository checksum available for {url}")


def verify_origin(path: Path, url: str) -> dict:
    algorithm, expected = remote_checksum(url)
    actual = digest(path, algorithm)
    if actual != expected:
        raise RuntimeError(f"Repository checksum mismatch: {path.name}")
    return {"algorithm": algorithm, "value": expected, "url": f"{url}.{algorithm}"}


def download(coordinate: tuple[str, str, str], force: bool) -> dict:
    group, artifact, version = coordinate
    name = f"{artifact}-{version}.jar"
    destination = (DOWNLOADS if group == "org.apache.doris" else OUTPUT) / name
    url = artifact_url(group, artifact, version)
    if force or not destination.exists():
        partial = destination.with_suffix(".jar.part")
        for attempt in range(1, 4):
            try:
                with urlopen(url, timeout=90) as response, partial.open("wb") as stream:
                    shutil.copyfileobj(response, stream, length=1024 * 1024)
                if not zipfile.is_zipfile(partial):
                    raise RuntimeError(f"Downloaded artifact is not a JAR: {name}")
                partial.replace(destination)
                break
            except (OSError, URLError) as error:
                partial.unlink(missing_ok=True)
                if attempt == 3:
                    raise RuntimeError(f"Download failed for {name}: {error}") from error
                time.sleep(attempt)
    origin = verify_origin(destination, url)
    print(f"Verified {name}", flush=True)
    return {
        "filename": name, "coordinate": ":".join(coordinate), "url": url,
        "sha256": digest(destination), "size_bytes": destination.stat().st_size,
        "repository_checksum": origin,
    }


def clean_doris(original: dict) -> dict:
    source = DOWNLOADS / original["filename"]
    name = source.stem + "-cleaned.jar"
    destination = OUTPUT / name
    removed = []
    with zipfile.ZipFile(source) as incoming, zipfile.ZipFile(destination, "w") as outgoing:
        for entry in incoming.infolist():
            if (entry.filename.startswith(DORIS_REMOVED_PREFIXES)
                    or entry.filename in DORIS_REMOVED_SERVICES
                    or entry.filename == "META-INF/log4j-provider.properties"
                    or re.fullmatch(r"META-INF/[^/]+\.(SF|DSA|RSA)", entry.filename)):
                removed.append(entry.filename)
                continue
            outgoing.writestr(entry, incoming.read(entry.filename))
    return {
        **original, "filename": name, "sha256": digest(destination),
        "size_bytes": destination.stat().st_size,
        "original_filename": original["filename"], "original_sha256": original["sha256"],
        "transformation": "Remove embedded logging classes and their SPI; use Flink's logging runtime",
        "removed_package_prefixes": list(DORIS_REMOVED_PREFIXES), "removed_entries": removed,
    }


def run_maven() -> Path:
    maven = shutil.which("mvn.cmd") or shutil.which("mvn")
    if not maven:
        raise RuntimeError("Maven 3.9+ and a JDK are required to package the JDBC runtime bundle.")
    logfile = BUILD / "maven.log"
    # The project has no Java sources: only released JARs are copied/shaded.
    command = [maven, "--batch-mode", "--no-transfer-progress", "-f", str(POM), "clean", "package"]
    print("Packaging JDBC/OpenLineage dependencies with Maven...", flush=True)
    with logfile.open("w", encoding="utf-8") as log:
        result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=False)
    if result.returncode:
        tail = logfile.read_text(encoding="utf-8", errors="replace").splitlines()[-35:]
        raise RuntimeError("Maven build failed (full output in .runtime/flink/build/maven.log):\n" + "\n".join(tail))
    return POM.parent / "target" / "flink-jdbc-mysql-bundle-4.1.0-2.2.jar"


def shade_inputs() -> list[dict]:
    records = []
    text = (POM.parent / "target" / "shade-inputs.txt").read_text(encoding="utf-8")
    # Maven dependency:list includes absolute paths; a Windows drive colon is kept
    # in the last field, rather than splitting the record on every colon.
    pattern = r"^\s*([^\s:]+):([^\s:]+):jar:([^\s:]+):(compile|runtime):(.+?)(?:\s+--\s+module\s+.*)?\s*$"
    for line in text.splitlines():
        match = re.match(pattern, line)
        if not match:
            continue
        group, artifact, version, scope, filename = match.groups()
        path = Path(filename.strip())
        if not path.is_file():
            raise RuntimeError(f"Cannot locate resolved Maven input: {artifact}-{version}")
        if group.startswith(("org.slf4j", "org.scala-lang", "org.apache.logging.log4j", "ch.qos.logback")):
            raise RuntimeError(f"A Flink-provided logging/Scala artifact was resolved: {group}:{artifact}")
        if group == "org.apache.flink" and artifact not in {"flink-connector-jdbc-core", "flink-connector-jdbc-mysql"}:
            raise RuntimeError(f"Flink runtime unexpectedly included in JDBC inputs: {artifact}")
        url = artifact_url(group, artifact, version)
        records.append({
            "coordinate": f"{group}:{artifact}:{version}", "filename": path.name,
            "scope": scope, "url": url, "sha256": digest(path),
            "repository_checksum": verify_origin(path, url),
        })
    if len(records) < 10:
        raise RuntimeError("Incomplete JDBC runtime dependency list; expected full OpenLineage transitive dependencies.")
    return sorted(records, key=lambda item: item["coordinate"])


def inspect_jars(artifacts: list[dict]) -> dict:
    owners = defaultdict(list)
    factories = set()
    native_resources = []
    upstream_optional_spi_warnings = []
    for artifact in artifacts:
        name = artifact["filename"]
        with zipfile.ZipFile(OUTPUT / name) as jar:
            entries = jar.namelist()
            duplicate_entries = len(entries) - len(set(entries))
            if duplicate_entries:
                raise RuntimeError(f"Duplicate ZIP entries in {name}")
            for entry in entries:
                if entry.endswith(".class") and not entry.endswith("module-info.class"):
                    owners[entry].append(name)
                if entry.startswith("META-INF/services/"):
                    for provider in jar.read(entry).decode("utf-8").splitlines():
                        provider = provider.split("#", 1)[0].strip()
                        if provider and entry == "META-INF/services/org.apache.flink.table.factories.Factory":
                            factories.add(provider)
                        if provider and provider.replace(".", "/") + ".class" not in entries:
                            if (name.startswith("flink-jdbc-mysql-bundle-")
                                    or entry == "META-INF/services/org.apache.flink.table.factories.Factory"):
                                raise RuntimeError(f"Missing SPI implementation {provider} in {name}")
                            # Some upstream SQL/fat JARs retain optional third-party SPI
                            # names from before their own relocations. These are not Flink
                            # factory services; preserve original artifacts and report them.
                            upstream_optional_spi_warnings.append({
                                "filename": name, "service": entry, "provider": provider,
                            })
            if name.startswith("flink-jdbc-mysql-bundle-"):
                if any(entry.startswith(RELOCATED) and entry.endswith(".class") for entry in entries):
                    raise RuntimeError("Unisolated third-party package found in the JDBC bundle")
                if any(entry.startswith(("org/slf4j/", "com/mysql/", "org/apache/flink/api/", "org/apache/flink/runtime/")) for entry in entries):
                    raise RuntimeError("Driver, logging API or Flink runtime leaked into JDBC bundle")
                if any(re.fullmatch(r"META-INF/[^/]+\.(SF|DSA|RSA)", entry) for entry in entries):
                    raise RuntimeError("Signature metadata survived JDBC shading")
                native_resources = [entry for entry in entries if entry.endswith((".so", ".dll", ".dylib"))]
                if not any(entry.endswith(".so") for entry in native_resources):
                    raise RuntimeError("OpenLineage Linux JNI resource missing from the JDBC bundle")
                if not any(entry.startswith("io/openlineage/sql/") and entry.endswith(".class") for entry in entries):
                    raise RuntimeError("OpenLineage JNI Java classes were unexpectedly relocated")
    duplicates = {entry: jars for entry, jars in owners.items() if len(jars) > 1}
    if duplicates:
        examples = list(duplicates.items())[:10]
        raise RuntimeError(f"Duplicate classes across installed connector JARs: {examples}")
    missing = FACTORIES - factories
    if missing:
        raise RuntimeError(f"Connector table factory SPI missing: {sorted(missing)}; found {sorted(factories)}")
    return {
        "duplicate_classes": 0, "class_count": len(owners), "table_factories": sorted(factories),
        "openlineage_native_resources": native_resources,
        "jdbc_third_party_packages_relocated": True,
        "jdbc_signatures_removed": True,
        "upstream_optional_spi_warnings": upstream_optional_spi_warnings,
        "driver_count": sum(item["coordinate"].startswith("com.mysql:mysql-connector-j:") for item in artifacts),
        "json_csv": "Provided by base image; confirmed separately by SQL Gateway integration acceptance",
    }


def check_dependency_lock(artifacts: list[dict], inputs: list[dict], write: bool) -> None:
    sources = [{
        "coordinate": item["coordinate"], "url": item["url"],
        "sha256": item.get("original_sha256", item["sha256"]),
    } for item in artifacts if not item["url"].startswith("local-build:")]
    sources.extend({"coordinate": item["coordinate"], "url": item["url"], "sha256": item["sha256"]} for item in inputs)
    lock = {
        "schema_version": 1, "base_image": BASE_IMAGE,
        "sources": sorted(sources, key=lambda item: item["coordinate"]),
    }
    if write:
        LOCK.write_text(json.dumps(lock, indent=2) + "\n", encoding="utf-8")
    elif not LOCK.is_file():
        raise RuntimeError("Source dependency lock missing; run with --write-lock when initially pinning dependencies.")
    elif json.loads(LOCK.read_text(encoding="utf-8")) != lock:
        raise RuntimeError("Resolved artifact bytes or coordinates differ from dependencies.lock.json; inspect before updating the lock.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force-download", action="store_true", help="Download official artifacts again instead of rechecking the cache")
    parser.add_argument("--write-lock", action="store_true", help="Explicitly pin resolved source artifact SHA256 values in the tracked dependency lock")
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    BUILD.mkdir(parents=True, exist_ok=True)
    artifacts = []
    for coordinate in PINNED:
        artifact = download(coordinate, args.force_download)
        artifacts.append(clean_doris(artifact) if coordinate[0] == "org.apache.doris" else artifact)
    bundle = run_maven()
    inputs = shade_inputs()
    destination = OUTPUT / bundle.name
    shutil.copyfile(bundle, destination)
    artifacts.append({
        "filename": destination.name, "coordinate": "com.fakedataworks.flink:flink-jdbc-mysql-bundle:4.1.0-2.2",
        "url": "local-build:infra/flink/connectors/pom.xml", "sha256": digest(destination),
        "size_bytes": destination.stat().st_size, "pom_sha256": digest(POM), "shade_inputs": inputs,
        "shade_plugin": "org.apache.maven.plugins:maven-shade-plugin:3.6.1",
    })
    expected_names = {item["filename"] for item in artifacts}
    unexpected = [file.name for file in OUTPUT.glob("*.jar") if file.name not in expected_names]
    if unexpected:
        raise RuntimeError(f"Stale connector JARs in output directory; remove them before building: {unexpected}")
    checks = inspect_jars(artifacts)
    check_dependency_lock(artifacts, inputs, args.write_lock)
    manifest = {
        "schema_version": 1, "base_image": BASE_IMAGE, "flink_version": "2.2.1",
        "artifacts": artifacts, "verification": checks,
    }
    manifest_path = OUTPUT / "manifest.json"
    previous = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {}
    previous_time = previous.pop("generated_at_utc", None)
    # Preserve timestamps/content on identical rebuilds, keeping Docker COPY cache
    # and the host/container checksum manifests stable.
    manifest["generated_at_utc"] = previous_time if previous == manifest else datetime.now(timezone.utc).isoformat()
    encoded = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    if not manifest_path.is_file() or manifest_path.read_text(encoding="utf-8") != encoded:
        manifest_path.write_text(encoded, encoding="utf-8")
    print(f"Built and verified {len(artifacts)} connector JARs; {len(inputs)} JDBC shade inputs.", flush=True)
    print(f"Manifest: {OUTPUT / 'manifest.json'}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, URLError, RuntimeError, zipfile.BadZipFile) as error:
        print(f"Connector build failed: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1)
