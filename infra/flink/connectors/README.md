# Flink 2.2.1 连接器构建

在仓库根目录执行：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' scripts/build-flink-connectors.py
# 同时构建包含连接器的 Flink Docker 镜像：
.\scripts\build-flink.ps1
# 仅准备 JAR，不调用 Docker：
.\scripts\build-flink.ps1 -ConnectorsOnly
```

要求宿主机安装 Maven 3.9+、JDK；构建只打包正式发布的 JAR，没有 Java 源码编译。Python 脚本仅依赖标准库。输出目录是 `.runtime/flink/connectors/`，包含六个最终 JAR 和 `manifest.json`。原始 Doris JAR 单独保存在 `.runtime/flink/downloads/`，Maven 日志保存在 `.runtime/flink/build/maven.log`；这些文件不提交 Git。

`dependencies.lock.json` 固定五个正式制品与全部 JDBC shade 输入的 Maven 坐标、Maven Central URL 和 SHA256。每次构建验证官方仓库公布的 SHA256 或 SHA1，并与此锁文件的 SHA256 比较。依赖坐标或字节发生变化时构建失败。明确审核新的依赖版本后，可以使用 `--write-lock` 更新锁文件；`--force-download` 重新下载正式制品。

| 功能 | 坐标/版本 |
| --- | --- |
| Kafka / Upsert Kafka | `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2` |
| MySQL CDC | `org.apache.flink:flink-sql-connector-mysql-cdc:3.6.0-2.2` |
| JDBC core / MySQL | `org.apache.flink:flink-connector-jdbc-core:4.1.0-2.2`、`org.apache.flink:flink-connector-jdbc-mysql:4.1.0-2.2` |
| OpenLineage Java / SQL JNI | `io.openlineage:openlineage-java:1.32.0`、`io.openlineage:openlineage-sql-java:1.32.0` |
| Doris | `org.apache.doris:flink-doris-connector-2.2:26.3.0` |
| 独立 MySQL JDBC 驱动 | `com.mysql:mysql-connector-j:26.7.0` |
| 独立 Protobuf | `com.google.protobuf:protobuf-java:4.31.1` |

## JDBC 隔离与 SPI

JDBC core、MySQL 和完整非 optional 运行依赖打成 `flink-jdbc-mysql-bundle-4.1.0-2.2.jar`。Jackson、HTTPClient/Core、Commons、SnakeYAML、Micrometer、HdrHistogram 和 LatencyUtils 包移到 `com.fakedataworks.flink.jdbc.shaded` 命名空间；JDBC 本身和 `io.openlineage.*` 不搬迁。OpenLineage JNI 的 Linux x86_64/aarch64 `.so` 和 macOS `.dylib` 原样保留，避免破坏 Java/native 符号关联。

使用 `ServicesResourceTransformer` 合并并改写 SPI，没有启用 minimize。移除旧签名、模块描述、Flink 本体、Scala、SLF4J API 和日志实现。MySQL 驱动与 Protobuf 仅独立安装一份，不进入 JDBC bundle。JDBC MySQL 的传递 `flink-connector-base:2.0.0` 排除，使用 Flink 基础镜像提供的 2.2.1 版本。OpenLineage SQL 原始 JAR 内嵌 Commons Lang 3.14.0，过滤该副本，统一使用 OpenLineage Java 的正式非 optional 依赖 Commons Lang 3.17.0。

## Doris 可追溯清理

正式 Doris fat JAR 内嵌 SLF4J API 1.7.36 共 34 个 class，与 Flink 基础镜像的日志 API 重复；未发现 StaticLoggerBinder、SLF4JServiceProvider、Log4j 或 Logback 实现。构建生成带 `-cleaned.jar` 后缀的副本，剔除 SLF4J/Log4j/Logback 包和相关 SPI/签名，使用 Flink 基础镜像的日志栈。清单记录原始与最终 SHA256、原始下载 URL、实际移除条目和规则。其余内容保留。

## 构建检查和运行验收

构建强制检查最终 JAR 之间没有重复 class、每个 Flink 表工厂 SPI 对应的 class 存在、JDBC 所有 SPI provider class 存在、第三方包隔离和 OpenLineage native 资源完好。`manifest.json` 还记录全部 shade 输入与 SHA256。官方 CDC/Doris fat JAR 某些可选第三方 SPI 的名字没有随上游内部 shade 搬迁，构建将其记录到 `upstream_optional_spi_warnings`；Flink connector Factory SPI 依然强制校验。JSON/CSV 使用基础镜像自带的实现。

静态 JAR 检查不能替代 SQL Gateway、Checkpoint/Savepoint 与真实 MySQL/Kafka/Doris 链路验收；完整验收由仓库的 Flink 验证脚本完成。
