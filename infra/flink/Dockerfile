FROM flink:2.2.1-scala_2.12-java17

USER root
COPY --chown=flink:flink .runtime/flink/connectors/*.jar /opt/flink/lib/
COPY --chown=flink:flink .runtime/flink/connectors/manifest.json /opt/flink/connectors-manifest.json
RUN mkdir -p /opt/flink/state/checkpoints /opt/flink/state/savepoints \
    && chown -R flink:flink /opt/flink/state \
    && test -f /opt/flink/opt/flink-sql-gateway-2.2.1.jar \
    && ln -s /opt/flink/opt/flink-sql-gateway-2.2.1.jar /opt/flink/lib/flink-sql-gateway-2.2.1.jar
USER flink
