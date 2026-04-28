ARG BASE_IMAGE
ARG KAFKA_SQL_CONNECTOR_JAR=flink-sql-connector-kafka-3.3.0.jar
ARG KAFKA_CLIENTS_JAR=kafka-clients-3.4.0.jar
ARG ROCKSDB_OPTIONS_JAR=rocksdb-options-1.0-SNAPSHOT.jar

FROM ${BASE_IMAGE}

ARG KAFKA_SQL_CONNECTOR_JAR
ARG KAFKA_CLIENTS_JAR
ARG ROCKSDB_OPTIONS_JAR

RUN mkdir -p /opt/flink/examples/justin
COPY --chown=flink:flink datastream-jars/ /opt/flink/examples/justin/
COPY --chown=flink:flink nexmark-flink-0.3-SNAPSHOT.jar /opt/flink/lib/nexmark-flink-0.3-SNAPSHOT.jar
COPY --chown=flink:flink ${ROCKSDB_OPTIONS_JAR} /opt/flink/lib/${ROCKSDB_OPTIONS_JAR}
COPY --chown=flink:flink ${KAFKA_SQL_CONNECTOR_JAR} /opt/flink/lib/${KAFKA_SQL_CONNECTOR_JAR}
COPY --chown=flink:flink ${KAFKA_CLIENTS_JAR} /opt/flink/lib/${KAFKA_CLIENTS_JAR}
