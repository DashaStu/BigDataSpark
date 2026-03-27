FROM public.ecr.aws/bitnami/spark:3.5

USER root
RUN apt-get update && apt-get install -y curl
RUN pip install requests

# Качаем максимально стабильный драйвер
RUN curl -L https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.3.2-patch11/clickhouse-jdbc-0.3.2-patch11-all.jar -o /opt/bitnami/spark/jars/clickhouse-jdbc.jar && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o /opt/bitnami/spark/jars/postgresql.jar

WORKDIR /opt/bitnami/spark/app
COPY scripts/ /opt/bitnami/spark/app/scripts/

USER 1001