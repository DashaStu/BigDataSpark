#!/bin/bash



while ! timeout 1s bash -c "echo > /dev/tcp/postgres/5432" 2>/dev/null; do
  sleep 2
done


while ! timeout 1s bash -c "echo > /dev/tcp/clickhouse/8123" 2>/dev/null; do
  sleep 2
done


spark-submit /opt/bitnami/spark/app/scripts/etl.py
echo "ETL завершен. Контейнер остается в режиме ожидания."
tail -f /dev/null