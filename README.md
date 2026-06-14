# Airflow + ClickHouse ETL Pipeline

## Quick Start

1. Copy .env.example to .env and edit passwords
2. Run: docker compose up -d
3. Wait 60 seconds
4. Trigger: docker compose exec airflow airflow dags trigger etl

## Access

Airflow UI: http://localhost:8080 \
ClickHouse: http://localhost:8123

## Stop

docker compose down \
to clear: docker system prune -a -f
