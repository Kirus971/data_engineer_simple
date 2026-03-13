# data_engineer_simple

## Senior Data Engineer Platform (Airflow + Kafka + Spark + Iceberg + MinIO + HMS + ClickHouse)

Production-like локальный стенд :

- **Orchestration**: Apache Airflow
- **Streaming**: Kafka
- **Lakehouse**: S3 (MinIO) + Apache Iceberg
- **Catalog/Metastore**: Hive Metastore (HMS) + Postgres
- **Compute**: Spark (batch + Structured Streaming)
- **Serving / OLAP**: ClickHouse

### Архитектура данных

- **Bronze**: raw/append-only события (из Kafka или batch источников) в Iceberg
- **Silver**: очищенные, дедуплицированные таблицы + **SCD2** (Iceberg MERGE)
- **Gold**: агрегаты/витрины (Iceberg) + выгрузка в ClickHouse

### Быстрый старт

1. Предпосылки: Docker Desktop, 8+ GB RAM (лучше 12+), 4+ CPU.
2. Создайте `.env`:

```bash
cp .env.example .env
```

1. Поднимите стек:

```bash
docker compose up -d
```

1. UI:

- **Airflow**: `http://localhost:8080` (login/password см. `.env`)
- **MinIO Console**: `http://localhost:9001`
- **Kafka UI**: `http://localhost:8082`
- **ClickHouse**: `http://localhost:8123`

### Пайплайны (пример)

- `**demo_incremental_bronze_silver_gold`**:
  - генерирует изменения “customers” (источник) и кладет в Kafka
  - Spark streaming пишет события в **bronze** Iceberg
  - Spark batch делает **SCD2 merge** в **silver**
  - Spark batch строит **gold** агрегаты
  - Airflow грузит gold в **ClickHouse**

### Где что лежит

- `**docker-compose.yml`**: весь стенд
- `**airflow/dags/**`: DAG'и
- `**spark/jobs/**`: Spark jobs (batch/streaming)
- `**infra/**`: конфиги Iceberg/HMS/Spark/ClickHouse
- `**scripts/**`: init-скрипты (buckets/topics/tables)

### Заметки

- Это шаблон для GitHub портфолио: с фокусом на читаемость, слои данных, инкрементальность и SCD2.
- Для “боевого” окружения добавьте: IaC (Terraform), секреты (Vault), наблюдаемость (Prometheus/Grafana), CI/CD.
