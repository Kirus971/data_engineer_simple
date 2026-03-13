from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="demo_incremental_bronze_silver_gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    dagrun_timeout=timedelta(hours=2),
    tags=["demo", "lakehouse", "scd2", "iceberg"],
) as dag:
    produce_customer_cdc_30s = BashOperator(
        task_id="produce_customer_cdc_30s",
        bash_command=(
            "EVENTS_PER_SEC=10 MAX_CUSTOMER_ID=200 "
            "timeout 30s python /opt/airflow/project-scripts/kafka_produce_customer_cdc.py || true"
        ),
    )

    note_run_spark_jobs = BashOperator(
        task_id="note_run_spark_jobs",
        bash_command=(
            "echo 'Spark jobs are run inside the spark container (see scripts/run_spark_jobs.*). '"
            "&& echo '1) stream_kafka_to_iceberg_bronze.py (streaming, keep running)'"
            "&& echo '2) scd2_merge_customers_silver.py (batch incremental) '"
            "&& echo '3) build_gold_customer_daily.py (batch) '"
            "&& echo '4) export_gold_to_clickhouse_jdbc.py (batch) '"
        ),
    )

    produce_customer_cdc_30s >> note_run_spark_jobs

