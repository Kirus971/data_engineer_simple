$ErrorActionPreference = "Stop"

# End-to-end demo runner (Windows / PowerShell)

Write-Host "1) Starting stack..."
docker compose up -d

Write-Host "2) Starting Kafka -> Iceberg Bronze streaming job (runs in background terminal)..."
Start-Process powershell -ArgumentList @(
  "-NoProfile",
  "-Command",
  ".\scripts\run_spark_jobs.ps1 /opt/spark-apps/jobs/stream_kafka_to_iceberg_bronze.py"
)

Write-Host "3) Producing CDC events for 30s..."
docker compose exec -T de-tools bash -lc "EVENTS_PER_SEC=10 MAX_CUSTOMER_ID=200 timeout 30s python scripts/kafka_produce_customer_cdc.py || true" | Out-Null

Write-Host "4) Running SCD2 merge (silver)..."
.\scripts\run_spark_jobs.ps1 /opt/spark-apps/jobs/scd2_merge_customers_silver.py

Write-Host "5) Building gold aggregate..."
.\scripts\run_spark_jobs.ps1 /opt/spark-apps/jobs/build_gold_customer_daily.py

Write-Host "6) Exporting gold to ClickHouse via JDBC..."
.\scripts\run_spark_jobs.ps1 /opt/spark-apps/jobs/export_gold_to_clickhouse_jdbc.py

Write-Host "Done. Check ClickHouse table analytics.customer_daily."

