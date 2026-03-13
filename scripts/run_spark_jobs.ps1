$ErrorActionPreference = "Stop"

# Runs Spark jobs inside the spark container with required Maven packages.

$packages = @(
  "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
  "org.apache.hadoop:hadoop-aws:3.3.6",
  "com.amazonaws:aws-java-sdk-bundle:1.12.262",
  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
  "com.clickhouse:clickhouse-jdbc:0.6.0"
)

$job = $args[0]
if ([string]::IsNullOrWhiteSpace($job)) {
  Write-Host "Usage: .\scripts\run_spark_jobs.ps1 <job_py_path_inside_container>"
  Write-Host "Example: .\scripts\run_spark_jobs.ps1 /opt/spark-apps/jobs/scd2_merge_customers_silver.py"
  exit 1
}

$packagesCsv = ($packages -join ",")

docker compose exec -T spark bash -lc "/opt/bitnami/spark/bin/spark-submit --master spark://spark:7077 --packages '$packagesCsv' $job"

