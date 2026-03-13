import os
from datetime import timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from jobs.lib.spark_session import build_spark


def main() -> None:
    spark = build_spark("scd2_merge_customers_silver")

    pipeline = os.environ.get("PIPELINE_NAME", "dim_customer_scd2")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hms_cat.meta")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS hms_cat.meta.watermarks (
          pipeline STRING,
          updated_at_gte TIMESTAMP,
          written_at TIMESTAMP
        )
        USING iceberg
        """
    )

    last_wm_row = (
        spark.table("hms_cat.meta.watermarks")
        .where(F.col("pipeline") == F.lit(pipeline))
        .agg(F.max("updated_at_gte").alias("wm"))
        .collect()[0]["wm"]
    )
    last_wm = last_wm_row.isoformat() if last_wm_row else None

    bronze = spark.table("hms_cat.bronze.customer_cdc")
    if last_wm:
        bronze = bronze.where(F.col("updated_at") >= F.to_timestamp(F.lit(last_wm)))

    # Dedupe to latest change per customer within this batch.
    w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc(), F.col("ingest_ts").desc())
    batch_latest = (
        bronze.withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
        .drop("rn")
        .withColumn("business_key", F.col("customer_id").cast("string"))
        .withColumn("hashdiff", F.sha2(F.concat_ws("||", F.col("name"), F.col("status")), 256))
        .select("customer_id", "name", "status", "updated_at", "business_key", "hashdiff")
    )

    if batch_latest.rdd.isEmpty():
        return

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hms_cat.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS hms_cat.silver.dim_customer (
          customer_id INT,
          name STRING,
          status STRING,
          business_key STRING,
          hashdiff STRING,
          effective_from TIMESTAMP,
          effective_to TIMESTAMP,
          is_current BOOLEAN,
          updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(effective_from))
        """
    )

    batch_latest.createOrReplaceTempView("stg_customer")

    # SCD2 in Iceberg via MERGE:
    # 1) If current record exists and attributes changed -> close it (set effective_to, is_current=false)
    # 2) Insert new current version
    spark.sql(
        """
        MERGE INTO hms_cat.silver.dim_customer t
        USING stg_customer s
        ON t.business_key = s.business_key AND t.is_current = true
        WHEN MATCHED AND t.hashdiff <> s.hashdiff THEN
          UPDATE SET
            t.effective_to = s.updated_at,
            t.is_current = false
        WHEN NOT MATCHED THEN
          INSERT (
            customer_id, name, status, business_key, hashdiff,
            effective_from, effective_to, is_current, updated_at
          )
          VALUES (
            s.customer_id, s.name, s.status, s.business_key, s.hashdiff,
            s.updated_at, TIMESTAMP('9999-12-31 00:00:00'), true, s.updated_at
          )
        """
    )

    # For changed rows we also need to insert the new current version. Iceberg MERGE doesn't support
    # "update then insert" in a single matched branch, so do a second insert for changed keys.
    spark.sql(
        """
        INSERT INTO hms_cat.silver.dim_customer
        SELECT
          s.customer_id,
          s.name,
          s.status,
          s.business_key,
          s.hashdiff,
          s.updated_at AS effective_from,
          TIMESTAMP('9999-12-31 00:00:00') AS effective_to,
          true AS is_current,
          s.updated_at
        FROM stg_customer s
        INNER JOIN hms_cat.silver.dim_customer t
          ON t.business_key = s.business_key
        WHERE t.effective_to = s.updated_at AND t.is_current = false
        """
    )

    new_wm = batch_latest.agg(F.max("updated_at").alias("mx")).collect()[0]["mx"]
    if new_wm:
        wm_df = spark.createDataFrame([(pipeline, new_wm)], "pipeline string, updated_at_gte timestamp").withColumn(
            "written_at", F.current_timestamp()
        )
        wm_df.writeTo("hms_cat.meta.watermarks").append()


if __name__ == "__main__":
    main()

