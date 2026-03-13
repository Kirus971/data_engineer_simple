from pyspark.sql import functions as F

from jobs.lib.spark_session import build_spark


def main() -> None:
    spark = build_spark("build_gold_customer_daily")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hms_cat.gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS hms_cat.gold.customer_daily (
          day DATE,
          customer_status STRING,
          cnt BIGINT
        )
        USING iceberg
        PARTITIONED BY (day)
        """
    )

    silver = spark.table("hms_cat.silver.dim_customer").where("is_current = true")

    gold = (
        silver.withColumn("day", F.to_date("effective_from"))
        .groupBy("day", F.col("status").alias("customer_status"))
        .agg(F.countDistinct("customer_id").alias("cnt"))
    )

    # idempotent daily overwrite (demo). In real systems prefer MERGE/partition overwrite.
    (
        gold.writeTo("hms_cat.gold.customer_daily")
        .overwritePartitions()
    )


if __name__ == "__main__":
    main()

