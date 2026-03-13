import os

from pyspark.sql import functions as F

from jobs.lib.spark_session import build_spark


def main() -> None:
    spark = build_spark("export_gold_to_clickhouse_jdbc")

    ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    ch_port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    ch_db = os.environ.get("CLICKHOUSE_DATABASE", "analytics")

    jdbc_url = f"jdbc:clickhouse://{ch_host}:{ch_port}/{ch_db}"

    gold = spark.table("hms_cat.gold.customer_daily").withColumn("day", F.col("day").cast("date"))

    # Demo approach: overwrite full table (small). For production prefer partitioned loads.
    (
        gold.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "customer_daily")
        .option("user", ch_user)
        .option("password", ch_password)
        .mode("overwrite")
        .save()
    )


if __name__ == "__main__":
    main()

