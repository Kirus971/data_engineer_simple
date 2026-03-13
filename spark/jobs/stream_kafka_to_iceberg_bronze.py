import os

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from jobs.lib.spark_session import build_spark


def main() -> None:
    spark = build_spark("stream_kafka_to_iceberg_bronze")

    brokers = os.environ.get("KAFKA_BROKERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC_CUSTOMER_CDC", "customer_cdc")
    s3_bucket = os.environ.get("S3_BUCKET", "lakehouse")

    schema = StructType(
        [
            StructField("op", StringType(), False),
            StructField("event_ts", StringType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("updated_at", StringType(), False),
            StructField("source", StringType(), True),
        ]
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        df.select(F.col("value").cast("string").alias("json_str"))
        .select(F.from_json("json_str", schema).alias("r"))
        .select("r.*")
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hms_cat.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS hms_cat.bronze.customer_cdc (
          op STRING,
          event_ts TIMESTAMP,
          customer_id INT,
          name STRING,
          status STRING,
          updated_at TIMESTAMP,
          source STRING,
          ingest_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(updated_at))
        """
    )

    checkpoint = f"s3a://{s3_bucket}/checkpoints/bronze/customer_cdc"

    (
        parsed.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .toTable("hms_cat.bronze.customer_cdc")
    ).awaitTermination()


if __name__ == "__main__":
    main()

