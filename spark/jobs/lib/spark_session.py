import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    s3_bucket = os.environ.get("S3_BUCKET", "lakehouse")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.environ.get("S3_ACCESS_KEY", "minio")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "minio12345")

    # The catalog is configured via spark-defaults.conf in docker-compose.
    # We still keep S3 overrides here to make local runs more robust.
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.sql.warehouse.dir", f"s3a://{s3_bucket}/warehouse")
        .getOrCreate()
    )

