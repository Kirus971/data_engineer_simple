import os

import clickhouse_connect


def main() -> None:
    host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    database = os.environ.get("CLICKHOUSE_DATABASE", "analytics")

    # In a real pipeline we'd read from Iceberg via Spark and then load.
    # For the local demo, assume you materialize gold as CSV/Parquet first OR
    # query ClickHouse after Spark inserts. This script provides a simple
    # idempotent upsert-by-replace for the demo table.
    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database
    )

    # Demo: keep table clean for reruns
    client.command("TRUNCATE TABLE IF EXISTS analytics.customer_daily")

    # Expect data prepared by user as env var CSV lines: day,status,cnt
    raw = os.environ.get("CUSTOMER_DAILY_ROWS", "").strip()
    if not raw:
        print(
            "CUSTOMER_DAILY_ROWS is empty. Provide rows like: 2026-03-13,active,10\\n2026-03-13,vip,2"
        )
        return

    rows = []
    for line in raw.splitlines():
        day, status, cnt = [x.strip() for x in line.split(",")]
        rows.append((day, status, int(cnt)))

    client.insert(
        "analytics.customer_daily",
        rows,
        column_names=["day", "customer_status", "cnt"],
    )
    print(f"Inserted {len(rows)} rows into analytics.customer_daily")


if __name__ == "__main__":
    main()

