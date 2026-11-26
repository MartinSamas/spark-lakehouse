"""Bronze ingestion for HR domain (`employees` CSV snapshots and `timesheets` Parquet).

- `employees/date=<batch_date>/part-*.csv`: daily full snapshot, all columns kept as raw
  strings (dates, booleans and rates are typed in Silver).
- `timesheets/date=<batch_date>/part-*.parquet`: read with its native Parquet schema
  (already typed upstream; no casting applied).

Both are written partitioned by `_batch_date`, replacing only the target partition so
re-runs are idempotent. Both daily partitions are mandatory: a missing file fails the job.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.2, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from spark_jobs.bronze._io import (
    add_bronze_metadata,
    read_daily_partition,
    validate_batch_date,
    write_daily_partition,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

EMPLOYEES_RAW_SCHEMA = StructType(
    [
        StructField(name, StringType(), nullable=True)
        for name in (
            "emp_id",
            "full_name",
            "department",
            "role",
            "seniority_level",
            "hourly_rate_eur",
            "hire_date",
            "last_promotion_date",
            "work_mode",
            "distance_km",
            "manager_id",
            "is_active",
            "termination_date",
            "churn_type",
            "snapshot_date",
        )
    ]
)


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Ingest `employees` and `timesheets` for `batch_date`; return rows written per table."""
    batch_date = validate_batch_date(batch_date)
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)

    readers = {
        "employees": lambda path: (
            spark.read.option("header", "true")
            .option("pathGlobFilter", "part-*.csv")
            .schema(EMPLOYEES_RAW_SCHEMA)
            .csv(path)
        ),
        "timesheets": lambda path: spark.read.option("pathGlobFilter", "part-*.parquet").parquet(
            path
        ),
    }

    counts: dict[str, int] = {}
    for table, reader in readers.items():
        raw_df = read_daily_partition(spark, landing, table, batch_date, reader)
        bronze_df = add_bronze_metadata(raw_df, batch_date)
        write_daily_partition(bronze_df, join_path(lakehouse, "bronze", table), batch_date)
        counts[table] = (
            spark.read.format("delta")
            .load(join_path(lakehouse, "bronze", table))
            .where(f"_batch_date = '{batch_date}'")
            .count()
        )
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest HR datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-hr")
    try:
        counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze HR ingest batch_date={args.batch_date}: {counts}")
    finally:
        spark.stop()
