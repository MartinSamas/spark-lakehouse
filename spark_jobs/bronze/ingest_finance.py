"""Bronze ingestion for Finance domain (`fx_rates`, `invoices`, `expenses`).

- `fx_rates/date=<batch_date>/part-*.json`: multiline JSON array, **weekdays only**.
  A missing partition (weekend/holiday) is skipped without failing; the weekday
  completeness check belongs to the Bronze DQ gate and the LOCF forward-fill to Silver.
- `invoices/date=<batch_date>/part-*.jsonl`: `issued_at` / `paid_at` kept as raw strings
  (mixed ISO `YYYY-MM-DD` and European `DD.MM.YYYY`); negative amounts kept (credit notes).
- `expenses/date=<batch_date>/part-*.csv`: all columns kept as raw strings.

Malformed JSON records are captured in `_corrupt_record` rather than dropped. Each table is
written partitioned by `_batch_date`, replacing only the target partition.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.4, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_jobs.bronze._io import (
    CORRUPT_RECORD_COL,
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

FX_RATES_RAW_SCHEMA = StructType(
    [
        StructField("rate_date", StringType(), nullable=True),
        StructField("currency_from", StringType(), nullable=True),
        StructField("currency_to", StringType(), nullable=True),
        StructField("rate", DoubleType(), nullable=True),
        StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
    ]
)

INVOICES_RAW_SCHEMA = StructType(
    [
        StructField("inv_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("issued_at", StringType(), nullable=True),
        StructField("paid_at", StringType(), nullable=True),
        StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
    ]
)

EXPENSES_RAW_SCHEMA = StructType(
    [
        StructField(name, StringType(), nullable=True)
        for name in ("exp_id", "department", "category", "amount", "currency", "expense_date")
    ]
)


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Ingest Finance datasets for `batch_date`; return rows written per table.

    Tables whose landing partition is legitimately absent (weekend `fx_rates`) are
    reported with a count of 0 and left untouched in Bronze.
    """
    batch_date = validate_batch_date(batch_date)
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)

    def _json_reader(schema: StructType, glob: str, multiline: bool):
        return lambda path: (
            spark.read.option("multiLine", str(multiline).lower())
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
            .option("pathGlobFilter", glob)
            .schema(schema)
            .json(path)
        )

    sources = {
        # table: (reader, required)
        "fx_rates": (_json_reader(FX_RATES_RAW_SCHEMA, "part-*.json", multiline=True), False),
        "invoices": (_json_reader(INVOICES_RAW_SCHEMA, "part-*.jsonl", multiline=False), True),
        "expenses": (
            lambda path: (
                spark.read.option("header", "true")
                .option("pathGlobFilter", "part-*.csv")
                .schema(EXPENSES_RAW_SCHEMA)
                .csv(path)
            ),
            True,
        ),
    }

    counts: dict[str, int] = {}
    for table, (reader, required) in sources.items():
        raw_df = read_daily_partition(spark, landing, table, batch_date, reader, required=required)
        if raw_df is None:
            counts[table] = 0
            continue
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
    parser = argparse.ArgumentParser(description="Ingest Finance datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-finance")
    try:
        counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze Finance ingest batch_date={args.batch_date}: {counts}")
    finally:
        spark.stop()
