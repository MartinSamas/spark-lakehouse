"""Bronze ingestion for `crm_events` and `employee_surveys` (JSONL daily batches).

- `crm_events/date=<batch_date>/part-*.jsonl`: the polymorphic `payload` object is kept as
  its raw JSON text (StringType), so no event-type-specific field is ever dropped.
  `timestamp` stays a raw ISO-8601 string (parsed to TIMESTAMP in Silver).
- `employee_surveys/date=<batch_date>/part-*.jsonl`: the nested `answers` array is kept as
  raw JSON text; Silver parses, explodes and validates it. Surveys only drop on pulse-wave
  dates (e.g. Days 9-16 for Wave 1), so a missing partition is skipped, not an error.

Malformed JSON lines are captured in `_corrupt_record` rather than dropped. Both tables are
written partitioned by `_batch_date`, replacing only the target partition.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

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

CRM_EVENTS_RAW_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        # Nested JSON object captured verbatim as a JSON string.
        StructField("payload", StringType(), nullable=True),
        StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
    ]
)

EMPLOYEE_SURVEYS_RAW_SCHEMA = StructType(
    [
        StructField("survey_id", StringType(), nullable=True),
        StructField("emp_id", StringType(), nullable=True),
        StructField("submitted_at", StringType(), nullable=True),
        StructField("enps_score", LongType(), nullable=True),
        StructField("overall_comment", StringType(), nullable=True),
        # Nested array<struct<question_id, dimension, numeric_score, written_answer>>
        # captured verbatim as a JSON string.
        StructField("answers", StringType(), nullable=True),
        StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
    ]
)


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Ingest CRM events and employee surveys for `batch_date`; return rows written per table.

    `employee_surveys` is reported with a count of 0 on dates without a pulse-wave drop.
    """
    batch_date = validate_batch_date(batch_date)
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)

    def _jsonl_reader(schema: StructType):
        return lambda path: (
            spark.read.option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
            .option("pathGlobFilter", "part-*.jsonl")
            .schema(schema)
            .json(path)
        )

    sources = {
        # table: (reader, required)
        "crm_events": (_jsonl_reader(CRM_EVENTS_RAW_SCHEMA), True),
        "employee_surveys": (_jsonl_reader(EMPLOYEE_SURVEYS_RAW_SCHEMA), False),
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
    parser = argparse.ArgumentParser(description="Ingest CRM & Survey datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-crm-surveys")
    try:
        counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze CRM & Surveys ingest batch_date={args.batch_date}: {counts}")
    finally:
        spark.stop()
