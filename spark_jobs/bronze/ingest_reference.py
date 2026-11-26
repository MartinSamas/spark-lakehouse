"""Bronze ingestion for `projects` and `survey_questions` (full-load reference datasets).

- `projects/projects.csv`: CSV with header, all columns kept as raw strings.
- `survey_questions/survey_questions.json`: multiline JSON array of 10 question definitions.

Both are full loads, so the Bronze Delta tables are overwritten on every run (idempotent;
older snapshots stay available through Delta time travel).

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_jobs.bronze._io import (
    CORRUPT_RECORD_COL,
    add_bronze_metadata,
    validate_batch_date,
    write_full_snapshot,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

PROJECTS_RAW_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("budget_eur", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)

SURVEY_QUESTIONS_RAW_SCHEMA = StructType(
    [
        StructField("question_id", StringType(), nullable=True),
        StructField("dimension", StringType(), nullable=True),
        StructField("scale_min", LongType(), nullable=True),
        StructField("scale_max", LongType(), nullable=True),
        StructField("prompt_text", StringType(), nullable=True),
        StructField(CORRUPT_RECORD_COL, StringType(), nullable=True),
    ]
)


def ingest_projects(
    spark: SparkSession, batch_date: str, landing_root: str, lakehouse_root: str
) -> DataFrame:
    source_path = join_path(landing_root, "projects", "projects.csv")
    raw_df = spark.read.option("header", "true").schema(PROJECTS_RAW_SCHEMA).csv(source_path)
    bronze_df = add_bronze_metadata(raw_df, batch_date)
    write_full_snapshot(bronze_df, join_path(lakehouse_root, "bronze", "projects"))
    return bronze_df


def ingest_survey_questions(
    spark: SparkSession, batch_date: str, landing_root: str, lakehouse_root: str
) -> DataFrame:
    source_path = join_path(landing_root, "survey_questions", "survey_questions.json")
    raw_df = (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .schema(SURVEY_QUESTIONS_RAW_SCHEMA)
        .json(source_path)
    )
    bronze_df = add_bronze_metadata(raw_df, batch_date)
    write_full_snapshot(bronze_df, join_path(lakehouse_root, "bronze", "survey_questions"))
    return bronze_df


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Ingest `projects` and `survey_questions`; return Bronze row counts per table."""
    batch_date = validate_batch_date(batch_date)
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)

    counts: dict[str, int] = {}
    for table, ingest in (
        ("projects", ingest_projects),
        ("survey_questions", ingest_survey_questions),
    ):
        ingest(spark, batch_date, landing, lakehouse)
        counts[table] = (
            spark.read.format("delta").load(join_path(lakehouse, "bronze", table)).count()
        )
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest reference datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-reference")
    try:
        counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze reference ingest batch_date={args.batch_date}: {counts}")
    finally:
        spark.stop()
