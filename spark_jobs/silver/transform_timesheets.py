"""Silver transformation & quarantine for `silver_timesheets`.

- Validates each row and routes failures to `silver_quarantine/timesheets` with
  `dq_reasons`: `missing_ts_id`, `missing_emp_id`, `missing_work_date`,
  `missing_project_id`, `orphan_project_id`, `missing_hours`, `non_positive_hours`,
  `hours_exceed_24`, `unknown_employee_on_work_date`, `post_termination_work`.
- Post-termination ("zombie") entries are detected with a point-in-time join against
  `silver_employees_scd2` (`work_date BETWEEN valid_from AND valid_to`), flagged when
  `work_date >= termination_date`.
- Deduplicates on `ts_id` with a deterministic tie-breaker (documented in
  `docs/architecture.md` section 4): valid rows first, then the lowest `hours` (conservative
  billing for near-duplicates), then `project_id`, `emp_id`, `work_date`. Superseded rows go
  to quarantine with `duplicate_superseded`, so bronze = silver + quarantine.
- Writes only the `_batch_date` partition (`replaceWhere`): idempotent per batch.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dq.checks import (
    check_not_null_columns,
    check_quarantine_ratio,
    check_ratio_threshold,
    check_unique_key,
)
from spark_jobs.common.spark_session import get_spark_session
from spark_jobs.silver._common import (
    SilverContext,
    clean_str,
    deduplicate,
    reasons_array,
    replace_partition,
    split_valid,
    write_quarantine,
)

OUTPUT_COLUMNS = (
    "ts_id",
    "emp_id",
    "project_id",
    "work_date",
    "hours",
    "is_weekend",
    "_batch_date",
    "_ingested_at",
    "_source_file",
)


def transform_timesheets(
    bronze_df: DataFrame, employees_scd2: DataFrame, projects: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return `(silver_timesheets, quarantine)` for one Bronze batch."""
    emp = employees_scd2.select(
        F.col("emp_id").alias("_emp_id"),
        "valid_from",
        "valid_to",
        F.col("termination_date").alias("_termination_date"),
    )
    known_projects = projects.select(F.col("project_id").alias("_known_project_id")).distinct()

    ts = (
        bronze_df.withColumn("ts_id", clean_str("ts_id"))
        .withColumn("emp_id", clean_str("emp_id"))
        .withColumn("project_id", clean_str("project_id"))
        .withColumn("hours", F.col("hours").cast("double"))
        .withColumn("work_date", F.col("work_date").cast("date"))
    )
    joined = ts.join(
        emp,
        (F.col("emp_id") == F.col("_emp_id"))
        & F.col("work_date").between(F.col("valid_from"), F.col("valid_to")),
        "left",
    ).join(F.broadcast(known_projects), F.col("project_id") == F.col("_known_project_id"), "left")

    evaluated = joined.withColumn(
        "dq_reasons",
        reasons_array(
            (F.col("ts_id").isNull(), "missing_ts_id"),
            (F.col("emp_id").isNull(), "missing_emp_id"),
            (F.col("work_date").isNull(), "missing_work_date"),
            (F.col("project_id").isNull(), "missing_project_id"),
            (
                F.col("project_id").isNotNull() & F.col("_known_project_id").isNull(),
                "orphan_project_id",
            ),
            (F.col("hours").isNull(), "missing_hours"),
            (F.col("hours") <= 0, "non_positive_hours"),
            (F.col("hours") > 24, "hours_exceed_24"),
            (
                F.col("emp_id").isNotNull()
                & F.col("work_date").isNotNull()
                & F.col("_emp_id").isNull(),
                "unknown_employee_on_work_date",
            ),
            (F.col("work_date") >= F.col("_termination_date"), "post_termination_work"),
        ),
    ).drop("_emp_id", "valid_from", "valid_to", "_termination_date", "_known_project_id")

    kept, superseded = deduplicate(
        evaluated,
        ["ts_id"],
        [
            F.size("dq_reasons").asc(),
            F.col("hours").asc_nulls_last(),
            F.col("project_id").asc_nulls_last(),
            F.col("emp_id").asc_nulls_last(),
            F.col("work_date").asc_nulls_last(),
        ],
    )
    valid, invalid = split_valid(kept)
    silver = valid.withColumn("is_weekend", F.dayofweek("work_date").isin(1, 7)).select(
        *OUTPUT_COLUMNS
    )
    return silver, invalid.unionByName(superseded)


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    ctx = SilverContext(spark, batch_date, lakehouse_root, run_id)
    bronze = ctx.read_partition("bronze", "timesheets")
    silver, quarantine = transform_timesheets(
        bronze,
        ctx.read("silver", "silver_employees_scd2"),
        ctx.read("silver", "silver_projects"),
    )
    replace_partition(
        silver.withColumn("_silver_updated_at", F.current_timestamp()),
        ctx.path("silver", "silver_timesheets"),
        ctx.batch_date,
    )
    write_quarantine(ctx, "timesheets", quarantine)

    written = ctx.read_partition("silver", "silver_timesheets")
    quarantine = ctx.read_partition("silver_quarantine", "timesheets")
    bronze_count = bronze.count()
    valid_count = written.count()
    duplicates = quarantine.filter(F.array_contains("dq_reasons", "duplicate_superseded")).count()
    invalid_kept = quarantine.count() - duplicates
    zombies = quarantine.filter(F.array_contains("dq_reasons", "post_termination_work")).count()

    common = dict(
        run_id=ctx.run_id, batch_date=ctx.batch_date, layer="silver", table_name="silver_timesheets"
    )
    ctx.publish_checks(
        [
            check_not_null_columns(written, ["ts_id", "emp_id", "project_id"], **common),
            check_unique_key(written, ["ts_id"], **common),
            check_quarantine_ratio(valid_count, invalid_kept, **common),
            check_ratio_threshold(
                duplicates, bronze_count, check_name="duplicate_rate", max_ratio=0.05, **common
            ),
            check_ratio_threshold(
                zombies,
                bronze_count,
                check_name="zombie_timesheet_count",
                max_ratio=0.0,
                **common,
            ),
        ]
    )
    return {"silver_timesheets": valid_count, "quarantine": quarantine.count()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform bronze/timesheets into silver_timesheets."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-timesheets")
    try:
        print(run(spark, args.batch_date, args.lakehouse_root, args.run_id))
    finally:
        spark.stop()
