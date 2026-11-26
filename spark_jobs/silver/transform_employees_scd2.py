"""Silver SCD Type 2 transformation for `silver_employees_scd2`.

Design (documented in `docs/architecture.md` section 3):
- Source: every daily full snapshot in `bronze/employees`. The table is rebuilt from all of
  Bronze and overwritten, so a run is idempotent for a given Bronze state and re-running an
  old `batch_date` never truncates history that later batches already added.
- Interval convention: closed `[valid_from, valid_to]` at day grain. `valid_to` is the day
  before the next version starts; the open end is the sentinel `9999-12-31`
  (`is_current = true`). Point-in-time join:
  `ts.work_date BETWEEN emp.valid_from AND emp.valid_to`.
- Change detection: a hash over the tracked attributes is compared with the previous
  snapshot of the same employee (`lag`); unchanged consecutive snapshots collapse into one
  version. Tracked: the required `department`, `role`, `hourly_rate_eur`, `is_active`,
  `termination_date`, `churn_type`, plus `seniority_level`, `work_mode`, `manager_id`,
  `last_promotion_date`, `distance_km` (needed for point-in-time HR/churn features).
  `full_name` and `hire_date` are carried from the snapshot that opened the version.
- Invalid snapshot rows are excluded from history; those of the current batch are
  quarantined to `silver_quarantine/employees`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.checks import (
    check_not_null_columns,
    check_quarantine_ratio,
    check_scd2_no_overlaps,
    check_scd2_single_current,
    check_unique_key,
)
from spark_jobs.common.spark_session import get_spark_session
from spark_jobs.silver._common import (
    OPEN_END_DATE,
    SilverContext,
    clean_str,
    deduplicate,
    failed_cast,
    overwrite_table,
    parse_flexible_date,
    reasons_array,
    split_valid,
    write_quarantine,
)

VALID_DEPARTMENTS = ("Engineering", "Data", "Consulting", "Sales", "Finance", "Operations")
VALID_SENIORITY = ("Junior", "Mid", "Senior", "Lead", "Principal")
VALID_WORK_MODES = ("Remote", "Hybrid", "Onsite")
VALID_CHURN_TYPES = ("voluntary", "involuntary")

TRACKED_ATTRIBUTES = (
    "department",
    "role",
    "hourly_rate_eur",
    "is_active",
    "termination_date",
    "churn_type",
    "seniority_level",
    "work_mode",
    "manager_id",
    "last_promotion_date",
    "distance_km",
)
OUTPUT_COLUMNS = (
    "emp_id",
    "full_name",
    "hire_date",
    *TRACKED_ATTRIBUTES,
    "valid_from",
    "valid_to",
    "is_current",
    "version_number",
    "record_hash",
)

_STRING_COLUMNS = (
    "emp_id",
    "full_name",
    "department",
    "role",
    "seniority_level",
    "work_mode",
    "manager_id",
    "churn_type",
)
_DATE_COLUMNS = ("hire_date", "last_promotion_date", "termination_date", "snapshot_date")
_DOUBLE_COLUMNS = ("hourly_rate_eur", "distance_km")


def _in_domain(column: str, domain: tuple[str, ...]) -> F.Column:
    return F.coalesce(F.col(column).isin(*domain), F.lit(False))


def validate_snapshots(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Type and validate Bronze snapshot rows; return `(valid, quarantine)`.

    Duplicates per `(emp_id, snapshot_date)` keep the latest ingestion.
    """
    typed = bronze_df
    for c in _STRING_COLUMNS:
        typed = typed.withColumn(c, clean_str(c))
    for c in (*_DATE_COLUMNS, *_DOUBLE_COLUMNS, "is_active"):
        typed = typed.withColumn(f"_raw_{c}", F.col(c))
    for c in _DATE_COLUMNS:
        typed = typed.withColumn(c, parse_flexible_date(F.col(c)))
    for c in _DOUBLE_COLUMNS:
        typed = typed.withColumn(c, F.col(c).cast("double"))
    typed = typed.withColumn(
        "is_active",
        F.when(F.lower(F.trim("is_active")) == "true", True).when(
            F.lower(F.trim("is_active")) == "false", False
        ),
    )

    checks = [
        (F.col("emp_id").isNull(), "missing_emp_id"),
        (F.col("snapshot_date").isNull(), "invalid_snapshot_date"),
        (
            F.col("hourly_rate_eur").isNull() | (F.col("hourly_rate_eur") <= 0),
            "invalid_hourly_rate_eur",
        ),
        (F.col("is_active").isNull(), "invalid_is_active"),
        (~_in_domain("department", VALID_DEPARTMENTS), "invalid_department"),
        (~_in_domain("seniority_level", VALID_SENIORITY), "invalid_seniority_level"),
        (~_in_domain("work_mode", VALID_WORK_MODES), "invalid_work_mode"),
        (
            F.col("churn_type").isNotNull() & ~_in_domain("churn_type", VALID_CHURN_TYPES),
            "invalid_churn_type",
        ),
        (
            ~F.col("is_active") & F.col("termination_date").isNull(),
            "inactive_without_termination_date",
        ),
    ]
    checks += [
        (failed_cast(F.col(f"_raw_{c}"), F.col(c)), f"unparseable_{c}")
        for c in ("hire_date", "last_promotion_date", "termination_date", "distance_km")
    ]
    evaluated = typed.withColumn("dq_reasons", reasons_array(*checks))

    kept, superseded = deduplicate(
        evaluated,
        ["emp_id", "snapshot_date"],
        [F.size("dq_reasons").asc(), F.col("_ingested_at").desc(), F.col("_source_file").asc()],
    )
    valid, invalid = split_valid(kept)
    raw_cols = [c for c in valid.columns if c.startswith("_raw_")]
    return valid.drop(*raw_cols), invalid.unionByName(superseded)


def build_scd2(snapshots: DataFrame) -> DataFrame:
    """Collapse validated daily snapshots into SCD Type 2 versions (closed intervals)."""
    by_emp = Window.partitionBy("emp_id").orderBy("snapshot_date")
    hashed = snapshots.withColumn(
        "record_hash",
        F.sha2(F.to_json(F.struct(*[F.col(c) for c in TRACKED_ATTRIBUTES])), 256),
    ).withColumn("_prev_hash", F.lag("record_hash").over(by_emp))

    changes = hashed.filter(
        F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("record_hash"))
    ).withColumnRenamed("snapshot_date", "valid_from")

    next_from = F.lead("valid_from").over(Window.partitionBy("emp_id").orderBy("valid_from"))
    return (
        changes.withColumn(
            "valid_to",
            F.coalesce(F.date_sub(next_from, 1), F.to_date(F.lit(OPEN_END_DATE))),
        )
        .withColumn("is_current", F.col("valid_to") == F.to_date(F.lit(OPEN_END_DATE)))
        .withColumn(
            "version_number",
            F.row_number().over(Window.partitionBy("emp_id").orderBy("valid_from")),
        )
        .select(*OUTPUT_COLUMNS)
    )


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    ctx = SilverContext(spark, batch_date, lakehouse_root, run_id)
    valid, quarantine_all = validate_snapshots(ctx.read("bronze", "employees"))
    scd2 = build_scd2(valid).withColumn("_silver_updated_at", F.current_timestamp())

    overwrite_table(scd2, ctx.path("silver", "silver_employees_scd2"))
    batch_quarantine = quarantine_all.filter(F.col("_batch_date") == F.lit(ctx.batch_date))
    write_quarantine(ctx, "employees", batch_quarantine)

    written = ctx.read("silver", "silver_employees_scd2")
    batch_valid = valid.filter(F.col("_batch_date") == F.lit(ctx.batch_date)).count()
    batch_quarantined = batch_quarantine.count()
    common = dict(
        run_id=ctx.run_id,
        batch_date=ctx.batch_date,
        layer="silver",
        table_name="silver_employees_scd2",
    )
    ctx.publish_checks(
        [
            check_not_null_columns(written, ["emp_id", "valid_from", "valid_to"], **common),
            check_unique_key(written, ["emp_id", "valid_from"], **common),
            check_scd2_no_overlaps(written, **common),
            check_scd2_single_current(written, **common),
            check_quarantine_ratio(batch_valid, batch_quarantined, **common),
        ]
    )
    return {
        "silver_employees_scd2": written.count(),
        "current_versions": written.filter("is_current").count(),
        "quarantine": batch_quarantined,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_employees_scd2.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-employees-scd2")
    try:
        print(run(spark, args.batch_date, args.lakehouse_root, args.run_id))
    finally:
        spark.stop()
