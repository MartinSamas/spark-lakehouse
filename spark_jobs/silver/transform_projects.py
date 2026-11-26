"""Silver transformation & quarantine for `silver_projects` (full-load dimension).

- Types `budget_eur` as DOUBLE and validates `status` against the documented domain.
- Quarantines rows with a missing `project_id`, an unparseable/negative budget, an unknown
  status, or a `client_id` that does not exist in `silver_clients`.
- Deduplicates on `project_id` (latest ingestion wins, then lowest budget for determinism).
- Overwrites `silver/silver_projects`; `silver_invoices` uses it for the orphan check.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dq.checks import check_quarantine_ratio, check_unique_key
from spark_jobs.common.spark_session import get_spark_session
from spark_jobs.silver._common import (
    SilverContext,
    clean_str,
    deduplicate,
    failed_cast,
    overwrite_table,
    reasons_array,
    split_valid,
    write_quarantine,
)

VALID_PROJECT_STATUSES = ("Planned", "Active", "Completed", "OnHold")


def build_projects(bronze_df: DataFrame, clients_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return `(silver_projects, quarantine)` from Bronze projects and `silver_clients`."""
    known_clients = clients_df.select(F.col("client_id").alias("_known_client_id")).distinct()
    typed = (
        bronze_df.withColumn("project_id", clean_str("project_id"))
        .withColumn("client_id", clean_str("client_id"))
        .withColumn("status", clean_str("status"))
        .withColumn("budget_eur_raw", F.col("budget_eur"))
        .withColumn("budget_eur", F.col("budget_eur").cast("double"))
        .join(F.broadcast(known_clients), F.col("client_id") == F.col("_known_client_id"), "left")
    )
    evaluated = typed.withColumn(
        "dq_reasons",
        reasons_array(
            (F.col("project_id").isNull(), "missing_project_id"),
            (failed_cast(F.col("budget_eur_raw"), F.col("budget_eur")), "invalid_budget_eur"),
            (F.col("budget_eur") < 0, "negative_budget_eur"),
            (
                ~F.coalesce(F.col("status").isin(*VALID_PROJECT_STATUSES), F.lit(False)),
                "invalid_project_status",
            ),
            (F.col("_known_client_id").isNull(), "orphan_client_id"),
        ),
    ).drop("_known_client_id")

    kept, superseded = deduplicate(
        evaluated,
        ["project_id"],
        [F.size("dq_reasons").asc(), F.col("_ingested_at").desc(), F.col("budget_eur").asc()],
    )
    valid, invalid = split_valid(kept)
    silver = valid.select(
        "project_id",
        "client_id",
        "budget_eur",
        "status",
        "_batch_date",
        "_ingested_at",
        "_source_file",
    ).withColumn("_silver_updated_at", F.current_timestamp())
    return silver, invalid.unionByName(superseded)


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    ctx = SilverContext(spark, batch_date, lakehouse_root, run_id)
    silver, quarantine = build_projects(
        ctx.read("bronze", "projects"), ctx.read("silver", "silver_clients")
    )

    overwrite_table(silver, ctx.path("silver", "silver_projects"))
    write_quarantine(ctx, "projects", quarantine)

    written = ctx.read("silver", "silver_projects")
    valid_count, quarantined_count = written.count(), quarantine.count()
    common = dict(
        run_id=ctx.run_id, batch_date=ctx.batch_date, layer="silver", table_name="silver_projects"
    )
    ctx.publish_checks(
        [
            check_unique_key(written, ["project_id"], **common),
            check_quarantine_ratio(valid_count, quarantined_count, **common),
        ]
    )
    return {"silver_projects": valid_count, "quarantine": quarantined_count}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_projects.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-projects")
    try:
        print(run(spark, args.batch_date, args.lakehouse_root, args.run_id))
    finally:
        spark.stop()
