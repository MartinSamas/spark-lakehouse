"""Silver transformation & quarantine for `silver_invoices` and `silver_expenses`.

`silver_invoices`:
- Parses ISO `YYYY-MM-DD` and European `DD.MM.YYYY` strings in `issued_at` / `paid_at`
  into `issued_date` / `paid_date` (raw strings kept as `issued_at_raw` / `paid_at_raw`).
- Quarantines orphan `project_id` references (`left_anti`-equivalent left join against
  `silver_projects`), unknown clients, unparseable dates, missing amounts and unknown
  currencies into `silver_quarantine/invoices`.
- Keeps negative amounts as valid credit notes (`is_credit_note`) and unpaid invoices
  (`paid_date IS NULL`, `is_paid = false`).

`silver_expenses`:
- Deduplicates exact and near-duplicates on `exp_id` (valid rows first, then the lowest
  `amount`, then the remaining attributes for a total order).

Both: `amount_eur = round(amount * rate, 2)` using `silver_fx_rates_daily` on the business
date (`issued_date` / `expense_date`, LOCF-filled for weekends); EUR uses rate 1.0. A missing
rate leaves `amount_eur` NULL, which fails the hard `zero_null_amount_eur` gate (it signals an
FX pipeline problem, not a bad source row). Only the `_batch_date` partition is replaced.
"""

from __future__ import annotations

import argparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dq.checks import (
    check_not_null_columns,
    check_quarantine_ratio,
    check_ratio_threshold,
    check_unique_key,
    check_zero_null_eur,
)
from spark_jobs.common.spark_session import get_spark_session
from spark_jobs.silver._common import (
    DUPLICATE_REASON,
    SilverContext,
    clean_str,
    deduplicate,
    failed_cast,
    parse_flexible_date,
    reasons_array,
    replace_partition,
    split_valid,
    write_quarantine,
)
from spark_jobs.silver.transform_employees_scd2 import VALID_DEPARTMENTS

VALID_CURRENCIES = ("EUR", "USD", "CZK")
VALID_EXPENSE_CATEGORIES = (
    "Cloud Infrastructure",
    "Software Licenses",
    "Travel & Lodging",
    "Training",
    "Hardware",
    "Office & Events",
    "Subcontractors",
)

INVOICE_COLUMNS = (
    "inv_id",
    "client_id",
    "project_id",
    "amount",
    "currency",
    "issued_date",
    "paid_date",
    "issued_at_raw",
    "paid_at_raw",
    "is_credit_note",
    "is_paid",
    "fx_rate_to_eur",
    "amount_eur",
    "net_amount_eur",
    "_batch_date",
    "_ingested_at",
    "_source_file",
)
EXPENSE_COLUMNS = (
    "exp_id",
    "department",
    "category",
    "amount",
    "currency",
    "expense_date",
    "fx_rate_to_eur",
    "amount_eur",
    "_batch_date",
    "_ingested_at",
    "_source_file",
)


def _currency_invalid(column: str) -> Column:
    return ~F.coalesce(F.col(column).isin(*VALID_CURRENCIES), F.lit(False))


def convert_to_eur(df: DataFrame, fx_daily: DataFrame, date_column: str) -> DataFrame:
    """Add `fx_rate_to_eur` and `amount_eur` using the daily rate on `date_column`."""
    to_eur = fx_daily.filter(F.col("currency_to") == "EUR").select(
        F.col("rate_date").alias("_fx_date"),
        F.col("currency_from").alias("_fx_currency"),
        F.col("rate").alias("_fx_rate"),
    )
    return (
        df.join(
            F.broadcast(to_eur),
            (F.col(date_column) == F.col("_fx_date"))
            & (F.col("currency") == F.col("_fx_currency")),
            "left",
        )
        .withColumn(
            "fx_rate_to_eur",
            F.when(F.col("currency") == "EUR", F.lit(1.0)).otherwise(F.col("_fx_rate")),
        )
        .withColumn("amount_eur", F.round(F.col("amount") * F.col("fx_rate_to_eur"), 2))
        .drop("_fx_date", "_fx_currency", "_fx_rate")
    )


def transform_invoices(
    bronze_df: DataFrame, projects: DataFrame, clients: DataFrame, fx_daily: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return `(silver_invoices, quarantine)` for one Bronze batch."""
    known_projects = projects.select(F.col("project_id").alias("_known_project_id")).distinct()
    known_clients = clients.select(F.col("client_id").alias("_known_client_id")).distinct()

    typed = (
        bronze_df.withColumn("inv_id", clean_str("inv_id"))
        .withColumn("client_id", clean_str("client_id"))
        .withColumn("project_id", clean_str("project_id"))
        .withColumn("currency", F.upper(clean_str("currency")))
        .withColumn("issued_at_raw", F.col("issued_at"))
        .withColumn("paid_at_raw", F.col("paid_at"))
        .withColumn("issued_date", parse_flexible_date(F.col("issued_at")))
        .withColumn("paid_date", parse_flexible_date(F.col("paid_at")))
        .join(
            F.broadcast(known_projects), F.col("project_id") == F.col("_known_project_id"), "left"
        )
        .join(F.broadcast(known_clients), F.col("client_id") == F.col("_known_client_id"), "left")
    )
    evaluated = typed.withColumn(
        "dq_reasons",
        reasons_array(
            (F.col("_corrupt_record").isNotNull(), "corrupt_json_record"),
            (F.col("inv_id").isNull(), "missing_inv_id"),
            (F.col("amount").isNull(), "missing_amount"),
            (_currency_invalid("currency"), "invalid_currency"),
            (F.col("issued_date").isNull(), "invalid_issued_at"),
            (failed_cast(F.col("paid_at_raw"), F.col("paid_date")), "invalid_paid_at"),
            (F.col("project_id").isNull(), "missing_project_id"),
            (
                F.col("project_id").isNotNull() & F.col("_known_project_id").isNull(),
                "orphan_project_id",
            ),
            (F.col("client_id").isNull() | F.col("_known_client_id").isNull(), "orphan_client_id"),
        ),
    ).drop("_known_project_id", "_known_client_id", "issued_at", "paid_at")

    kept, superseded = deduplicate(
        evaluated,
        ["inv_id"],
        [
            F.size("dq_reasons").asc(),
            F.col("_ingested_at").desc(),
            F.col("amount").asc_nulls_last(),
            F.col("issued_date").asc_nulls_last(),
        ],
    )
    valid, invalid = split_valid(kept)
    silver = (
        convert_to_eur(valid, fx_daily, "issued_date")
        .withColumn("is_credit_note", F.col("amount") < 0)
        .withColumn("is_paid", F.col("paid_date").isNotNull())
        .withColumn("net_amount_eur", F.col("amount_eur"))
        .select(*INVOICE_COLUMNS)
    )
    return silver, invalid.unionByName(superseded)


def transform_expenses(bronze_df: DataFrame, fx_daily: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return `(silver_expenses, quarantine)` for one Bronze batch."""
    typed = (
        bronze_df.withColumn("exp_id", clean_str("exp_id"))
        .withColumn("department", clean_str("department"))
        .withColumn("category", clean_str("category"))
        .withColumn("currency", F.upper(clean_str("currency")))
        .withColumn("amount_raw", F.col("amount"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("expense_date_raw", F.col("expense_date"))
        .withColumn("expense_date", parse_flexible_date(F.col("expense_date")))
    )
    evaluated = typed.withColumn(
        "dq_reasons",
        reasons_array(
            (F.col("exp_id").isNull(), "missing_exp_id"),
            (F.col("amount").isNull(), "invalid_amount"),
            (F.col("amount") <= 0, "non_positive_amount"),
            (_currency_invalid("currency"), "invalid_currency"),
            (F.col("expense_date").isNull(), "invalid_expense_date"),
            (
                ~F.coalesce(F.col("department").isin(*VALID_DEPARTMENTS), F.lit(False)),
                "invalid_department",
            ),
            (
                ~F.coalesce(F.col("category").isin(*VALID_EXPENSE_CATEGORIES), F.lit(False)),
                "invalid_category",
            ),
        ),
    )
    kept, superseded = deduplicate(
        evaluated,
        ["exp_id"],
        [
            F.size("dq_reasons").asc(),
            F.col("amount").asc_nulls_last(),
            F.col("department").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
            F.col("currency").asc_nulls_last(),
            F.col("expense_date").asc_nulls_last(),
        ],
    )
    valid, invalid = split_valid(kept)
    silver = convert_to_eur(valid, fx_daily, "expense_date").select(*EXPENSE_COLUMNS)
    return silver, invalid.unionByName(superseded)


def _table_checks(
    ctx: SilverContext,
    table_name: str,
    quarantine_table: str,
    key: str,
    bronze_count: int,
    extra_rate_reason: str | None = None,
) -> list:
    written = ctx.read_partition("silver", table_name)
    quarantine = ctx.read_partition("silver_quarantine", quarantine_table)
    valid_count = written.count()
    duplicates = quarantine.filter(F.array_contains("dq_reasons", DUPLICATE_REASON)).count()
    common = dict(
        run_id=ctx.run_id, batch_date=ctx.batch_date, layer="silver", table_name=table_name
    )
    results = [
        check_not_null_columns(written, [key], **common),
        check_unique_key(written, [key], **common),
        check_zero_null_eur(written, "amount_eur", **common),
        check_quarantine_ratio(valid_count, quarantine.count() - duplicates, **common),
        check_ratio_threshold(
            duplicates, bronze_count, check_name="duplicate_rate", max_ratio=0.05, **common
        ),
    ]
    if extra_rate_reason:
        flagged = quarantine.filter(F.array_contains("dq_reasons", extra_rate_reason)).count()
        results.append(
            check_ratio_threshold(
                flagged,
                bronze_count,
                check_name=f"{extra_rate_reason}_rate",
                max_ratio=0.02,
                **common,
            )
        )
    return results


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    ctx = SilverContext(spark, batch_date, lakehouse_root, run_id)
    fx_daily = ctx.read("silver", "silver_fx_rates_daily")
    bronze_invoices = ctx.read_partition("bronze", "invoices")
    bronze_expenses = ctx.read_partition("bronze", "expenses")

    invoices, invoices_q = transform_invoices(
        bronze_invoices,
        ctx.read("silver", "silver_projects"),
        ctx.read("silver", "silver_clients"),
        fx_daily,
    )
    expenses, expenses_q = transform_expenses(bronze_expenses, fx_daily)

    for silver_df, table, quarantine_df, q_table in (
        (invoices, "silver_invoices", invoices_q, "invoices"),
        (expenses, "silver_expenses", expenses_q, "expenses"),
    ):
        replace_partition(
            silver_df.withColumn("_silver_updated_at", F.current_timestamp()),
            ctx.path("silver", table),
            ctx.batch_date,
        )
        write_quarantine(ctx, q_table, quarantine_df)

    results = _table_checks(
        ctx, "silver_invoices", "invoices", "inv_id", bronze_invoices.count(), "orphan_project_id"
    ) + _table_checks(ctx, "silver_expenses", "expenses", "exp_id", bronze_expenses.count())
    ctx.publish_checks(results)
    return {
        "silver_invoices": ctx.read_partition("silver", "silver_invoices").count(),
        "invoices_quarantine": ctx.read_partition("silver_quarantine", "invoices").count(),
        "silver_expenses": ctx.read_partition("silver", "silver_expenses").count(),
        "expenses_quarantine": ctx.read_partition("silver_quarantine", "expenses").count(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_invoices and silver_expenses.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-invoices-expenses")
    try:
        print(run(spark, args.batch_date, args.lakehouse_root, args.run_id))
    finally:
        spark.stop()
