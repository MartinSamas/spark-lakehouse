"""Silver FX calendar spine & LOCF forward-fill for `silver_fx_rates_daily`.

Design:
- `fx_rates` arrive on weekdays only. Silver builds a complete daily calendar spine
  (first observed `rate_date` .. end date) cross-joined with every observed currency pair,
  then forward-fills gaps with `last(rate, ignorenulls=True)` (LOCF).
- The spine end is `greatest(batch_date, latest observed rate_date, current table end)`,
  so a Saturday/Sunday batch gets rates, and re-running an old batch never shrinks the table.
- The table is small (days x 6 pairs), so it is rebuilt from all of Bronze and overwritten:
  idempotent for a given Bronze state.
- Invalid Bronze rows of the batch (unparseable date, non-positive rate, unknown currency,
  corrupt JSON) are quarantined to `silver_quarantine/fx_rates`; duplicates per
  `(rate_date, pair)` keep the latest ingestion.

Columns: `rate_date`, `currency_from`, `currency_to`, `rate`, `source_rate_date`
(the business day the rate was observed), `is_forward_filled`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
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
    overwrite_table,
    parse_flexible_date,
    path_is_delta,
    reasons_array,
    split_valid,
    write_quarantine,
)

VALID_CURRENCIES = ("EUR", "USD", "CZK")
PAIR_KEYS = ["currency_from", "currency_to"]
MAX_FILL_GAP_DAYS = 4  # Fri -> Mon is 3 days; anything longer means a missing weekday file.


def validate_fx(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Type and validate Bronze FX rows; return `(valid, quarantine)`."""
    typed = (
        bronze_df.withColumn("currency_from", F.upper(clean_str("currency_from")))
        .withColumn("currency_to", F.upper(clean_str("currency_to")))
        .withColumn("rate_date_raw", F.col("rate_date"))
        .withColumn("rate_date", parse_flexible_date(F.col("rate_date")))
    )
    evaluated = typed.withColumn(
        "dq_reasons",
        reasons_array(
            (F.col("_corrupt_record").isNotNull(), "corrupt_json_record"),
            (F.col("rate_date").isNull(), "invalid_rate_date"),
            (F.col("rate").isNull() | (F.col("rate") <= 0), "invalid_rate"),
            (
                ~F.coalesce(F.col("currency_from").isin(*VALID_CURRENCIES), F.lit(False)),
                "invalid_currency_from",
            ),
            (
                ~F.coalesce(F.col("currency_to").isin(*VALID_CURRENCIES), F.lit(False)),
                "invalid_currency_to",
            ),
        ),
    )
    kept, superseded = deduplicate(
        evaluated,
        ["rate_date", *PAIR_KEYS],
        [F.size("dq_reasons").asc(), F.col("_ingested_at").desc(), F.col("rate").asc()],
    )
    valid, invalid = split_valid(kept)
    return valid, invalid.unionByName(superseded)


def build_daily_rates(observed: DataFrame, end_date: str | None = None) -> DataFrame:
    """Build the LOCF-filled daily spine from validated observed rates.

    `end_date` extends the spine past the last observation (e.g. a weekend batch date).
    """
    bounds = observed.agg(F.min("rate_date").alias("start"), F.max("rate_date").alias("last"))
    end_col = F.greatest(F.col("last"), F.to_date(F.lit(end_date))) if end_date else F.col("last")
    calendar = bounds.select(
        F.explode(F.sequence(F.col("start"), end_col, F.expr("INTERVAL 1 DAY"))).alias("rate_date")
    )
    pairs = observed.select(*PAIR_KEYS).distinct()
    spine = calendar.crossJoin(pairs)

    obs = observed.select(
        "rate_date", *PAIR_KEYS, "rate", F.col("rate_date").alias("_observed_date")
    )
    locf = (
        Window.partitionBy(*PAIR_KEYS)
        .orderBy("rate_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        spine.join(obs, ["rate_date", *PAIR_KEYS], "left")
        .withColumn("is_forward_filled", F.col("rate").isNull())
        .withColumn("rate", F.last("rate", ignorenulls=True).over(locf))
        .withColumn("source_rate_date", F.last("_observed_date", ignorenulls=True).over(locf))
        .drop("_observed_date")
        .select("rate_date", *PAIR_KEYS, "rate", "source_rate_date", "is_forward_filled")
    )


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    ctx = SilverContext(spark, batch_date, lakehouse_root, run_id)
    silver_path = ctx.path("silver", "silver_fx_rates_daily")

    valid_all, quarantine_all = validate_fx(ctx.read("bronze", "fx_rates"))

    end_candidates = [ctx.batch_date]
    if path_is_delta(spark, silver_path):
        current_end = ctx.read("silver", "silver_fx_rates_daily").agg(F.max("rate_date")).first()[0]
        if current_end is not None:
            end_candidates.append(current_end.isoformat())
    daily = build_daily_rates(valid_all, end_date=max(end_candidates)).withColumn(
        "_silver_updated_at", F.current_timestamp()
    )

    overwrite_table(daily, silver_path)
    batch_quarantine = quarantine_all.filter(F.col("_batch_date") == F.lit(ctx.batch_date))
    write_quarantine(ctx, "fx_rates", batch_quarantine)

    written = ctx.read("silver", "silver_fx_rates_daily")
    total = written.count()
    stale = written.filter(F.datediff("rate_date", "source_rate_date") >= MAX_FILL_GAP_DAYS).count()
    batch_valid = valid_all.filter(F.col("_batch_date") == F.lit(ctx.batch_date)).count()
    batch_quarantined = batch_quarantine.count()

    common = dict(
        run_id=ctx.run_id,
        batch_date=ctx.batch_date,
        layer="silver",
        table_name="silver_fx_rates_daily",
    )
    ctx.publish_checks(
        [
            check_not_null_columns(written, ["rate_date", *PAIR_KEYS, "rate"], **common),
            check_unique_key(written, ["rate_date", *PAIR_KEYS], **common),
            check_ratio_threshold(
                stale, total, check_name="stale_forward_filled_rate", max_ratio=0.0, **common
            ),
            check_quarantine_ratio(batch_valid, batch_quarantined, **common),
        ]
    )
    return {"silver_fx_rates_daily": total, "quarantine": batch_quarantined}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_fx_rates_daily.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-fx-rates")
    try:
        print(run(spark, args.batch_date, args.lakehouse_root, args.run_id))
    finally:
        spark.stop()
