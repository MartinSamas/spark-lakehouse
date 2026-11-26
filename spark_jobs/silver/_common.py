"""Shared Silver helpers: typing, quarantine routing, deterministic dedup, idempotent writes.

Conventions applied by every Silver job:
- Invalid rows are never dropped: they land in `silver_quarantine/<table>` with
  `dq_reasons: array<string>` and `_batch_date`.
- Superseded duplicates are also routed to quarantine (reason `duplicate_superseded`) so
  the row count is fully reconciled: bronze = silver + quarantine.
- Fact tables are partitioned by `_batch_date` and only that partition is replaced
  (`replaceWhere`); dimensions (SCD2, FX spine, projects) are rebuilt and overwritten.
- All checks of one table are written to `dq_audit_results` in a single call (the audit
  writer replaces records per `(batch_date, layer, table_name)`), then the gate is enforced.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.framework import DQCheckResult, enforce_dq_gate, write_audit_results
from spark_jobs.bronze._io import validate_batch_date
from spark_jobs.common.spark_session import get_lakehouse_root, join_path

DUPLICATE_REASON = "duplicate_superseded"
OPEN_END_DATE = "9999-12-31"
ISO_DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
EU_DATE_RE = r"^\d{2}\.\d{2}\.\d{4}$"


class SilverContext:
    """Resolved paths and identifiers shared by one Silver job run."""

    def __init__(
        self,
        spark: SparkSession,
        batch_date: str,
        lakehouse_root: str | None,
        run_id: str | None,
    ) -> None:
        self.spark = spark
        self.batch_date = validate_batch_date(batch_date)
        self.lakehouse = get_lakehouse_root(lakehouse_root)
        self.run_id = run_id or f"manual-{self.batch_date}-{uuid.uuid4().hex[:8]}"

    def path(self, zone: str, table: str) -> str:
        return join_path(self.lakehouse, zone, table)

    def read(self, zone: str, table: str) -> DataFrame:
        return self.spark.read.format("delta").load(self.path(zone, table))

    def read_partition(self, zone: str, table: str) -> DataFrame:
        """Read the current `_batch_date` partition of a partitioned table."""
        return self.read(zone, table).filter(F.col("_batch_date") == F.lit(self.batch_date))

    def publish_checks(self, results: Sequence[DQCheckResult]) -> None:
        """Persist audit records, then raise if any hard check failed."""
        write_audit_results(self.spark, results, self.path("dq", "dq_audit_results"))
        enforce_dq_gate(results)


def path_is_delta(spark: SparkSession, path: str) -> bool:
    """True when `path` holds a Delta table."""
    return DeltaTable.isDeltaTable(spark, path)


def clean_str(column_name: str) -> Column:
    """Trim a string column and turn blanks into NULL."""
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def parse_flexible_date(column: Column) -> Column:
    """Parse ISO `YYYY-MM-DD` or European `DD.MM.YYYY` strings; anything else -> NULL.

    Dispatching on a regex first keeps Spark's strict (non-legacy) parser from raising on
    the "wrong" pattern, and rejects partial matches such as `2026-01-01T10:00`.
    """
    trimmed = F.trim(column)
    return (
        F.when(trimmed.rlike(ISO_DATE_RE), F.to_date(trimmed, "yyyy-MM-dd"))
        .when(trimmed.rlike(EU_DATE_RE), F.to_date(trimmed, "dd.MM.yyyy"))
        .otherwise(F.lit(None).cast("date"))
    )


def reasons_array(*checks: tuple[Column, str]) -> Column:
    """Build `dq_reasons` from `(violation_condition, reason)` pairs, dropping NULLs."""
    flagged = F.array(*[F.when(cond, F.lit(reason)) for cond, reason in checks])
    return F.filter(flagged, lambda r: r.isNotNull())


def failed_cast(raw: Column, typed: Column) -> Column:
    """True when a non-blank raw value could not be cast (the typed value is NULL)."""
    return raw.isNotNull() & (F.length(F.trim(raw)) > 0) & typed.isNull()


def deduplicate(
    df: DataFrame, key_columns: Sequence[str], order_by: Sequence[Column]
) -> tuple[DataFrame, DataFrame]:
    """Keep the first row per key under a deterministic ordering.

    Returns `(kept, superseded)`; superseded rows get `duplicate_superseded` appended to
    their `dq_reasons` (the column is created when missing). Callers must make `order_by`
    a total order on every non-identical row so re-runs always keep the same record.
    """
    if "dq_reasons" not in df.columns:
        df = df.withColumn("dq_reasons", F.array().cast("array<string>"))
    w = Window.partitionBy(*key_columns).orderBy(*order_by)
    ranked = df.withColumn("_dedup_rank", F.row_number().over(w))
    kept = ranked.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")
    superseded = (
        ranked.filter(F.col("_dedup_rank") > 1)
        .drop("_dedup_rank")
        .withColumn("dq_reasons", F.array_union("dq_reasons", F.array(F.lit(DUPLICATE_REASON))))
    )
    return kept, superseded


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split on `dq_reasons`: `(valid without dq_reasons, invalid with dq_reasons)`."""
    valid = df.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    invalid = df.filter(F.size("dq_reasons") > 0)
    return valid, invalid


def replace_partition(df: DataFrame, path: str, batch_date: str) -> None:
    """Idempotently replace the `_batch_date` partition (an empty df clears it)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("_batch_date")
        .option("replaceWhere", f"_batch_date = '{batch_date}'")
        .save(path)
    )


def overwrite_table(df: DataFrame, path: str, partition_by: Sequence[str] = ()) -> None:
    """Overwrite a rebuilt dimension table (previous versions stay in Delta time travel)."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_quarantine(ctx: SilverContext, table: str, quarantine_df: DataFrame) -> None:
    """Replace the batch partition of `silver_quarantine/<table>`."""
    replace_partition(
        quarantine_df.withColumn("_batch_date", F.to_date(F.lit(ctx.batch_date))),
        ctx.path("silver_quarantine", table),
        ctx.batch_date,
    )
