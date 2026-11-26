"""Reusable Data Quality checks for the Bronze, Silver and Gold layers.

Every check returns a `DQCheckResult` (see `dq.framework`). Status semantics:
- `PASS`: no violations.
- `WARN`: violations found on a `severity="WARN"` (soft) check -> logged, pipeline continues.
- `FAIL`: violations found on a `severity="ERROR"` (hard) check -> `enforce_dq_gate` raises.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Section 4.3) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from dq.framework import DQCheckResult, _parse_batch_date


def build_result(
    *,
    check_name: str,
    failed_count: int,
    total_count: int,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
    violated: bool | None = None,
) -> DQCheckResult:
    """Build a `DQCheckResult`; `violated` defaults to `failed_count > 0`."""
    severity = severity.upper()
    is_violated = failed_count > 0 if violated is None else violated
    if not is_violated:
        status = "PASS"
    elif severity == "WARN":
        status = "WARN"
    else:
        status = "FAIL"
    return DQCheckResult(
        run_id=run_id,
        batch_date=_parse_batch_date(batch_date),
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        severity=severity,
        status=status,
        failed_count=int(failed_count),
        total_count=int(total_count),
        checked_at=datetime.now(timezone.utc),
    )


def check_unique_key(
    df: DataFrame,
    key_columns: Sequence[str],
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify uniqueness of a primary/composite key (e.g. `(emp_id, valid_from)`).

    `failed_count` is the number of rows sharing a key with at least one other row.
    """
    keys = list(key_columns)
    total_count = df.count()
    failed_count = (
        df.groupBy(*keys).count().filter(F.col("count") > 1).agg(F.sum("count")).first()[0] or 0
    )
    return build_result(
        check_name=f"unique_{'_'.join(keys)}",
        failed_count=failed_count,
        total_count=total_count,
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
    )


def check_not_null_columns(
    df: DataFrame,
    columns: Sequence[str],
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify that none of `columns` is NULL (e.g. primary key components)."""
    total_count = df.count()
    any_null = " OR ".join(f"`{c}` IS NULL" for c in columns)
    failed_count = df.filter(any_null).count() if columns else 0
    return build_result(
        check_name=f"not_null_{'_'.join(columns)}",
        failed_count=failed_count,
        total_count=total_count,
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
    )


def check_expected_schema(
    df: DataFrame,
    expected: Mapping[str, str],
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify every expected column exists with the expected Spark SQL type (`simpleString`).

    Extra columns are allowed. `failed_count` = missing or mistyped columns.
    """
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    mismatches = [name for name, dtype in expected.items() if actual.get(name) != dtype]
    return build_result(
        check_name="expected_schema",
        failed_count=len(mismatches),
        total_count=len(expected),
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
    )


def check_non_empty_partition(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    allow_weekends_empty: bool = False,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify the `batch_date` partition of a mandatory table contains rows.

    With `allow_weekends_empty=True` (weekday-only feeds such as `fx_rates`) an empty
    Saturday/Sunday partition passes. `df` may be the whole table or the partition only;
    rows are filtered on `_batch_date` when that column exists.
    """
    parsed = _parse_batch_date(batch_date)
    partition_df = (
        df.filter(F.col("_batch_date") == F.lit(parsed)) if "_batch_date" in df.columns else df
    )
    row_count = partition_df.count()
    is_weekend = parsed.weekday() >= 5
    violated = row_count == 0 and not (allow_weekends_empty and is_weekend)
    return build_result(
        check_name="non_empty_partition",
        failed_count=1 if violated else 0,
        total_count=row_count,
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
        violated=violated,
    )


def check_scd2_no_overlaps(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str = "silver_employees_scd2",
    severity: str = "ERROR",
    key_column: str = "emp_id",
) -> DQCheckResult:
    """Verify no overlapping closed `[valid_from, valid_to]` intervals per key.

    A row fails when the next version (ordered by `valid_from`) starts on or before this
    version's `valid_to`, or when `valid_to < valid_from`.
    """
    w = Window.partitionBy(key_column).orderBy("valid_from")
    flagged = df.withColumn("_next_from", F.lead("valid_from").over(w)).filter(
        (F.col("valid_to") < F.col("valid_from"))
        | (F.col("_next_from").isNotNull() & (F.col("_next_from") <= F.col("valid_to")))
    )
    return build_result(
        check_name="scd2_no_overlapping_intervals",
        failed_count=flagged.count(),
        total_count=df.count(),
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
    )


def check_scd2_single_current(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str = "silver_employees_scd2",
    severity: str = "ERROR",
    key_column: str = "emp_id",
) -> DQCheckResult:
    """Verify every key has exactly one `is_current = true` version."""
    per_key = df.groupBy(key_column).agg(
        F.sum(F.col("is_current").cast("int")).alias("current_versions")
    )
    return build_result(
        check_name="scd2_exactly_one_current_version",
        failed_count=per_key.filter(F.col("current_versions") != 1).count(),
        total_count=per_key.count(),
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
    )


def check_zero_null_eur(
    df: DataFrame,
    eur_column: str,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify zero NULL EUR amounts after FX conversion (invoices & expenses)."""
    return build_result(
        check_name=f"zero_null_{eur_column}",
        failed_count=df.filter(F.col(eur_column).isNull()).count(),
        total_count=df.count(),
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
    )


def check_ratio_threshold(
    failed_count: int,
    total_count: int,
    *,
    check_name: str,
    max_ratio: float,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "WARN",
) -> DQCheckResult:
    """Generic rate check: violated when `failed_count / total_count > max_ratio`.

    Used for soft metrics such as orphan-invoice, duplicate and missing-comment rates.
    `max_ratio=0` turns it into a "count must be zero" check.
    """
    ratio = failed_count / total_count if total_count else 0.0
    return build_result(
        check_name=check_name,
        failed_count=failed_count,
        total_count=total_count,
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
        violated=ratio > max_ratio,
    )


def check_quarantine_ratio(
    valid_count: int,
    quarantined_count: int,
    *,
    max_ratio: float = 0.08,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str,
    severity: str = "WARN",
) -> DQCheckResult:
    """Emit WARN when quarantined_count / (valid_count + quarantined_count) > max_ratio."""
    return check_ratio_threshold(
        quarantined_count,
        valid_count + quarantined_count,
        check_name="quarantine_ratio",
        max_ratio=max_ratio,
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        severity=severity,
    )
