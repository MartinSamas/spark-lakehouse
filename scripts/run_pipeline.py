"""Run Medallion layers for a date range in a single SparkSession (dev / backfill helper).

Useful for iterating on the 14-day slice without Airflow, e.g. inside the Spark container:

    docker compose exec spark-master python3 scripts/run_pipeline.py \
        --start 2026-01-01 --end 2026-01-14 --layers bronze silver --master local[4]

Each layer runs every batch_date in order, so the result matches a sequential daily backfill.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402

from spark_jobs.common.spark_session import get_spark_session  # noqa: E402

Step = tuple[str, Callable[[SparkSession, str], object]]


def bronze_steps() -> list[Step]:
    from spark_jobs.bronze import (
        ingest_clients,
        ingest_crm_surveys,
        ingest_finance,
        ingest_hr,
        ingest_reference,
    )

    return [
        ("bronze.clients", lambda s, d: ingest_clients.ingest_clients(s, batch_date=d)),
        ("bronze.reference", ingest_reference.run),
        ("bronze.hr", ingest_hr.run),
        ("bronze.finance", ingest_finance.run),
        ("bronze.crm_surveys", ingest_crm_surveys.run),
    ]


def silver_steps() -> list[Step]:
    from spark_jobs.silver import (
        transform_clients,
        transform_crm_events,
        transform_employees_scd2,
        transform_fx_rates,
        transform_invoices_expenses,
        transform_projects,
        transform_surveys,
        transform_timesheets,
    )

    # Dimensions & FX first, then facts (mirrors the DAG dependency order).
    return [
        ("silver.clients", lambda s, d: transform_clients.transform_clients(s, batch_date=d)),
        ("silver.projects", transform_projects.run),
        ("silver.fx_rates", transform_fx_rates.run),
        ("silver.employees_scd2", transform_employees_scd2.run),
        ("silver.timesheets", transform_timesheets.run),
        ("silver.invoices_expenses", transform_invoices_expenses.run),
        ("silver.crm_events", transform_crm_events.run),
        ("silver.surveys", transform_surveys.run),
    ]


LAYERS: dict[str, Callable[[], list[Step]]] = {"bronze": bronze_steps, "silver": silver_steps}


def date_range(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", required=True, help="First batch date (YYYY-MM-DD).")
    parser.add_argument("--end", required=True, help="Last batch date (YYYY-MM-DD).")
    parser.add_argument("--layers", nargs="+", choices=list(LAYERS), default=list(LAYERS))
    parser.add_argument("--only", nargs="*", default=None, help="Run only these step names.")
    parser.add_argument("--master", default=None, help="Spark master (default: env / local[*]).")
    args = parser.parse_args()

    spark = get_spark_session("medallion-range-runner", master=args.master)
    try:
        for batch_date in date_range(args.start, args.end):
            for layer in args.layers:
                for name, step in LAYERS[layer]():
                    if args.only and name not in args.only:
                        continue
                    started = time.monotonic()
                    result = step(spark, batch_date)
                    summary = result if isinstance(result, dict) else ""
                    print(
                        f"[{batch_date}] {name} ok ({time.monotonic() - started:.1f}s) {summary}",
                        flush=True,
                    )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
