"""Silver skew-aware transformation for `silver_crm_events`.

- Parses `timestamp` (ISO-8601 UTC) to `event_ts` / `event_date` and the polymorphic
  `payload` JSON with a superset schema (the 5 event types use disjoint keys), flattening
  typed attributes while keeping the original text in `payload_raw`.
- Quarantines corrupt JSON, missing `event_id`, unparseable timestamps, unknown
  `event_type`, unparseable payloads and unknown `client_id` into
  `silver_quarantine/crm_events`; deduplicates on `event_id`.
- Enriches with `silver_clients` (`client_name`, `client_tier`). `VIP_CORP_001` carries ~45%
  of all events, so the join strategy is explicit and selectable (`--strategy`):
    * `broadcast` (default): `silver_clients` is a 300-row dimension, so broadcasting it
      removes the shuffle on the skewed key entirely.
    * `salted`: key salting with `SALT_BUCKETS` replicas of each client row; spreads the
      hot key across shuffle partitions when the dimension is too large to broadcast.
    * `sort_merge`: unoptimized baseline (broadcast + AQE skew handling disabled) used only
      to capture "before" Spark UI evidence for `docs/architecture.md`.
- Only the `_batch_date` partition is replaced.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from dq.checks import (
    check_not_null_columns,
    check_quarantine_ratio,
    check_ratio_threshold,
    check_unique_key,
)
from spark_jobs.common.spark_session import get_spark_session
from spark_jobs.silver._common import (
    DUPLICATE_REASON,
    SilverContext,
    clean_str,
    deduplicate,
    reasons_array,
    replace_partition,
    split_valid,
    write_quarantine,
)

VALID_EVENT_TYPES = (
    "page_view",
    "support_ticket",
    "contract_renewal",
    "api_call",
    "meeting_logged",
)
JOIN_STRATEGIES = ("broadcast", "salted", "sort_merge")
SALT_BUCKETS = 16
SKEWED_CLIENT_ID = "VIP_CORP_001"

# Superset of the per-event-type payload keys (no key is shared across event types).
PAYLOAD_SCHEMA = StructType(
    [
        # page_view
        StructField("path", StringType()),
        StructField("referrer", StringType()),
        StructField("duration_sec", LongType()),
        StructField("device", StringType()),
        # support_ticket
        StructField("ticket_id", StringType()),
        StructField("severity", StringType()),
        StructField("module", StringType()),
        StructField("sla_hours", LongType()),
        # contract_renewal
        StructField("opportunity_id", StringType()),
        StructField("stage", StringType()),
        StructField("arr_delta_eur", DoubleType()),
        StructField("probability", DoubleType()),
        # api_call
        StructField("endpoint", StringType()),
        StructField("status_code", LongType()),
        StructField("latency_ms", LongType()),
        StructField("region", StringType()),
        # meeting_logged
        StructField("meeting_type", StringType()),
        StructField("attendees_count", LongType()),
        StructField("outcome", StringType()),
        StructField("next_step_days", LongType()),
    ]
)
PAYLOAD_FIELDS = tuple(f.name for f in PAYLOAD_SCHEMA.fields)

OUTPUT_COLUMNS = (
    "event_id",
    "client_id",
    "client_name",
    "client_tier",
    "event_type",
    "event_ts",
    "event_date",
    *PAYLOAD_FIELDS,
    "is_critical_ticket",
    "payload_raw",
    "_batch_date",
    "_ingested_at",
    "_source_file",
)


@contextmanager
def _session_conf(spark: SparkSession, overrides: dict[str, str]) -> Iterator[None]:
    """Temporarily set SQL confs (restored afterwards, even on error)."""
    previous = {key: spark.conf.get(key, None) for key in overrides}
    try:
        for key, value in overrides.items():
            spark.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def parse_events(bronze_df: DataFrame) -> DataFrame:
    """Type the raw events and flatten the payload; adds `dq_reasons` (except FK checks)."""
    parsed = (
        bronze_df.withColumn("event_id", clean_str("event_id"))
        .withColumn("client_id", clean_str("client_id"))
        .withColumn("event_type", clean_str("event_type"))
        .withColumn("event_ts", F.col("timestamp").cast("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("payload_raw", F.col("payload"))
        .withColumn("_payload", F.from_json("payload", PAYLOAD_SCHEMA))
        .select("*", *[F.col(f"_payload.{name}").alias(name) for name in PAYLOAD_FIELDS])
        .withColumn(
            "is_critical_ticket",
            (F.col("event_type") == "support_ticket") & (F.col("severity") == "critical"),
        )
    )
    return parsed.withColumn(
        "dq_reasons",
        reasons_array(
            (F.col("_corrupt_record").isNotNull(), "corrupt_json_record"),
            (F.col("event_id").isNull(), "missing_event_id"),
            (F.col("client_id").isNull(), "missing_client_id"),
            (F.col("event_ts").isNull(), "invalid_timestamp"),
            (
                ~F.coalesce(F.col("event_type").isin(*VALID_EVENT_TYPES), F.lit(False)),
                "invalid_event_type",
            ),
            (F.col("payload").isNull() | F.col("_payload").isNull(), "invalid_payload"),
        ),
    ).drop("_payload", "payload", "timestamp")


def join_clients(events: DataFrame, clients: DataFrame, strategy: str) -> DataFrame:
    """Left-join `client_name` / `client_tier` onto events with the chosen skew strategy."""
    dim = clients.select(
        F.col("client_id").alias("_dim_client_id"),
        "client_name",
        F.col("tier").alias("client_tier"),
    )
    key_match = F.col("client_id") == F.col("_dim_client_id")

    if strategy == "broadcast":
        return events.join(F.broadcast(dim), key_match, "left").drop("_dim_client_id")

    if strategy == "salted":
        salted_events = events.withColumn(
            "_salt", F.pmod(F.xxhash64("event_id"), F.lit(SALT_BUCKETS)).cast("int")
        )
        salted_dim = dim.withColumn(
            "_salt", F.explode(F.sequence(F.lit(0), F.lit(SALT_BUCKETS - 1)))
        )
        joined = salted_events.join(
            salted_dim.hint("merge"),
            key_match & (salted_events["_salt"] == salted_dim["_salt"]),
            "left",
        )
        return joined.drop("_dim_client_id", "_salt")

    if strategy == "sort_merge":
        return events.join(dim.hint("merge"), key_match, "left").drop("_dim_client_id")

    raise ValueError(f"Unknown join strategy {strategy!r}; expected one of {JOIN_STRATEGIES}")


def strategy_conf(strategy: str) -> dict[str, str]:
    """Session confs that make the chosen strategy observable in the Spark UI."""
    if strategy == "broadcast":
        return {}
    # Disable automatic broadcast and AQE skew splitting so the shuffle join (and its skew,
    # or the effect of salting) is what actually runs.
    return {
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.skewJoin.enabled": "false",
    }


def transform_crm_events(
    bronze_df: DataFrame, clients: DataFrame, strategy: str = "broadcast"
) -> tuple[DataFrame, DataFrame]:
    """Return `(silver_crm_events, quarantine)` for one Bronze batch (lazy DataFrames)."""
    enriched = join_clients(parse_events(bronze_df), clients, strategy)
    evaluated = enriched.withColumn(
        "dq_reasons",
        F.when(
            F.col("client_id").isNotNull() & F.col("client_name").isNull(),
            F.array_union("dq_reasons", F.array(F.lit("orphan_client_id"))),
        ).otherwise(F.col("dq_reasons")),
    )
    kept, superseded = deduplicate(
        evaluated,
        ["event_id"],
        [
            F.size("dq_reasons").asc(),
            F.col("_ingested_at").desc(),
            F.col("event_ts").asc_nulls_last(),
            F.col("payload_raw").asc_nulls_last(),
        ],
    )
    valid, invalid = split_valid(kept)
    return valid.select(*OUTPUT_COLUMNS), invalid.unionByName(superseded)


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
    strategy: str = "broadcast",
) -> dict[str, int]:
    ctx = SilverContext(spark, batch_date, lakehouse_root, run_id)
    bronze = ctx.read_partition("bronze", "crm_events")
    with _session_conf(spark, strategy_conf(strategy)):
        silver, quarantine = transform_crm_events(
            bronze, ctx.read("silver", "silver_clients"), strategy
        )
        replace_partition(
            silver.withColumn("_silver_updated_at", F.current_timestamp()),
            ctx.path("silver", "silver_crm_events"),
            ctx.batch_date,
        )
        write_quarantine(ctx, "crm_events", quarantine)

    written = ctx.read_partition("silver", "silver_crm_events")
    quarantine = ctx.read_partition("silver_quarantine", "crm_events")
    valid_count = written.count()
    duplicates = quarantine.filter(F.array_contains("dq_reasons", DUPLICATE_REASON)).count()
    vip_events = written.filter(F.col("client_id") == SKEWED_CLIENT_ID).count()
    common = dict(
        run_id=ctx.run_id, batch_date=ctx.batch_date, layer="silver", table_name="silver_crm_events"
    )
    ctx.publish_checks(
        [
            check_not_null_columns(written, ["event_id", "client_id", "event_ts"], **common),
            check_unique_key(written, ["event_id"], **common),
            check_quarantine_ratio(valid_count, quarantine.count() - duplicates, **common),
            # Informational skew metric: share of events on the hot key (expected ~45%).
            check_ratio_threshold(
                vip_events,
                valid_count,
                check_name="skewed_key_share_vip_corp_001",
                max_ratio=0.60,
                **common,
            ),
        ]
    )
    return {"silver_crm_events": valid_count, "quarantine": quarantine.count(), "vip": vip_events}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_crm_events.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    parser.add_argument("--strategy", choices=JOIN_STRATEGIES, default="broadcast")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-crm-events")
    try:
        print(run(spark, args.batch_date, args.lakehouse_root, args.run_id, args.strategy))
    finally:
        spark.stop()
