"""Shared Bronze ingestion helpers: metadata columns, idempotent Delta writes, path checks.

Conventions applied by every Bronze job:
- Zero lossy transformations: CSV columns are read as strings, nested JSON objects/arrays
  are kept as their raw JSON text, and malformed JSON lines land in `_corrupt_record`
  instead of being silently dropped.
- Standard technical metadata: `_batch_date` (date), `_ingested_at` (timestamp),
  `_source_file` (string).
- Idempotency:
  * daily tables are partitioned by `_batch_date` and only the target partition is
    replaced (`replaceWhere`), so re-running a `batch_date` never duplicates rows;
  * full-load tables are overwritten (older snapshots remain available via Delta
    time travel).
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.spark_session import join_path

logger = logging.getLogger(__name__)

CORRUPT_RECORD_COL = "_corrupt_record"
METADATA_COLUMNS = ("_batch_date", "_ingested_at", "_source_file")


def validate_batch_date(batch_date: str) -> str:
    """Return `batch_date` if it is a valid ISO `YYYY-MM-DD` date, else raise ValueError.

    Validation also guards the `replaceWhere` predicate against malformed input.
    """
    try:
        return date.fromisoformat(batch_date).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"batch_date must be YYYY-MM-DD, got {batch_date!r}") from exc


def daily_partition_dir(landing_root: str, dataset: str, batch_date: str) -> str:
    """Return the Hive-style landing directory `<root>/<dataset>/date=<batch_date>`."""
    return join_path(landing_root, dataset, f"date={batch_date}")


def path_exists(spark: SparkSession, path: str) -> bool:
    """Check path existence through Hadoop FS so it works for local paths and s3a:// URIs."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(hadoop_conf).exists(hpath)


def add_bronze_metadata(df: DataFrame, batch_date: str) -> DataFrame:
    """Append `_batch_date`, `_ingested_at`, and `_source_file` technical columns."""
    return (
        df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def write_daily_partition(df: DataFrame, target_path: str, batch_date: str) -> None:
    """Idempotently replace a single `_batch_date` partition of a Bronze Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("_batch_date")
        .option("replaceWhere", f"_batch_date = '{batch_date}'")
        .save(target_path)
    )


def write_full_snapshot(df: DataFrame, target_path: str) -> None:
    """Overwrite a full-load Bronze Delta table (prior snapshots kept via time travel)."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)


def read_daily_partition(
    spark: SparkSession,
    landing_root: str,
    dataset: str,
    batch_date: str,
    reader,
    *,
    required: bool = True,
) -> DataFrame | None:
    """Read `<dataset>/date=<batch_date>/` using `reader(path) -> DataFrame`.

    Returns None (with a log line) when the partition is absent and `required` is False,
    e.g. weekend `fx_rates` or non-wave `employee_surveys` dates. Raises
    FileNotFoundError when a required partition is missing.
    """
    partition_dir = daily_partition_dir(landing_root, dataset, batch_date)
    if not path_exists(spark, partition_dir):
        if required:
            raise FileNotFoundError(
                f"Missing mandatory landing partition for {dataset}: {partition_dir}"
            )
        logger.info("No %s partition for batch_date=%s; skipping.", dataset, batch_date)
        return None
    return reader(partition_dir)
