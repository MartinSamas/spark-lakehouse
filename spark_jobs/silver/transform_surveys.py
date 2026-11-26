"""Silver transformation & quarantine for survey responses and exploded answers.

`silver_survey_responses` (grain: `survey_id`):
- Deduplicates on `survey_id` (valid first, earliest `submitted_ts`, then payload hash).
- Derives `enps_category` (`Promoter` 9-10, `Passive` 7-8, `Detractor` 0-6) and
  `survey_wave`: consecutive survey drop dates form one wave and a gap of more than one day
  starts the next (Wave 1 = 2026-01-09..16, Wave 2 = 2026-02-20..27, ...). The wave is
  derived from Bronze partition dates up to `batch_date`, so it is stable on re-runs and
  needs no hard-coded calendar for the holdout.
- Quarantines responses with a missing id/emp_id, unparseable `submitted_at` or answers,
  an unknown employee, or an `enps_score` outside `[0, 10]` (`silver_quarantine/survey_responses`).

`silver_survey_answers` (grain: `(survey_id, question_id)`):
- `explode()`s the 10-element `answers` array of valid responses and joins
  `survey_questions` metadata (`dimension`, `scale_min`, `scale_max`).
- Quarantines answers outside `[scale_min, scale_max]`, with a missing score, an unknown
  `question_id` or a repeated question (`silver_quarantine/survey_answers`). Answers of a
  quarantined response are quarantined too (`parent_response_quarantined`), keeping the
  eNPS and driver tables consistent.
- Blank/NULL `written_answer` is valid: normalised to NULL and flagged
  `has_written_comment = false`.
"""

from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

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
    path_is_delta,
    reasons_array,
    replace_partition,
    split_valid,
    write_quarantine,
)

ANSWERS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("question_id", StringType()),
            StructField("dimension", StringType()),
            StructField("numeric_score", LongType()),
            StructField("written_answer", StringType()),
        ]
    )
)
PARENT_QUARANTINED = "parent_response_quarantined"

RESPONSE_COLUMNS = (
    "survey_id",
    "emp_id",
    "submitted_ts",
    "survey_date",
    "survey_wave",
    "enps_score",
    "enps_category",
    "overall_comment",
    "has_overall_comment",
    "answer_count",
    "_batch_date",
    "_ingested_at",
    "_source_file",
)
ANSWER_COLUMNS = (
    "survey_id",
    "emp_id",
    "question_id",
    "dimension",
    "numeric_score",
    "scale_min",
    "scale_max",
    "written_answer",
    "has_written_comment",
    "submitted_ts",
    "survey_date",
    "survey_wave",
    "_batch_date",
    "_ingested_at",
    "_source_file",
)


def wave_number(survey_dates: list[date], batch_date: date) -> int | None:
    """Wave index of `batch_date` among survey drop dates (gaps > 1 day start a new wave)."""
    dates = sorted(d for d in set(survey_dates) if d <= batch_date)
    if batch_date not in dates:
        return None
    wave = 1
    for previous, current in zip(dates, dates[1:]):
        if (current - previous).days > 1:
            wave += 1
        if current == batch_date:
            break
    return wave


def _non_blank(column: str) -> F.Column:
    trimmed = F.trim(F.col(column))
    return F.when(F.length(trimmed) > 0, trimmed)


def transform_responses(
    bronze_df: DataFrame, employees_scd2: DataFrame, survey_wave: int | None
) -> tuple[DataFrame, DataFrame]:
    """Return `(valid responses incl. parsed answers, quarantine)` for one batch."""
    known_emps = employees_scd2.select(F.col("emp_id").alias("_known_emp_id")).distinct()
    parsed = (
        bronze_df.withColumn("survey_id", clean_str("survey_id"))
        .withColumn("emp_id", clean_str("emp_id"))
        .withColumn("submitted_ts", F.col("submitted_at").cast("timestamp"))
        .withColumn("survey_date", F.to_date("submitted_ts"))
        .withColumn("survey_wave", F.lit(survey_wave).cast("int"))
        .withColumn("overall_comment", _non_blank("overall_comment"))
        .withColumn("has_overall_comment", F.col("overall_comment").isNotNull())
        .withColumn("answers_parsed", F.from_json("answers", ANSWERS_SCHEMA))
        .withColumn("answer_count", F.size("answers_parsed"))
        .join(F.broadcast(known_emps), F.col("emp_id") == F.col("_known_emp_id"), "left")
    )
    evaluated = parsed.withColumn(
        "dq_reasons",
        reasons_array(
            (F.col("_corrupt_record").isNotNull(), "corrupt_json_record"),
            (F.col("survey_id").isNull(), "missing_survey_id"),
            (F.col("emp_id").isNull(), "missing_emp_id"),
            (F.col("emp_id").isNotNull() & F.col("_known_emp_id").isNull(), "unknown_emp_id"),
            (F.col("submitted_ts").isNull(), "invalid_submitted_at"),
            (F.col("answers_parsed").isNull(), "invalid_answers_payload"),
            (
                F.col("enps_score").isNull() | ~F.col("enps_score").between(0, 10),
                "enps_score_out_of_range",
            ),
        ),
    ).drop("_known_emp_id")

    kept, superseded = deduplicate(
        evaluated,
        ["survey_id"],
        [
            F.size("dq_reasons").asc(),
            F.col("submitted_ts").asc_nulls_last(),
            F.sha2(F.coalesce(F.col("answers"), F.lit("")), 256).asc(),
        ],
    )
    valid, invalid = split_valid(kept)
    valid = valid.withColumn(
        "enps_category",
        F.when(F.col("enps_score") >= 9, "Promoter")
        .when(F.col("enps_score") >= 7, "Passive")
        .otherwise("Detractor"),
    )
    return valid, invalid.unionByName(superseded)


def explode_answers(responses: DataFrame, questions: DataFrame) -> DataFrame:
    """Explode parsed answers, join question metadata and add answer-level `dq_reasons`.

    `responses` must carry `answers_parsed` and its own `dq_reasons` (non-empty for
    quarantined parents, which propagates `parent_response_quarantined`).
    """
    q = questions.select(
        F.col("question_id").alias("_q_id"),
        F.col("dimension").alias("_q_dimension"),
        F.col("scale_min").cast("long").alias("scale_min"),
        F.col("scale_max").cast("long").alias("scale_max"),
    )
    exploded = (
        responses.select(
            "survey_id",
            "emp_id",
            "submitted_ts",
            "survey_date",
            "survey_wave",
            "_batch_date",
            "_ingested_at",
            "_source_file",
            F.col("dq_reasons").alias("_parent_reasons"),
            F.explode("answers_parsed").alias("a"),
        )
        .select(
            "*",
            F.col("a.question_id").alias("question_id"),
            F.col("a.numeric_score").alias("numeric_score"),
            F.col("a.dimension").alias("_answer_dimension"),
            F.trim(F.col("a.written_answer")).alias("written_answer"),
        )
        .drop("a")
    )

    joined = exploded.join(F.broadcast(q), F.col("question_id") == F.col("_q_id"), "left")
    evaluated = (
        joined.withColumn("dimension", F.coalesce("_q_dimension", "_answer_dimension"))
        .withColumn(
            "written_answer",
            F.when(F.length("written_answer") > 0, F.col("written_answer")),
        )
        .withColumn("has_written_comment", F.col("written_answer").isNotNull())
        .withColumn(
            "dq_reasons",
            reasons_array(
                (F.size("_parent_reasons") > 0, PARENT_QUARANTINED),
                (F.col("question_id").isNull() | F.col("_q_id").isNull(), "unknown_question_id"),
                (F.col("numeric_score").isNull(), "missing_numeric_score"),
                (
                    ~F.col("numeric_score").between(F.col("scale_min"), F.col("scale_max")),
                    "score_out_of_range",
                ),
            ),
        )
        .drop("_q_id", "_q_dimension", "_answer_dimension", "_parent_reasons")
    )
    kept, superseded = deduplicate(
        evaluated,
        ["survey_id", "question_id"],
        [
            F.size("dq_reasons").asc(),
            F.col("numeric_score").asc_nulls_last(),
            F.col("written_answer").asc_nulls_last(),
        ],
    )
    return kept.unionByName(superseded)


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    ctx = SilverContext(spark, batch_date, lakehouse_root, run_id)
    if not path_is_delta(spark, ctx.path("bronze", "employee_surveys")):
        # No pulse wave has dropped yet (e.g. Days 1-8): nothing to transform.
        return {"survey_wave": 0, "silver_survey_responses": 0, "silver_survey_answers": 0}
    bronze_all = ctx.read("bronze", "employee_surveys")
    bronze = bronze_all.filter(F.col("_batch_date") == F.lit(ctx.batch_date))
    survey_dates = [r[0] for r in bronze_all.select("_batch_date").distinct().collect()]
    wave = wave_number(survey_dates, date.fromisoformat(ctx.batch_date))

    valid_responses, quarantined_responses = transform_responses(
        bronze, ctx.read("silver", "silver_employees_scd2"), wave
    )
    questions = ctx.read("bronze", "survey_questions")
    answers = explode_answers(
        valid_responses.withColumn("dq_reasons", F.array().cast("array<string>")).unionByName(
            # Superseded duplicates are not exploded: their answers are already kept once.
            quarantined_responses.filter(~F.array_contains("dq_reasons", DUPLICATE_REASON)),
            allowMissingColumns=True,
        ),
        questions,
    )
    valid_answers, quarantined_answers = split_valid(answers)

    writes = (
        (
            valid_responses.select(*RESPONSE_COLUMNS),
            "silver_survey_responses",
            quarantined_responses.drop("answers_parsed"),
            "survey_responses",
        ),
        (
            valid_answers.select(*ANSWER_COLUMNS),
            "silver_survey_answers",
            quarantined_answers,
            "survey_answers",
        ),
    )
    for silver_df, table, quarantine_df, q_table in writes:
        replace_partition(
            silver_df.withColumn("_silver_updated_at", F.current_timestamp()),
            ctx.path("silver", table),
            ctx.batch_date,
        )
        write_quarantine(ctx, q_table, quarantine_df)

    responses_w = ctx.read_partition("silver", "silver_survey_responses")
    answers_w = ctx.read_partition("silver", "silver_survey_answers")
    responses_q = ctx.read_partition("silver_quarantine", "survey_responses")
    answers_q = ctx.read_partition("silver_quarantine", "survey_answers")
    n_resp, n_ans = responses_w.count(), answers_w.count()
    resp_dupes = responses_q.filter(F.array_contains("dq_reasons", DUPLICATE_REASON)).count()
    out_of_range = answers_q.filter(F.array_contains("dq_reasons", "score_out_of_range")).count()
    missing_comments = answers_w.filter(~F.col("has_written_comment")).count()

    def common(table: str) -> dict:
        return dict(run_id=ctx.run_id, batch_date=ctx.batch_date, layer="silver", table_name=table)

    ctx.publish_checks(
        [
            check_not_null_columns(
                responses_w, ["survey_id", "emp_id"], **common("silver_survey_responses")
            ),
            check_unique_key(responses_w, ["survey_id"], **common("silver_survey_responses")),
            check_quarantine_ratio(
                n_resp, responses_q.count() - resp_dupes, **common("silver_survey_responses")
            ),
            check_ratio_threshold(
                resp_dupes,
                bronze.count(),
                check_name="duplicate_rate",
                max_ratio=0.05,
                **common("silver_survey_responses"),
            ),
            check_not_null_columns(
                answers_w, ["survey_id", "question_id"], **common("silver_survey_answers")
            ),
            check_unique_key(
                answers_w, ["survey_id", "question_id"], **common("silver_survey_answers")
            ),
            check_quarantine_ratio(n_ans, answers_q.count(), **common("silver_survey_answers")),
            check_ratio_threshold(
                out_of_range,
                n_ans + answers_q.count(),
                check_name="score_out_of_range_rate",
                max_ratio=0.01,
                **common("silver_survey_answers"),
            ),
            check_ratio_threshold(
                missing_comments,
                n_ans,
                check_name="missing_written_comment_rate",
                max_ratio=0.02,
                **common("silver_survey_answers"),
            ),
        ]
    )
    return {
        "survey_wave": wave or 0,
        "silver_survey_responses": n_resp,
        "responses_quarantine": responses_q.count(),
        "silver_survey_answers": n_ans,
        "answers_quarantine": answers_q.count(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver_survey_responses and silver_survey_answers."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-surveys")
    try:
        print(run(spark, args.batch_date, args.lakehouse_root, args.run_id))
    finally:
        spark.stop()
