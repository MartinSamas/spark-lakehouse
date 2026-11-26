"""Unit tests for Silver transformations (pure DataFrame functions) and DQ checks."""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from dq.checks import (
    check_non_empty_partition,
    check_quarantine_ratio,
    check_scd2_no_overlaps,
    check_scd2_single_current,
    check_unique_key,
)
from spark_jobs.silver._common import DUPLICATE_REASON, parse_flexible_date
from spark_jobs.silver.transform_crm_events import JOIN_STRATEGIES, transform_crm_events
from spark_jobs.silver.transform_employees_scd2 import build_scd2, validate_snapshots
from spark_jobs.silver.transform_fx_rates import build_daily_rates, validate_fx
from spark_jobs.silver.transform_invoices_expenses import transform_expenses, transform_invoices
from spark_jobs.silver.transform_surveys import (
    explode_answers,
    transform_responses,
    wave_number,
)
from spark_jobs.silver.transform_timesheets import transform_timesheets

TS = datetime(2026, 1, 5, 12, 0, 0)
DQ = dict(run_id="t", batch_date="2026-01-05", layer="silver", table_name="t")


def _meta(batch: str = "2026-01-05", ingested: datetime = TS) -> dict:
    return {
        "_batch_date": date.fromisoformat(batch),
        "_ingested_at": ingested,
        "_source_file": f"file:///landing/date={batch}/part-000",
    }


def _reasons(df, key: str) -> dict[str, list[str]]:
    return {r[key]: sorted(r["dq_reasons"]) for r in df.collect()}


# --------------------------------------------------------------------------- common


def test_parse_flexible_date_handles_iso_and_european(spark) -> None:
    df = spark.createDataFrame(
        [("2026-01-31",), ("31.01.2026",), ("2026-01-31T10:00",), ("31/01/2026",), (None,)],
        "raw string",
    )
    parsed = [r[0] for r in df.select(parse_flexible_date(F.col("raw"))).collect()]
    assert parsed == [date(2026, 1, 31), date(2026, 1, 31), None, None, None]


# --------------------------------------------------------------------------- fx rates


def _fx_bronze(spark, rows: list[tuple[str, str, str, float | None]]):
    return spark.createDataFrame(
        [
            Row(
                rate_date=d,
                currency_from=f,
                currency_to=t,
                rate=r,
                _corrupt_record=None,
                **_meta(d),
            )
            for d, f, t, r in rows
        ],
        "rate_date string, currency_from string, currency_to string, rate double, "
        "_corrupt_record string, _batch_date date, _ingested_at timestamp, _source_file string",
    )


def test_fx_spine_forward_fills_weekend_and_extends_to_batch_date(spark) -> None:
    bronze = _fx_bronze(
        spark,
        [
            ("2026-01-02", "USD", "EUR", 0.92),  # Friday
            ("2026-01-05", "USD", "EUR", 0.93),  # Monday
            ("2026-01-02", "CZK", "EUR", 0.04),
            ("2026-01-05", "CZK", "EUR", 0.041),
        ],
    )
    valid, quarantine = validate_fx(bronze)
    assert quarantine.count() == 0

    daily = build_daily_rates(valid, end_date="2026-01-06")
    assert daily.count() == 5 * 2  # Jan 2..6 x 2 pairs
    assert daily.filter("rate IS NULL").count() == 0
    saturday = daily.filter("rate_date = '2026-01-03' AND currency_from = 'USD'").first()
    assert saturday.rate == 0.92
    assert saturday.is_forward_filled is True
    assert saturday.source_rate_date == date(2026, 1, 2)
    tuesday = daily.filter("rate_date = '2026-01-06' AND currency_from = 'USD'").first()
    assert (tuesday.rate, tuesday.is_forward_filled) == (0.93, True)


def test_fx_quarantines_invalid_rows_and_duplicates(spark) -> None:
    bronze = _fx_bronze(
        spark,
        [
            ("2026-01-05", "USD", "EUR", 0.93),
            ("2026-01-05", "USD", "EUR", 0.93),  # exact duplicate
            ("2026-01-05", "GBP", "EUR", 1.15),  # unknown currency
            ("2026-01-05", "CZK", "EUR", -1.0),  # non-positive rate
        ],
    )
    valid, quarantine = validate_fx(bronze)
    assert valid.count() == 1
    reasons = sorted(tuple(r.dq_reasons) for r in quarantine.collect())
    assert reasons == [(DUPLICATE_REASON,), ("invalid_currency_from",), ("invalid_rate",)]


# --------------------------------------------------------------------------- employees SCD2

EMP_COLUMNS = (
    "emp_id full_name department role seniority_level hourly_rate_eur hire_date "
    "last_promotion_date work_mode distance_km manager_id is_active termination_date "
    "churn_type snapshot_date"
).split()


def _emp_row(snapshot: str, **overrides) -> dict:
    row = dict(
        emp_id="EMP_1",
        full_name="Ada",
        department="Data",
        role="Engineer",
        seniority_level="Mid",
        hourly_rate_eur="50.0",
        hire_date="2024-01-01",
        last_promotion_date="2025-01-01",
        work_mode="Remote",
        distance_km="10.0",
        manager_id="EMP_9",
        is_active="true",
        termination_date=None,
        churn_type=None,
        snapshot_date=snapshot,
    )
    row.update(overrides)
    return {**row, **_meta(snapshot)}


def _emp_bronze(spark, rows: list[dict]):
    schema = ", ".join(f"{c} string" for c in EMP_COLUMNS)
    return spark.createDataFrame(
        rows, f"{schema}, _batch_date date, _ingested_at timestamp, _source_file string"
    )


def test_scd2_collapses_unchanged_snapshots_and_closes_intervals(spark) -> None:
    bronze = _emp_bronze(
        spark,
        [
            _emp_row("2026-01-01"),
            _emp_row("2026-01-02"),  # unchanged -> collapsed
            _emp_row("2026-01-03", hourly_rate_eur="55.0"),  # raise
            _emp_row("2026-01-04", hourly_rate_eur="55.0"),
            _emp_row(
                "2026-01-05",
                hourly_rate_eur="55.0",
                is_active="false",
                termination_date="2026-01-05",
                churn_type="voluntary",
            ),
        ],
    )
    valid, quarantine = validate_snapshots(bronze)
    assert quarantine.count() == 0
    versions = build_scd2(valid).orderBy("valid_from").collect()

    assert [(v.valid_from, v.valid_to, v.is_current) for v in versions] == [
        (date(2026, 1, 1), date(2026, 1, 2), False),
        (date(2026, 1, 3), date(2026, 1, 4), False),
        (date(2026, 1, 5), date(9999, 12, 31), True),
    ]
    assert [v.hourly_rate_eur for v in versions] == [50.0, 55.0, 55.0]
    assert versions[-1].termination_date == date(2026, 1, 5)
    assert [v.version_number for v in versions] == [1, 2, 3]

    scd2 = build_scd2(valid)
    assert check_scd2_no_overlaps(scd2, **DQ).status == "PASS"
    assert check_scd2_single_current(scd2, **DQ).status == "PASS"
    assert check_unique_key(scd2, ["emp_id", "valid_from"], **DQ).status == "PASS"


def test_scd2_validation_quarantines_bad_snapshot_rows(spark) -> None:
    bronze = _emp_bronze(
        spark,
        [
            _emp_row("2026-01-01"),
            _emp_row("2026-01-02", emp_id="EMP_2", hourly_rate_eur="abc"),
            _emp_row("2026-01-02", emp_id="EMP_3", department="Marketing"),
            _emp_row("2026-01-02", emp_id="EMP_4", is_active="false"),
        ],
    )
    valid, quarantine = validate_snapshots(bronze)
    assert valid.count() == 1
    assert _reasons(quarantine, "emp_id") == {
        "EMP_2": ["invalid_hourly_rate_eur"],
        "EMP_3": ["invalid_department"],
        "EMP_4": ["inactive_without_termination_date"],
    }


def test_scd2_dq_checks_detect_overlaps_and_multiple_current(spark) -> None:
    bad = spark.createDataFrame(
        [
            ("EMP_1", date(2026, 1, 1), date(2026, 1, 10), False),
            ("EMP_1", date(2026, 1, 5), date(9999, 12, 31), True),
            ("EMP_2", date(2026, 1, 1), date(9999, 12, 31), True),
            ("EMP_2", date(2026, 1, 3), date(9999, 12, 31), True),
        ],
        "emp_id string, valid_from date, valid_to date, is_current boolean",
    )
    overlaps = check_scd2_no_overlaps(bad, **DQ)
    assert (overlaps.status, overlaps.failed_count) == ("FAIL", 2)
    current = check_scd2_single_current(bad, **DQ)
    assert (current.status, current.failed_count) == ("FAIL", 1)


# --------------------------------------------------------------------------- timesheets


def _scd2_fixture(spark):
    return spark.createDataFrame(
        [
            ("EMP_1", date(2026, 1, 1), date(9999, 12, 31), None),
            ("EMP_2", date(2026, 1, 1), date(2026, 1, 4), None),
            ("EMP_2", date(2026, 1, 5), date(9999, 12, 31), date(2026, 1, 5)),
        ],
        "emp_id string, valid_from date, valid_to date, termination_date date",
    )


def test_timesheets_dedup_tiebreaker_and_quarantine_reasons(spark) -> None:
    d = date(2026, 1, 5)
    rows = [
        ("TS_1", "EMP_1", "PRJ_1", d, 2.25),  # near-duplicate (higher hours)
        ("TS_1", "EMP_1", "PRJ_1", d, 1.75),  # kept: lowest valid hours
        ("TS_2", "EMP_1", "PRJ_1", d, 30.0),  # invalid duplicate loses to valid one
        ("TS_2", "EMP_1", "PRJ_1", d, 8.0),
        ("TS_3", "EMP_1", None, d, 4.0),
        ("TS_4", "EMP_1", "PRJ_1", d, 0.0),
        ("TS_5", "EMP_2", "PRJ_1", d, 4.0),  # zombie: work_date >= termination_date
        ("TS_6", "EMP_1", "PRJ_X", d, 4.0),  # unknown project
        ("TS_7", "EMP_404", "PRJ_1", d, 4.0),
    ]
    bronze = spark.createDataFrame(
        [
            Row(ts_id=a, emp_id=b, project_id=c, work_date=w, hours=h, **_meta())
            for a, b, c, w, h in rows
        ],
        "ts_id string, emp_id string, project_id string, work_date date, hours double, "
        "_batch_date date, _ingested_at timestamp, _source_file string",
    )
    projects = spark.createDataFrame([("PRJ_1",)], "project_id string")
    silver, quarantine = transform_timesheets(bronze, _scd2_fixture(spark), projects)

    assert {r.ts_id: r.hours for r in silver.collect()} == {"TS_1": 1.75, "TS_2": 8.0}
    q = sorted((r.ts_id, tuple(r.dq_reasons)) for r in quarantine.collect())
    assert q == [
        ("TS_1", (DUPLICATE_REASON,)),
        ("TS_2", ("hours_exceed_24", DUPLICATE_REASON)),
        ("TS_3", ("missing_project_id",)),
        ("TS_4", ("non_positive_hours",)),
        ("TS_5", ("post_termination_work",)),
        ("TS_6", ("orphan_project_id",)),
        ("TS_7", ("unknown_employee_on_work_date",)),
    ]
    assert silver.count() + quarantine.count() == bronze.count()  # zero silent loss


# --------------------------------------------------------------------------- invoices & expenses


def _fx_daily(spark):
    return spark.createDataFrame(
        [
            (date(2026, 1, 3), "USD", "EUR", 0.9),  # Saturday, forward-filled in real data
            (date(2026, 1, 3), "CZK", "EUR", 0.04),
        ],
        "rate_date date, currency_from string, currency_to string, rate double",
    )


def test_invoices_parse_dates_keep_credit_notes_and_quarantine_orphans(spark) -> None:
    rows = [
        ("INV_1", "CLI_1", "PRJ_1", 100.0, "USD", "2026-01-03", None),
        ("INV_2", "CLI_1", "PRJ_1", -50.0, "CZK", "03.01.2026", "04.01.2026"),
        ("INV_3", "CLI_1", "PRJ_1", 10.0, "EUR", "2026-01-03", "2026-01-04"),
        ("INV_4", "CLI_1", "PRJ_ORPHAN_9", 10.0, "EUR", "2026-01-03", None),
        ("INV_5", "CLI_1", "PRJ_1", 10.0, "EUR", "3rd Jan", None),
    ]
    bronze = spark.createDataFrame(
        [
            Row(
                inv_id=i,
                client_id=c,
                project_id=p,
                amount=a,
                currency=cur,
                issued_at=iss,
                paid_at=paid,
                _corrupt_record=None,
                **_meta("2026-01-03"),
            )
            for i, c, p, a, cur, iss, paid in rows
        ],
        "inv_id string, client_id string, project_id string, amount double, currency string, "
        "issued_at string, paid_at string, _corrupt_record string, _batch_date date, "
        "_ingested_at timestamp, _source_file string",
    )
    projects = spark.createDataFrame([("PRJ_1",)], "project_id string")
    clients = spark.createDataFrame([("CLI_1",)], "client_id string")
    silver, quarantine = transform_invoices(bronze, projects, clients, _fx_daily(spark))

    by_id = {r.inv_id: r for r in silver.collect()}
    assert set(by_id) == {"INV_1", "INV_2", "INV_3"}
    assert (by_id["INV_1"].amount_eur, by_id["INV_1"].is_paid) == (90.0, False)
    assert by_id["INV_2"].is_credit_note is True
    assert by_id["INV_2"].amount_eur == -2.0
    assert by_id["INV_2"].paid_date == date(2026, 1, 4)
    assert (by_id["INV_3"].fx_rate_to_eur, by_id["INV_3"].amount_eur) == (1.0, 10.0)
    assert _reasons(quarantine, "inv_id") == {
        "INV_4": ["orphan_project_id"],
        "INV_5": ["invalid_issued_at"],
    }


def test_expenses_dedup_and_missing_fx_rate_yields_null_eur(spark) -> None:
    rows = [
        ("EXP_1", "Data", "Training", "656.04", "USD", "2026-01-03"),
        ("EXP_1", "Data", "Training", "635.77", "USD", "2026-01-03"),  # kept: lowest amount
        ("EXP_2", "Data", "Training", "100", "CZK", "2026-01-09"),  # no FX rate on that date
        ("EXP_3", "Marketing", "Training", "1", "EUR", "2026-01-03"),
    ]
    bronze = spark.createDataFrame(
        [
            Row(
                exp_id=e,
                department=d,
                category=c,
                amount=a,
                currency=cur,
                expense_date=x,
                **_meta(),
            )
            for e, d, c, a, cur, x in rows
        ],
        "exp_id string, department string, category string, amount string, currency string, "
        "expense_date string, _batch_date date, _ingested_at timestamp, _source_file string",
    )
    silver, quarantine = transform_expenses(bronze, _fx_daily(spark))
    by_id = {r.exp_id: r for r in silver.collect()}
    assert by_id["EXP_1"].amount == 635.77
    assert by_id["EXP_1"].amount_eur == round(635.77 * 0.9, 2)
    assert by_id["EXP_2"].amount_eur is None  # caught by the hard zero_null_amount_eur check
    assert _reasons(quarantine, "exp_id") == {
        "EXP_1": [DUPLICATE_REASON],
        "EXP_3": ["invalid_department"],
    }


# --------------------------------------------------------------------------- CRM events


def _crm_bronze(spark):
    events = [
        (
            "E1",
            "VIP_CORP_001",
            "api_call",
            "2026-01-05T10:00:00Z",
            {"latency_ms": 120, "status_code": 200, "endpoint": "/v1", "region": "eu"},
        ),
        (
            "E2",
            "CLI_2",
            "support_ticket",
            "2026-01-05T11:00:00Z",
            {"severity": "critical", "ticket_id": "T1", "module": "m", "sla_hours": 4},
        ),
        ("E3", "CLI_404", "page_view", "2026-01-05T12:00:00Z", {"path": "/"}),
        ("E4", "CLI_2", "unknown_type", "not-a-timestamp", {"x": 1}),
    ]
    return spark.createDataFrame(
        [
            Row(
                event_id=i,
                client_id=c,
                event_type=t,
                timestamp=ts,
                payload=json.dumps(p),
                _corrupt_record=None,
                **_meta(),
            )
            for i, c, t, ts, p in events
        ],
        "event_id string, client_id string, event_type string, timestamp string, "
        "payload string, _corrupt_record string, _batch_date date, _ingested_at timestamp, "
        "_source_file string",
    )


@pytest.mark.parametrize("strategy", JOIN_STRATEGIES)
def test_crm_events_all_join_strategies_give_identical_results(spark, strategy) -> None:
    clients = spark.createDataFrame(
        [("VIP_CORP_001", "VIP", "Enterprise"), ("CLI_2", "Two", "Gold")],
        "client_id string, client_name string, tier string",
    )
    silver, quarantine = transform_crm_events(_crm_bronze(spark), clients, strategy)
    by_id = {r.event_id: r for r in silver.collect()}

    assert set(by_id) == {"E1", "E2"}
    assert (by_id["E1"].client_tier, by_id["E1"].latency_ms) == ("Enterprise", 120)
    assert by_id["E1"].event_ts == datetime(2026, 1, 5, 10, 0, 0)
    assert by_id["E2"].is_critical_ticket is True
    assert _reasons(quarantine, "event_id") == {
        "E3": ["orphan_client_id"],
        "E4": ["invalid_event_type", "invalid_timestamp"],
    }


# --------------------------------------------------------------------------- surveys


def test_wave_number_groups_consecutive_drop_dates() -> None:
    dates = [date(2026, 1, d) for d in range(9, 17)] + [date(2026, 2, 20), date(2026, 2, 21)]
    assert wave_number(dates, date(2026, 1, 9)) == 1
    assert wave_number(dates, date(2026, 1, 16)) == 1
    assert wave_number(dates, date(2026, 2, 21)) == 2
    assert wave_number(dates, date(2026, 2, 1)) is None


def test_surveys_dedup_explode_bounds_and_blank_comments(spark) -> None:
    def answers(q01: int, q02: int, q02_text: str | None) -> str:
        return json.dumps(
            [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "numeric_score": q01,
                    "written_answer": "ok",
                },
                {
                    "question_id": "Q02",
                    "dimension": "work_life_balance",
                    "numeric_score": q02,
                    "written_answer": q02_text,
                },
            ]
        )

    rows = [
        ("S1", "EMP_1", 9, answers(9, 4, "  ")),  # blank written answer -> flagged
        ("S1", "EMP_1", 9, answers(9, 4, "  ")),  # exact duplicate submission
        ("S2", "EMP_1", 6, answers(6, 11, "too high")),  # Q02 out of [1, 5]
        ("S3", "EMP_1", -1, answers(-1, 3, "x")),  # response out of range
    ]
    bronze = spark.createDataFrame(
        [
            Row(
                survey_id=s,
                emp_id=e,
                submitted_at="2026-01-09T08:00:00Z",
                enps_score=n,
                overall_comment=None,
                answers=a,
                _corrupt_record=None,
                **_meta("2026-01-09"),
            )
            for s, e, n, a in rows
        ],
        "survey_id string, emp_id string, submitted_at string, enps_score long, "
        "overall_comment string, answers string, _corrupt_record string, _batch_date date, "
        "_ingested_at timestamp, _source_file string",
    )
    employees = spark.createDataFrame([("EMP_1",)], "emp_id string")
    questions = spark.createDataFrame(
        [("Q01", "enps", 0, 10), ("Q02", "work_life_balance", 1, 5)],
        "question_id string, dimension string, scale_min long, scale_max long",
    )

    valid, quarantined = transform_responses(bronze, employees, survey_wave=1)
    assert {r.survey_id: r.enps_category for r in valid.collect()} == {
        "S1": "Promoter",
        "S2": "Detractor",
    }
    assert sorted(tuple(r.dq_reasons) for r in quarantined.collect()) == [
        (DUPLICATE_REASON,),
        ("enps_score_out_of_range",),
    ]

    parents = valid.withColumn("dq_reasons", F.array().cast("array<string>")).unionByName(
        quarantined.filter(~F.array_contains("dq_reasons", DUPLICATE_REASON)),
        allowMissingColumns=True,
    )
    answers_df = explode_answers(parents, questions)
    ok = {
        (r.survey_id, r.question_id): r for r in answers_df.filter("size(dq_reasons) = 0").collect()
    }
    bad = {
        (r.survey_id, r.question_id): sorted(r.dq_reasons)
        for r in answers_df.filter("size(dq_reasons) > 0").collect()
    }
    assert set(ok) == {("S1", "Q01"), ("S1", "Q02"), ("S2", "Q01")}
    assert ok[("S1", "Q02")].has_written_comment is False
    assert ok[("S1", "Q02")].written_answer is None
    assert bad == {
        ("S2", "Q02"): ["score_out_of_range"],
        ("S3", "Q01"): ["parent_response_quarantined", "score_out_of_range"],
        ("S3", "Q02"): ["parent_response_quarantined"],
    }


# --------------------------------------------------------------------------- generic DQ checks


def test_generic_checks_status_semantics(spark) -> None:
    df = spark.createDataFrame(
        [("a", date(2026, 1, 3)), ("a", date(2026, 1, 3))], "k string, _batch_date date"
    )
    dup = check_unique_key(df, ["k"], **DQ)
    assert (dup.status, dup.failed_count, dup.total_count) == ("FAIL", 2, 2)

    assert check_quarantine_ratio(90, 10, **DQ).status == "WARN"  # 10% > 8%
    assert check_quarantine_ratio(95, 5, **DQ).status == "PASS"

    empty = df.limit(0)
    weekend = {**DQ, "batch_date": "2026-01-03"}  # Saturday
    assert check_non_empty_partition(empty, allow_weekends_empty=True, **weekend).status == "PASS"
    assert check_non_empty_partition(empty, **weekend).status == "FAIL"
    assert check_non_empty_partition(df, **weekend).status == "PASS"
