"""Unit tests for Bronze ingestion jobs: metadata, raw preservation, idempotency, missing files."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spark_jobs.bronze import ingest_crm_surveys, ingest_finance, ingest_hr, ingest_reference
from spark_jobs.bronze._io import METADATA_COLUMNS, validate_batch_date
from spark_jobs.common.spark_session import join_path

D1 = "2026-01-02"  # Friday
D2 = "2026-01-03"  # Saturday (no fx_rates, no surveys)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _jsonl(rows: list[dict]) -> str:
    return "\n".join(json.dumps(r) for r in rows) + "\n"


def _part(landing: Path, dataset: str, batch_date: str, ext: str) -> Path:
    return landing / dataset / f"date={batch_date}" / f"part-000.{ext}"


def _bronze(spark, lakehouse: Path, table: str):
    return spark.read.format("delta").load(join_path(str(lakehouse), "bronze", table))


def _assert_metadata(df, expected_dates: set[date]) -> None:
    assert set(METADATA_COLUMNS).issubset(df.columns)
    assert df.filter(" OR ".join(f"{c} IS NULL" for c in METADATA_COLUMNS)).count() == 0
    assert {r["_batch_date"] for r in df.select("_batch_date").distinct().collect()} == (
        expected_dates
    )


@pytest.fixture()
def roots(tmp_path: Path) -> tuple[Path, Path]:
    return tmp_path / "landing", tmp_path / "lakehouse"


def test_validate_batch_date_rejects_bad_input() -> None:
    assert validate_batch_date("2026-01-02") == "2026-01-02"
    for bad in ("2026-13-01", "02.01.2026", "2026-01-02' OR '1'='1"):
        with pytest.raises(ValueError):
            validate_batch_date(bad)


def test_reference_full_load_overwrite_is_idempotent(spark, roots) -> None:
    landing, lakehouse = roots
    _write(
        landing / "projects" / "projects.csv",
        "project_id,client_id,budget_eur,status\n"
        "PRJ_0001,CLI_0009,12200.0,Active\nPRJ_0002,CLI_0064,105900.0,Planned\n",
    )
    _write(
        landing / "survey_questions" / "survey_questions.json",
        json.dumps(
            [
                {
                    "question_id": "Q01",
                    "dimension": "enps",
                    "scale_min": 0,
                    "scale_max": 10,
                    "prompt_text": "Recommend?",
                },
                {
                    "question_id": "Q02",
                    "dimension": "work_life_balance",
                    "scale_min": 1,
                    "scale_max": 5,
                    "prompt_text": "Balance?",
                },
            ],
            indent=2,
        ),
    )

    for _ in range(2):
        counts = ingest_reference.run(spark, D1, str(landing), str(lakehouse))
    assert counts == {"projects": 2, "survey_questions": 2}

    projects = _bronze(spark, lakehouse, "projects")
    _assert_metadata(projects, {date(2026, 1, 2)})
    # Raw string preserved exactly (no numeric re-formatting in Bronze).
    assert projects.filter("project_id = 'PRJ_0001'").first()["budget_eur"] == "12200.0"

    questions = _bronze(spark, lakehouse, "survey_questions")
    assert questions.filter("_corrupt_record IS NOT NULL").count() == 0
    assert questions.filter("question_id = 'Q01'").first()["scale_max"] == 10


def test_hr_daily_partitions_replace_only_target_date(spark, roots) -> None:
    landing, lakehouse = roots
    header = (
        "emp_id,full_name,department,role,seniority_level,hourly_rate_eur,hire_date,"
        "last_promotion_date,work_mode,distance_km,manager_id,is_active,termination_date,"
        "churn_type,snapshot_date\n"
    )
    for d, n in ((D1, 2), (D2, 3)):
        rows = "".join(
            f"EMP_{i:04d},Name {i},Data,Engineer,Mid,50.10,2024-01-01,,Remote,1.0,,true,,,{d}\n"
            for i in range(n)
        )
        _write(_part(landing, "employees", d, "csv"), header + rows)
        ts_path = _part(landing, "timesheets", d, "parquet")
        ts_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "ts_id": [f"TS_{d}_{i}" for i in range(n)],
                    "emp_id": [f"EMP_{i:04d}" for i in range(n)],
                    "project_id": [None] + ["PRJ_0001"] * (n - 1),
                    "work_date": [date.fromisoformat(d)] * n,
                    "hours": [-1.0] + [8.0] * (n - 1),
                }
            ),
            ts_path,
        )

    ingest_hr.run(spark, D1, str(landing), str(lakehouse))
    ingest_hr.run(spark, D2, str(landing), str(lakehouse))
    # Re-run D1 after the upstream file changed: only D1 is replaced, D2 is untouched.
    _write(
        _part(landing, "employees", D1, "csv"),
        header + f"EMP_0000,Renamed,Data,Engineer,Mid,50.10,2024-01-01,,Remote,1.0,,true,,,{D1}\n",
    )
    counts = ingest_hr.run(spark, D1, str(landing), str(lakehouse))
    assert counts == {"employees": 1, "timesheets": 2}

    employees = _bronze(spark, lakehouse, "employees")
    _assert_metadata(employees, {date(2026, 1, 2), date(2026, 1, 3)})
    assert employees.filter(f"_batch_date = '{D2}'").count() == 3
    assert employees.filter(f"_batch_date = '{D1}'").first()["full_name"] == "Renamed"
    # Raw strings are not cast in Bronze.
    assert employees.first()["is_active"] == "true"

    timesheets = _bronze(spark, lakehouse, "timesheets")
    assert timesheets.count() == 5
    # Invalid rows (NULL project_id, hours <= 0) are preserved for Silver to quarantine.
    assert timesheets.filter("project_id IS NULL AND hours = -1.0").count() == 2


def test_hr_missing_mandatory_partition_fails(spark, roots) -> None:
    landing, lakehouse = roots
    with pytest.raises(FileNotFoundError, match="employees"):
        ingest_hr.run(spark, D1, str(landing), str(lakehouse))


def test_finance_raw_dates_credit_notes_and_weekend_fx(spark, roots) -> None:
    landing, lakehouse = roots
    _write(
        _part(landing, "fx_rates", D1, "json"),
        json.dumps(
            [{"rate_date": D1, "currency_from": "EUR", "currency_to": "USD", "rate": 1.08}],
            indent=2,
        ),
    )
    for d in (D1, D2):
        _write(
            _part(landing, "invoices", d, "jsonl"),
            _jsonl(
                [
                    {
                        "inv_id": f"INV-{d}-1",
                        "client_id": "CLI_0002",
                        "project_id": "PRJ_0001",
                        "amount": 100.5,
                        "currency": "EUR",
                        "issued_at": "02.01.2026",
                        "paid_at": None,
                    },
                    {
                        "inv_id": f"INV-{d}-2",
                        "client_id": "CLI_0002",
                        "project_id": "PRJ_ORPHAN_1",
                        "amount": -40.0,
                        "currency": "USD",
                        "issued_at": d,
                        "paid_at": d,
                    },
                ]
            )
            + "{not valid json\n",
        )
        _write(
            _part(landing, "expenses", d, "csv"),
            "exp_id,department,category,amount,currency,expense_date\n"
            f"EXP-1,Data,Training,110.19,EUR,{d}\nEXP-1,Data,Training,110.19,EUR,{d}\n",
        )

    assert ingest_finance.run(spark, D1, str(landing), str(lakehouse)) == {
        "fx_rates": 1,
        "invoices": 3,
        "expenses": 2,
    }
    # Saturday: no fx_rates file -> skipped without failing.
    for _ in range(2):
        counts = ingest_finance.run(spark, D2, str(landing), str(lakehouse))
    assert counts == {"fx_rates": 0, "invoices": 3, "expenses": 2}

    fx = _bronze(spark, lakehouse, "fx_rates")
    _assert_metadata(fx, {date(2026, 1, 2)})

    invoices = _bronze(spark, lakehouse, "invoices")
    _assert_metadata(invoices, {date(2026, 1, 2), date(2026, 1, 3)})
    assert invoices.count() == 6
    # European date string kept verbatim; credit note and NULL paid_at retained.
    assert invoices.filter("issued_at = '02.01.2026' AND paid_at IS NULL").count() == 2
    assert invoices.filter("amount < 0").count() == 2
    # Malformed line captured, not dropped.
    assert invoices.filter("_corrupt_record = '{not valid json'").count() == 2

    # Exact duplicates are preserved in Bronze (deduplication is a Silver concern).
    assert _bronze(spark, lakehouse, "expenses").count() == 4


def test_crm_surveys_preserve_raw_nested_json_and_skip_missing_surveys(spark, roots) -> None:
    landing, lakehouse = roots
    payload = {
        "endpoint": "/v1/metrics",
        "status_code": 200,
        "latency_ms": 123,
        "nested": {"k": [1, 2]},
    }
    _write(
        _part(landing, "crm_events", D1, "jsonl"),
        _jsonl(
            [
                {
                    "event_id": "EVT-1",
                    "client_id": "VIP_CORP_001",
                    "event_type": "api_call",
                    "timestamp": "2026-01-02T00:01:00Z",
                    "payload": payload,
                },
                {
                    "event_id": "EVT-2",
                    "client_id": "CLI_0002",
                    "event_type": "page_view",
                    "timestamp": "2026-01-02T00:02:00Z",
                    "payload": {"path": "/dashboard"},
                },
            ]
        ),
    )
    _write(_part(landing, "crm_events", D2, "jsonl"), _jsonl([]))
    answers = [
        {"question_id": "Q01", "dimension": "enps", "numeric_score": 11, "written_answer": None},
        {
            "question_id": "Q02",
            "dimension": "work_life_balance",
            "numeric_score": 3,
            "written_answer": "ok",
        },
    ]
    _write(
        _part(landing, "employee_surveys", D1, "jsonl"),
        _jsonl(
            [
                {
                    "survey_id": "SRV-1",
                    "emp_id": "EMP_0001",
                    "submitted_at": "2026-01-02T08:00:31Z",
                    "enps_score": 11,
                    "overall_comment": "",
                    "answers": answers,
                },
            ]
        ),
    )

    for _ in range(2):
        assert ingest_crm_surveys.run(spark, D1, str(landing), str(lakehouse)) == {
            "crm_events": 2,
            "employee_surveys": 1,
        }
    # Non-wave date: no survey drop -> skipped without failing.
    assert ingest_crm_surveys.run(spark, D2, str(landing), str(lakehouse)) == {
        "crm_events": 0,
        "employee_surveys": 0,
    }

    crm = _bronze(spark, lakehouse, "crm_events")
    _assert_metadata(crm, {date(2026, 1, 2)})
    raw_payload = crm.filter("event_id = 'EVT-1'").first()["payload"]
    assert json.loads(raw_payload) == payload

    surveys = _bronze(spark, lakehouse, "employee_surveys")
    _assert_metadata(surveys, {date(2026, 1, 2)})
    row = surveys.first()
    assert row["enps_score"] == 11  # out-of-range kept for Silver to quarantine
    assert json.loads(row["answers"]) == answers
