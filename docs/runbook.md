# Operations Runbook

> **Instructions:** Keep this runbook up to date with your verified commands, backfill procedures, and troubleshooting notes.

---

## 1. Prerequisites & Environment Setup

- **Required:** Docker Desktop (with Docker Compose v2)
- **Optional (local linting / IDE support):** [`uv`](https://docs.astral.sh/uv/) (Python 3.11–3.12)

1. Create your local environment file from the template:
   ```bash
   cp .env.example .env
   ```
   *(On Linux/macOS, update `AIRFLOW_UID` in `.env` with `id -u` so container logs/outputs match your host user.)*

2. Build and start the local stack:
   ```bash
   docker compose up -d --build
   ```
   To include the optional local **MinIO** S3-compatible object store:
   ```bash
   docker compose --profile minio up -d --build
   ```

3. Verify all services are healthy:
   ```bash
   docker compose ps
   ```

---

## 2. Downloading & Verifying the Landing Dataset

Configure the Cloudflare R2 credentials provided by your instructor in `.env` (`R2_ACCOUNT_ID`, `R2_BUCKET_NAME`, `R2_PREFIX=course-2026`, `R2_STUDENT_ACCESS_KEY_ID`, `R2_STUDENT_SECRET_ACCESS_KEY`). The downloader reads `MANIFEST.sha256` under `R2_PREFIX`, downloads and verifies the ZIP archive, and extracts the 200-day dataset (`2026-01-01` – `2026-07-19`) into `./data/landing`.

### macOS / Linux / Git Bash
```bash
# Download & verify using MANIFEST.sha256 from Cloudflare R2 (.env)
./scripts/download_data.sh

# Force re-download if ./data/landing already exists
./scripts/download_data.sh --force

# Verify existing ./data/landing without re-downloading
./scripts/download_data.sh --verify-only
```

### Windows (PowerShell)
```powershell
# Download & verify using MANIFEST.sha256 from Cloudflare R2 (.env)
.\scripts\download_data.ps1

# Force re-download if .\data\landing already exists
.\scripts\download_data.ps1 -Force

# Verify existing .\data\landing without re-downloading
.\scripts\download_data.ps1 -VerifyOnly
```

### Inside the Spark Container (no local Python/uv required)
```bash
docker compose run --rm spark-master python3 /opt/workspace/scripts/download_data.py
```

---

## 3. Running Individual PySpark Jobs

Execute PySpark jobs inside the `spark-master` container against the local Spark cluster (or `local[*]`):

```bash
# Reference Bronze job (clients)
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/workspace/spark_jobs/bronze/ingest_clients.py \
  --batch-date 2026-01-01

# Reference Silver job (clients + quarantine + sample DQ audit)
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/workspace/spark_jobs/silver/transform_clients.py \
  --batch-date 2026-01-01
```

### Running a Date Range Without Airflow (dev / backfill helper)

`scripts/run_pipeline.py` runs Bronze and/or Silver for every date in a range, sequentially, in one SparkSession. Silver jobs run in dependency order: clients → projects → fx_rates → employees_scd2 → timesheets → invoices/expenses → crm_events → surveys.

```bash
# 14-day slice, Bronze + Silver (about 12 min on a laptop with local[4])
docker compose exec spark-master python3 scripts/run_pipeline.py \
  --start 2026-01-01 --end 2026-01-14 --master 'local[4]'

# Re-run only some Silver jobs for one date
docker compose exec spark-master python3 scripts/run_pipeline.py \
  --start 2026-01-05 --end 2026-01-05 --layers silver \
  --only silver.timesheets silver.invoices_expenses
```

Individual Silver jobs also accept `--batch-date` via `spark-submit`. `transform_crm_events.py` also takes `--strategy {broadcast,salted,sort_merge}` for the skew comparison.

### Running Unit Tests
```bash
docker compose run --rm spark-master pytest -v
```

---

## 4. Daily Runs & Historical Backfills (Airflow)

1. Open the Airflow UI at `http://localhost:8080` (default credentials: `admin` / `admin`).
2. Unpause the `medallion_daily_pipeline` DAG.
3. Trigger a single logical date or run a backfill via CLI:

```bash
# Trigger a single batch date
docker compose exec airflow-scheduler airflow dags trigger medallion_daily_pipeline --logical-date 2026-01-01

# TODO: Document your verified 14-day slice (`2026-01-01` to `2026-01-14`) and full 200-day backfill (`2026-01-01` to `2026-07-19`) commands below, and ensure incremental runs remain ready for the 70-day holdout (`2026-07-20` to `2026-09-27`):
# docker compose exec airflow-scheduler airflow dags backfill ...
```

<!-- TODO: Document how long the 14-day slice and 200-day backfill took, and any concurrency/pool settings you tuned. -->

---

## 5. Idempotency Verification Procedure

<!-- TODO: Run the same `batch_date` twice (e.g., `2026-01-05`) across Bronze, Silver, Quarantine, Gold, and `dq_audit_results`, and document the verification commands and observed row counts proving zero duplicates. -->

```bash
# Example verification command template:
# 1. Run pipeline for 2026-01-05
# 2. Record row counts across bronze/*, silver/*, silver_quarantine/*, gold/*, dq/dq_audit_results
# 3. Re-run pipeline for 2026-01-05
# 4. Assert exact count equality
```

---

## 6. Service Web UIs

| Service | URL | Notes |
| :--- | :--- | :--- |
| **Apache Airflow** | `http://localhost:8080` | Default login: `admin` / `admin` |
| **Spark Master UI** | `http://localhost:8081` | Cluster workers & active applications |
| **Spark Worker UI** | `http://localhost:8082` | Worker executors & logs |
| **Spark History Server** | `http://localhost:18080` | Completed job stages & skew inspection |
| **Streamlit Reporting App** | `http://localhost:8501` | Reads Gold & DQ tables via DuckDB |
| **MinIO Console** *(optional profile)* | `http://localhost:9001` | Default login: `minioadmin` / `minioadmin` |

---

## 7. Troubleshooting & Resetting State

- **Clean Lakehouse Outputs (keep downloaded `./data/landing`):**
  ```bash
  rm -rf ./lakehouse/*
  ```
- **Full Stack Reset (wipes Airflow Postgres metadata database):**
  ```bash
  docker compose down -v
  docker compose up -d --build
  ```
- <!-- TODO: Add any project-specific issues and fixes you encountered (e.g., weekend `fx_rates` sensor handling, Spark memory tuning, DuckDB Delta lock/refresh notes). -->
