# MIMIC-IV PostgreSQL Setup Tutorial

This tutorial walks through loading the full MIMIC-IV dataset into a local PostgreSQL 17
database running in Docker, using the official MIT-LCP build scripts and an automated
Python pipeline.

### About the mimic-code repository

The [MIT-LCP mimic-code](https://github.com/MIT-LCP/mimic-code) repository provides three
components to help researchers navigate MIMIC-IV:

1. **Build scripts** (`buildmimic/postgres/`) — SQL to create schemas, load raw CSV data,
   add constraints, and build indexes. This is what steps 1–15 of the pipeline execute.
2. **Concepts** (`concepts_postgres/`) — SQL to extract clinically meaningful derived tables
   from the raw data. Examples include severity scores (SOFA, SAPS II, OASIS), lab
   measurements, medication exposures, and sepsis criteria. Full description:
   [Pollard et al., JAMIA 2018](https://academic.oup.com/jamia/article/25/1/32/4259424).
3. **Tutorials** — Jupyter notebooks demonstrating common analyses.

This tutorial installs component 1 (steps 1–15) and component 2 (steps 16–17).

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Setup](#2-repository-setup)
3. [Download MIMIC-IV Data](#3-download-mimic-iv-data)
   - 3a. [MIMIC-IV (core)](#3a-mimic-iv-core)
   - 3b. [MIMIC-IV-ED (emergency department)](#3b-mimic-iv-ed-emergency-department)
4. [Configure Environment](#4-configure-environment)
5. [Set Up Python Environment (uv)](#5-set-up-python-environment-uv)
6. [Run the Build Pipeline](#6-run-the-build-pipeline)
7. [Verify the Installation](#7-verify-the-installation)
8. [Run Integration Tests](#8-run-integration-tests)
9. [Explore Derived Concepts](#9-explore-derived-concepts)
10. [Connecting to the Database](#10-connecting-to-the-database)
11. [Re-running and Idempotency](#11-re-running-and-idempotency)
12. [Troubleshooting](#12-troubleshooting)


---

## 1. Prerequisites

### Docker Desktop

Docker is required to run the PostgreSQL container and the psql build scripts.

- **macOS**: Install [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
- **Linux**: Install [Docker Engine](https://docs.docker.com/engine/install/)
- **Windows**: Install [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

Verify Docker is running:
```bash
docker info
```

### uv — Python Package Manager

`uv` is a fast Python package manager used instead of pip/conda.

Install uv:
```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with Homebrew
brew install uv
```

Verify:
```bash
uv --version
```

### PhysioNet Account

MIMIC-IV requires credentialed access. You must:
1. Create an account at [physionet.org](https://physionet.org)
2. Complete the required CITI training course
3. Request access to [MIMIC-IV](https://physionet.org/content/mimiciv/)

---

## 2. Repository Setup

### Clone this repository

```bash
git clone https://github.com/marcofanti/LearningProject2.git
cd LearningProject2
```

### Clone mimic-code (if not already present)

The build scripts from MIT-LCP are required. Clone into the project root:

```bash
git clone https://github.com/MIT-LCP/mimic-code.git
```

After cloning, your directory should contain:

```
LearningProject2/
├── mimic-code/            ← official MIT-LCP build scripts
│   └── mimic-iv/
│       └── buildmimic/
│           └── postgres/  ← create.sql, load_gz.sql, constraint.sql, index.sql, validate.sql
├── docker/                ← standalone Dockerfiles
├── build_mimic.py         ← automated pipeline script
├── .env.local             ← environment variable reference
└── ...
```

> `mimic-code/` is listed in `.gitignore` and will not be committed.

---

## 3. Download MIMIC-IV Data

Both MIMIC-IV and MIMIC-IV-ED require credentialed PhysioNet access. Download them into
the **same base data directory** — the pipeline expects `hosp/`, `icu/`, and `ed/` as
subdirectories under `MIMIC_DATA_DIR`.

### 3a. MIMIC-IV (core)

Download the MIMIC-IV 3.1 compressed CSV files from PhysioNet.

```bash
# Replace <your-username> with your PhysioNet username
wget -r -N -c -np \
  --user <your-username> --ask-password \
  https://physionet.org/files/mimiciv/3.1/

# Move hosp/ and icu/ into your data directory
mv physionet.org/files/mimiciv/3.1/hosp /path/to/your/mimic-data/hosp
mv physionet.org/files/mimiciv/3.1/icu  /path/to/your/mimic-data/icu
```

### 3b. MIMIC-IV-ED (emergency department)

Download the MIMIC-IV-ED 2.2 files and place them in an `ed/` subdirectory:

```bash
wget -r -N -c -np \
  --user <your-username> --ask-password \
  https://physionet.org/files/mimic-iv-ed/2.2/

mv physionet.org/files/mimic-iv-ed/2.2/ed /path/to/your/mimic-data/ed
```

> MIMIC-IV-ED requires separate access approval on PhysioNet even if you already have
> MIMIC-IV access. Request it at [physionet.org/content/mimic-iv-ed/](https://physionet.org/content/mimic-iv-ed/).

### Expected directory structure

After both downloads, your data directory must contain:

```
/path/to/your/mimic-data/
├── hosp/
│   ├── admissions.csv.gz
│   ├── patients.csv.gz
│   ├── diagnoses_icd.csv.gz
│   ├── labevents.csv.gz
│   └── ... (all hosp module files)
├── icu/
│   ├── chartevents.csv.gz
│   ├── icustays.csv.gz
│   └── ... (all icu module files)
└── ed/
    ├── edstays.csv.gz
    ├── diagnosis.csv.gz
    ├── triage.csv.gz
    ├── vitalsign.csv.gz
    ├── medrecon.csv.gz
    └── pyxis.csv.gz
```

> Large files to expect: `chartevents.csv.gz` (~14 GB), `labevents.csv.gz` (~3 GB),
> `emar_detail.csv.gz` (~4 GB). The full MIMIC-IV core dataset is approximately 50 GB
> compressed. MIMIC-IV-ED adds ~300 MB.

---

## 4. Configure Environment

Copy the example environment file and fill in your values:

```bash
cp .env.local .env
```

Open `.env` in your editor and update:

```dotenv
# Required: strong password for the database
POSTGRES_PASSWORD=your_secure_password_here

# Required: absolute path to your downloaded MIMIC-IV data
# This directory must contain hosp/, icu/, and ed/ subdirectories
MIMIC_DATA_DIR=/absolute/path/to/your/mimic-data
```

All other values have sensible defaults and can be left unchanged for a standard setup.

> `.env` is gitignored — it will never be committed. Only `.env.local` (with example
> values) is tracked in git.

---

## 5. Set Up Python Environment (uv)

From the project root:

```bash
# Create virtual environment with Python 3.11 and install dependencies
uv sync

# Verify the environment
uv run python -c "import psycopg, dotenv; print('Dependencies OK')"
```

The dependencies installed are:
- `psycopg[binary]` — PostgreSQL adapter for idempotency checks
- `python-dotenv` — reads `.env` configuration

---

## 6. Run the Build Pipeline

Two equivalent options are available — choose whichever fits your workflow:

### Option A — Python script (recommended for automation)

```bash
uv run python build_mimic.py
```

Runs all 17 steps end-to-end, logging progress to both the console and `build_mimic.log`.
Best for unattended runs where you want a persistent log file.

### Option B — Jupyter notebook (recommended for exploration)

```bash
uv run jupyter lab build_mimic.ipynb
```

The notebook `build_mimic.ipynb` contains the same 17 steps, one cell per step, so you
can run individual steps, inspect intermediate results, or re-run a single step without
repeating the entire pipeline.

> Both the script and the notebook are **idempotent** — each step checks whether it has
> already been completed and skips it if so, making them safe to re-run at any time.

The pipeline executes 17 steps in sequence, logging progress to both the console and
`build_mimic.log`.

### What each step does

**MIMIC-IV core (steps 1–10)**

| Step | Script used | Duration (approx.) |
|------|-------------|---------------------|
| 1. Check Docker | — | Instant |
| 2. Create Docker network `mimic-net` | — | Instant |
| 3. Build `mimic-db` image | `docker/Dockerfile.db` | ~30 s |
| 4. Start PostgreSQL container | — | ~5 s |
| 5. Wait for PostgreSQL ready | — | ~10–30 s |
| 6. Create schemas + tables | `mimic-iv create.sql` | ~5 s |
| 7. Load all MIMIC-IV data | `mimic-iv load_gz.sql` | **4–12 hours** |
| 8. Add primary key constraints | `mimic-iv constraint.sql` | ~5–15 min |
| 9. Create indexes | `mimic-iv index.sql` | ~30–90 min |
| 10. Validate row counts | `mimic-iv validate.sql` | ~5 min |

**MIMIC-IV-ED (steps 11–15)** — data loaded from `MIMIC_DATA_DIR/ed/`

| Step | Script used | Duration (approx.) |
|------|-------------|---------------------|
| 11. Create `mimiciv_ed` schema + tables | `mimic-iv-ed create.sql` | ~5 s |
| 12. Load all ED data | `mimic-iv-ed load_gz.sql` | ~5–15 min |
| 13. Add ED constraints | `mimic-iv-ed constraint.sql` | ~1 min |
| 14. Create ED indexes | `mimic-iv-ed index.sql` | ~1 min |
| 15. Validate ED row counts | `mimic-iv-ed validate.sql` | ~1 min |

**MIMIC-IV Concepts (steps 16–17)** — derived tables in `mimiciv_derived` schema

| Step | Script used | Duration (approx.) |
|------|-------------|---------------------|
| 16. Create `mimiciv_derived` schema + helper functions | `concepts_postgres/postgres-functions.sql` | ~5 s |
| 17. Build all derived concepts (~65 tables) | `concepts_postgres/postgres-make-concepts.sql` | ~15–60 min |

> **Step 7 takes the most time.** The `chartevents` table alone has ~433 million rows.
> Do not interrupt the process; ensure your computer stays awake.
> Progress is continuously logged to `build_mimic.log`.

### Monitoring progress

In a separate terminal:
```bash
# Follow the log file in real time
tail -f build_mimic.log

# Check container status
docker ps

# View container logs directly
docker logs -f mimic_postgres
```

---

## 7. Verify the Installation

After the pipeline completes, run either validation script independently.

**MIMIC-IV core:**
```bash
docker run --rm \
  --network mimic-net \
  -v "$(pwd)/mimic-code/mimic-iv/buildmimic/postgres/validate.sql:/script.sql:ro" \
  postgres:17 \
  psql "host=mimic_postgres dbname=mimiciv user=mimicuser password=your_password" \
  -f /script.sql
```

**MIMIC-IV-ED:**
```bash
docker run --rm \
  --network mimic-net \
  -v "$(pwd)/mimic-code/mimic-iv-ed/buildmimic/postgres/validate.sql:/script.sql:ro" \
  postgres:17 \
  psql "host=mimic_postgres dbname=mimiciv user=mimicuser password=your_password" \
  -f /script.sql
```

Expected output shows each table with its actual row count compared to the expected count
for that dataset version. Any `FAILED` rows indicate a partial load.

**MIMIC-IV Concepts:**
```bash
psql -h localhost -p 5432 -U mimicuser -d mimiciv -c \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'mimiciv_derived';"
```

Expected output is approximately 65 tables. You can also spot-check a specific concept:
```bash
psql -h localhost -p 5432 -U mimicuser -d mimiciv -c \
  "SELECT COUNT(*) FROM mimiciv_derived.sepsis3;"
```

---

## 8. Run Integration Tests

Integration tests are ported from the official
[mimic-code test suite](https://github.com/MIT-LCP/mimic-code/tree/main/mimic-iv/tests)
(originally targeting Google BigQuery). Tests connect directly to `localhost:5432` using
the same credentials as the build pipeline — no cloud account required.

Two equivalent options are available:

### Option A — Python script (recommended for CI / scripted runs)

```bash
uv run python test_mimic.py
```

Prints results to the console and exits with code `0` on success or `1` on failures/errors.

### Option B — Jupyter notebook (recommended for exploration)

```bash
uv run jupyter lab test_mimic.ipynb
```

The notebook `test_mimic.ipynb` runs the same nine tests, one cell per test, and displays
results inline with colour-coded **PASS** / **FAIL** / **SKIP** / **ERROR** badges.
Each test cell also shows the underlying data (as a DataFrame) so you can immediately
inspect what was queried. A final summary cell counts results across all tests.

### What is tested

| Test | Schema | What it checks |
|------|--------|----------------|
| `d_labitems: blood gas item IDs + labels` | `mimiciv_hosp` | 26 blood gas lab item IDs have the expected labels in `d_labitems` |
| `inputevents: vasopressor units` | `mimiciv_icu` | Every vasopressor row has a non-null `rateuom`; non-standard units are bounded |
| `all concept tables have ≥1 row` | `mimiciv_derived` | Every `.sql` in `concepts_postgres/` has a matching populated table |
| `bg: key columns non-null in >50% of rows` | `mimiciv_derived` | `specimen`, `po2`, `pco2`, `ph`, `baseexcess` are non-null for the majority of blood gas records |
| `gcs: verbal carry-forward / ETT imputation` | `mimiciv_derived` | GCS verbal score is correctly carried forward (or zeroed) for intubated patients |
| `first_day_gcs: ≥98% coverage + spot-check` | `mimiciv_derived` | ≥98% of ICU stays have a first-day GCS; three known stay_ids match expected values |
| `vasopressor doses within clinical limits` | `mimiciv_derived` | No `vaso_rate` exceeds 2× the maximum refractory-shock dose (UpToDate thresholds) |
| `sofa: unique (stay_id, hr)` | `mimiciv_derived` | No duplicate hour-level rows in the SOFA score table |
| `sepsis3: unique stay_id` | `mimiciv_derived` | No duplicate stay-level rows in the Sepsis-3 table |

### Interpreting results

```
══════════════════════════════════════════════════════════════
MIMIC-IV PostgreSQL Integration Tests
  db=mimiciv  host=localhost:5432
══════════════════════════════════════════════════════════════

── Core data (mimiciv_hosp / mimiciv_icu) ──────────────────
  PASS   d_labitems: blood gas item IDs + labels
  PASS   inputevents: vasopressor units

── Derived concepts (mimiciv_derived, requires step 17) ────
  PASS   all concept tables have ≥1 row
  PASS   bg: key columns non-null in >50 %% of rows
  SKIP   gcs: verbal carry-forward / ETT imputation  (stay_ids not found — row-limited load)
  PASS   first_day_gcs: ≥98 %% coverage + spot-check
  PASS   vasopressor doses within clinical limits
  PASS   sofa: unique (stay_id, hr)
  PASS   sepsis3: unique stay_id

══════════════════════════════════════════════════════════════
  8/9 passed  |  0 failed  |  1 skipped  |  0 errors
══════════════════════════════════════════════════════════════
```

- **PASS** — assertion succeeded.
- **SKIP** — table not yet built (step 17 incomplete) or specific stay_ids absent from
  a row-limited load. Not a failure.
- **FAIL** — data does not meet the expected condition. Investigate the output detail.
- **ERROR** — unexpected exception (e.g. database unreachable). Check that
  `mimic_postgres` is running.

The script exits with code `0` if all tests pass or skip, and `1` if any test fails or
errors.

### When to run

| Situation | Recommended action |
|-----------|-------------------|
| After step 15 (core + ED loaded) | Run tests — the two core-data tests should pass |
| After step 17 (concepts built) | Run tests — all derived-concept tests should pass |
| After any re-load or schema change | Run tests to verify data integrity |

---

## 9. Explore Derived Concepts

`concepts.py` and `concepts.ipynb` showcase the `mimiciv_derived` schema built in
step 17. For each concept category they explain the clinical purpose, run a
representative query, and display the result. They serve as a starting point for
research queries and analysis notebooks.

### Option A — Python script

```bash
uv run python concepts.py
```

Prints all sections to the console. Useful for a quick orientation or as a reference
you can redirect to a file.

### Option B — Jupyter notebook (recommended)

```bash
uv run jupyter lab concepts.ipynb
```

The notebook has one cell per query, with markdown explanations between them. Run
cells individually to explore specific concepts without re-running the whole script.

### What is covered

| Section | Tables queried | Clinical topic |
|---------|---------------|----------------|
| 0. Overview | all `mimiciv_derived` tables | Row counts and disk sizes |
| 1. Demographics | `icustay_detail`, `age` | Cohort characteristics, ICU admission metadata |
| 2. Severity scores | `sofa`, `sapsii`, `oasis`, `first_day_sofa` | Illness severity at admission; mortality prediction |
| 3. Sepsis-3 | `sepsis3`, `suspicion_of_infection` | Reproducible sepsis cohort construction |
| 4. Blood gas | `bg` | Respiratory function, acid-base, P:F ratio (ARDS) |
| 5. Vital signs | `vitalsign` | Haemodynamics, shock index |
| 6. Glasgow Coma Scale | `gcs`, `first_day_gcs` | Neurological status, ETT imputation |
| 7. AKI (KDIGO) | `kdigo_stages`, `kdigo_creatinine`, `kdigo_uo` | Acute kidney injury staging |
| 8. Vasopressors | `vasoactive_agent`, `norepinephrine_equivalent_dose` | Vasopressor burden, refractory shock |
| 9. Antibiotics | `antibiotic` | Antibiotic stewardship, infection episodes |
| 10. Comorbidities | `charlson` | Baseline health; covariate for outcome studies |
| 11. First-day summaries | `first_day_vitalsign`, `first_day_lab`, `first_day_sofa` | Admission feature matrix for ML/regression |
| 12. Cohort example | multi-table join | Septic shock feature table (end-to-end example) |

### Dependencies

`concepts.py` and `concepts.ipynb` require `pandas`, `tabulate`, and `jupyterlab`,
which are included in the project dependencies. Install or update with:

```bash
uv sync
```

---

## 10. Connecting to the Database

### psql (command line)

```bash
psql -h localhost -p 5432 -U mimicuser -d mimiciv
```

### Adminer (web UI)

Start the Adminer container:

```bash
docker build -f docker/Dockerfile.adminer -t mimic-adminer .

docker run -d \
  --name mimic_adminer \
  --network mimic-net \
  -p 28080:8080 \
  mimic-adminer
```

Open `http://localhost:28080` in your browser and log in with:
- **Server**: `mimic_postgres`
- **Username**: `mimicuser`
- **Password**: *(your POSTGRES_PASSWORD)*
- **Database**: `mimiciv`

### Python (psycopg)

```python
import psycopg

conn = psycopg.connect(
    host="localhost",
    port=5432,
    user="mimicuser",
    password="your_password",
    dbname="mimiciv",
)
cur = conn.cursor()

# MIMIC-IV core
cur.execute("SELECT COUNT(*) FROM mimiciv_hosp.patients")
print(cur.fetchone()[0])   # → 364627

# MIMIC-IV-ED
cur.execute("SELECT COUNT(*) FROM mimiciv_ed.edstays")
print(cur.fetchone()[0])   # → 425087

# MIMIC-IV Concepts (derived tables)
cur.execute("SELECT COUNT(*) FROM mimiciv_derived.sepsis3")
print(cur.fetchone()[0])   # → number of ICU stays meeting Sepsis-3 criteria

conn.close()
```

---

## 11. Re-running and Idempotency

The pipeline is safe to re-run at any time. Each step checks whether it was already
completed and skips with a log message if so:

```
2025-03-11 14:00:01 [INFO    ] SKIP: All 3 MIMIC-IV schemas already exist with 21 tables in mimiciv_hosp.
2025-03-11 14:00:01 [INFO    ] SKIP: mimiciv_hosp.patients already contains 364627 rows — data already loaded.
2025-03-11 14:00:02 [INFO    ] SKIP: Constraints already exist (admissions_pk + admissions_patients_fk) — skipping constraint.sql.
2025-03-11 14:00:02 [INFO    ] SKIP: Index 'admissions_idx01' already exists — skipping index.sql.
2025-03-11 14:00:03 [INFO    ] SKIP: mimiciv_ed schema already exists with 6 tables.
2025-03-11 14:00:03 [INFO    ] SKIP: mimiciv_ed.edstays already contains 425087 rows — ED data already loaded.
2025-03-11 14:00:03 [INFO    ] SKIP: ED constraints already exist (edstays_pk + diagnosis_edstays_fk) — skipping ED constraint.sql.
2025-03-11 14:00:03 [INFO    ] SKIP: Index 'edstays_idx01' already exists — skipping ED index.sql.
2025-03-11 14:00:04 [INFO    ] mimiciv_derived schema is ready.
2025-03-11 14:00:04 [INFO    ] SKIP: Helper function 'regexp_extract' already exists in mimiciv_derived — skipping postgres-functions.sql.
2025-03-11 14:00:04 [INFO    ] Building all concepts — this may take 15–60 minutes ...
```

> **Note:** Step 17 (concepts) always re-runs because each concept script uses
> `DROP TABLE IF EXISTS ... CREATE TABLE AS ...`, rebuilding from the raw data.
> This is safe and ensures concepts stay in sync with the underlying raw tables.

### Starting fresh

To wipe the database and reload from scratch:

```bash
# Stop and remove the container
docker stop mimic_postgres && docker rm mimic_postgres

# Remove the persisted data directory
rm -rf data/db

# Re-run the pipeline
uv run python build_mimic.py
```

---

## 12. Troubleshooting

### Docker permission denied

```
permission denied while trying to connect to the Docker daemon
```

Add your user to the `docker` group (Linux):
```bash
sudo usermod -aG docker $USER
# Then log out and back in
```

### PostgreSQL not becoming ready

```
PostgreSQL did not become ready after 30 attempts.
```

Check container logs for errors:
```bash
docker logs mimic_postgres
```

Common causes:
- Port 5432 already in use by another PostgreSQL instance — change `POSTGRES_PORT` in `.env`
- Insufficient disk space — the full dataset requires ~200 GB

### Data load stuck on chartevents

This is expected. `chartevents` has ~433 million rows and is the slowest table to load.
Check that the container is still running (`docker ps`) and the log is still progressing
(`tail -f build_mimic.log`). Give it several hours.

### Schema already exists error

```
ERROR: schema "mimiciv_hosp" already exists
```

The `create.sql` script uses `DROP SCHEMA ... CASCADE`, which normally handles this.
If you see this error, it may be a permissions issue. Verify the container is on
`mimic-net` and the credentials match `.env`.

### gzip: command not found

If the load step fails with `gzip not found`, it means the `postgres:17` image being
used for psql does not have gzip. Pull a fresh copy:
```bash
docker pull postgres:17
```

### Checking table row counts manually

```sql
SELECT
    table_schema,
    table_name,
    (xpath('/row/cnt/text()',
           query_to_xml(format('SELECT COUNT(*) AS cnt FROM %I.%I', table_schema, table_name),
                        false, true, '')))[1]::text::int AS row_count
FROM information_schema.tables
WHERE table_schema IN ('mimiciv_hosp', 'mimiciv_icu', 'mimiciv_ed', 'mimiciv_derived')
ORDER BY table_schema, table_name;
```

### ED data directory not found

```
Expected ED subdirectory '/path/to/mimic-data/ed' not found.
```

The pipeline expects MIMIC-IV-ED files at `MIMIC_DATA_DIR/ed/`. Download the ED dataset
and place the `.csv.gz` files there (see [section 3b](#3b-mimic-iv-ed-emergency-department)).

### Concepts script not found

```
Script not found: .../mimic-code/mimic-iv/concepts_postgres/postgres-make-concepts.sql
```

The `mimic-code` repository must be cloned at `MIMIC_CODE_DIR` (default: `./mimic-code`):
```bash
git clone https://github.com/MIT-LCP/mimic-code.git
```

### Concepts build fails with relation does not exist

Some concept scripts depend on raw tables being fully loaded. Ensure steps 1–15 (core +
ED data load, constraints, indexes) completed successfully before running step 17. Check
`build_mimic.log` for the failing concept name, then run it manually:

```bash
psql -h localhost -p 5432 -U mimicuser -d mimiciv \
  -c "SET search_path TO mimiciv_derived, mimiciv_hosp, mimiciv_icu, mimiciv_ed;" \
  -f mimic-code/mimic-iv/concepts_postgres/measurement/vitalsign.sql
```
