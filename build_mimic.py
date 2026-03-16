#!/usr/bin/env python3
"""
build_mimic.py
--------------
Orchestrates the full MIMIC-IV + MIMIC-IV-ED PostgreSQL setup pipeline using
plain Docker containers. Each step checks whether it has already been completed
and skips it on subsequent runs, logging the reason.

All configuration is read from a .env file (see .env.local for reference).

Usage:
    uv run python build_mimic.py

Steps (MIMIC-IV core):
    1.  Verify Docker is running
    2.  Ensure Docker network exists
    3.  Build PostgreSQL image (docker/Dockerfile.db)
    4.  Start PostgreSQL container
    5.  Wait for PostgreSQL to be ready
    6.  Create schemas and tables   (mimic-iv create.sql)
    7.  Load MIMIC-IV data          (mimic-iv load_gz.sql)
    8.  Add primary key constraints (mimic-iv constraint.sql)
    9.  Create indexes              (mimic-iv index.sql)
    10. Validate row counts         (mimic-iv validate.sql)

Steps (MIMIC-IV-ED):
    11. Create ED schema and tables   (mimic-iv-ed create.sql)
    12. Load MIMIC-IV-ED data         (mimic-iv-ed load_gz.sql, from MIMIC_DATA_DIR/ed/)
    13. Add ED primary/foreign keys   (mimic-iv-ed constraint.sql)
    14. Create ED indexes             (mimic-iv-ed index.sql)
    15. Validate ED row counts        (mimic-iv-ed validate.sql)

Steps (MIMIC-IV Concepts):
    16.  Create mimiciv_derived schema + helper functions (concepts_postgres/postgres-functions.sql)
    17. Build all derived concepts     (concepts_postgres/postgres-make-concepts.sql)
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging — console + rotating file
# ---------------------------------------------------------------------------

LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(message)s"
LOG_DATE   = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATE,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("build_mimic.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (from .env)
# ---------------------------------------------------------------------------

load_dotenv()

POSTGRES_DB        = os.environ.get("POSTGRES_DB",        "mimiciv")
POSTGRES_USER      = os.environ.get("POSTGRES_USER",      "mimicuser")
POSTGRES_PASSWORD  = os.environ.get("POSTGRES_PASSWORD",  "mimicpass")
POSTGRES_PORT      = int(os.environ.get("POSTGRES_PORT",  "5432"))
DB_CONTAINER_NAME  = os.environ.get("DB_CONTAINER_NAME",  "mimic_postgres")
DB_IMAGE_NAME      = os.environ.get("DB_IMAGE_NAME",      "mimic-db")
DOCKER_NETWORK     = os.environ.get("DOCKER_NETWORK",     "mimic-net")
MIMIC_DATA_DIR     = os.environ.get("MIMIC_DATA_DIR",     "")
MIMIC_CODE_DIR     = os.environ.get("MIMIC_CODE_DIR",     "./mimic-code")
LOAD_LIMITS_FILE   = os.environ.get("LOAD_LIMITS_FILE",   "")

# Path to the official mimic-code PostgreSQL build scripts
MIMIC_BUILD_DIR      = Path(MIMIC_CODE_DIR) / "mimic-iv"    / "buildmimic" / "postgres"
MIMIC_ED_BUILD_DIR   = Path(MIMIC_CODE_DIR) / "mimic-iv-ed" / "buildmimic" / "postgres"
MIMIC_CONCEPTS_DIR   = Path(MIMIC_CODE_DIR) / "mimic-iv"    / "concepts_postgres"

# ---------------------------------------------------------------------------
# Path display helpers — never log full absolute paths
# ---------------------------------------------------------------------------

def _rel_path(p) -> str:
    """Return a path relative to cwd, falling back to the filename only."""
    try:
        return str(Path(p).relative_to(Path.cwd()))
    except ValueError:
        return Path(p).name


def _data_dir_str(p) -> str:
    """For large data directories show '...' + last 10 chars only."""
    s = str(p)
    return f"...{s[-10:]}" if len(s) > 10 else s


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a subprocess command with logging. Returns CompletedProcess."""
    log.debug("Executing: %s", " ".join(str(c) for c in cmd))
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
    )


def db_connect() -> psycopg.Connection:
    """Open a psycopg connection from the host to the running PostgreSQL container."""
    return psycopg.connect(
        host="localhost",
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def run_psql_in_docker(
    script_path: Path,
    extra_psql_vars: dict[str, str] | None = None,
    extra_volumes: list[str] | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    """
    Run a psql script inside a temporary postgres:17 container on DOCKER_NETWORK,
    connecting to DB_CONTAINER_NAME by container name.

    Args:
        script_path:      Host path to the .sql file to execute.
        extra_psql_vars:  Dict of psql -v KEY=VALUE variable overrides.
        extra_volumes:    Additional -v mount strings, e.g. ["/host/path:/container/path:ro"].
    """
    abs_script = Path(script_path).resolve()
    conn_str = (
        f"host={DB_CONTAINER_NAME} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    cmd = [
        "docker", "run", "--rm",
        "--network", DOCKER_NETWORK,
        "-v", f"{abs_script}:/script.sql:ro",
    ]
    if extra_volumes:
        for vol in extra_volumes:
            cmd += ["-v", vol]

    cmd += [
        "postgres:17",
        "psql", conn_str,
        "-v", "ON_ERROR_STOP=1",
    ]
    if extra_psql_vars:
        for key, val in extra_psql_vars.items():
            cmd += ["-v", f"{key}={val}"]

    cmd += ["-f", "/script.sql"]
    return run(cmd, capture=capture)


# ---------------------------------------------------------------------------
# Row-limit helpers
# ---------------------------------------------------------------------------

def load_limits() -> dict | None:
    """
    Load per-table row limits from LOAD_LIMITS_FILE.
    Returns the parsed dict, or None if LOAD_LIMITS_FILE is unset/missing.
    Expected JSON shape: { "default": 10000, "overrides": { "patients": 400000 } }
    """
    if not LOAD_LIMITS_FILE:
        return None
    path = Path(LOAD_LIMITS_FILE)
    if not path.exists():
        log.warning("LOAD_LIMITS_FILE '%s' not found — loading without row limits.", _rel_path(path))
        return None
    with path.open() as f:
        return json.load(f)


def _row_limit(table: str, limits: dict) -> int | None:
    """Return the row limit for a table, or None for unlimited."""
    override = limits.get("overrides", {}).get(table)
    if override is not None:
        return override
    return limits.get("default")


_COPY_RE = re.compile(
    r"(\\COPY\s+\w+\.(\w+)\s+FROM\s+PROGRAM\s+')(gzip -dc [^']+)('.*)",
    re.IGNORECASE,
)

_TIMING_RE = re.compile(r"\[TABLE_(START|END)\] (\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")

_TS_FMT = "YYYY-MM-DD HH24:MI:SS"


def _find_core_table_paths(original_sql: Path, data_path: Path) -> dict[str, str]:
    """
    MIMIC-IV v1 stored admissions/patients/transfers under core/; v2 moved them to hosp/.
    For any \\COPY whose CSV is missing from hosp/ but present in core/, return a mapping
    of table_name -> absolute container path (under /data/mimic/core/) so the SQL can be
    patched to use the correct location.
    """
    hosp_dir = data_path / "hosp"
    core_dir = data_path / "core"
    core_tables: dict[str, str] = {}

    if not core_dir.is_dir():
        return core_tables

    for line in original_sql.read_text().splitlines():
        m = _COPY_RE.match(line)
        if not m:
            continue
        _, table, program, _ = m.groups()
        filename = program.split()[-1]
        if not (hosp_dir / filename).exists() and (core_dir / filename).exists():
            core_tables[table] = f"/data/mimic/core/{filename}"
            log.info("  %-30s → sourcing from core/ (not in hosp/)", table)

    return core_tables


def generate_timed_load_sql(
    original_sql: Path,
    out_dir: Path,
    limits: dict | None = None,
    core_tables: dict[str, str] | None = None,
) -> Path:
    """
    Rewrite load_gz.sql to wrap each \\COPY with SELECT clock_timestamp() statements
    that emit [TABLE_START] / [TABLE_END] markers for per-table load timing.
    When limits is provided, also pipes each gzip stream through head -n <limit+1>.
    When core_tables is provided, overrides the file path for tables found in core/.
    """
    lines = []
    for line in original_sql.read_text().splitlines(keepends=True):
        m = _COPY_RE.match(line.rstrip("\n"))
        if m:
            prefix, table, program, suffix = m.groups()
            if core_tables and table in core_tables:
                program = f"gzip -dc {core_tables[table]}"
            if limits is not None:
                limit = _row_limit(table, limits)
                if limit is not None:
                    program = f"{program} | head -n {limit + 1}"
                    log.info("  %-30s → limit %d rows", table, limit)
            lines.append(
                f"SELECT '[TABLE_START] {table} ' || to_char(clock_timestamp(), '{_TS_FMT}') AS ts;\n"
            )
            lines.append(f"{prefix}{program}{suffix}\n")
            lines.append(
                f"SELECT '[TABLE_END] {table} ' || to_char(clock_timestamp(), '{_TS_FMT}') AS ts;\n"
            )
        else:
            lines.append(line)

    out_path = out_dir / "load_timed.sql"
    out_path.write_text("".join(lines))
    return out_path


def _truncate_all_tables(
    schemas: tuple[str, ...] = ("mimiciv_hosp", "mimiciv_icu"),
) -> None:
    """
    Truncate all data tables in the given schemas before a reload.
    Called when the sentinel table is empty but other tables may have partial
    data from a previous run, which would cause duplicate-key errors on reload.
    Defaults to mimiciv_hosp + mimiciv_icu; pass schemas=('mimiciv_ed',) for ED.
    """
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """,
            (list(schemas),),
        )
        tables = cur.fetchall()
        if not tables:
            return
        for schema, table in tables:
            cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        conn.commit()
        log.info("Truncated %d tables for clean reload.", len(tables))


def _drop_fk_constraints(
    schemas: tuple[str, ...] = ("mimiciv_hosp", "mimiciv_icu"),
) -> None:
    """
    Drop all FK constraints in the given schemas before a data load.
    Safe to call when reloading: the corresponding constraint.sql step will re-add them.
    This prevents FK violations when loading tables in the order load_gz.sql specifies.
    Defaults to mimiciv_hosp + mimiciv_icu; pass schemas=('mimiciv_ed',) for ED.
    """
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.conname, n.nspname, cl.relname
            FROM pg_constraint c
            JOIN pg_class cl ON cl.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            WHERE c.contype = 'f'
              AND n.nspname = ANY(%s)
            ORDER BY n.nspname, cl.relname, c.conname
            """,
            (list(schemas),),
        )
        fk_constraints = cur.fetchall()

        if not fk_constraints:
            return

        log.info(
            "Dropping %d FK constraint(s) before load (constraint.sql will re-add them) ...",
            len(fk_constraints),
        )
        for conname, nspname, relname in fk_constraints:
            cur.execute(
                f"ALTER TABLE {nspname}.{relname} DROP CONSTRAINT IF EXISTS {conname}"
            )
            log.info("  Dropped FK: %s.%s -> %s", nspname, relname, conname)
        conn.commit()


def _log_table_timings(stdout: str) -> None:
    """Parse [TABLE_START]/[TABLE_END] markers from psql output and log per-table durations."""
    starts: dict[str, datetime] = {}
    for line in stdout.splitlines():
        m = _TIMING_RE.search(line)
        if not m:
            continue
        kind, table, ts = m.groups()
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            continue
        if kind == "START":
            starts[table] = dt
            log.info("  %-30s  started  %s", table, ts)
        elif kind == "END" and table in starts:
            secs = (dt - starts[table]).total_seconds()
            log.info("  %-30s  finished %s  (%.1f s)", table, ts, secs)


# ED load_gz.sql uses unqualified table names (relies on SET search_path TO mimiciv_ed).
# A separate regex is needed to match these; the schema name is not captured from the line.
_ED_COPY_RE = re.compile(
    r"(\\COPY\s+(\w+)\s+FROM\s+PROGRAM\s+')(gzip -dc [^']+)('.*)",
    re.IGNORECASE,
)


def generate_timed_ed_load_sql(
    original_sql: Path,
    out_dir: Path,
    limits: dict | None = None,
) -> Path:
    """
    Rewrite the ED load_gz.sql to wrap each \\COPY with SELECT clock_timestamp()
    timing markers, identical to generate_timed_load_sql() for MIMIC-IV core.

    The ED script uses unqualified table names (e.g. ``\\copy edstays FROM PROGRAM ...``)
    with SET search_path TO mimiciv_ed at the top, so _ED_COPY_RE is used instead of
    _COPY_RE. No core-table path fallback is needed for ED.

    When limits is provided, pipes each gzip stream through head -n <limit+1>.
    """
    lines = []
    for line in original_sql.read_text().splitlines(keepends=True):
        m = _ED_COPY_RE.match(line.rstrip("\n"))
        if m:
            prefix, table, program, suffix = m.groups()
            if limits is not None:
                limit = _row_limit(table, limits)
                if limit is not None:
                    program = f"{program} | head -n {limit + 1}"
                    log.info("  %-30s → limit %d rows", table, limit)
            lines.append(
                f"SELECT '[TABLE_START] {table} ' || to_char(clock_timestamp(), '{_TS_FMT}') AS ts;\n"
            )
            lines.append(f"{prefix}{program}{suffix}\n")
            lines.append(
                f"SELECT '[TABLE_END] {table} ' || to_char(clock_timestamp(), '{_TS_FMT}') AS ts;\n"
            )
        else:
            lines.append(line)

    out_path = out_dir / "load_ed_timed.sql"
    out_path.write_text("".join(lines))
    return out_path


_REFERENCES_RE = re.compile(
    r"(REFERENCES\s+\S+\s*\([^)]+\))\s*;",
    re.IGNORECASE,
)


def generate_not_valid_constraint_sql(original_sql: Path, out_dir: Path) -> Path:
    """
    Rewrite constraint.sql so every FOREIGN KEY is added with NOT VALID,
    meaning PostgreSQL won't scan existing rows for violations. Correct for
    a truncated/limited dataset where referential integrity isn't guaranteed.
    """
    text = original_sql.read_text()
    patched = _REFERENCES_RE.sub(r"\1 NOT VALID;", text)
    out_path = out_dir / "constraint_not_valid.sql"
    out_path.write_text(patched)
    return out_path


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def step1_check_docker() -> None:
    log.info("=" * 60)
    log.info("STEP 1: Verifying Docker is running")
    log.info("=" * 60)

    result = run(["docker", "info"], check=False, capture=True)
    if result.returncode != 0:
        log.error("Docker is not running or not accessible.")
        log.error("Please start Docker Desktop (or the Docker daemon) and retry.")
        sys.exit(1)

    # Extract and log Docker version for diagnostics
    version_result = run(["docker", "version", "--format", "{{.Server.Version}}"],
                         check=False, capture=True)
    version = version_result.stdout.strip() if version_result.returncode == 0 else "unknown"
    log.info("Docker is running. Server version: %s", version)


def step2_ensure_network() -> None:
    log.info("=" * 60)
    log.info("STEP 2: Ensuring Docker network '%s' exists", DOCKER_NETWORK)
    log.info("=" * 60)

    result = run(["docker", "network", "inspect", DOCKER_NETWORK], check=False, capture=True)
    if result.returncode == 0:
        log.info("SKIP: Network '%s' already exists.", DOCKER_NETWORK)
        return

    log.info("Network '%s' not found — creating it ...", DOCKER_NETWORK)
    run(["docker", "network", "create", DOCKER_NETWORK])
    log.info("Network '%s' created successfully.", DOCKER_NETWORK)


def step3_build_image() -> None:
    log.info("=" * 60)
    log.info("STEP 3: Building PostgreSQL image '%s'", DB_IMAGE_NAME)
    log.info("=" * 60)

    result = run(["docker", "image", "inspect", DB_IMAGE_NAME], check=False, capture=True)
    if result.returncode == 0:
        log.info("SKIP: Image '%s' already exists — skipping build.", DB_IMAGE_NAME)
        log.info("      To force a rebuild: docker rmi %s", DB_IMAGE_NAME)
        return

    dockerfile = Path("docker/Dockerfile.db")
    if not dockerfile.exists():
        log.error("Dockerfile not found at '%s'. Run from the project root.", dockerfile)
        sys.exit(1)

    log.info("Building image '%s' from %s ...", DB_IMAGE_NAME, dockerfile)
    run(["docker", "build", "-f", str(dockerfile), "-t", DB_IMAGE_NAME, "."])
    log.info("Image '%s' built successfully.", DB_IMAGE_NAME)


def _ensure_container_on_network() -> None:
    """Connect DB_CONTAINER_NAME to DOCKER_NETWORK if it isn't already."""
    inspect = run(
        ["docker", "container", "inspect", DB_CONTAINER_NAME],
        check=False, capture=True,
    )
    if inspect.returncode != 0:
        return  # container doesn't exist yet; nothing to do
    info = json.loads(inspect.stdout)
    networks = info[0].get("NetworkSettings", {}).get("Networks", {})
    if DOCKER_NETWORK not in networks:
        log.warning(
            "Container '%s' is not on network '%s' — connecting it now ...",
            DB_CONTAINER_NAME, DOCKER_NETWORK,
        )
        run(["docker", "network", "connect", DOCKER_NETWORK, DB_CONTAINER_NAME])
        log.info("Container '%s' connected to network '%s'.", DB_CONTAINER_NAME, DOCKER_NETWORK)
    else:
        log.info("Container '%s' is already on network '%s'.", DB_CONTAINER_NAME, DOCKER_NETWORK)


def step4_start_db() -> None:
    log.info("=" * 60)
    log.info("STEP 4: Starting PostgreSQL container '%s'", DB_CONTAINER_NAME)
    log.info("=" * 60)

    inspect = run(
        ["docker", "container", "inspect", DB_CONTAINER_NAME],
        check=False, capture=True,
    )

    if inspect.returncode == 0:
        info  = json.loads(inspect.stdout)
        state = info[0]["State"]["Status"]

        if state == "running":
            log.info("Container '%s' is already running.", DB_CONTAINER_NAME)
        else:
            log.info("Container '%s' exists but is in state '%s' — starting it ...",
                     DB_CONTAINER_NAME, state)
            run(["docker", "start", DB_CONTAINER_NAME])
            log.info("Container '%s' started.", DB_CONTAINER_NAME)

        _ensure_container_on_network()
        return

    # Container does not exist — create data directory and launch
    data_dir = Path("data/db").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    log.info("Data directory: %s", _data_dir_str(data_dir))

    log.info("Creating and starting container '%s' ...", DB_CONTAINER_NAME)
    run([
        "docker", "run", "-d",
        "--name",      DB_CONTAINER_NAME,
        "--network",   DOCKER_NETWORK,
        "--shm-size",  "256m",
        "-p",          f"{POSTGRES_PORT}:5432",
        "-v",          f"{data_dir}:/var/lib/postgresql/data",
        DB_IMAGE_NAME,
    ])
    log.info("Container '%s' created and started.", DB_CONTAINER_NAME)


def step5_wait_for_db(max_attempts: int = 30, delay: float = 5.0) -> None:
    log.info("=" * 60)
    log.info("STEP 5: Waiting for PostgreSQL to accept connections")
    log.info("=" * 60)
    log.info("Polling every %.0f s (up to %d attempts) ...", delay, max_attempts)

    for attempt in range(1, max_attempts + 1):
        try:
            conn = db_connect()
            conn.close()
            log.info("PostgreSQL is ready (attempt %d/%d).", attempt, max_attempts)
            return
        except Exception as exc:
            log.info(
                "Not ready yet [%d/%d]: %s — retrying in %.0f s ...",
                attempt, max_attempts, exc, delay,
            )
            time.sleep(delay)

    log.error("PostgreSQL did not become ready after %d attempts.", max_attempts)
    log.error("Check container logs with: docker logs %s", DB_CONTAINER_NAME)
    sys.exit(1)


def step6_create_schema() -> None:
    log.info("=" * 60)
    log.info("STEP 6: Creating MIMIC-IV schemas and tables (create.sql)")
    log.info("=" * 60)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.schemata
            WHERE schema_name IN ('mimiciv_hosp', 'mimiciv_icu', 'mimiciv_derived')
        """)
        schema_count = cur.fetchone()[0]

    if schema_count == 3:
        # Also verify at least one table exists to confirm create.sql completed
        with db_connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'mimiciv_hosp'
            """)
            table_count = cur.fetchone()[0]

        if table_count > 0:
            log.info(
                "SKIP: All 3 MIMIC-IV schemas already exist with %d tables in mimiciv_hosp.",
                table_count,
            )
            return
        log.info("Schemas exist but have no tables — re-running create.sql ...")
    else:
        log.info("Found %d/3 required schemas — running create.sql ...", schema_count)

    script = MIMIC_BUILD_DIR / "create.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        log.error("Ensure mimic-code is cloned at MIMIC_CODE_DIR='%s'.", _rel_path(MIMIC_CODE_DIR))
        sys.exit(1)

    log.warning("NOTE: create.sql drops and recreates schemas. Existing data will be lost.")
    run_psql_in_docker(script)
    log.info("Schemas and tables created successfully.")


def step7_load_data() -> None:
    log.info("=" * 60)
    log.info("STEP 7: Loading MIMIC-IV data (load_gz.sql)")
    log.info("=" * 60)
    log.info("NOTE: This step can take many hours for the full dataset.")

    if not MIMIC_DATA_DIR:
        log.error("MIMIC_DATA_DIR is not set in .env")
        log.error("Set it to the directory containing hosp/ and icu/ subdirectories.")
        sys.exit(1)

    data_path = Path(MIMIC_DATA_DIR).resolve()
    if not data_path.exists():
        log.error("MIMIC_DATA_DIR '%s' does not exist.", _data_dir_str(data_path))
        sys.exit(1)

    # Check for expected subdirectories
    for subdir in ("hosp", "icu"):
        if not (data_path / subdir).is_dir():
            log.error("Expected subdirectory '%s/' not found in %s", subdir, _data_dir_str(data_path))
            sys.exit(1)

    # Idempotency check: skip if patients table already has rows
    try:
        with db_connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM mimiciv_hosp.patients")
            row_count = cur.fetchone()[0]
    except Exception:
        row_count = 0

    if row_count > 0:
        log.info(
            "SKIP: mimiciv_hosp.patients already contains %d rows — data already loaded.",
            row_count,
        )
        return

    log.info("No data found in mimiciv_hosp.patients — starting data load ...")
    _drop_fk_constraints()
    _truncate_all_tables()
    log.info("Data directory: %s", _data_dir_str(data_path))
    log.info("This may take many hours. Per-table timing will appear below when complete.")

    script = MIMIC_BUILD_DIR / "load_gz.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    limits = load_limits()
    if limits is not None:
        log.info("Row limits active (from '%s'): default=%s", _rel_path(LOAD_LIMITS_FILE), limits.get("default"))

    core_tables = _find_core_table_paths(script, data_path)
    if core_tables:
        log.info(
            "Sourcing %d table(s) from core/ instead of hosp/: %s",
            len(core_tables), ", ".join(core_tables),
        )

    with tempfile.TemporaryDirectory() as tmp:
        timed_script = generate_timed_load_sql(script, Path(tmp), limits=limits, core_tables=core_tables)
        result = run_psql_in_docker(
            timed_script,
            extra_psql_vars={"mimic_data_dir": "/data/mimic"},
            extra_volumes=[f"{data_path}:/data/mimic:ro"],
            capture=True,
        )

    _log_table_timings(result.stdout)
    log.info("Data loaded successfully.")


def step8_add_constraints() -> None:
    log.info("=" * 60)
    log.info("STEP 8: Adding primary key constraints (constraint.sql)")
    log.info("=" * 60)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'mimiciv_hosp'
              AND c.conname = 'admissions_pk'
        """)
        pk_exists = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'mimiciv_hosp'
              AND c.conname = 'admissions_patients_fk'
        """)
        fk_exists = cur.fetchone()[0]

    if pk_exists and fk_exists:
        log.info("SKIP: Constraints already exist (admissions_pk + admissions_patients_fk) — skipping constraint.sql.")
        return

    script = MIMIC_BUILD_DIR / "constraint.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    if load_limits() is not None:
        log.info("Row limits active — adding FK constraints with NOT VALID (skips data scan).")
        with tempfile.TemporaryDirectory() as tmp:
            patched_script = generate_not_valid_constraint_sql(script, Path(tmp))
            run_psql_in_docker(patched_script)
    else:
        log.info("Constraints not found — running constraint.sql ...")
        run_psql_in_docker(script)
    log.info("Constraints added successfully.")


def step9_create_indexes() -> None:
    log.info("=" * 60)
    log.info("STEP 9: Creating indexes (index.sql)")
    log.info("=" * 60)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE schemaname = 'mimiciv_hosp'
              AND indexname = 'admissions_idx01'
        """)
        exists = cur.fetchone()[0]

    if exists:
        log.info("SKIP: Index 'admissions_idx01' already exists — skipping index.sql.")
        return

    script = MIMIC_BUILD_DIR / "index.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    log.info("Indexes not found — running index.sql ...")
    run_psql_in_docker(script)
    log.info("Indexes created successfully.")


def step10_validate() -> None:
    log.info("=" * 60)
    log.info("STEP 10: Validating row counts (validate.sql)")
    log.info("=" * 60)

    script = MIMIC_BUILD_DIR / "validate.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    abs_script = script.resolve()
    conn_str = (
        f"host={DB_CONTAINER_NAME} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    # Capture output so we can log it cleanly
    result = run(
        [
            "docker", "run", "--rm",
            "--network", DOCKER_NETWORK,
            "-v", f"{abs_script}:/script.sql:ro",
            "postgres:17",
            "psql", conn_str,
            "-f", "/script.sql",
        ],
        check=False,
        capture=True,
    )

    log.info("Validation output:\n%s", result.stdout)

    if result.stderr.strip():
        log.warning("Validation stderr:\n%s", result.stderr)

    if result.returncode != 0:
        log.warning(
            "validate.sql exited with code %d — some row counts may not match expected values.",
            result.returncode,
        )
    else:
        log.info("Validation completed — all row counts match expected values.")


# ---------------------------------------------------------------------------
# MIMIC-IV-ED pipeline steps (11–15)
# ---------------------------------------------------------------------------

def step11_create_ed_schema() -> None:
    log.info("=" * 60)
    log.info("STEP 11: Creating MIMIC-IV-ED schema and tables (ED create.sql)")
    log.info("=" * 60)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.schemata
            WHERE schema_name = 'mimiciv_ed'
        """)
        schema_exists = cur.fetchone()[0]

    if schema_exists:
        with db_connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'mimiciv_ed'
            """)
            table_count = cur.fetchone()[0]

        if table_count > 0:
            log.info(
                "SKIP: mimiciv_ed schema already exists with %d tables.",
                table_count,
            )
            return
        log.info("mimiciv_ed schema exists but has no tables — re-running ED create.sql ...")
    else:
        log.info("mimiciv_ed schema not found — running ED create.sql ...")

    script = MIMIC_ED_BUILD_DIR / "create.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        log.error("Ensure mimic-code is cloned at MIMIC_CODE_DIR='%s'.", _rel_path(MIMIC_CODE_DIR))
        sys.exit(1)

    log.warning("NOTE: ED create.sql drops and recreates mimiciv_ed. Existing ED data will be lost.")
    run_psql_in_docker(script)
    log.info("ED schema and tables created successfully.")


def step12_load_ed_data() -> None:
    log.info("=" * 60)
    log.info("STEP 12: Loading MIMIC-IV-ED data (ED load_gz.sql)")
    log.info("=" * 60)

    if not MIMIC_DATA_DIR:
        log.error("MIMIC_DATA_DIR is not set in .env")
        log.error("Set it to the directory whose ed/ subdirectory contains the ED CSV files.")
        sys.exit(1)

    data_path = Path(MIMIC_DATA_DIR).resolve()
    ed_path = data_path / "ed"
    if not ed_path.is_dir():
        log.error("Expected ED subdirectory '%s' not found.", _data_dir_str(ed_path))
        log.error("Place MIMIC-IV-ED CSV files under %s/ed/", _data_dir_str(data_path))
        sys.exit(1)

    # Idempotency check: skip if edstays already has rows
    try:
        with db_connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM mimiciv_ed.edstays")
            row_count = cur.fetchone()[0]
    except Exception:
        row_count = 0

    if row_count > 0:
        log.info(
            "SKIP: mimiciv_ed.edstays already contains %d rows — ED data already loaded.",
            row_count,
        )
        return

    log.info("No data found in mimiciv_ed.edstays — starting ED data load ...")
    _drop_fk_constraints(schemas=("mimiciv_ed",))
    _truncate_all_tables(schemas=("mimiciv_ed",))
    log.info("ED data directory: %s", _data_dir_str(ed_path))
    log.info("Per-table timing will appear below when complete.")

    script = MIMIC_ED_BUILD_DIR / "load_gz.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    limits = load_limits()
    if limits is not None:
        log.info("Row limits active (from '%s'): default=%s", _rel_path(LOAD_LIMITS_FILE), limits.get("default"))

    with tempfile.TemporaryDirectory() as tmp:
        timed_script = generate_timed_ed_load_sql(script, Path(tmp), limits=limits)
        result = run_psql_in_docker(
            timed_script,
            # The ED load script uses \cd :mimic_data_dir then relative filenames.
            # Mount the ed/ directory at /data/mimic/ed and point mimic_data_dir there.
            extra_psql_vars={"mimic_data_dir": "/data/mimic/ed"},
            extra_volumes=[f"{ed_path}:/data/mimic/ed:ro"],
            capture=True,
        )

    _log_table_timings(result.stdout)
    log.info("ED data loaded successfully.")


def step13_add_ed_constraints() -> None:
    log.info("=" * 60)
    log.info("STEP 13: Adding MIMIC-IV-ED constraints (ED constraint.sql)")
    log.info("=" * 60)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'mimiciv_ed'
              AND c.conname = 'edstays_pk'
        """)
        pk_exists = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'mimiciv_ed'
              AND c.conname = 'diagnosis_edstays_fk'
        """)
        fk_exists = cur.fetchone()[0]

    if pk_exists and fk_exists:
        log.info(
            "SKIP: ED constraints already exist (edstays_pk + diagnosis_edstays_fk) — skipping ED constraint.sql."
        )
        return

    script = MIMIC_ED_BUILD_DIR / "constraint.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    if load_limits() is not None:
        log.info("Row limits active — adding ED FK constraints with NOT VALID (skips data scan).")
        with tempfile.TemporaryDirectory() as tmp:
            patched_script = generate_not_valid_constraint_sql(script, Path(tmp))
            run_psql_in_docker(patched_script)
    else:
        log.info("ED constraints not found — running ED constraint.sql ...")
        run_psql_in_docker(script)
    log.info("ED constraints added successfully.")


def step14_create_ed_indexes() -> None:
    log.info("=" * 60)
    log.info("STEP 14: Creating MIMIC-IV-ED indexes (ED index.sql)")
    log.info("=" * 60)

    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE schemaname = 'mimiciv_ed'
              AND indexname = 'edstays_idx01'
        """)
        exists = cur.fetchone()[0]

    if exists:
        log.info("SKIP: Index 'edstays_idx01' already exists — skipping ED index.sql.")
        return

    script = MIMIC_ED_BUILD_DIR / "index.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    log.info("ED indexes not found — running ED index.sql ...")
    run_psql_in_docker(script)
    log.info("ED indexes created successfully.")


def step15_validate_ed() -> None:
    log.info("=" * 60)
    log.info("STEP 15: Validating MIMIC-IV-ED row counts (ED validate.sql)")
    log.info("=" * 60)

    script = MIMIC_ED_BUILD_DIR / "validate.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        sys.exit(1)

    abs_script = script.resolve()
    conn_str = (
        f"host={DB_CONTAINER_NAME} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    result = run(
        [
            "docker", "run", "--rm",
            "--network", DOCKER_NETWORK,
            "-v", f"{abs_script}:/script.sql:ro",
            "postgres:17",
            "psql", conn_str,
            "-f", "/script.sql",
        ],
        check=False,
        capture=True,
    )

    log.info("ED validation output:\n%s", result.stdout)

    if result.stderr.strip():
        log.warning("ED validation stderr:\n%s", result.stderr)

    if result.returncode != 0:
        log.warning(
            "ED validate.sql exited with code %d — some row counts may not match expected values.",
            result.returncode,
        )
    else:
        log.info("ED validation completed — all row counts match expected values.")


# ---------------------------------------------------------------------------
# MIMIC-IV Concepts pipeline steps (16–17)
# ---------------------------------------------------------------------------

def step16_create_concepts_schema() -> None:
    log.info("=" * 60)
    log.info("STEP 16: Creating mimiciv_derived schema + helper functions")
    log.info("=" * 60)

    # Create the schema if it doesn't exist (idempotent)
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS mimiciv_derived")
        conn.commit()
    log.info("mimiciv_derived schema is ready.")

    # Step 16a: run postgres-functions.sql (BigQuery compatibility helpers)
    log.info("Step 16a: Installing helper functions (postgres-functions.sql) ...")

    functions_script = MIMIC_CONCEPTS_DIR / "postgres-functions.sql"
    if not functions_script.exists():
        log.error("Script not found: %s", _rel_path(functions_script))
        log.error("Ensure mimic-code is cloned at MIMIC_CODE_DIR='%s'.", _rel_path(MIMIC_CODE_DIR))
        sys.exit(1)

    # Idempotency check: skip if regexp_extract already exists in mimiciv_derived
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'mimiciv_derived'
              AND p.proname = 'regexp_extract'
        """)
        fn_exists = cur.fetchone()[0]

    if fn_exists:
        log.info("SKIP: Helper function 'regexp_extract' already exists in mimiciv_derived — skipping postgres-functions.sql.")
    else:
        run_psql_in_docker(functions_script)
        log.info("Helper functions installed successfully.")


def step17_build_concepts() -> None:
    log.info("=" * 60)
    log.info("STEP 17: Building MIMIC-IV derived concepts (postgres-make-concepts.sql)")
    log.info("=" * 60)

    script = MIMIC_CONCEPTS_DIR / "postgres-make-concepts.sql"
    if not script.exists():
        log.error("Script not found: %s", _rel_path(script))
        log.error("Ensure mimic-code is cloned at MIMIC_CODE_DIR='%s'.", _rel_path(MIMIC_CODE_DIR))
        sys.exit(1)

    # Warn if mimiciv_ed appears empty — concepts that reference ED will still be skipped
    # gracefully via search_path; concepts that don't touch ED will build fine.
    try:
        with db_connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM mimiciv_ed.edstays")
            ed_rows = cur.fetchone()[0]
        if ed_rows == 0:
            log.warning(
                "mimiciv_ed.edstays has 0 rows — concepts that reference ED tables "
                "may produce empty results. Run steps 11–15 first to load ED data."
            )
    except Exception:
        log.warning(
            "Could not query mimiciv_ed.edstays — ED schema may not exist yet. "
            "Concepts that reference ED tables will produce empty results."
        )

    # postgres-make-concepts.sql uses \i with relative paths (e.g. \i demographics/age.sql).
    # psql resolves \i relative to its working directory, so we mount the entire
    # concepts_postgres/ directory at /concepts and set --workdir /concepts.
    abs_concepts_dir = MIMIC_CONCEPTS_DIR.resolve()
    conn_str = (
        f"host={DB_CONTAINER_NAME} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    log.info("Building all concepts — this may take 15–60 minutes ...")
    result = run(
        [
            "docker", "run", "--rm",
            "--network", DOCKER_NETWORK,
            "--workdir", "/concepts",
            "-v", f"{abs_concepts_dir}:/concepts:ro",
            "postgres:17",
            "psql", conn_str,
            "-v", "ON_ERROR_STOP=1",
            "-f", "/concepts/postgres-make-concepts.sql",
        ],
        check=False,
        capture=True,
    )

    if result.stdout.strip():
        log.info("Concepts build output:\n%s", result.stdout)
    if result.stderr.strip():
        log.warning("Concepts build stderr:\n%s", result.stderr)

    if result.returncode != 0:
        log.warning(
            "postgres-make-concepts.sql exited with code %d — some concepts may not have built correctly.",
            result.returncode,
        )
    else:
        log.info("All derived concepts built successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("MIMIC-IV PostgreSQL Build Pipeline")
    log.info("=" * 60)
    log.info("Database   : %s", POSTGRES_DB)
    log.info("User       : %s", POSTGRES_USER)
    log.info("Container  : %s", DB_CONTAINER_NAME)
    log.info("Network    : %s", DOCKER_NETWORK)
    log.info("Port       : %d", POSTGRES_PORT)
    log.info("Data dir      : %s", _data_dir_str(MIMIC_DATA_DIR) if MIMIC_DATA_DIR else "(not set)")
    log.info("Limits file   : %s", _rel_path(LOAD_LIMITS_FILE) if LOAD_LIMITS_FILE else "(not set — no row limits)")
    log.info("mimic-code    : %s", _rel_path(MIMIC_CODE_DIR))
    log.info("Build dir     : %s", _rel_path(MIMIC_BUILD_DIR))
    log.info("ED build dir  : %s", _rel_path(MIMIC_ED_BUILD_DIR))
    log.info("Concepts dir  : %s", _rel_path(MIMIC_CONCEPTS_DIR))
    log.info("=" * 60)

    step1_check_docker()
    step2_ensure_network()
    step3_build_image()
    step4_start_db()
    step5_wait_for_db()
    step6_create_schema()
    step7_load_data()
    step8_add_constraints()
    step9_create_indexes()
    step10_validate()
    step11_create_ed_schema()
    step12_load_ed_data()
    step13_add_ed_constraints()
    step14_create_ed_indexes()
    step15_validate_ed()
    step16_create_concepts_schema()
    step17_build_concepts()

    log.info("=" * 60)
    log.info("Pipeline complete. MIMIC-IV, MIMIC-IV-ED, and derived concepts are ready.")
    log.info("Connect via: psql -h localhost -p %d -U %s -d %s",
             POSTGRES_PORT, POSTGRES_USER, POSTGRES_DB)
    log.info("Web UI: http://localhost:28080  (if Adminer container is running)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
