#!/usr/bin/env python3
"""
concepts.py
-----------
Showcase script for the mimiciv_derived schema — the ~65 derived concept tables
built by step 17 of build_mimic.py (postgres-make-concepts.sql from MIT-LCP/mimic-code).

Each section explains the clinical purpose of a concept category, runs a representative
query, and prints the result.  Tables that have not yet been built are skipped gracefully.

Run:
    uv run python concepts.py

Prerequisites:
    - The mimic_postgres container must be running (steps 1–5 of build_mimic.py)
    - mimiciv_hosp / mimiciv_icu data must be loaded (steps 6–15)
    - mimiciv_derived concepts must be built (steps 16–17)
"""

import os
import sys
import textwrap

import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

POSTGRES_HOST     = os.environ.get("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.environ.get("POSTGRES_DB",       "mimiciv")
POSTGRES_USER     = os.environ.get("POSTGRES_USER",     "mimicuser")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "mimicpass")


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        autocommit=True,
    )


def query(sql: str, conn: psycopg.Connection) -> pd.DataFrame | None:
    """Run a SQL query and return a DataFrame, or None if the table doesn't exist."""
    try:
        return pd.read_sql(sql, conn)
    except Exception as e:
        msg = str(e).split("\n")[0]
        print(f"  [SKIP] {msg}")
        return None


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

_HEADER = "─" * 70

def section(title: str, description: str) -> None:
    print()
    print("═" * 70)
    print(f"  {title}")
    print("═" * 70)
    for line in textwrap.wrap(description, 68):
        print(f"  {line}")
    print()


def subsection(title: str, note: str = "") -> None:
    print(_HEADER)
    print(f"  {title}")
    if note:
        for line in textwrap.wrap(note, 66):
            print(f"    {line}")
    print()


def show(df: pd.DataFrame | None, max_rows: int = 10) -> None:
    if df is None:
        return
    if df.empty:
        print("  (no rows returned)")
        return
    print(df.to_string(index=False, max_rows=max_rows))
    if len(df) > max_rows:
        print(f"  ... ({len(df)} rows total, showing first {max_rows})")
    print()


# ---------------------------------------------------------------------------
# Main showcase
# ---------------------------------------------------------------------------

def main() -> None:
    print("═" * 70)
    print("  MIMIC-IV Derived Concepts Showcase")
    print(f"  db={POSTGRES_DB}  host={POSTGRES_HOST}:{POSTGRES_PORT}")
    print("═" * 70)

    try:
        conn = _connect()
    except Exception as e:
        print(f"ERROR: Cannot connect to PostgreSQL — {e}")
        print(f"  Is the mimic_postgres container running?")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 0. Overview — all derived tables and their row counts
    # ------------------------------------------------------------------
    section(
        "0. Overview — mimiciv_derived tables",
        "All materialized views built by step 17. Each row represents one concept "
        "table, its category (inferred from the mimic-code subdirectory), and its "
        "current row count. A count of 0 means the concept built successfully but "
        "produced no rows (can happen with a row-limited load).",
    )
    overview_sql = """
        SELECT
            table_name,
            pg_size_pretty(pg_total_relation_size(
                quote_ident(table_schema) || '.' || quote_ident(table_name)
            )) AS size_on_disk
        FROM information_schema.tables
        WHERE table_schema = 'mimiciv_derived'
          AND table_type   = 'BASE TABLE'
        ORDER BY table_name
    """
    df_overview = query(overview_sql, conn)
    if df_overview is not None and not df_overview.empty:
        # Add row counts
        counts = []
        for tbl in df_overview["table_name"]:
            try:
                cur = conn.execute(f"SELECT COUNT(*) FROM mimiciv_derived.{tbl}")
                counts.append(cur.fetchone()[0])
            except Exception:
                counts.append(None)
        df_overview.insert(1, "row_count", counts)
        show(df_overview, max_rows=70)
        print(f"  Total concept tables: {len(df_overview)}")

    # ------------------------------------------------------------------
    # 1. Demographics & ICU stays
    # ------------------------------------------------------------------
    section(
        "1. Demographics & ICU Stays",
        "icustay_detail combines patient demographics (age, sex, ethnicity) with "
        "hospital and ICU admission metadata. It is the standard starting point for "
        "cohort selection: researchers filter by admission type, age range, or ICU "
        "length of stay. icustay_times and icustay_hourly provide the time grid "
        "used by all hour-level concepts (SOFA, vital signs, etc.).",
    )

    subsection(
        "icustay_detail — one row per ICU stay",
        "Useful columns: subject_id, hadm_id, stay_id, gender, admission_age, "
        "ethnicity, first_careunit, los_hospital, los_icu, hospital_expire_flag.",
    )
    show(query("""
        SELECT subject_id, hadm_id, stay_id, gender,
               ROUND(admission_age::numeric, 1) AS age_yrs,
               first_careunit,
               ROUND(los_icu::numeric, 2)       AS icu_los_days,
               hospital_expire_flag
        FROM mimiciv_derived.icustay_detail
        ORDER BY stay_id
        LIMIT 10
    """, conn))

    subsection(
        "Age distribution across ICU admissions",
        "Helps characterise the patient population; ICU cohorts are typically "
        "skewed toward older adults.",
    )
    show(query("""
        SELECT
            width_bucket(admission_age, 18, 100, 8) * 10 + 8 AS age_bucket_start,
            COUNT(*) AS n_stays
        FROM mimiciv_derived.icustay_detail
        WHERE admission_age >= 18
        GROUP BY 1
        ORDER BY 1
    """, conn))

    # ------------------------------------------------------------------
    # 2. Severity Scores
    # ------------------------------------------------------------------
    section(
        "2. Severity Scores",
        "Severity scores summarise illness severity at ICU admission using "
        "combinations of vital signs, lab values, and clinical data. They are "
        "widely used as covariates in regression models and for risk-adjusted "
        "outcome comparisons. MIMIC-IV derived tables provide SOFA (hourly), "
        "SAPS II, OASIS, APSIII, LODS, and SIRS.",
    )

    subsection(
        "SOFA — Sequential Organ Failure Assessment (hourly)",
        "SOFA scores 6 organ systems (respiration, coagulation, liver, "
        "cardiovascular, CNS, renal) from 0–4 each. Higher total = more organ "
        "failure. The table has one row per ICU stay per hour, enabling "
        "time-series analyses of deterioration.",
    )
    show(query("""
        SELECT stay_id, hr, sofa_24hours AS sofa_24h,
               respiration, coagulation, liver,
               cardiovascular, cns, renal
        FROM mimiciv_derived.sofa
        WHERE hr BETWEEN 0 AND 23
        ORDER BY stay_id, hr
        LIMIT 12
    """, conn))

    subsection(
        "SAPS II — Simplified Acute Physiology Score II (admission)",
        "SAPS II is calculated from the worst values in the first 24 h. It "
        "predicts hospital mortality using 17 variables. Score range 0–163; "
        ">56 associated with >80% mortality. Often used as an inclusion "
        "criterion or severity covariate.",
    )
    show(query("""
        SELECT stay_id, sapsii, sapsii_prob
        FROM mimiciv_derived.sapsii
        ORDER BY sapsii DESC
        LIMIT 10
    """, conn))

    subsection(
        "OASIS — Oxford Acute Severity of Illness Score",
        "OASIS uses 10 variables collected in the first hour. Unlike SAPS II it "
        "does not require lab values — useful when labs are missing. Predicts "
        "in-hospital mortality.",
    )
    show(query("""
        SELECT stay_id, oasis, oasis_prob
        FROM mimiciv_derived.oasis
        ORDER BY oasis DESC
        LIMIT 10
    """, conn))

    subsection(
        "Score correlation — SAPS II vs OASIS vs SOFA (first day)",
        "Checking that three independent severity scores agree directionally "
        "validates cohort quality and confirms correct joins.",
    )
    show(query("""
        SELECT
            ROUND(AVG(s.sapsii), 1)           AS mean_sapsii,
            ROUND(AVG(o.oasis), 1)            AS mean_oasis,
            ROUND(AVG(f.sofa_24hours), 1)     AS mean_sofa_24h,
            ROUND(CORR(s.sapsii, o.oasis)::numeric, 3) AS corr_sapsii_oasis,
            ROUND(CORR(s.sapsii, f.sofa_24hours)::numeric, 3) AS corr_sapsii_sofa
        FROM mimiciv_derived.sapsii s
        JOIN mimiciv_derived.oasis o USING (stay_id)
        JOIN mimiciv_derived.first_day_sofa f USING (stay_id)
    """, conn))

    # ------------------------------------------------------------------
    # 3. Sepsis-3
    # ------------------------------------------------------------------
    section(
        "3. Sepsis-3",
        "The Sepsis-3 definition (Singer et al., JAMA 2016) requires: (1) "
        "suspected infection — antibiotics plus blood cultures within a 2-day "
        "window — and (2) organ dysfunction — SOFA score increase ≥2 from "
        "baseline. MIMIC-IV provides both building blocks as derived tables, "
        "enabling reproducible Sepsis-3 cohort construction without manual chart "
        "review.",
    )

    subsection(
        "sepsis3 — one row per ICU stay meeting Sepsis-3 criteria",
        "Key columns: stay_id, subject_id, antibiotic_time, culture_time, "
        "suspected_infection_time, sofa_time, sofa_score, respiration, "
        "coagulation, liver, cardiovascular, cns, renal.",
    )
    show(query("""
        SELECT stay_id, suspected_infection_time,
               sofa_score,
               respiration, cardiovascular, renal, cns
        FROM mimiciv_derived.sepsis3
        ORDER BY stay_id
        LIMIT 10
    """, conn))

    subsection(
        "Sepsis-3 prevalence and average organ dysfunction",
        "How many ICU stays meet Sepsis-3 criteria, and which organ systems "
        "are most commonly affected?",
    )
    show(query("""
        SELECT
            COUNT(*)                              AS sepsis3_stays,
            ROUND(AVG(sofa_score)::numeric, 2)    AS mean_sofa,
            ROUND(AVG(respiration)::numeric, 2)   AS mean_respiratory,
            ROUND(AVG(cardiovascular)::numeric, 2) AS mean_cardiovascular,
            ROUND(AVG(renal)::numeric, 2)         AS mean_renal,
            ROUND(AVG(cns)::numeric, 2)           AS mean_cns
        FROM mimiciv_derived.sepsis3
    """, conn))

    subsection(
        "suspicion_of_infection — antibiotic + culture timing",
        "Intermediate table: each row is one antibiotic order with the matched "
        "blood culture time. sepsis3 is derived from this table by joining with "
        "SOFA.",
    )
    show(query("""
        SELECT stay_id, antibiotic_time, culture_time,
               specimen, positive_culture
        FROM mimiciv_derived.suspicion_of_infection
        ORDER BY stay_id
        LIMIT 10
    """, conn))

    # ------------------------------------------------------------------
    # 4. Blood Gas (arterial)
    # ------------------------------------------------------------------
    section(
        "4. Blood Gas Measurements",
        "Arterial blood gases (ABGs) measure respiratory function directly: "
        "pH (acid-base balance), PaO2 (oxygen), PaCO2 (CO2 clearance), "
        "bicarbonate, and base excess. The bg table joins lab events from "
        "d_labitems to produce a wide-format row per specimen, pivoting ~20 "
        "analytes into named columns. This is far more convenient than "
        "querying labevents directly.",
    )

    subsection(
        "bg — one row per blood gas specimen",
        "Selected analytes: specimen type, pH, PaO2, PaCO2, SpO2, "
        "bicarbonate, base excess, lactate, glucose.",
    )
    show(query("""
        SELECT stay_id, charttime, specimen,
               ROUND(ph::numeric, 2)          AS ph,
               ROUND(po2::numeric, 1)         AS pao2_mmhg,
               ROUND(pco2::numeric, 1)        AS paco2_mmhg,
               ROUND(bicarbonate::numeric, 1) AS hco3,
               ROUND(baseexcess::numeric, 1)  AS base_excess,
               ROUND(lactate::numeric, 2)     AS lactate,
               ROUND(glucose::numeric, 1)     AS glucose
        FROM mimiciv_derived.bg
        WHERE specimen = 'ART.'
        ORDER BY stay_id, charttime
        LIMIT 10
    """, conn))

    subsection(
        "PaO2/FiO2 ratio distribution (P:F ratio)",
        "P:F ratio < 300 defines mild ARDS; < 200 moderate; < 100 severe "
        "(Berlin definition). This query shows the distribution of ABG readings "
        "stratified by ARDS severity category.",
    )
    show(query("""
        SELECT
            CASE
                WHEN pf_ratio < 100  THEN '< 100  (severe ARDS)'
                WHEN pf_ratio < 200  THEN '100–200 (moderate ARDS)'
                WHEN pf_ratio < 300  THEN '200–300 (mild ARDS)'
                ELSE                      '≥ 300   (no ARDS)'
            END AS pf_category,
            COUNT(*) AS n_specimens
        FROM mimiciv_derived.bg
        WHERE pf_ratio IS NOT NULL
          AND specimen = 'ART.'
        GROUP BY 1
        ORDER BY 1
    """, conn))

    # ------------------------------------------------------------------
    # 5. Vital Signs
    # ------------------------------------------------------------------
    section(
        "5. Vital Signs",
        "vitalsign aggregates heart rate, blood pressure (systolic, diastolic, "
        "mean), respiratory rate, temperature, and SpO2 from chartevents. Each "
        "row covers a one-hour window. This is useful for time-series modelling, "
        "early warning score calculation, and shock detection.",
    )

    subsection(
        "vitalsign — hourly vital sign summary",
        "One row per (stay_id, charttime) window. Columns: heart_rate, "
        "sbp, dbp, mbp, resp_rate, temperature, spo2.",
    )
    show(query("""
        SELECT stay_id, charttime,
               heart_rate,
               sbp, dbp, mbp,
               resp_rate,
               ROUND(temperature::numeric, 1) AS temp_c,
               spo2
        FROM mimiciv_derived.vitalsign
        ORDER BY stay_id, charttime
        LIMIT 10
    """, conn))

    subsection(
        "Shock index distribution  (heart_rate / sbp)",
        "Shock index > 1.0 is a simple bedside flag for haemodynamic compromise. "
        "This query shows how common SI > 1 is in the cohort.",
    )
    show(query("""
        SELECT
            CASE
                WHEN heart_rate / NULLIF(sbp, 0) < 0.5  THEN '< 0.5  (normal)'
                WHEN heart_rate / NULLIF(sbp, 0) < 1.0  THEN '0.5–1.0 (borderline)'
                ELSE                                          '≥ 1.0   (shock index elevated)'
            END AS shock_index_category,
            COUNT(*) AS n_readings
        FROM mimiciv_derived.vitalsign
        WHERE heart_rate IS NOT NULL AND sbp IS NOT NULL AND sbp > 0
        GROUP BY 1
        ORDER BY 1
    """, conn))

    # ------------------------------------------------------------------
    # 6. Glasgow Coma Scale
    # ------------------------------------------------------------------
    section(
        "6. Glasgow Coma Scale (GCS)",
        "GCS quantifies level of consciousness: eye (1–4), verbal (1–5), motor "
        "(1–6) = total 3–15. GCS 3–8 indicates severe impairment. The gcs table "
        "provides one row per charted assessment, with special handling for "
        "intubated patients whose verbal score is imputed (ETT flag). "
        "first_day_gcs gives the minimum 24-hour GCS — used in SOFA, SAPS II, "
        "OASIS, and APACHE III.",
    )

    subsection(
        "gcs — one row per charted GCS assessment",
        "Columns: gcs_eye, gcs_verbal, gcs_motor, gcs (total), "
        "gcs_unable (verbal imputed due to intubation).",
    )
    show(query("""
        SELECT stay_id, charttime,
               gcs_eye, gcs_verbal, gcs_motor,
               gcs AS gcs_total,
               gcs_unable
        FROM mimiciv_derived.gcs
        ORDER BY stay_id, charttime
        LIMIT 10
    """, conn))

    subsection(
        "first_day_gcs — worst GCS in the first 24 h",
        "Distribution of worst-in-day GCS: a low GCS in the first 24 h is one "
        "of the strongest predictors of ICU mortality.",
    )
    show(query("""
        SELECT
            gcs_min,
            COUNT(*) AS n_stays,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM mimiciv_derived.first_day_gcs
        WHERE gcs_min IS NOT NULL
        GROUP BY gcs_min
        ORDER BY gcs_min
    """, conn))

    # ------------------------------------------------------------------
    # 7. Organ Failure — AKI (KDIGO)
    # ------------------------------------------------------------------
    section(
        "7. Acute Kidney Injury — KDIGO Staging",
        "The KDIGO (Kidney Disease: Improving Global Outcomes) criteria define "
        "AKI stages 1–3 based on creatinine rise relative to baseline and urine "
        "output. MIMIC-IV provides three building blocks: kdigo_uo (hourly urine "
        "output windows), kdigo_creatinine (rolling creatinine comparisons), and "
        "kdigo_stages (the final combined stage per hour). AKI is the most common "
        "ICU complication and a major mortality driver.",
    )

    subsection(
        "kdigo_stages — hourly AKI stage per ICU stay",
        "aki_stage_creat: 0–3 from creatinine alone. aki_stage_uo: 0–3 from "
        "urine output alone. aki_stage: combined worst of the two.",
    )
    show(query("""
        SELECT stay_id, charttime,
               aki_stage_creat,
               aki_stage_uo,
               aki_stage
        FROM mimiciv_derived.kdigo_stages
        WHERE aki_stage > 0
        ORDER BY stay_id, charttime
        LIMIT 10
    """, conn))

    subsection(
        "Peak AKI stage per ICU stay",
        "How many ICU stays develop each severity of AKI? Stage 3 (severe) "
        "often requires renal replacement therapy.",
    )
    show(query("""
        SELECT aki_stage AS peak_aki_stage, COUNT(*) AS n_stays
        FROM (
            SELECT stay_id, MAX(aki_stage) AS aki_stage
            FROM mimiciv_derived.kdigo_stages
            GROUP BY stay_id
        ) t
        GROUP BY aki_stage
        ORDER BY aki_stage
    """, conn))

    # ------------------------------------------------------------------
    # 8. Medications — Vasopressors
    # ------------------------------------------------------------------
    section(
        "8. Vasopressor and Vasoplegic Agents",
        "vasoactive_agent tracks all vasoactive drug infusions by stay and hour. "
        "norepinephrine_equivalent_dose converts all vasopressor rates to a "
        "common 'norepinephrine equivalent' (NEQ) dose in mcg/kg/min, allowing "
        "consistent quantification of vasopressor burden across drugs. This is "
        "the standard approach for vasopressor weaning studies and septic shock "
        "analyses.",
    )

    subsection(
        "vasoactive_agent — hourly infusion rates by drug",
        "Each row is one ICU stay hour. Columns exist for norepinephrine, "
        "epinephrine, dopamine, phenylephrine, vasopressin, dobutamine, "
        "and milrinone rates.",
    )
    show(query("""
        SELECT stay_id, starttime, endtime,
               norepinephrine, epinephrine, dopamine,
               phenylephrine, vasopressin,
               dobutamine, milrinone
        FROM mimiciv_derived.vasoactive_agent
        WHERE norepinephrine IS NOT NULL OR epinephrine IS NOT NULL
        ORDER BY stay_id, starttime
        LIMIT 10
    """, conn))

    subsection(
        "norepinephrine_equivalent_dose — combined vasopressor burden",
        "NEQ > 0.25 mcg/kg/min is the standard refractory septic shock "
        "threshold. Distribution shows how often patients cross this threshold.",
    )
    show(query("""
        SELECT
            CASE
                WHEN norepinephrine_equivalent_dose < 0.1  THEN '< 0.10'
                WHEN norepinephrine_equivalent_dose < 0.25 THEN '0.10–0.25'
                WHEN norepinephrine_equivalent_dose < 0.5  THEN '0.25–0.50 (high)'
                ELSE                                            '≥ 0.50   (very high / refractory)'
            END AS neq_bucket,
            COUNT(*) AS n_hours
        FROM mimiciv_derived.norepinephrine_equivalent_dose
        WHERE norepinephrine_equivalent_dose IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """, conn))

    # ------------------------------------------------------------------
    # 9. Antibiotics
    # ------------------------------------------------------------------
    section(
        "9. Antibiotics",
        "antibiotic flags each prescription/infusion row with the antibiotic "
        "class (beta-lactam, carbapenem, fluoroquinolone, etc.) and route. "
        "It is used by suspicion_of_infection (and therefore sepsis3) to "
        "identify infection episodes. It is also used directly to study "
        "antibiotic stewardship, broad-spectrum usage, and de-escalation.",
    )

    subsection(
        "antibiotic — one row per antibiotic order",
        "Columns: stay_id, starttime, stoptime, antibiotic, route, "
        "antibiotic_type (class).",
    )
    show(query("""
        SELECT stay_id, starttime, stoptime,
               antibiotic, route
        FROM mimiciv_derived.antibiotic
        ORDER BY stay_id, starttime
        LIMIT 10
    """, conn))

    subsection(
        "Most frequently administered antibiotics",
    )
    show(query("""
        SELECT antibiotic, COUNT(*) AS n_orders
        FROM mimiciv_derived.antibiotic
        GROUP BY antibiotic
        ORDER BY n_orders DESC
        LIMIT 15
    """, conn))

    # ------------------------------------------------------------------
    # 10. Comorbidities — Charlson Index
    # ------------------------------------------------------------------
    section(
        "10. Charlson Comorbidity Index",
        "charlson assigns a weighted comorbidity score per hospital admission "
        "by scanning ICD-9/10 diagnosis codes. Each comorbidity category "
        "(myocardial infarction, CHF, COPD, diabetes, renal disease, cancer, "
        "etc.) contributes a weight of 1–6. The total score predicts 10-year "
        "mortality risk and is widely used as a covariate to control for "
        "baseline health in outcome studies.",
    )

    subsection(
        "charlson — one row per hospital admission",
        "Columns: one binary flag per comorbidity plus charlson_comorbidity_index.",
    )
    show(query("""
        SELECT hadm_id,
               myocardial_infarct,
               congestive_heart_failure,
               chronic_pulmonary_disease,
               diabetes_without_cc,
               diabetes_with_cc,
               renal_disease,
               malignant_cancer,
               charlson_comorbidity_index AS cci
        FROM mimiciv_derived.charlson
        ORDER BY cci DESC
        LIMIT 10
    """, conn))

    subsection(
        "CCI score distribution",
        "Most ICU patients have significant comorbidity burden (CCI ≥ 2). "
        "This distribution shows how severe the comorbidity load is across "
        "the cohort.",
    )
    show(query("""
        SELECT charlson_comorbidity_index AS cci,
               COUNT(*) AS n_admissions
        FROM mimiciv_derived.charlson
        GROUP BY cci
        ORDER BY cci
    """, conn))

    # ------------------------------------------------------------------
    # 11. First-Day Summaries
    # ------------------------------------------------------------------
    section(
        "11. First-Day Summary Tables",
        "first_day_* tables aggregate the worst (or most relevant) value of "
        "each measurement during the first 24 hours of an ICU stay. These are "
        "the building blocks of admission-severity models: a single join across "
        "first_day_vitalsign, first_day_lab, first_day_gcs, first_day_sofa, "
        "first_day_urine_output produces a feature matrix ready for logistic "
        "regression or machine learning.",
    )

    subsection(
        "first_day_vitalsign — worst vital signs in first 24 h",
        "Columns: heart_rate_min/max, sbp_min/max, dbp_min/max, mbp_min, "
        "resp_rate_min/max, temperature_min/max, spo2_min.",
    )
    show(query("""
        SELECT stay_id,
               heart_rate_min, heart_rate_max,
               sbp_min, sbp_max,
               mbp_min,
               resp_rate_min, resp_rate_max,
               spo2_min,
               temperature_min
        FROM mimiciv_derived.first_day_vitalsign
        ORDER BY mbp_min NULLS LAST
        LIMIT 10
    """, conn))

    subsection(
        "first_day_lab — worst lab values in first 24 h",
        "Columns: creatinine_max, bun_max, potassium_min/max, sodium_min/max, "
        "wbc_min/max, hemoglobin_min, platelet_min, inr_max, pt_max.",
    )
    show(query("""
        SELECT stay_id,
               creatinine_max,
               bun_max,
               wbc_min, wbc_max,
               hemoglobin_min,
               platelet_min,
               inr_max
        FROM mimiciv_derived.first_day_lab
        ORDER BY creatinine_max DESC NULLS LAST
        LIMIT 10
    """, conn))

    subsection(
        "first_day_sofa — admission SOFA component scores",
        "The 24-hour SOFA provides a compact summary of multi-organ dysfunction "
        "at ICU admission. Used directly by sepsis3.",
    )
    show(query("""
        SELECT stay_id,
               sofa_24hours AS sofa_24h,
               respiration_24hours  AS resp,
               coagulation_24hours  AS coag,
               liver_24hours        AS liver,
               cardiovascular_24hours AS cardio,
               cns_24hours          AS cns,
               renal_24hours        AS renal
        FROM mimiciv_derived.first_day_sofa
        ORDER BY sofa_24hours DESC NULLS LAST
        LIMIT 10
    """, conn))

    # ------------------------------------------------------------------
    # 12. Putting it all together — a typical research cohort
    # ------------------------------------------------------------------
    section(
        "12. Putting It All Together — Septic Shock Cohort Example",
        "This query assembles a flat feature table for a septic shock analysis: "
        "patients meeting Sepsis-3 criteria who received vasopressors. For each "
        "ICU stay it collects admission severity, demographics, comorbidities, "
        "and first-day organ dysfunction — the typical starting point for a "
        "prognostic model. With row-limited data the cohort may be small, "
        "but the join pattern scales to the full dataset.",
    )
    show(query("""
        SELECT
            s3.stay_id,
            d.gender,
            ROUND(d.admission_age::numeric, 0)       AS age,
            ch.charlson_comorbidity_index            AS cci,
            sp.sapsii,
            ROUND(sp.sapsii_prob::numeric, 3)        AS sapsii_mort_prob,
            fs.sofa_24hours                          AS sofa_24h,
            fv.mbp_min                               AS min_map_day1,
            fl.creatinine_max                        AS max_creat_day1,
            ROUND(ne.norepinephrine_equivalent_dose::numeric, 3) AS peak_neq,
            d.hospital_expire_flag                   AS died_in_hospital
        FROM mimiciv_derived.sepsis3        s3
        JOIN mimiciv_derived.icustay_detail d  USING (stay_id)
        JOIN mimiciv_derived.charlson       ch USING (hadm_id)
        JOIN mimiciv_derived.sapsii         sp USING (stay_id)
        JOIN mimiciv_derived.first_day_sofa fs USING (stay_id)
        JOIN mimiciv_derived.first_day_vitalsign fv USING (stay_id)
        JOIN mimiciv_derived.first_day_lab  fl USING (stay_id)
        JOIN (
            SELECT stay_id, MAX(norepinephrine_equivalent_dose) AS norepinephrine_equivalent_dose
            FROM mimiciv_derived.norepinephrine_equivalent_dose
            GROUP BY stay_id
        ) ne USING (stay_id)
        ORDER BY sapsii DESC
        LIMIT 15
    """, conn))

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    print()
    print("═" * 70)
    print("  Showcase complete.")
    print("  Next steps:")
    print("    - Open concepts.ipynb for an interactive version of these queries")
    print("    - Run uv run python test_mimic.py to validate data integrity")
    print("═" * 70)

    conn.close()


if __name__ == "__main__":
    main()
