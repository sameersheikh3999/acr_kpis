"""FastAPI backend for ACR-KPIs dashboard: serves teacher and sector data from BigQuery."""

import base64
import json
import math
import os
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from google.cloud import bigquery
from google.oauth2 import service_account

# Single port for API and (when built) dashboard
PORT = int(os.getenv("PORT", "8000"))

# BigQuery table: now fico_kpis. Query: SELECT * FROM `tbproddb.fico_kpis`
# Override with env ACR_DATA_TABLE if needed.
ACR_DATA_TABLE = os.getenv("ACR_DATA_TABLE", "tbproddb.fico_kpis").strip()

# Student marks / results; joined to teachers via user_id (same as fico_kpis). Override with STUDENT_RESULTS_TABLE.
STUDENT_RESULTS_TABLE = os.getenv("STUDENT_RESULTS_TABLE", "tbproddb.student_results_data").strip()

# Column name variants for tbproddb.student_results_data (SELECT *).
STUDENT_RESULTS_ALIASES = {
    "user_id": [
        "user_id",
        "User_ID",
        "userId",
        "userid",
        "teacher_id",
        "Teacher_ID",
        "teacher_user_id",
        "Teacher_User_ID",
        "fk_user_id",
        "FK_User_ID",
    ],
    "session_year": [
        "session_year",
        "Session_Year",
        "academic_year",
        "Academic_Year",
        "year",
        "Year",
        "session",
        "Session",
    ],
    "term": ["term", "Term", "exam_term", "Exam_Term", "examination_term", "Examination_Term", "term_name", "Term_Name"],
    "grade": ["grade", "Grade", "class", "Class", "class_grade", "Class_Grade"],
    "subject": ["subject", "Subject", "subject_name", "Subject_Name", "course", "Course"],
    "student_name": [
        "student_name",
        "Student_Name",
        "student_full_name",
        "Student_Full_Name",
        "pupil_name",
    ],
    "student_id": [
        "student_id",
        "Student_ID",
        "roll_no",
        "Roll_No",
        "roll_number",
        "Roll_Number",
        "student_code",
        "Student_Code",
        "admission_no",
        "Admission_No",
    ],
    "obtained_marks": [
        "obtained_marks",
        "Obtained_Marks",
        "obtainedMarks",
        "ObtainedMarks",
        "marks_obtained",
        "Marks_Obtained",
        "score_obtained",
        "Score_Obtained",
        "obtained",
        "Obtained",
        "student_marks",
        "Student_Marks",
        "subject_marks",
        "Subject_Marks",
        "exam_marks",
        "Exam_Marks",
        "written_marks",
        "Written_Marks",
        # Generic "marks" last: some tables use it for remarks/non-numeric; prefer specific columns above.
        "marks",
        "Marks",
    ],
    "total_marks": [
        "total_marks",
        "Total_Marks",
        "totalMarks",
        "TotalMarks",
        "max_marks",
        "Max_Marks",
        "full_marks",
        "Full_Marks",
        "out_of_marks",
        "Out_Of_Marks",
        "total",
        "Total",
        "max_score",
        "Max_Score",
        "maximum_marks",
        "Maximum_Marks",
        "full_marks_total",
        "Full_Marks_Total",
    ],
    "marks": [
        "marks",
        "mark",
        "score",
        "Score",
        "combined_marks",
        "Combined_Marks",
    ],
    "result": ["result", "Result", "pass_fail", "Pass_Fail", "status", "Status", "grade_result", "Grade_Result"],
    "student_grades": [
        "student_grades",
        "Student_Grades",
        "student_grade",
        "Student_Grade",
        "letter_grade",
        "Letter_Grade",
        "grade_letter",
        "Grade_Letter",
    ],
    # Raw % from table (0–100); used when obtained/total are missing or to override computed %.
    "percentage": [
        "percentage",
        "Percentage",
        "percent",
        "Percent",
        "pct",
        "Pct",
        "marks_percent",
        "Marks_Percent",
        "marks_percentage",
        "Marks_Percentage",
        "score_percentage",
        "Score_Percentage",
        "marksPercentage",
        "MarksPercentage",
    ],
}

# Resolved BigQuery field name for linking teachers (from INFORMATION_SCHEMA / get_table).
_sr_uid_sql_column: str | None = None
_sr_student_id_sql_column: str | None = None

app = FastAPI(title="ACR-KPIs Performance Dashboard API", version="1.0.0", redirect_slashes=False)


@app.on_event("startup")
def _log_credential_env():
    """Log which credential-related env vars are set (names only) for Railway debugging."""
    creds_json = _get_creds_json_from_env()
    if creds_json:
        print("[ACR API] BigQuery credentials found in env (len=%d)" % len(creds_json))
    else:
        related = [k for k in os.environ if "GOOGLE" in k.upper() or "CREDENTIAL" in k.upper() or "KEYY" in k or "keyy" in k.lower()]
        print("[ACR API] Startup: no credential env. Names that might be relevant: %s" % (related or "(none)"))
    sr_paths = sorted(
        {
            getattr(r, "path", "")
            for r in app.routes
            if hasattr(r, "path")
            and ("student" in getattr(r, "path", "") or "teacher-student" in getattr(r, "path", ""))
        }
    )
    if sr_paths:
        print("[ACR API] Student-results routes registered: %s" % ", ".join(sr_paths))


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# fico_kpis section columns (order matches dashboard sections)
KPI_FIELDS = [
    "planning_and_preparation",
    "subject_knowledge",
    "classroom_management",
    "communication_skills",
    "professional_development",
    "use_of_technology",
]

# Column name variants for tbproddb.fico_kpis (add aliases here if your column names differ).
COLUMN_ALIASES = {
    "user_id": ["user_id", "User_ID", "userId", "userid", "teacher_id", "Teacher_ID"],
    "sector": ["Sector", "sector", "SECTOR"],
    "overall_percentage": ["overall_percentage", "overall_percent", "Overall_Percentage", "OverallPercent", "score_percentage", "avg_score", "kpi_score", "percentage"],
    "created_date": ["created_date", "Created_Date", "observation_date", "Observation_Date", "date", "Date", "assessment_date", "eval_date", "record_date"],
    "total_score_out_of_52": ["total_score_out_of_52", "total_score", "Total_Score_Out_Of_52", "score_out_of_52", "score", "max_score"],
    "total_score_out_of_60": ["total_score_out_of_60", "Total_Score_Out_Of_60", "score_out_of_60", "score_60"],
    "date_of_birth": ["date_of_birth", "Date_Of_Birth", "dob", "DOB"],
    "joining_date": ["joining_date", "Joining_Date", "join_date", "hire_date"],
    "qualifications": ["qualifications", "Qualifications", "qualification", "Qualification", "education"],
    "service_designation": ["service_designation", "Service_Designation", "designation", "Designation", "role", "title"],
    "gender": ["gender", "Gender", "GENDER"],
    "subject": ["subject", "Subject", "subject_name", "Subject_Name", "course", "Course", "subject_name_ur", "subject_name_en", "teaching_subject"],
    "EMIS": ["EMIS", "emis", "Emis", "School_EMIS", "school_emis", "EMIS_Code", "emis_code", "school_emis_code"],
    # fico_kpis KPI section columns (Planning_and_Preparation, etc.)
    "planning_and_preparation": ["planning_and_preparation", "Planning_and_Preparation", "Planning_And_Preparation"],
    "subject_knowledge": ["subject_knowledge", "Subject_Knowledge"],
    "classroom_management": ["classroom_management", "Classroom_Management", "Classroom_Manag"],
    "communication_skills": ["communication_skills", "Communication_Skills", "Communication_Skills"],
    "professional_development": ["professional_development", "Professional_Development", "Professional_Development"],
    "use_of_technology": ["use_of_technology", "Use_of_Technology", "Use_Of_Technology"],
}


def _bq_row_to_dict(row) -> dict:
    """Convert BigQuery Row to a plain dict with string keys (Row keys can be non-string in some clients)."""
    if hasattr(row, "keys") and hasattr(row, "__iter__"):
        return {str(k): row[k] for k in row.keys()}
    return dict(row)


def _json_safe_value(v):
    """Make a value JSON-serializable for the frontend."""
    if v is None:
        return v
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v)
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return {str(k): _json_safe_value(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_safe_value(x) for x in v]
    return v


def _get_from_row(row: dict, canonical_key: str):
    """Get value from row by canonical key, trying all known column name variants."""
    for alias in COLUMN_ALIASES.get(canonical_key, [canonical_key]):
        v = row.get(alias)
        if v is not None:
            return v
    return row.get(canonical_key)


def _sr_enrich_row_dict(d: dict) -> dict:
    """Lowercase keys on a BigQuery row dict so student_results aliases resolve."""
    out = dict(d)
    for k, v in list(d.items()):
        if isinstance(k, str):
            lk = k.lower().strip()
            if lk:
                out.setdefault(lk, v)
    return out


def _sr_value_present(v) -> bool:
    """True if v is usable (not null/blank/NaN)."""
    if v is None:
        return False
    if isinstance(v, str) and not str(v).strip():
        return False
    if isinstance(v, float) and math.isnan(v):
        return False
    return True


def _sr_logical_key_norm(name: str) -> str:
    """Compare column names ignoring case, underscores vs spaces (e.g. total_marks vs total marks)."""
    return "".join(str(name).lower().split()).replace("_", "")


def _sr_row_get_ci(row: dict, logical_name: str):
    """First usable value for a column: exact case-insensitive match, then normalized key match."""
    want_exact = logical_name.lower().strip()
    want_norm = _sr_logical_key_norm(logical_name)
    if not want_norm:
        return None
    for k, v in row.items():
        if not isinstance(k, str):
            continue
        if k.lower().strip() == want_exact and _sr_value_present(v):
            return v
    for k, v in row.items():
        if not isinstance(k, str):
            continue
        if _sr_logical_key_norm(k) == want_norm and _sr_value_present(v):
            return v
    return None


def _sr_get_from_row(row: dict, canonical_key: str):
    for alias in STUDENT_RESULTS_ALIASES.get(canonical_key, [canonical_key]):
        v = row.get(alias)
        if v is None and isinstance(alias, str):
            v = row.get(alias.lower().strip())
        if _sr_value_present(v):
            return v
    v = row.get(canonical_key)
    if _sr_value_present(v):
        return v
    return None


CLASS_GPA_WEIGHTS: dict[str, int] = {"A1": 6, "A": 5, "B": 4, "C": 3, "D": 2, "E": 1, "F": 0}


def _letter_grade_key_from_raw(raw) -> str | None:
    """Map a cell value to A1, A, B, C, D, E, or F for class GPA (A1 before A)."""
    if raw is None:
        return None
    s = "".join(str(raw).strip().upper().split())
    if not s:
        return None
    if s.startswith("A1"):
        return "A1"
    if s.startswith("A"):
        return "A"
    head = s[0]
    if head in CLASS_GPA_WEIGHTS and head != "A":
        return head
    return None


def _class_gpa_weight_from_student_row(row: dict) -> int | None:
    raw = _sr_get_from_row(row, "student_grades")
    key = _letter_grade_key_from_raw(raw)
    if key is None:
        return None
    return CLASS_GPA_WEIGHTS.get(key)


def _compute_class_gpa_for_rows(rows: list[dict]) -> tuple[float | None, int]:
    """Class GPA = Σ(count per letter grade × weight) / total students appeared.

    Same as sum of each student's grade weight (0 if unmapped / no letter grade)
    divided by len(rows). Students without a mappable A1–F grade count in the
    denominator with weight 0. Returns (gpa, count with a mappable grade) for UI.
    """
    rows = rows or []
    n_total = len(rows)
    if n_total == 0:
        return None, 0
    total_w = 0.0
    n_graded = 0
    for r in rows:
        w = _class_gpa_weight_from_student_row(r)
        if w is not None:
            total_w += float(w)
            n_graded += 1
    if n_graded == 0:
        return None, 0
    return round(total_w / n_total, 2), n_graded


def _normalize_student_id(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def _student_id_from_row(row: dict) -> str:
    sid = _sr_get_from_row(row, "student_id")
    return _normalize_student_id(sid)


def _normalize_teacher_uid_for_join(uid) -> str:
    """Same rules as dashboard teacher cards for joining fico_kpis.user_id ↔ student_results.user_id."""
    if uid is None or (isinstance(uid, str) and not str(uid).strip()):
        return "__unknown__"
    if isinstance(uid, (int, float)):
        return str(int(uid))
    s = str(uid).strip()
    if not s:
        return "__unknown__"
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


def _normalize_term_display(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    sl = " ".join(s.lower().replace("_", " ").split())
    if sl in (
        "1",
        "first",
        "i",
        "term 1",
        "term1",
        "1st",
        "first assessment",
        "1st assessment",
    ) or sl.startswith("first assessment"):
        return "First Assessment"
    if sl in (
        "2",
        "second",
        "ii",
        "term 2",
        "term2",
        "2nd",
        "second assessment",
        "2nd assessment",
    ) or sl.startswith("second assessment"):
        return "Second Assessment"
    if sl in (
        "3",
        "third",
        "iii",
        "term 3",
        "term3",
        "3rd",
        "third assessment",
        "3rd assessment",
    ) or sl.startswith("third assessment"):
        return "Third Assessment"
    if sl in (
        "4",
        "final",
        "iv",
        "annual",
        "term 4",
        "term4",
        "4th",
        "fourth",
        "final assessment",
        "4th assessment",
    ) or sl.startswith("final assessment"):
        return "Final Assessment"
    return s


def _sr_to_float_marks(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (int, float)):
        x = float(v)
        if math.isnan(x):
            return None
        return x
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        x = float(s)
        if math.isnan(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _bq_struct_to_dict(v) -> dict | None:
    """If v is a BigQuery STRUCT/Row or plain dict, return a string-keyed dict; else None."""
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    if isinstance(v, (str, bytes, int, float, bool, Decimal)):
        return None
    if hasattr(v, "keys") and hasattr(v, "__getitem__"):
        try:
            ks = list(v.keys())
            if not ks:
                return None
            return {str(k): v[k] for k in ks}
        except (TypeError, KeyError, ValueError):
            return None
    return None


def _sr_coerce_obtained_total_pct_from_cell(v) -> tuple[float | None, float | None, float | None]:
    """
    Parse one table cell: plain number/string, '42/50', or STRUCT with common field names.
    Returns (obtained, total, percentage).
    """
    if v is None:
        return None, None, None
    d = _bq_struct_to_dict(v)
    if d is not None:
        o = _sr_to_float_marks(
            d.get("obtained_marks")
            or d.get("marks_obtained")
            or d.get("obtained")
            or d.get("marks")
            or d.get("mark")
            or d.get("score")
        )
        t = _sr_to_float_marks(
            d.get("total_marks")
            or d.get("total_mark")
            or d.get("total")
            or d.get("max_marks")
            or d.get("out_of")
            or d.get("full_marks")
        )
        p = _sr_parse_percentage_cell(d.get("percentage") or d.get("percent") or d.get("pct"))
        return o, t, p
    if isinstance(v, (list, tuple)) and len(v) == 1:
        return _sr_coerce_obtained_total_pct_from_cell(v[0])
    if isinstance(v, str) and "/" in v:
        po, pt = _parse_obtained_total_from_combined_marks(v)
        return po, pt, None
    x = _sr_to_float_marks(v)
    return x, None, None


def _sr_parse_percentage_cell(v) -> float | None:
    """Parse percentage column: 72, 72.5, '72%', '72.5 %'."""
    if not _sr_value_present(v):
        return None
    if isinstance(v, (int, float, Decimal)):
        x = float(v)
        if isinstance(v, float) and math.isnan(x):
            return None
        return x
    s = str(v).strip().replace(",", "")
    if s.endswith("%"):
        s = s[:-1].strip()
    return _sr_to_float_marks(s)


def _sr_standard_marks_from_row(row: dict) -> tuple[float | None, float | None, float | None]:
    """
    Read tbproddb.student_results_data-style columns marks, total_marks, percentage
    with case-insensitive keys (direct names, not only alias lists).
    Supports BigQuery STRUCT/ROW cells and singular column names (mark, total_mark).
    """
    m = _sr_row_get_ci(row, "marks") or _sr_row_get_ci(row, "mark")
    tm = _sr_row_get_ci(row, "total_marks") or _sr_row_get_ci(row, "total_mark")
    pc = _sr_row_get_ci(row, "percentage") or _sr_row_get_ci(row, "percent")
    obtained: float | None = None
    total: float | None = None
    pct: float | None = None
    if m is not None:
        mo, mt, mp = _sr_coerce_obtained_total_pct_from_cell(m)
        obtained = mo
        total = mt
        pct = mp
    if tm is not None:
        _, tt, tp = _sr_coerce_obtained_total_pct_from_cell(tm)
        if tt is not None:
            total = tt
        if tp is not None and pct is None:
            pct = tp
    if pc is not None:
        _, _, pp = _sr_coerce_obtained_total_pct_from_cell(pc)
        if pp is not None:
            pct = pp
        elif pct is None:
            pct = _sr_parse_percentage_cell(pc)
    return obtained, total, pct


def _parse_obtained_total_from_combined_marks(raw) -> tuple[float | None, float | None]:
    """If marks is a single field like '42/50' or '42 / 50', return (obtained, total)."""
    if raw is None:
        return None, None
    s = str(raw).strip().replace(",", "")
    if not s:
        return None, None
    if "/" in s:
        parts = s.split("/", 1)
        return _sr_to_float_marks(parts[0]), _sr_to_float_marks(parts[1])
    return None, None


def _pretty_mark_num(n: float) -> str:
    if n == int(n):
        return str(int(n))
    return str(round(n, 2)).rstrip("0").rstrip(".")


# Student results are exposed on a fixed 0–100 scale (total marks always 100).
STUDENT_MARKS_TOTAL_FIXED = 100.0


def _normalize_student_marks_to_scale(
    obtained_raw: float | None, total_raw: float | None
) -> tuple[float | None, float | None, float | None]:
    """Map row marks to obtained / total=100 / percentage (percentage equals obtained on this scale)."""
    if obtained_raw is None:
        return None, None, None
    if total_raw is not None and total_raw > 0:
        on_hundred = round((obtained_raw / total_raw) * STUDENT_MARKS_TOTAL_FIXED, 1)
    else:
        on_hundred = round(obtained_raw, 1)
    total_fixed = STUDENT_MARKS_TOTAL_FIXED
    pct = round((on_hundred / total_fixed) * 100, 1)
    return on_hundred, total_fixed, pct


def _sr_supplement_marks_from_row(
    row: dict,
    obtained: float | None,
    total: float | None,
    raw_pct: float | None,
) -> tuple[float | None, float | None, float | None]:
    """Scan row for mark-like columns when canonical aliases miss (BQ naming drift)."""
    obts: list[tuple[int, int, float]] = []
    tots: list[tuple[int, int, float]] = []
    pcts: list[tuple[int, int, float]] = []
    for k, v in row.items():
        if not isinstance(k, str):
            continue
        if v is None:
            continue
        if isinstance(v, str) and not str(v).strip():
            continue
        lk = k.lower().strip().replace(" ", "_").replace("-", "_")
        if "remark" in lk:
            continue
        sval = str(v).strip()
        if "/" in sval:
            po, pt = _parse_obtained_total_from_combined_marks(v)
            if po is not None:
                obts.append((55, -len(k), po))
            if pt is not None:
                tots.append((55, -len(k), pt))
            continue
        fv = _sr_to_float_marks(v)
        if fv is None:
            co, ct, cp = _sr_coerce_obtained_total_pct_from_cell(v)
            if co is not None:
                obts.append((60, -len(k), co))
            if ct is not None:
                tots.append((60, -len(k), ct))
            if cp is not None:
                pcts.append((86, -len(k), cp))
            continue
        if "percent" in lk or lk in ("pct", "perc"):
            pcts.append((85, -len(k), fv))
        elif (("total" in lk or "max" in lk or "full" in lk) and ("mark" in lk or "score" in lk)) or lk in (
            "totalmarks",
            "totalmark",
            "maxmarks",
            "fullmarks",
            "out_of",
        ):
            tots.append((75, -len(k), fv))
        elif lk in (
            "marks",
            "mark",
            "obtained",
            "points",
            "raw_marks",
            "student_marks",
            "subject_marks",
            "exam_marks",
            "written_marks",
            "marks_obtained",
            "obtained_marks",
            "obtainedmarks",
            "mark_obtained",
        ) or ("obtain" in lk and "mark" in lk):
            obts.append((70, -len(k), fv))
        elif lk.endswith("_marks") and "total" not in lk and "percent" not in lk:
            obts.append((45, -len(k), fv))

    def _pick(xs: list[tuple[int, int, float]]) -> float | None:
        if not xs:
            return None
        xs.sort(key=lambda z: (-z[0], z[1]))
        return xs[0][2]

    o = obtained if obtained is not None else _pick(obts)
    t = total if total is not None else _pick(tots)
    p = raw_pct if raw_pct is not None else _pick(pcts)
    return o, t, p


def _student_results_group_key(row: dict) -> tuple[str, str, str, str, str]:
    uid = _normalize_teacher_uid_for_join(_sr_get_from_row(row, "user_id"))
    sy = str(_sr_get_from_row(row, "session_year") or "").strip()
    term = _normalize_term_display(_sr_get_from_row(row, "term"))
    grade = str(_sr_get_from_row(row, "grade") or "").strip()
    subj = str(_sr_get_from_row(row, "subject") or "").strip()
    return uid, sy, term, grade, subj


def _row_to_student_result_payload(row: dict) -> dict:
    name = _sr_get_from_row(row, "student_name")
    student_id = _student_id_from_row(row)
    std_obt, std_tot, std_pct = _sr_standard_marks_from_row(row)
    marks_combined = _sr_get_from_row(row, "marks")
    if marks_combined is None and std_obt is not None:
        marks_combined = _sr_row_get_ci(row, "marks")
    result = _sr_get_from_row(row, "result")
    om_raw = _sr_get_from_row(row, "obtained_marks")
    tm_raw = _sr_get_from_row(row, "total_marks")
    obtained = _sr_to_float_marks(om_raw)
    total = _sr_to_float_marks(tm_raw)
    if om_raw is not None:
        co, ct, _ = _sr_coerce_obtained_total_pct_from_cell(om_raw)
        if obtained is None and co is not None:
            obtained = co
        if total is None and ct is not None:
            total = ct
    if tm_raw is not None:
        co2, ct2, _ = _sr_coerce_obtained_total_pct_from_cell(tm_raw)
        if obtained is None and co2 is not None:
            obtained = co2
        if total is None and ct2 is not None:
            total = ct2
    if obtained is None and std_obt is not None:
        obtained = std_obt
    if total is None and std_tot is not None:
        total = std_tot
    if obtained is None and total is None and marks_combined is not None:
        po, pt, _pmc = _sr_coerce_obtained_total_pct_from_cell(marks_combined)
        if po is not None:
            obtained = po
        if pt is not None:
            total = pt
        if obtained is None and total is None:
            po2, pt2 = _parse_obtained_total_from_combined_marks(marks_combined)
            if po2 is not None:
                obtained = po2
            if pt2 is not None:
                total = pt2
        if obtained is None and total is None:
            only = _sr_to_float_marks(marks_combined)
            if only is not None:
                obtained = only

    pc_raw = _sr_get_from_row(row, "percentage")
    raw_pct = _sr_parse_percentage_cell(pc_raw)
    if raw_pct is None and pc_raw is not None:
        _, _, rp = _sr_coerce_obtained_total_pct_from_cell(pc_raw)
        if rp is not None:
            raw_pct = rp
    if raw_pct is None and std_pct is not None:
        raw_pct = std_pct
    obtained, total, raw_pct = _sr_supplement_marks_from_row(row, obtained, total, raw_pct)

    obt_100, tot_fixed, marks_pct = _normalize_student_marks_to_scale(obtained, total)
    if raw_pct is not None:
        raw_pct = min(100.0, max(0.0, raw_pct))
    if obt_100 is None and tot_fixed is None and marks_pct is None and raw_pct is not None:
        marks_pct = round(raw_pct, 1)
        obt_100 = round((raw_pct / 100.0) * STUDENT_MARKS_TOTAL_FIXED, 1)
        tot_fixed = STUDENT_MARKS_TOTAL_FIXED
    elif marks_pct is None and raw_pct is not None:
        marks_pct = round(raw_pct, 1)
        if obt_100 is None:
            obt_100 = round((raw_pct / 100.0) * STUDENT_MARKS_TOTAL_FIXED, 1)
        if tot_fixed is None:
            tot_fixed = STUDENT_MARKS_TOTAL_FIXED

    marks_display = marks_combined
    if obt_100 is not None:
        marks_display = _pretty_mark_num(obt_100) + "/" + _pretty_mark_num(tot_fixed)

    letter_grade = _sr_get_from_row(row, "student_grades")
    out = {
        "student_id": student_id,
        "student_name": str(name).strip() if name is not None else "",
        "marks": _json_safe_value(marks_display if marks_display is not None else marks_combined),
        "obtained_marks": obt_100,
        "total_marks": tot_fixed,
        "marks_percentage": marks_pct,
        "result": str(result).strip() if result is not None else "",
        "student_grade": str(letter_grade).strip() if letter_grade is not None else "",
    }
    used_lower = set()
    for c in (
        "user_id",
        "session_year",
        "term",
        "grade",
        "subject",
        "student_id",
        "student_name",
        "marks",
        "obtained_marks",
        "total_marks",
        "percentage",
        "result",
        "student_grades",
    ):
        for a in STUDENT_RESULTS_ALIASES.get(c, [c]):
            used_lower.add(str(a).lower())
    extras = {}
    for k, v in row.items():
        if not isinstance(k, str):
            continue
        if k.lower() in used_lower:
            continue
        if v is None or (isinstance(v, str) and not str(v).strip()):
            continue
        extras[k] = _json_safe_value(v)
    if extras:
        out["extra"] = extras
    return out


def _term_sort_order(term: str) -> int:
    t = str(term or "").strip().lower()
    return {
        "first assessment": 0,
        "second assessment": 1,
        "third assessment": 2,
        "final assessment": 3,
        "first": 0,
        "second": 1,
        "third": 2,
        "final": 3,
    }.get(t, 50)


def _aggregate_student_result_summaries(rows: list[dict]) -> dict[str, list[dict]]:
    """user_id -> [{ session_year, term, grade, subject, student_count, class_gpa, ... }]."""
    buckets: dict[tuple[str, str, str, str, str], list[dict]] = defaultdict(list)
    for row in rows:
        uid, sy, term, grade, subj = _student_results_group_key(row)
        if uid == "__unknown__":
            continue
        buckets[(uid, sy, term, grade, subj)].append(row)
    by_uid: dict[str, list[dict]] = defaultdict(list)
    for (uid, sy, term, grade, subj), lst in buckets.items():
        gpa, gpa_n = _compute_class_gpa_for_rows(lst)
        by_uid[uid].append({
            "session_year": sy,
            "term": term,
            "grade": grade,
            "subject": subj,
            "student_count": len(lst),
            "class_gpa": gpa,
            "class_gpa_students_count": gpa_n,
        })
    for uid, groups in by_uid.items():
        groups.sort(key=lambda g: (g["session_year"], _term_sort_order(g["term"]), g["grade"], g["subject"]))
    return dict(by_uid)


def _groups_with_students_for_teacher(rows: list[dict]) -> list[dict]:
    """Build grouped list with students for one teacher (rows already filtered by user_id)."""
    buckets: dict[tuple[str, str, str, str], list[dict]] = defaultdict(list)
    for row in rows:
        _, sy, term, grade, subj = _student_results_group_key(row)
        buckets[(sy, term, grade, subj)].append(row)
    groups = []
    for (sy, term, grade, subj), lst in buckets.items():
        gpa, gpa_n = _compute_class_gpa_for_rows(lst)
        groups.append({
            "session_year": sy,
            "term": term,
            "grade": grade,
            "subject": subj,
            "student_count": len(lst),
            "class_gpa": gpa,
            "class_gpa_students_count": gpa_n,
            "students": [_row_to_student_result_payload(r) for r in lst],
        })
    groups.sort(key=lambda g: (g["session_year"], _term_sort_order(g["term"]), g["grade"], g["subject"]))
    return groups


def _full_student_groups_by_teacher_uid(rows: list[dict]) -> dict[str, list[dict]]:
    """One copy of full session/term/grade/subject + students per teacher uid (same scan as summaries)."""
    by_uid: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        uid = _normalize_teacher_uid_for_join(_sr_get_from_row(row, "user_id"))
        if uid == "__unknown__":
            continue
        by_uid[uid].append(row)
    return {uid: _groups_with_students_for_teacher(lst) for uid, lst in by_uid.items()}


def _fetch_all_student_result_rows(client: bigquery.Client) -> list[dict]:
    if not STUDENT_RESULTS_TABLE:
        return []
    query = f"SELECT * FROM `{STUDENT_RESULTS_TABLE}`"
    return [_sr_enrich_row_dict(_bq_row_to_dict(r)) for r in client.query(query).result()]


def _bq_backtick_ident(field_name: str) -> str:
    return "`" + str(field_name).replace("`", "") + "`"


def _resolve_student_results_uid_sql_column(client: bigquery.Client) -> str:
    """Physical column name in student_results_data for the teacher key (matches fico_kpis.user_id)."""
    global _sr_uid_sql_column
    if _sr_uid_sql_column:
        return _sr_uid_sql_column
    if not STUDENT_RESULTS_TABLE:
        _sr_uid_sql_column = "user_id"
        return _sr_uid_sql_column
    try:
        table = client.get_table(STUDENT_RESULTS_TABLE)
    except Exception as e:
        print(f"[student_results] get_table({STUDENT_RESULTS_TABLE}) failed: {e}; using user_id")
        _sr_uid_sql_column = "user_id"
        return _sr_uid_sql_column
    aliases_lower = {a.lower() for a in STUDENT_RESULTS_ALIASES["user_id"]}
    for field in table.schema:
        if field.name.lower() in aliases_lower:
            _sr_uid_sql_column = field.name
            print(f"[student_results] Teacher key column: {_sr_uid_sql_column}")
            return _sr_uid_sql_column
    for field in table.schema:
        fl = field.name.lower()
        if "user" in fl and "id" in fl:
            _sr_uid_sql_column = field.name
            print(f"[student_results] Teacher key column (heuristic): {_sr_uid_sql_column}")
            return _sr_uid_sql_column
    _sr_uid_sql_column = "user_id"
    print("[student_results] Teacher key column (fallback): user_id")
    return _sr_uid_sql_column


def _resolve_student_results_student_id_sql_column(client: bigquery.Client) -> str:
    """Physical student identifier column in student_results_data."""
    global _sr_student_id_sql_column
    if _sr_student_id_sql_column:
        return _sr_student_id_sql_column
    if not STUDENT_RESULTS_TABLE:
        _sr_student_id_sql_column = "student_id"
        return _sr_student_id_sql_column
    try:
        table = client.get_table(STUDENT_RESULTS_TABLE)
    except Exception:
        _sr_student_id_sql_column = "student_id"
        return _sr_student_id_sql_column
    aliases_lower = {a.lower() for a in STUDENT_RESULTS_ALIASES["student_id"]}
    for field in table.schema:
        if field.name.lower() in aliases_lower:
            _sr_student_id_sql_column = field.name
            return _sr_student_id_sql_column
    for field in table.schema:
        fl = field.name.lower()
        if "student" in fl and "id" in fl:
            _sr_student_id_sql_column = field.name
            return _sr_student_id_sql_column
    _sr_student_id_sql_column = "student_id"
    return _sr_student_id_sql_column


def _fetch_student_result_rows_for_student_id(client: bigquery.Client, student_id: str) -> list[dict]:
    if not STUDENT_RESULTS_TABLE:
        return []
    sid = str(student_id).strip()
    col = _bq_backtick_ident(_resolve_student_results_student_id_sql_column(client))
    query = f"""
SELECT * FROM `{STUDENT_RESULTS_TABLE}`
WHERE CAST({col} AS STRING) = @sid
   OR (
        SAFE_CAST(@sid AS INT64) IS NOT NULL
        AND SAFE_CAST(CAST({col} AS STRING) AS INT64) = SAFE_CAST(@sid AS INT64)
   )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("sid", "STRING", sid)]
    )
    try:
        return [_sr_enrich_row_dict(_bq_row_to_dict(r)) for r in client.query(query, job_config=job_config).result()]
    except Exception:
        sid_norm = _normalize_student_id(sid)
        all_rows = _fetch_all_student_result_rows(client)
        return [r for r in all_rows if _student_id_from_row(r) == sid_norm]


def _fetch_student_result_rows_for_user(client: bigquery.Client, user_id: str) -> list[dict]:
    if not STUDENT_RESULTS_TABLE:
        return []
    uid = str(user_id).strip()
    col = _bq_backtick_ident(_resolve_student_results_uid_sql_column(client))
    # Match string IDs and numeric IDs (e.g. INT user_id vs STRING teacher id from KPIs).
    query = f"""
SELECT * FROM `{STUDENT_RESULTS_TABLE}`
WHERE CAST({col} AS STRING) = @uid
   OR (
        SAFE_CAST(@uid AS INT64) IS NOT NULL
        AND SAFE_CAST(CAST({col} AS STRING) AS INT64) = SAFE_CAST(@uid AS INT64)
   )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("uid", "STRING", uid)]
    )
    try:
        return [_sr_enrich_row_dict(_bq_row_to_dict(r)) for r in client.query(query, job_config=job_config).result()]
    except Exception as e:
        print(f"[student_results] Filtered query failed ({e}); falling back to in-memory filter for uid={uid!r}")
        uid_norm = _normalize_teacher_uid_for_join(uid)
        all_rows = _fetch_all_student_result_rows(client)
        return [
            r
            for r in all_rows
            if _normalize_teacher_uid_for_join(_sr_get_from_row(r, "user_id")) == uid_norm
        ]


def _normalize_row(row: dict) -> dict:
    """Ensure teacher dict has canonical (lowercase) keys so frontend/backend work regardless of table column names."""
    out = dict(row)
    for k, v in row.items():
        if v is not None and isinstance(k, str):
            out.setdefault(k.lower(), v)
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            v = row.get(alias)
            if v is not None:
                out[canonical] = v
                break
    return out


def _get_creds_json_from_env() -> str:
    """Get service account JSON from any known or credential-like env var."""
    # Exact names first
    for name in ("GOOGLE_APPLICATION_CREDENTIALS_JSON", "KEYY_JSON", "keyy.json"):
        v = os.getenv(name, "").strip()
        if v:
            return v
    # Fallback: any env var whose name suggests credentials and value looks like JSON
    for key, value in os.environ.items():
        if not value or not value.strip():
            continue
        key_upper = key.upper()
        if (
            "GOOGLE" in key_upper
            or "CREDENTIAL" in key_upper
            or "KEYY" in key_upper
            or "KEYY" in key
        ) and value.strip().startswith("{"):
            return value.strip()
    return ""


def get_bigquery_client() -> bigquery.Client:
    """Create BigQuery client. Uses env var(s) on Railway, else keyy.json file."""
    creds_json = _get_creds_json_from_env()
    if not creds_json:
        # Log which credential-related env vars exist (names only, no values)
        related = [k for k in os.environ if "GOOGLE" in k.upper() or "CREDENTIAL" in k.upper() or "KEYY" in k or "keyy" in k.lower()]
        print("[BigQuery] No credentials in env. Related env var names: %s" % (related or "(none)"))
    if creds_json:
        raw = creds_json
        # Try JSON first; if it fails, try base64 (Railway/some hosts mangle newlines)
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            try:
                raw = base64.b64decode(creds_json).decode("utf-8")
                info = json.loads(raw)
            except Exception:
                raise ValueError(
                    "Credentials env var is set but invalid JSON. "
                    "Use the full service account JSON, or its base64-encoded string."
                )
        creds = service_account.Credentials.from_service_account_info(info)
    else:
        key_path = Path(__file__).with_name("keyy.json")
        if not key_path.is_file():
            raise FileNotFoundError(
                "BigQuery credentials not found. On Railway: set Variable GOOGLE_APPLICATION_CREDENTIALS_JSON "
                "(or KEYY_JSON or keyy.json) to the full JSON key. Locally: add keyy.json in the project root."
            )
        creds = service_account.Credentials.from_service_account_file(str(key_path))
    return bigquery.Client(credentials=creds, project=creds.project_id)


NUMERIC_FIELDS = frozenset(KPI_FIELDS + ["overall_percentage", "total_score_out_of_52", "total_score_out_of_60", "EMIS"])


def row_to_teacher(row: dict) -> dict:
    """Convert BigQuery row to dashboard teacher payload with kpis list. Normalizes column names for fico_kpis."""
    normalized = _normalize_row(row)

    def _cell(k: str, v):
        if v is None:
            return 0 if (k and k.lower() in NUMERIC_FIELDS) else ""
        if k and k.lower() in NUMERIC_FIELDS:
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0
        return v

    teacher = {k: _cell(k, v) for k, v in row.items()}
    teacher.update({k: _cell(k, normalized.get(k)) for k in normalized})
    teacher["kpis"] = [
        {"name": k.replace("_", " ").title(), "value": float(normalized.get(k) or 0)}
        for k in KPI_FIELDS
    ]
    return teacher


def _load_heads() -> list:
    """Load heads.json (EMIS -> head name, contact). Returns list of dicts with EMIS, head_names, head_contact_numbers."""
    path = Path(__file__).resolve().parent / "heads.json"
    if not path.is_file():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _empty_dashboard_payload():
    """Return empty dashboard structure when no data table is configured."""
    return {
        "overall": {
            "total_teachers": 0,
            "total_observations": 0,
            "teachers_with_multiple_observations": 0,
            "teachers_with_multiple_dates": None,
            "avg_percentage": 0,
            "avg_score_out_of_60": None,
            "sector_count": 0,
        },
        "sectors": [],
        "teachers": [],
        "all_observations": [],
        "reports": {
            "age": [],
            "gender": [],
            "qualification": [],
            "experience": [],
            "designation": [],
            "by_created_date": [],
            "subject": [],
            "subject_by_sector": [],
        },
        "heads": [],
        "student_results_by_user_id": {},
    }


@app.get("/api/dashboard")
def get_dashboard(response: Response):
    """Return all teachers grouped by sector with overall summary for dashboard. No caching so data is always fresh."""
    print("[ACR API] GET /api/dashboard — building payload")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    if not ACR_DATA_TABLE:
        return _empty_dashboard_payload()
    try:
        client = get_bigquery_client()
    except FileNotFoundError as e:
        print(f"[ACR API] Credentials missing: {e}")
        response.status_code = 503
        return {"error": "credentials_missing", "message": str(e)}
    except ValueError as e:
        print(f"[ACR API] Invalid credentials JSON: {e}")
        response.status_code = 503
        return {"error": "credentials_invalid", "message": str(e)}
    query = f"SELECT * FROM `{ACR_DATA_TABLE}`"
    rows = list(client.query(query).result())
    print(f"[Dashboard] Loaded {len(rows)} rows from {ACR_DATA_TABLE}")
    teachers = [row_to_teacher(_bq_row_to_dict(r)) for r in rows]
    # Sort by created_date ascending (use canonical key from normalization)
    def _sort_key(t):
        k = _get_from_row(t, "created_date")
        if k is None:
            for key, v in t.items():
                if key and "created" in key.lower() and "date" in key.lower():
                    k = v
                    break
        if k is None:
            return "0000-00-00"
        if hasattr(k, "strftime"):
            return k.strftime("%Y-%m-%d")
        s = str(k).strip()[:10]
        return s if s else "0000-00-00"
    teachers.sort(key=_sort_key)

    by_sector: dict[str, list] = {}
    for t in teachers:
        sector = (str(_get_from_row(t, "sector") or t.get("Sector") or "Unknown")).strip()
        if sector not in by_sector:
            by_sector[sector] = []
        by_sector[sector].append(t)

    def avg_pct(ts: list) -> float:
        vals = [float(_get_from_row(t, "overall_percentage") or 0) for t in ts]
        return sum(vals) / len(vals) if vals else 0

    def avg_score_out_of_60(ts: list) -> float | None:
        vals = [float(_get_from_row(t, "total_score_out_of_60") or 0) for t in ts if _get_from_row(t, "total_score_out_of_60") is not None]
        if not vals:
            return None
        return round(sum(vals) / len(vals), 1)

    def _teacher_uid(teacher: dict) -> str:
        """Get teacher user_id from row (uses normalized canonical key)."""
        uid = _get_from_row(teacher, "user_id")
        if uid is None or (isinstance(uid, str) and not str(uid).strip()):
            return "__unknown__"
        if isinstance(uid, (int, float)):
            return str(int(uid))
        s = str(uid).strip()
        if not s:
            return "__unknown__"
        try:
            return str(int(float(s)))
        except (ValueError, TypeError):
            return s

    def merge_teacher_observations(obs_list: list) -> dict:
        """Merge multiple observation rows for one teacher into a single card: averaged stats, one profile."""
        if not obs_list:
            return {}
        n = len(obs_list)
        base = dict(obs_list[0])
        base["observation_count"] = n
        base["overall_percentage"] = round(avg_pct(obs_list), 1)
        if any(_get_from_row(t, "total_score_out_of_52") is not None for t in obs_list):
            scores = [float(_get_from_row(t, "total_score_out_of_52") or 0) for t in obs_list]
            base["total_score_out_of_52"] = round(sum(scores) / len(scores), 1)
        if any(_get_from_row(t, "total_score_out_of_60") is not None for t in obs_list):
            scores_60 = [float(_get_from_row(t, "total_score_out_of_60") or 0) for t in obs_list]
            base["total_score_out_of_60"] = round(sum(scores_60) / len(scores_60), 1)
        kpis = base.get("kpis") or []
        if kpis and len(kpis) >= 6:
            for i in range(len(kpis)):
                vals = [float((o.get("kpis") or [{}])[i].get("value", 0) if len((o.get("kpis") or [])) > i else 0) for o in obs_list]
                kpis[i] = {"name": kpis[i]["name"], "value": round(sum(vals) / len(vals), 1)}
            base["kpis"] = kpis
        return base

    sectors = []
    for name, teachers_list in sorted(by_sector.items()):
        by_uid_in_sector: dict[str, list] = defaultdict(list)
        for t in teachers_list:
            uid = _teacher_uid(t)
            by_uid_in_sector[uid].append(t)
        merged_teachers = []
        for uid, obs_list in by_uid_in_sector.items():
            if uid == "__unknown__":
                continue
            merged_teachers.append(merge_teacher_observations(obs_list))
        sectors.append({
            "name": name,
            "teacher_count": len(merged_teachers),
            "avg_percentage": round(avg_pct(teachers_list), 1),
            "avg_score_out_of_60": avg_score_out_of_60(teachers_list),
            "teachers": merged_teachers,
        })

    all_teachers = teachers
    overall_avg = avg_pct(all_teachers)
    overall_avg_score_60 = avg_score_out_of_60(all_teachers)

    # Cohort reports: ACR-KPIs performance vs Age, Gender, Qualification, Experience
    def parse_date(s, fmt=None):
        if s is None:
            return None
        if hasattr(s, "isoformat"):  # date/datetime from BigQuery
            return s if isinstance(s, datetime) else datetime.combine(s, datetime.min.time())
        if not isinstance(s, str):
            s = str(s).strip()
        else:
            s = s.strip()
        if not s:
            return None
        for f in (fmt,) if fmt else ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y"):
            try:
                return datetime.strptime(s[:10] if len(s) >= 10 else s, f)
            except Exception:
                continue
        return None

    def created_date_key(teacher):
        """Normalise created_date to YYYY-MM-DD for grouping/sort; return '' if missing."""
        raw = _get_from_row(teacher, "created_date")
        if raw is None:
            for k, v in teacher.items():
                if k and "created" in k.lower() and "date" in k.lower() and v is not None:
                    raw = v
                    break
        dt = parse_date(raw)
        return dt.strftime("%Y-%m-%d") if dt else ""

    def age_cohort(teacher):
        dob = parse_date(_get_from_row(teacher, "date_of_birth"))
        if not dob:
            return "Unknown"
        today = datetime.now()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        if age < 30:
            return "<30"
        if age < 40:
            return "30-40"
        if age < 50:
            return "40-50"
        return "50+"

    def experience_cohort(teacher):
        jd = parse_date(_get_from_row(teacher, "joining_date"))
        if not jd:
            return "Unknown"
        years = (datetime.now() - jd).days / 365.25
        if years < 5:
            return "0-5 yrs"
        if years < 10:
            return "5-10 yrs"
        if years < 20:
            return "10-20 yrs"
        return "20+ yrs"

    def qual_cohort(teacher):
        q = (str(_get_from_row(teacher, "qualifications") or "")).strip()
        if not q:
            return "Unknown"
        q = q[:50].lower()
        if "phd" in q or "doctorate" in q:
            return "PhD/Doctorate"
        if "master" in q or "ms " in q or "m.sc" in q or "m.ed" in q:
            return "Master"
        if "bachelor" in q or "b.ed" in q or "ba " in q or "bsc" in q:
            return "Bachelor"
        return "Other"

    def designation_cohort(teacher):
        d = (str(_get_from_row(teacher, "service_designation") or "")).strip()
        return d if d else "Unknown"

    def group_and_agg(teachers_list, key_fn, order_keys=None):
        groups = {}
        for t in teachers_list:
            k = key_fn(t)
            if k not in groups:
                groups[k] = []
            groups[k].append(t)
        order = order_keys or sorted(groups.keys())
        return [
            {"name": k, "teacher_count": len(groups.get(k, [])), "avg_percentage": round(avg_pct(groups.get(k, [])), 1)}
            for k in order
        ]

    # Build reports and drop the 'Unknown' bucket so graphs stay focused
    designation_groups = group_and_agg(teachers, designation_cohort)
    designation_sorted = sorted(
        [g for g in designation_groups if g["name"] != "Unknown" and g["teacher_count"] > 0],
        key=lambda g: -g["teacher_count"],
    )
    # Date-wise report: group by created_date, sort by date
    by_date: dict[str, list] = defaultdict(list)
    for t in teachers:
        key = created_date_key(t)
        if key:
            by_date[key].append(t)
    by_created_date = [
        {"name": dt, "teacher_count": len(obs), "avg_percentage": round(avg_pct(obs), 1)}
        for dt, obs in sorted(by_date.items())
    ]

    def subject_cohort(teacher):
        """Normalize subject to ENG, URDU, Math or keep as-is; empty -> Other."""
        raw = _get_from_row(teacher, "subject")
        if raw is None:
            return "Other"
        s = str(raw).strip()
        if not s:
            return "Other"
        lower = s.lower()
        if "english" in lower or lower in ("eng", "english"):
            return "ENG"
        if "urdu" in lower or lower == "urdu":
            return "URDU"
        if "math" in lower or "mathematics" in lower or lower in ("math", "maths"):
            return "Math"
        return s

    subject_groups = group_and_agg(teachers, subject_cohort)
    subject_order = ["ENG", "URDU", "Math"]
    subject_ordered = [g for g in subject_groups if g["name"] in subject_order]
    subject_ordered.sort(key=lambda g: subject_order.index(g["name"]))
    subject_others = [g for g in subject_groups if g["name"] not in subject_order and g["teacher_count"] > 0]
    subject_others.sort(key=lambda g: (-g["teacher_count"], g["name"]))
    subject_report = subject_ordered + subject_others

    # Subject performance per sector: average of overall_percentage (fico_kpis) by ENG, URDU, Math per sector
    # Same format as before: one row per sector with ENG, URDU, Math bars (0 if no observations)
    subject_keys = ["ENG", "URDU", "Math"]
    subject_by_sector = []
    for sector_name, sector_teachers in sorted(by_sector.items()):
        by_subj = {}
        for t in sector_teachers:
            k = subject_cohort(t)
            if k not in by_subj:
                by_subj[k] = []
            by_subj[k].append(t)
        row = {"sector": sector_name}
        for sk in subject_keys:
            obs_list = by_subj.get(sk, [])
            # avg_pct uses _get_from_row(., "overall_percentage") — fico_kpis column (or alias)
            row[sk] = round(avg_pct(obs_list), 1) if obs_list else 0
            row[sk + "_teacher_count"] = len({_teacher_uid(t) for t in obs_list})
            row[sk + "_observation_count"] = len(obs_list)
        subject_by_sector.append(row)

    reports_raw = {
        "age": group_and_agg(teachers, age_cohort, ["<30", "30-40", "40-50", "50+", "Unknown"]),
        "gender": group_and_agg(teachers, lambda t: (str(_get_from_row(t, "gender") or "Unknown")).strip() or "Unknown"),
        "qualification": group_and_agg(teachers, qual_cohort, ["PhD/Doctorate", "Master", "Bachelor", "Other", "Unknown"]),
        "experience": group_and_agg(teachers, experience_cohort, ["0-5 yrs", "5-10 yrs", "10-20 yrs", "20+ yrs", "Unknown"]),
        "designation": designation_sorted,
        "by_created_date": by_created_date,
        "subject": [g for g in subject_report if g["name"] != "Science"],
        "subject_by_sector": subject_by_sector,
    }

    # Filter out Unknown (and any zero-count cohorts) from all report series; keep by_created_date as-is
    reports = {}
    for key, series in reports_raw.items():
        if key in ("by_created_date", "subject_by_sector"):
            reports[key] = series
        else:
            reports[key] = [g for g in series if g["name"] != "Unknown" and g["teacher_count"] > 0]

    # Teachers with multiple observations (by row = observation; optionally by distinct date)
    by_user_id: dict[str, list] = defaultdict(list)
    for t in all_teachers:
        uid = _teacher_uid(t)
        by_user_id[uid].append(t)
    unique_teachers = len(by_user_id)
    total_observations = len(all_teachers)
    teachers_with_multiple_observations = sum(1 for obs in by_user_id.values() if len(obs) > 1)
    # By distinct date per teacher (if any date-like column exists on rows)
    date_columns = [k for k in (all_teachers[0].keys() if all_teachers else []) if k and "date" in k.lower() and k not in ("date_of_birth", "joining_date")]
    teachers_with_multiple_dates = 0
    if date_columns:
        for obs_list in by_user_id.values():
            if len(obs_list) <= 1:
                continue
            dates = set()
            for t in obs_list:
                for col in date_columns:
                    v = t.get(col)
                    if v is not None and str(v).strip():
                        dates.add(str(v).strip()[:10])
                        break
            if len(dates) > 1:
                teachers_with_multiple_dates += 1

    # Add observation count per teacher (total observations for this user_id) — live count for each card
    for t in all_teachers:
        uid = _teacher_uid(t)
        t["observation_count"] = len(by_user_id.get(uid, []))

    # One merged card per unique teacher for overview (so dashboard shows correct unique count and averaged stats)
    merged_unique_teachers = []
    for uid, obs_list in by_user_id.items():
        if uid == "__unknown__":
            continue
        merged_unique_teachers.append(merge_teacher_observations(obs_list))
    merged_unique_teachers.sort(key=lambda t: (float(t.get("overall_percentage") or 0), str(t.get("teacher_name") or "")), reverse=True)

    student_summaries: dict[str, list] = {}
    student_results_by_user_id: dict[str, list] = {}
    if STUDENT_RESULTS_TABLE:
        try:
            sr_rows = _fetch_all_student_result_rows(client)
            student_summaries = _aggregate_student_result_summaries(sr_rows)
            student_results_by_user_id = _full_student_groups_by_teacher_uid(sr_rows)
        except Exception as e:
            print(f"[Dashboard] student_results_data failed: {e}")

    def _attach_student_results(tlist: list) -> None:
        for t in tlist:
            uid = _normalize_teacher_uid_for_join(_get_from_row(t, "user_id"))
            t["student_results"] = [] if uid == "__unknown__" else student_summaries.get(uid, [])

    for sec in sectors:
        _attach_student_results(sec["teachers"])
    _attach_student_results(merged_unique_teachers)

    payload = {
        "overall": {
            "total_teachers": unique_teachers,
            "total_observations": total_observations,
            "teachers_with_multiple_observations": teachers_with_multiple_observations,
            "teachers_with_multiple_dates": teachers_with_multiple_dates if date_columns else None,
            "avg_percentage": round(overall_avg, 1),
            "avg_score_out_of_60": overall_avg_score_60,
            "sector_count": len(by_sector),
        },
        "sectors": sectors,
        "teachers": merged_unique_teachers,
        "all_observations": all_teachers,
        "reports": reports,
        "heads": _load_heads(),
        "student_results_by_user_id": student_results_by_user_id,
    }
    return _json_safe_value(payload)


@app.get("/api/heads")
def get_heads():
    """Return heads list (EMIS -> head name, contact) from heads.json for frontend lookup."""
    return _load_heads()


@app.get("/api/teachers/{user_id}")
def get_teacher(user_id: str):
    """Return a single teacher profile and KPIs by user_id."""
    if not ACR_DATA_TABLE:
        return None
    client = get_bigquery_client()
    query = f"SELECT * FROM `{ACR_DATA_TABLE}` WHERE user_id = @uid"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("uid", "STRING", user_id)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None
    teacher = row_to_teacher(_bq_row_to_dict(rows[0]))
    return _json_safe_value(teacher)


def _serve_student_results(user_id: str, response: Response):
    """Shared handler: teacher key = fico_kpis.user_id, same value as student_results_data.teacher_id (or user_id)."""
    user_id = (user_id or "").strip().strip("/").strip()
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    if not STUDENT_RESULTS_TABLE:
        response.status_code = 503
        return {
            "user_id": user_id,
            "groups": [],
            "error": "table_not_configured",
            "message": "Set STUDENT_RESULTS_TABLE (default tbproddb.student_results_data).",
        }
    try:
        client = get_bigquery_client()
    except FileNotFoundError as e:
        response.status_code = 503
        return {"user_id": user_id, "groups": [], "error": "credentials_missing", "message": str(e)}
    except ValueError as e:
        response.status_code = 503
        return {"user_id": user_id, "groups": [], "error": "credentials_invalid", "message": str(e)}
    try:
        rows = _fetch_student_result_rows_for_user(client, user_id)
        groups = _groups_with_students_for_teacher(rows)
    except Exception as e:
        print(f"[ACR API] student-results/{user_id}: {e}")
        response.status_code = 502
        return {"user_id": user_id, "groups": [], "error": "query_failed", "message": str(e)}
    return _json_safe_value({"user_id": user_id, "groups": groups})


def _teacher_name_by_uid(client: bigquery.Client) -> dict[str, str]:
    out: dict[str, str] = {}
    if not ACR_DATA_TABLE:
        return out
    try:
        rows = client.query(f"SELECT user_id, teacher_name FROM `{ACR_DATA_TABLE}` WHERE user_id IS NOT NULL").result()
    except Exception:
        return out
    for r in rows:
        d = _bq_row_to_dict(r)
        uid = _normalize_teacher_uid_for_join(d.get("user_id"))
        if uid == "__unknown__" or uid in out:
            continue
        nm = d.get("teacher_name")
        if nm is not None and str(nm).strip():
            out[uid] = str(nm).strip()
    return out


def _serve_student_history(student_id: str, teacher_user_id: str | None, response: Response):
    student_id = _normalize_student_id(student_id)
    teacher_user_id = _normalize_teacher_uid_for_join(teacher_user_id) if teacher_user_id else ""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    if not student_id:
        response.status_code = 400
        return {"student_id": "", "records": [], "error": "student_id_required", "message": "Provide a valid student_id."}
    try:
        client = get_bigquery_client()
    except FileNotFoundError as e:
        response.status_code = 503
        return {"student_id": student_id, "records": [], "error": "credentials_missing", "message": str(e)}
    except ValueError as e:
        response.status_code = 503
        return {"student_id": student_id, "records": [], "error": "credentials_invalid", "message": str(e)}
    try:
        rows = _fetch_student_result_rows_for_student_id(client, student_id)
    except Exception as e:
        response.status_code = 502
        return {"student_id": student_id, "records": [], "error": "query_failed", "message": str(e)}

    tname_by_uid = _teacher_name_by_uid(client)
    records = []
    for row in rows:
        sid = _student_id_from_row(row)
        if sid != student_id:
            continue
        uid = _normalize_teacher_uid_for_join(_sr_get_from_row(row, "user_id"))
        if teacher_user_id and teacher_user_id != "__unknown__" and uid != teacher_user_id:
            continue
        uid_out = "" if uid == "__unknown__" else uid
        payload = _row_to_student_result_payload(row)
        rec = {
            "student_id": student_id,
            "student_name": payload.get("student_name", ""),
            "student_grade": payload.get("student_grade", ""),
            "teacher_user_id": uid_out,
            "teacher_name": tname_by_uid.get(uid_out, ""),
            "session_year": str(_sr_get_from_row(row, "session_year") or "").strip(),
            "term": _normalize_term_display(_sr_get_from_row(row, "term")),
            "grade": str(_sr_get_from_row(row, "grade") or "").strip(),
            "subject": str(_sr_get_from_row(row, "subject") or "").strip(),
            "obtained_marks": payload.get("obtained_marks"),
            "total_marks": payload.get("total_marks"),
            "marks_percentage": payload.get("marks_percentage"),
            "marks_display": payload.get("marks"),
            "result": payload.get("result", ""),
        }
        ex = payload.get("extra")
        if isinstance(ex, dict) and ex:
            rec["extra"] = ex
        records.append(rec)

    records.sort(key=lambda r: (r["session_year"], _term_sort_order(r["term"]), r["grade"], r["subject"], r["teacher_user_id"]))
    return _json_safe_value({
        "student_id": student_id,
        "teacher_user_id": "" if teacher_user_id in ("", "__unknown__") else teacher_user_id,
        "records": records,
    })


# Prefer this URL in the browser (works behind proxies and avoids path-routing 404s).
@app.get("/api/student-results")
def get_student_results_by_query(
    response: Response,
    user_id: str = Query(..., description="Teacher id (same as fico_kpis.user_id / student_results.teacher_id)"),
):
    return _serve_student_results(user_id, response)


@app.get("/api/student-results/{user_id}")
def get_student_results_by_teacher_path(user_id: str, response: Response):
    """Same as GET /api/student-results?user_id=… (path style for bookmarks / curl)."""
    return _serve_student_results(user_id, response)


@app.get("/api/student-results/")
def get_student_results_by_query_trailing_slash(
    response: Response,
    user_id: str = Query(..., description="Teacher id (same as fico_kpis.user_id / student_results.teacher_id)"),
):
    """Trailing-slash variant (some clients/proxies add `/` before `?`)."""
    return _serve_student_results(user_id, response)


@app.get("/api/dashboard/teacher-student-results")
def get_dashboard_teacher_student_results(
    response: Response,
    user_id: str = Query(..., description="Teacher id (same as fico_kpis.user_id / student_results.teacher_id)"),
):
    """Same payload as /api/student-results — lives under /api/dashboard/ for environments where that prefix is already routed."""
    return _serve_student_results(user_id, response)


@app.get("/api/student-history")
def get_student_history_by_query(
    response: Response,
    student_id: str = Query(..., description="Student identifier from student_results_data.student_id"),
    teacher_user_id: str | None = Query(None, description="Optional teacher user_id filter"),
):
    return _serve_student_history(student_id, teacher_user_id, response)


@app.get("/api/student-history/{student_id}")
def get_student_history_by_path(student_id: str, response: Response, teacher_user_id: str | None = None):
    return _serve_student_history(student_id, teacher_user_id, response)


# Root: serve Performance Dashboard (static HTML)
_performance_dashboard = Path(__file__).resolve().parent / "static" / "dashboard.html"


@app.get("/api/env-check")
def env_check():
    """Safe diagnostic: whether credentials are visible in this container (no secrets returned)."""
    creds_json = _get_creds_json_from_env()
    key_path = Path(__file__).resolve().parent / "keyy.json"
    credential_like = [
        k for k in os.environ
        if "GOOGLE" in k.upper() or "CREDENTIAL" in k.upper() or "KEYY" in k or "keyy" in k.lower()
    ]
    return {
        "credentials_in_env": bool(creds_json),
        "creds_length_if_present": len(creds_json) if creds_json else 0,
        "keyy_json_file_exists": key_path.is_file(),
        "env_var_names_we_check": ["GOOGLE_APPLICATION_CREDENTIALS_JSON", "KEYY_JSON", "keyy.json"],
        "credential_like_var_names_in_container": sorted(credential_like),
        "hint": (
            "Set GOOGLE_APPLICATION_CREDENTIALS_JSON (or KEYY_JSON) in Railway Variables to the full JSON key, then Redeploy."
            if not creds_json else "Credentials are present."
        ),
    }


@app.get("/favicon.ico")
def _favicon():
    """Avoid 404 for favicon; return no content."""
    return Response(status_code=204)


@app.get("/")
def _root():
    print("[ACR API] GET / — serving dashboard")
    if _performance_dashboard.is_file():
        return FileResponse(_performance_dashboard)
    from fastapi.responses import HTMLResponse
    return HTMLResponse(
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>ACR-KPIs Dashboard</title></head><body style='font-family:system-ui;max-width:600px;margin:3rem auto;padding:1rem;background:#0a0f1a;color:#e2e8f0'>"
        "<h1>ACR-KPIs Performance Dashboard API</h1><p>API is running.</p>"
        "<p><a href='/api/dashboard' style='color:#0ea5e9'>/api/dashboard</a> — dashboard data</p>"
        "<p><a href='/api/env-check' style='color:#0ea5e9'>/api/env-check</a> — credential env diagnostic (Railway)</p>"
        "<p><a href='/docs' style='color:#0ea5e9'>/docs</a> — API docs</p></body></html>"
    )
