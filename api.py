"""FastAPI backend for ACR-KPIs dashboard: serves teacher and sector data from BigQuery."""

import base64
import json
import os
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from google.cloud import bigquery
from google.oauth2 import service_account

# Single port for API and (when built) dashboard
PORT = int(os.getenv("PORT", "8000"))

# BigQuery table: now fico_kpis. Query: SELECT * FROM `tbproddb.fico_kpis`
# Override with env ACR_DATA_TABLE if needed.
ACR_DATA_TABLE = os.getenv("ACR_DATA_TABLE", "tbproddb.fico_kpis").strip()

app = FastAPI(title="ACR-KPIs Performance Dashboard API", version="1.0.0")


@app.on_event("startup")
def _log_credential_env():
    """Log which credential-related env vars are set (names only) for Railway debugging."""
    creds_json = _get_creds_json_from_env()
    if creds_json:
        print("[ACR API] BigQuery credentials found in env (len=%d)" % len(creds_json))
    else:
        related = [k for k in os.environ if "GOOGLE" in k.upper() or "CREDENTIAL" in k.upper() or "KEYY" in k or "keyy" in k.lower()]
        print("[ACR API] Startup: no credential env. Names that might be relevant: %s" % (related or "(none)"))


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
