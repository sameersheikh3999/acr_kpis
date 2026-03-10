"""FastAPI backend for ACR-KPIs dashboard: serves teacher and sector data from BigQuery."""

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

# BigQuery table for ACR data (override with ACR_DATA_TABLE env var if needed)
ACR_DATA_TABLE = os.getenv("ACR_DATA_TABLE", "tbproddb.dc_acr_data_updated").strip()

app = FastAPI(title="ACR-KPIs Performance Dashboard API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KPI_FIELDS = [
    "accurate_lesson_planning",
    "timely_lesson_delivery",
    "subject_command",
    "effective_pedagogy",
    "effective_resource_use",
    "activity_based_learning",
    "student_participation",
    "critical_thinking",
    "inclusive_practices",
    "technology_integration",
    "technology_handling",
    "verbal_communication",
    "non_verbal_communication",
]

# Possible column names in BigQuery (e.g. dc_acr_data_updated) for each canonical field
COLUMN_ALIASES = {
    "user_id": ["user_id", "User_ID", "userId", "userid"],
    "sector": ["Sector", "sector", "SECTOR"],
    "overall_percentage": ["overall_percentage", "overall_percent", "Overall_Percentage", "OverallPercent"],
    "created_date": ["created_date", "Created_Date", "observation_date", "Observation_Date", "date", "Date"],
    "total_score_out_of_52": ["total_score_out_of_52", "total_score", "Total_Score_Out_Of_52", "score_out_of_52"],
    "date_of_birth": ["date_of_birth", "Date_Of_Birth", "dob"],
    "joining_date": ["joining_date", "Joining_Date", "join_date"],
    "qualifications": ["qualifications", "Qualifications", "qualification"],
    "service_designation": ["service_designation", "Service_Designation", "designation", "Designation"],
    "gender": ["gender", "Gender", "GENDER"],
    "subject": ["subject", "Subject", "subject_name", "Subject_Name", "course", "Course", "subject_name_ur", "subject_name_en"],
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


def get_bigquery_client() -> bigquery.Client:
    """Create BigQuery client. Uses GOOGLE_APPLICATION_CREDENTIALS_JSON env var on Railway, else keyy.json locally."""
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info)
    else:
        key_path = Path(__file__).with_name("keyy.json")
        if not key_path.is_file():
            raise FileNotFoundError(
                "No credentials: set GOOGLE_APPLICATION_CREDENTIALS_JSON (Railway) or add keyy.json (local)."
            )
        creds = service_account.Credentials.from_service_account_file(str(key_path))
    return bigquery.Client(credentials=creds, project=creds.project_id)


NUMERIC_FIELDS = frozenset(KPI_FIELDS + ["overall_percentage", "total_score_out_of_52", "EMIS"])


def row_to_teacher(row: dict) -> dict:
    """Convert BigQuery row to dashboard teacher payload with kpis list. Normalizes column names for dc_acr_data_updated."""
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


def _empty_dashboard_payload():
    """Return empty dashboard structure when no data table is configured."""
    return {
        "overall": {
            "total_teachers": 0,
            "total_observations": 0,
            "teachers_with_multiple_observations": 0,
            "teachers_with_multiple_dates": None,
            "avg_percentage": 0,
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
    }


@app.get("/api/dashboard")
def get_dashboard(response: Response):
    """Return all teachers grouped by sector with overall summary for dashboard. No caching so data is always fresh."""
    print("[ACR API] GET /api/dashboard — building payload")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    if not ACR_DATA_TABLE:
        return _empty_dashboard_payload()
    client = get_bigquery_client()
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
        kpis = base.get("kpis") or []
        if kpis and len(kpis) >= 13:
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
            "teachers": merged_teachers,
        })

    all_teachers = teachers
    overall_avg = avg_pct(all_teachers)

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

    # Subject performance per sector: one row per sector with ENG, URDU, Math (0 if no observations)
    # Also include teacher_count and observation_count per subject for hover tooltips
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
            "sector_count": len(by_sector),
        },
        "sectors": sectors,
        "teachers": merged_unique_teachers,
        "all_observations": all_teachers,
        "reports": reports,
    }
    return _json_safe_value(payload)


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
        "<p><a href='/docs' style='color:#0ea5e9'>/docs</a> — API docs</p></body></html>"
    )
