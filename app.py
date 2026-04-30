"""Sales Prospecting Agent — Streamlit Dashboard.

Workflow-oriented dashboard for Autodesk CZ territory event prospecting.
Tabs: Overview | Events | Companies | Contacts | Outreach | Activity Log
"""

import json
import os
import re
import sqlite3
import sys
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db.database import (
    init_db,
    get_accounts,
    get_account,
    get_contacts,
    get_contact,
    get_opportunities,
    get_outreach_log,
    get_stats,
    get_cached_enrichment,
    upsert_account,
    upsert_contact,
    log_outreach,
    update_outreach_status,
    get_events,
    get_event,
    get_event_companies,
    get_all_leads,
    get_upcoming_event_leads,
    get_companies_with_events,
)
from outreach.message_crafter import (
    craft_message,
    regenerate_with_feedback,
    list_plays,
    parse_ai_response,
)
from outreach.outlook_drafter import create_outlook_draft
from scraper.site_configs import EVENTS as SITE_EVENTS
from scoring.competitors import normalize_products, recommend_upsell, detect_industry_segment
from outreach.persona_templates import (
    match_persona as _match_persona_module,
    get_persona as _get_persona,
    build_signal_hooks,
    build_event_hook,
    get_industry_hooks,
)

HAS_LLM_KEY = bool(os.getenv("OPENAI_API_KEY"))

from translations import t, tval, LEAD_CLASS_LABELS, PLAY_TYPE_LABELS, PRIORITY_LABELS, ENTITY_STATUS_LABELS, ATTENDANCE_LABELS, TYPE_LABELS, YES_NO
from enrichment.email_guesser import guess_email

DB_PATH = Path(__file__).resolve().parent / "prospects.db"
ENRICHMENT_DIR = Path(__file__).resolve().parent / "enrichment_data"

st.set_page_config(
    page_title="Autodesk CZ — Sales Agent",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

init_db()

if "lang" not in st.session_state:
    st.session_state["lang"] = "en"
if "active_user" not in st.session_state:
    st.session_state["active_user"] = "martin"

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    [data-testid="stHeader"] { display: none !important; }
    [data-testid="stToolbar"] { display: none !important; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #0d1117 0%, #131920 100%); }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1 {
        background: linear-gradient(135deg, #00bfa5 0%, #4fc3f7 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        font-weight: 700; letter-spacing: -0.5px;
    }
    div[data-testid="stMetric"] {
        background: rgba(22,27,34,0.8); border: 1px solid rgba(0,191,165,0.15);
        border-radius: 12px; padding: 16px 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.15);
        transition: border-color 0.2s;
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(0,191,165,0.4); }
    div[data-testid="stMetric"] label { color: #8b949e !important; font-size: 0.8rem !important; font-weight: 500 !important; text-transform: uppercase; letter-spacing: 0.5px; }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-weight: 700 !important; font-size: 1.6rem !important; color: #e6edf3 !important; }
    [data-testid="stTabs"] button[data-baseweb="tab"] { font-weight: 500; font-size: 0.9rem; border-radius: 8px 8px 0 0; padding: 0.6rem 1.2rem; }
    [data-testid="stTabs"] [aria-selected="true"] { background: rgba(0,191,165,0.1) !important; border-bottom: 2px solid #00bfa5 !important; }
    [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; border: 1px solid rgba(0,191,165,0.1); }
    .stButton > button[kind="primary"] { background: linear-gradient(135deg, #00bfa5 0%, #00897b 100%); border: none; border-radius: 8px; font-weight: 600; transition: all 0.2s; }
    .stButton > button[kind="primary"]:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(0,191,165,0.3); }
    .badge { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; letter-spacing: 0.3px; }
    .badge-aec { background: rgba(79,195,247,0.2); color: #4fc3f7; }
    .badge-dm { background: rgba(255,138,101,0.2); color: #ff8a65; }
    .badge-me { background: rgba(171,71,188,0.2); color: #ab47bc; }
    .badge-mixed { background: rgba(120,144,156,0.2); color: #78909c; }
    .badge-client { background: rgba(0,230,118,0.2); color: #00e676; }
    .badge-new { background: rgba(255,202,40,0.2); color: #ffca28; }
    .badge-displace { background: rgba(244,67,54,0.2); color: #ef5350; }
    .event-card { background: rgba(22,27,34,0.7); border: 1px solid rgba(0,191,165,0.12); border-radius: 14px; padding: 20px 24px; margin-bottom: 12px; transition: border-color 0.2s, box-shadow 0.2s; }
    .event-card:hover { border-color: rgba(0,191,165,0.35); box-shadow: 0 4px 16px rgba(0,0,0,0.2); }
    .event-card h4 { margin: 0 0 8px 0; color: #e6edf3; }
    .event-card .meta { color: #8b949e; font-size: 0.82rem; }
    .event-card .meta strong { color: #c9d1d9; }
    .countdown-card { background: linear-gradient(135deg, rgba(0,191,165,0.08) 0%, rgba(22,27,34,0.9) 100%); border: 1px solid rgba(0,191,165,0.2); border-radius: 14px; padding: 20px; text-align: center; }
    .countdown-card .days { font-size: 2.2rem; font-weight: 700; color: #00bfa5; }
    .countdown-card .label { color: #8b949e; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.5px; }
    .countdown-card .event-title { font-size: 1rem; font-weight: 600; color: #e6edf3; margin-top: 6px; }
    .detail-card { background: rgba(22,27,34,0.7); border: 1px solid rgba(0,191,165,0.15); border-radius: 14px; padding: 24px; margin-bottom: 16px; }
    .detail-card h3, .detail-card h4 { margin: 0 0 12px 0; color: #e6edf3; }
    .detail-label { color: #8b949e; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px; }
    .detail-value { color: #e6edf3; font-size: 0.95rem; margin-bottom: 12px; }
    .detail-value a { color: #4fc3f7; text-decoration: none; }
    .contact-row { background: rgba(22,27,34,0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; padding: 14px 18px; margin-bottom: 8px; transition: border-color 0.2s; }
    .contact-row:hover { border-color: rgba(0,191,165,0.3); }
    .contact-name { font-weight: 600; color: #e6edf3; font-size: 0.95rem; }
    .contact-title { color: #8b949e; font-size: 0.82rem; }
    .contact-detail { color: #6e7681; font-size: 0.78rem; margin-top: 4px; }
    .signal-tag { display: inline-block; padding: 2px 8px; border-radius: 8px; font-size: 0.72rem; font-weight: 500; margin-right: 4px; margin-bottom: 4px; background: rgba(255,202,40,0.15); color: #ffca28; }
    .score-bar { height: 6px; border-radius: 3px; background: rgba(255,255,255,0.08); overflow: hidden; }
    .score-fill { height: 100%; border-radius: 3px; }
    .section-divider { border: none; border-top: 1px solid rgba(0,191,165,0.15); margin: 1.5rem 0; }
    .version-badge { font-size: 0.7rem; color: #484f58; text-align: center; padding-top: 16px; }
    .playbook-card { background: rgba(22,27,34,0.7); border: 1px solid rgba(0,191,165,0.15); border-radius: 14px; padding: 20px 24px; margin-bottom: 12px; }
    .playbook-card h4 { margin: 0 0 12px 0; color: #e6edf3; }
    .play-badge { display: inline-block; padding: 4px 12px; border-radius: 10px; font-size: 0.78rem; font-weight: 600; letter-spacing: 0.3px; }
    .play-upsell { background: rgba(0,230,118,0.15); color: #00e676; }
    .play-displacement { background: rgba(244,67,54,0.15); color: #ef5350; }
    .play-greenfield { background: rgba(255,202,40,0.15); color: #ffca28; }
    .persona-card { background: rgba(22,27,34,0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; padding: 14px 18px; margin-bottom: 8px; }
    .persona-card:hover { border-color: rgba(0,191,165,0.3); }
    .persona-name { font-weight: 600; color: #e6edf3; font-size: 0.95rem; }
    .persona-title { color: #8b949e; font-size: 0.82rem; }
    .persona-label { display: inline-block; padding: 2px 8px; border-radius: 8px; font-size: 0.72rem; font-weight: 600; margin-right: 4px; }
    .persona-high { background: rgba(0,230,118,0.15); color: #00e676; }
    .persona-medium { background: rgba(255,202,40,0.15); color: #ffca28; }
    .persona-low { background: rgba(120,144,156,0.15); color: #78909c; }
    .pain-point { color: #c9d1d9; font-size: 0.82rem; padding: 2px 0 2px 12px; border-left: 2px solid rgba(0,191,165,0.3); margin-bottom: 4px; }
    .talk-track { color: #8b949e; font-size: 0.82rem; font-style: italic; padding: 8px 12px; background: rgba(0,191,165,0.05); border-radius: 8px; margin-top: 6px; }
    .product-tag { display: inline-block; padding: 2px 8px; border-radius: 8px; font-size: 0.72rem; font-weight: 500; margin-right: 4px; margin-bottom: 4px; background: rgba(79,195,247,0.15); color: #4fc3f7; }
    .play-detail { background: rgba(22,27,34,0.5); border: 1px solid rgba(0,191,165,0.1); border-radius: 10px; padding: 12px 16px; margin-bottom: 8px; }
    .play-detail-title { font-weight: 600; color: #e6edf3; font-size: 0.88rem; margin-bottom: 4px; }
    .play-detail-body { color: #8b949e; font-size: 0.82rem; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _load_event_enrichment():
    path = ENRICHMENT_DIR / "event_company_enrichment.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _load_account_enrichment():
    path = ENRICHMENT_DIR / "account_enrichment.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


@st.cache_data(ttl=30)
def _cached_event_enrichment():
    return _load_event_enrichment()


@st.cache_data(ttl=30)
def _cached_account_enrichment():
    return _load_account_enrichment()


def _load_contact_playbook():
    path = ENRICHMENT_DIR / "contact_playbook.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


@st.cache_data(ttl=30)
def _cached_contact_playbook():
    return _load_contact_playbook()


def segment_badge(segment):
    seg = (segment or "mixed").strip().upper()
    cls_map = {"AEC": "badge-aec", "D&M": "badge-dm", "M&E": "badge-me"}
    return '<span class="badge {}">{}</span>'.format(cls_map.get(seg, "badge-mixed"), seg)


def lead_class_badge(lc):
    lc = (lc or "").strip().lower()
    cls_map = {"whitespace": "badge-client", "new_market": "badge-new", "displacement": "badge-displace"}
    label = tval(LEAD_CLASS_LABELS, lc)
    return '<span class="badge {}">{}</span>'.format(cls_map.get(lc, "badge-mixed"), label or lc.upper() or "---")


def days_until(date_str):
    if not date_str:
        return None
    try:
        event_date = datetime.strptime(date_str.split(" ")[0].strip()[:10], "%Y-%m-%d").date()
        return (event_date - date.today()).days
    except (ValueError, IndexError):
        return None


def plotly_dark(fig, height=350):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#c9d1d9"),
        margin=dict(l=20, r=20, t=40, b=20), height=height,
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.05)", zeroline=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.05)", zeroline=False)
    return fig


def _score_color(score):
    if score >= 60:
        return "#00e676"
    if score >= 35:
        return "#ffca28"
    return "#78909c"


def _score_bar_html(score, max_score=100):
    pct = min(100, score / max_score * 100)
    color = _score_color(score)
    return '<div class="score-bar"><div class="score-fill" style="width:{}%;background:{}"></div></div>'.format(pct, color)


SENIORITY_MAP = {
    "ceo": 100, "chief executive": 100, "chief operating": 95, "coo": 95,
    "chief technology": 95, "cto": 95, "chief financial": 90, "cfo": 90,
    "chief information": 90, "cio": 90, "chief digital": 90,
    "jednatel": 95, "výkonný ředitel": 95, "generální ředitel": 100,
    "managing director": 95, "general manager": 90, "owner": 95,
    "founder": 95, "partner": 85,
    "vp ": 85, "vice president": 85, "svp": 85,
    "director": 75, "head of": 75, "vedoucí": 75, "ředitel": 80,
    "senior manager": 65, "manager": 55, "team lead": 50,
    "architect": 50, "principal": 55, "lead": 45,
    "senior engineer": 40, "senior": 40, "specialist": 35,
    "engineer": 30, "designer": 30, "koordinátor": 35,
    "coordinator": 30, "analyst": 25, "technolog": 30, "technik": 25,
    "projektant": 35,
}

ADSK_RELEVANT_TITLES = {
    "bim", "cad", "design", "architect", "engineer", "construction",
    "project", "manufacturing", "production", "r&d", "development",
    "it ", "technology", "digital", "innovation", "technical",
    "infrastructure", "facility", "operations", "procurement",
    "purchasing", "nákup", "výrob", "projekce", "technolog",
    "chief executive", "ceo", "chief operating", "coo", "chief technology", "cto",
    "managing director", "general manager", "owner", "founder", "jednatel",
    "ředitel", "výkonný",
}


def score_contact_priority(contact, company_enrichment=None):
    """Score a contact 0-100 for outreach priority within a company.

    Factors:
    - Seniority / decision-making power (0-40)
    - Relevance to Autodesk products based on title (0-25)
    - Persona match from templates (0-15)
    - Contact completeness: email, phone (0-10)
    - Signal alignment with company data (0-10)
    """
    title = (contact.get("title") or contact.get("Title") or "").lower().strip()
    score = 0

    # --- Seniority / decision power (0-35) ---
    seniority_raw = 0
    for keyword, pts in SENIORITY_MAP.items():
        if keyword in title:
            seniority_raw = max(seniority_raw, pts)
    score += int(seniority_raw * 0.35)

    # --- ADSK relevance by title + persona metadata (0-30) ---
    relevance = 0
    for kw in ADSK_RELEVANT_TITLES:
        if kw in title:
            relevance = 12
            break
    persona_type = contact.get("persona_type", "")
    adsk_persona = contact.get("adsk_persona", "")
    if persona_type in ("economic_buyer", "executive_sponsor"):
        relevance += 12
    elif persona_type in ("champion", "technical_influencer"):
        relevance += 10
    elif persona_type == "end_user_leader":
        relevance += 6
    if adsk_persona in ("technical_decision_maker", "business_decision_maker", "champion"):
        relevance += 6
    elif adsk_persona == "end_user":
        relevance += 3
    score += min(relevance, 30)

    # --- Persona template match (0-15) ---
    persona_key = _match_persona_module(title)
    if persona_key:
        score += 15

    # --- Contact completeness (0-10) ---
    email = contact.get("email") or contact.get("Email") or ""
    phone = contact.get("phone") or contact.get("Phone") or ""
    if email:
        score += 6
    if phone:
        score += 4

    # --- Signal alignment (0-10) ---
    if company_enrichment:
        if company_enrichment.get("hiring_signal") and any(kw in title for kw in ("engineer", "design", "bim", "cad", "architect", "manager")):
            score += 5
        adsk_tools = company_enrichment.get("autodesk_tools_in_jobs", [])
        if adsk_tools and any(kw in title for kw in ("cad", "bim", "design", "engineer", "techno")):
            score += 5

    return min(score, 100)


def _priority_label(score):
    if score >= 70:
        return "high"
    if score >= 45:
        return "medium"
    if score >= 25:
        return "low"
    return "info"


def _priority_color(label):
    return {"high": "#00e676", "medium": "#ffca28", "low": "#78909c", "info": "#546e7a"}.get(label, "#546e7a")


def get_company_contacts(company_name):
    """Gather contacts from persona cache and event data for a company."""
    contacts = []
    seen = set()
    cache_key = company_name.lower().strip()

    conn = _db()

    persona_rows = conn.execute(
        "SELECT result_json FROM enrichment_cache WHERE lookup_type='personas'"
    ).fetchall()
    for pr in persona_rows:
        data = json.loads(dict(pr)["result_json"])
        stored_name = (data.get("company_name") or "").lower().strip()
        if not stored_name:
            continue
        is_match = (stored_name == cache_key
                    or cache_key.startswith(stored_name + " ")
                    or stored_name.startswith(cache_key + " ")
                    or (len(stored_name) >= 4 and re.search(r'\b' + re.escape(stored_name) + r'\b', cache_key))
                    or (len(cache_key) >= 4 and re.search(r'\b' + re.escape(cache_key) + r'\b', stored_name)))
        if is_match:
            for c in data.get("contacts", []):
                key = "{}_{}".format(c.get("first_name", "").lower(), c.get("last_name", "").lower())
                if key not in seen and key != "_":
                    seen.add(key)
                    contacts.append({
                        "name": "{} {}".format(c.get("first_name", ""), c.get("last_name", "")).strip(),
                        "title": c.get("title", ""),
                        "email": c.get("email", ""),
                        "phone": c.get("phone", ""),
                        "country": c.get("country", ""),
                        "company": c.get("company", company_name),
                        "source": "ZoomInfo",
                        "accuracy": c.get("accuracy_score", ""),
                    })

    rows = conn.execute(
        "SELECT person_name, person_title, person_linkedin FROM event_companies "
        "WHERE LOWER(TRIM(company_name)) = ? AND person_name IS NOT NULL AND person_name != ''",
        (cache_key,),
    ).fetchall()

    for r in rows:
        d = dict(r)
        name = d.get("person_name", "").strip()
        name_key = name.lower().replace(" ", "_")
        if name_key not in seen and name_key:
            seen.add(name_key)
            contacts.append({
                "name": name,
                "title": d.get("person_title", ""),
                "email": "",
                "phone": "",
                "linkedin": d.get("person_linkedin", ""),
                "company": company_name,
                "source": "Event Data",
            })

    # Also pull contacts from account_enrichment.json (CRM contacts, enriched contacts)
    acct_enrichment = _cached_account_enrichment()
    acct_entry = _find_account_enrichment_entry(company_name, acct_enrichment)
    if acct_entry and acct_entry.get("contacts"):
        for c in acct_entry["contacts"]:
            fn = c.get("first_name", "").strip()
            ln = c.get("last_name", "").strip()
            name = "{} {}".format(fn, ln).strip()
            email = c.get("email", "")
            if not name and email:
                name = email.split("@")[0]
            name_key = name.lower().replace(" ", "_") if name else ""
            if name_key and name_key not in seen and name_key != "_":
                seen.add(name_key)
                contacts.append({
                    "name": name,
                    "title": c.get("title", ""),
                    "email": email,
                    "phone": c.get("phone", ""),
                    "country": "",
                    "company": company_name,
                    "source": c.get("source", "CRM").upper(),
                    "accuracy": c.get("confidence", ""),
                })

    # Pull purchaser emails from master list
    master = _load_master_list_lookup()
    master_entry = master.get(cache_key)
    if master_entry and master_entry.get("contact_email"):
        for email_str in master_entry["contact_email"].split("|"):
            email_str = email_str.strip()
            if not email_str or email_str.lower() == "unknown":
                continue
            email_key = email_str.lower().replace("@", "_at_")
            if email_key not in seen:
                seen.add(email_key)
                local = email_str.split("@")[0]
                name_parts = local.replace(".", " ").replace("_", " ").title()
                contacts.append({
                    "name": name_parts,
                    "title": "Purchaser (CRM)",
                    "email": email_str,
                    "phone": "",
                    "company": company_name,
                    "source": "CRM",
                })

    conn.close()

    # Guess emails for contacts that have a name but no email
    domain = None
    ev_enrich = _cached_event_enrichment()
    ee = ev_enrich.get(cache_key, {})
    domain = ee.get("domain") or ee.get("zi_domain") or ""
    if not domain:
        master_lu = _load_master_list_lookup()
        me = master_lu.get(cache_key, {})
        domain = me.get("website") or me.get("domain") or ""
    if not domain:
        conn2 = _db()
        row = conn2.execute(
            "SELECT company_domain FROM event_companies WHERE LOWER(TRIM(company_name))=? AND company_domain IS NOT NULL LIMIT 1",
            (cache_key,),
        ).fetchone()
        conn2.close()
        if row:
            domain = dict(row).get("company_domain", "")

    if domain:
        domain = domain.lower().strip().replace("http://", "").replace("https://", "").split("/")[0]
        known_emails = [c["email"] for c in contacts if c.get("email")]
        for ct in contacts:
            if ct.get("email"):
                continue
            parts = ct.get("name", "").split(None, 1)
            fn = parts[0] if parts else ""
            ln = parts[1] if len(parts) > 1 else ""
            if fn and ln:
                result = guess_email(fn, ln, domain, known_emails)
                if result.get("email"):
                    ct["email"] = result["email"]
                    ct["email_guessed"] = True
                    ct["email_confidence"] = result.get("confidence", "medium")
                    ct["email_pattern"] = result.get("pattern", "")

    return contacts


@st.cache_data(ttl=60)
def _load_master_list_lookup():
    """Load master list CSV into a dict keyed by lowercase company name."""
    import pandas as pd
    lookup = {}
    csv_paths = [
        str(Path(__file__).resolve().parent.parent / "prioritized_accounts_enriched.csv"),
        str(Path(__file__).resolve().parent.parent / "prioritized_accounts.csv"),
    ]
    for p in csv_paths:
        if Path(p).exists():
            try:
                df = pd.read_csv(p, encoding="utf-8-sig", dtype=str, keep_default_na=False)
                for _, row in df.iterrows():
                    name = row.get("company_name", "").strip()
                    if name:
                        lookup[name.lower()] = dict(row)
            except Exception:
                pass
            break
    return lookup


def _find_account_enrichment_entry(company_name, account_enrichment):
    """Find the account_enrichment.json entry for a company by name matching."""
    if not account_enrichment:
        return None
    cache_key = company_name.lower().strip()
    for csn, data in account_enrichment.items():
        official = (data.get("official_name") or "").lower().strip()
        zi_name = (data.get("zi_company_name") or "").lower().strip()
        if official and (official == cache_key or cache_key in official or official in cache_key):
            return data
        if zi_name and (zi_name == cache_key or cache_key in zi_name or zi_name in cache_key):
            return data
    return None


# ---------------------------------------------------------------------------
# Zuzana's client list & user-based client matching
# ---------------------------------------------------------------------------

_LEGAL_SUFFIXES_RE = re.compile(
    r"\b(s\.?r\.?o\.?|a\.?s\.?|spol\.?\s*s\s*r\.?\s*o\.?|v\.?o\.?s\.?|k\.?s\.?|z\.?s\.?|s\.?p\.?)\s*[.,]?\s*$",
    re.IGNORECASE,
)


def _normalize_company(name):
    """Lowercase, strip legal suffixes and punctuation for matching."""
    n = name.lower().strip()
    n = _LEGAL_SUFFIXES_RE.sub("", n).strip().rstrip(",. ")
    return n


@st.cache_data(ttl=300)
def _load_zuzana_companies():
    """Load Zuzana's company names from her Excel file and return a set of normalized names."""
    xlsx = Path(__file__).resolve().parent / "ZuzzFy27.2.xlsx"
    if not xlsx.exists():
        xlsx = Path(__file__).resolve().parent.parent / "research-dashboard" / "ZuzzFy27.2.xlsx"
    if not xlsx.exists():
        return set()
    try:
        df = pd.read_excel(str(xlsx), engine="openpyxl", header=None)
        names = set()
        for i in range(5, len(df)):
            val = df.iloc[i, 0]
            if pd.notna(val):
                v = str(val).strip()
                if v and v != "Grand Total" and not v.startswith("(blank)"):
                    names.add(_normalize_company(v))
        return names
    except Exception:
        return set()


@st.cache_data(ttl=300)
def _build_zuzana_match_cache():
    """Pre-match all event companies against Zuzana's list. Returns set of matched company_name keys (lowercase)."""
    zuz = _load_zuzana_companies()
    if not zuz:
        return set()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT DISTINCT company_name FROM event_companies WHERE COALESCE(entity_status, 'pending') != 'rejected'"
    ).fetchall()
    conn.close()

    from thefuzz import fuzz
    matched = set()
    for r in rows:
        cn = dict(r)["company_name"]
        cn_norm = _normalize_company(cn)
        cn_key = cn.lower().strip()
        if cn_norm in zuz:
            matched.add(cn_key)
            continue
        for zc in zuz:
            if fuzz.token_sort_ratio(cn_norm, zc) >= 85:
                matched.add(cn_key)
                break
    return matched


def is_user_client(company_name, matched_account_id=None):
    """Check if a company is a client of the active user (Martin or Zuzana)."""
    user = st.session_state.get("active_user", "martin")
    if user == "martin":
        return bool(matched_account_id)
    else:
        key = company_name.lower().strip()
        return key in _build_zuzana_match_cache()


def get_company_full_enrichment(company_name, event_enrichment=None, account_enrichment=None):
    """Merge ALL enrichment sources for a company into a single dict."""
    result = {}
    cache_key = company_name.lower().strip()

    # 1) Event company enrichment (opportunity scores, etc.)
    if event_enrichment and cache_key in event_enrichment:
        result.update(event_enrichment[cache_key])

    # 2) Account enrichment JSON (rich signals: hiring, leadership, ARES, ZI, contacts)
    acct_enrich = _find_account_enrichment_entry(company_name, account_enrichment)
    if acct_enrich:
        result["_acct_enrichment"] = acct_enrich
        for field in ("ico", "official_name", "legal_form", "nace_codes",
                       "ares_segments", "ares_primary_segment", "ares_address",
                       "employee_count", "revenue", "zi_company_name", "zi_domain",
                       "zi_city", "zi_country", "hiring_signal", "total_jobs",
                       "engineering_hiring", "autodesk_tools_in_jobs",
                       "competitor_tools_in_jobs", "leadership_change",
                       "statutory_body", "has_public_contracts",
                       "public_contracts_count", "public_contracts_value_czk",
                       "revenue_czk", "profit_czk", "revenue_growth",
                       "hiring_intensity"):
            if field not in result or not result[field]:
                val = acct_enrich.get(field)
                if val is not None and val != "" and val != []:
                    result[field] = val

    # 3) Master list (products, seats, ACV, upsells, renewal)
    master = _load_master_list_lookup()
    master_entry = master.get(cache_key)
    if master_entry:
        result["_master"] = master_entry

    # 4) ZoomInfo company from enrichment_cache
    zi = get_cached_enrichment("company", cache_key)
    if not zi and acct_enrich and acct_enrich.get("zi_domain"):
        zi = get_cached_enrichment("company", acct_enrich["zi_domain"].lower())
    if zi:
        result["zi_company"] = zi
        if not result.get("employee_count") and zi.get("employee_count"):
            result["employee_count"] = zi["employee_count"]
        if not result.get("revenue") and zi.get("revenue"):
            result["revenue"] = zi["revenue"]

    # 5) Technographics from research_data
    _tech_key = re.sub(r"[^a-z0-9]+", "_", cache_key).strip("_")
    _tech_path = Path(__file__).resolve().parent / "research_data" / f"{_tech_key}.json"
    if _tech_path.exists():
        try:
            with open(_tech_path) as _tf:
                _rd = json.load(_tf)
            _techno = _rd.get("technographics", {})
            if isinstance(_techno, dict) and _techno.get("success"):
                result["autodesk_products_zi"] = _techno.get("autodesk_products", [])
                result["competitor_products_zi"] = _techno.get("competitor_products", [])
                result["cad_bim_other_zi"] = _techno.get("cad_bim_other", [])
                result["total_technologies"] = _techno.get("total_technologies", 0)
        except (json.JSONDecodeError, OSError):
            pass

    # 6) ARES from enrichment_cache
    ares = get_cached_enrichment("ares", cache_key)
    if not ares:
        ares = get_cached_enrichment("ares", "ares:" + cache_key)
    if not ares:
        conn = _db()
        row = conn.execute(
            "SELECT result_json FROM enrichment_cache WHERE lookup_type='ares' AND LOWER(lookup_key) LIKE ?",
            ("%" + cache_key + "%",),
        ).fetchone()
        conn.close()
        if row:
            ares = json.loads(dict(row)["result_json"])
    if ares:
        result["ares_data"] = ares

    return result


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    _lang_choice = st.radio("🌐", ["EN", "SK"], horizontal=True, label_visibility="collapsed", key="lang_toggle",
                            index=0 if st.session_state.get("lang", "en") == "en" else 1)
    st.session_state["lang"] = _lang_choice.lower()

    _user_choice = st.radio("👤", ["Martin", "Zuzana"], horizontal=True, label_visibility="collapsed", key="user_toggle",
                            index=0 if st.session_state.get("active_user", "martin") == "martin" else 1)
    st.session_state["active_user"] = _user_choice.lower()

    st.title(t("sidebar_title"))
    st.caption(t("sidebar_caption"))
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    stats = get_stats()
    m1, m2 = st.columns(2)
    m1.metric(t("accounts"), stats["accounts"])
    m2.metric(t("contacts"), stats["contacts"])
    m1.metric(t("events"), stats.get("events", 0))
    m2.metric(t("leads"), stats.get("event_leads", 0))
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    if not HAS_LLM_KEY:
        st.caption(t("mode_cursor"))
    else:
        st.caption(t("mode_api"))

    st.markdown('<p class="version-badge">{}</p>'.format(t("version")), unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_events, tab_companies, tab_contacts, tab_outreach, tab_log = st.tabs(
    [t("tab_overview"), t("tab_events"), t("tab_companies"), t("tab_contacts"), t("tab_outreach"), t("tab_activity")]
)


# =====================================================================
# TAB 1: OVERVIEW
# =====================================================================
with tab_overview:
    st.markdown(f"## {t('pipeline_overview')}")
    try:

        db_events = get_events()
        all_upcoming = get_upcoming_event_leads()
        outreach_log = get_outreach_log(limit=500)
        ev_enrich = _cached_event_enrichment()

        clients_at_events = len({
            dict(l)["company_name"].lower().strip()
            for l in all_upcoming if is_user_client(dict(l)["company_name"], dict(l).get("matched_account_id"))
        })
        non_clients = len({
            dict(l)["company_name"].lower().strip()
            for l in all_upcoming if not is_user_client(dict(l)["company_name"], dict(l).get("matched_account_id"))
        })
        enriched_count = sum(1 for v in ev_enrich.values() if v.get("opportunity_score"))
        outreach_sent = sum(1 for o in outreach_log if dict(o).get("status") == "sent")

        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric(t("event_companies"), len(set(dict(l)["company_name"].lower().strip() for l in all_upcoming)))
        k2.metric(t("your_clients"), clients_at_events)
        k3.metric(t("new_market"), non_clients)
        k4.metric(t("enriched"), enriched_count)
        k5.metric(t("events"), stats.get("events", 0))
        k6.metric(t("outreach_sent"), outreach_sent)

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

        c1, c2 = st.columns(2)

        with c1:
            st.markdown(f"##### {t('event_timeline')}")
            tl_data = []
            for key, cfg in SITE_EVENTS.items():
                start = cfg.get("event_date", "")
                end = cfg.get("event_end_date", start)
                seg = cfg.get("industry_focus", "mixed")
                if start:
                    tl_data.append({"Event": cfg["event_name"], "Start": start, "End": end, "Segment": seg.upper()})
            if tl_data:
                df_tl = pd.DataFrame(tl_data)
                df_tl["Start"] = pd.to_datetime(df_tl["Start"])
                df_tl["End"] = pd.to_datetime(df_tl["End"])
                df_tl = df_tl.sort_values("Start")
                fig_tl = px.timeline(df_tl, x_start="Start", x_end="End", y="Event", color="Segment",
                                     color_discrete_map={"AEC": "#4fc3f7", "D&M": "#ff8a65", "M&E": "#ab47bc", "MIXED": "#78909c"})
                fig_tl.update_yaxes(autorange="reversed")
                plotly_dark(fig_tl, 420)
                st.plotly_chart(fig_tl)
            else:
                st.info(t("no_events_configured"))

        with c2:
            st.markdown(f"##### {t('companies_by_class')}")
            class_counts = {}
            for l in all_upcoming:
                lc = dict(l).get("lead_class") or "unscored"
                class_counts[lc] = class_counts.get(lc, 0) + 1
            if class_counts:
                df_cls = pd.DataFrame([{"Class": k, "Count": v} for k, v in class_counts.items()])
                fig_cls = px.pie(df_cls, names="Class", values="Count", hole=0.55,
                                 color="Class",
                                 color_discrete_map={"whitespace": "#00e676", "new_market": "#ffca28", "displacement": "#ef5350", "unscored": "#78909c"})
                fig_cls.update_traces(textposition="inside", textinfo="percent+value")
                plotly_dark(fig_cls, 350)
                st.plotly_chart(fig_cls)

        c3, c4 = st.columns(2)

        with c3:
            st.markdown(f"##### {t('top_20_companies')}")
            scored = [(k, v.get("opportunity_score", 0), v.get("company_name", k)) for k, v in ev_enrich.items() if v.get("opportunity_score")]
            scored.sort(key=lambda x: x[1], reverse=True)
            top20 = scored[:20]
            if top20:
                df_top = pd.DataFrame([{"Company": t[2], "Score": t[1]} for t in top20])
                fig_top = px.bar(df_top, x="Score", y="Company", orientation="h",
                                 color="Score", color_continuous_scale=["#78909c", "#00bfa5"])
                fig_top.update_yaxes(autorange="reversed")
                plotly_dark(fig_top, 450)
                st.plotly_chart(fig_top)

        with c4:
            st.markdown(f"##### {t('upcoming_events')}")
            upcoming_events = []
            for key, cfg in SITE_EVENTS.items():
                d = days_until(cfg.get("event_date", ""))
                if d is not None and d >= 0:
                    upcoming_events.append((d, cfg))
            upcoming_events.sort(key=lambda x: x[0])
            for d, cfg in upcoming_events[:6]:
                st.markdown(
                    '<div class="countdown-card">'
                    '<div class="days">{}</div>'
                    '<div class="label">{}</div>'
                    '<div class="event-title">{}</div>'
                    '</div>'.format(d, t("days_away"), cfg["event_name"]),
                    unsafe_allow_html=True,
                )

    except Exception as _tab_err:
        import traceback as _tb
        st.error(t("overview_error").format(_tab_err))
        st.code(_tb.format_exc())


# =====================================================================
# TAB 2: EVENTS
# =====================================================================
with tab_events:
    st.markdown(f"## {t('events_heading')}")

    event_list = get_events()
    ev_enrich_events = _cached_event_enrichment()
    acct_enrich_events = _cached_account_enrichment()

    ev_options = []
    for key, cfg in SITE_EVENTS.items():
        if cfg.get("event_name", "").startswith("[ARCHIVED]"):
            continue
        d = days_until(cfg.get("event_date", ""))
        status = t("in_days").format(d) if d is not None and d >= 0 else t("passed")
        ev_options.append({"key": key, "cfg": cfg, "days": d, "status": status})
    ev_options.sort(key=lambda x: x["days"] if x["days"] is not None and x["days"] >= 0 else 9999)

    ev_labels = ["{} --- {} ({})".format(e["cfg"]["event_name"], e["cfg"].get("event_date", "TBD"), e["status"]) for e in ev_options]
    sel_ev_idx = st.selectbox(t("select_event"), range(len(ev_labels)),
                              format_func=lambda i: ev_labels[i], key="ev_picker")

    if sel_ev_idx is not None and ev_options:
        sel_cfg = ev_options[sel_ev_idx]["cfg"]
        sel_key = ev_options[sel_ev_idx]["key"]
        sel_days = ev_options[sel_ev_idx]["days"]
        seg = sel_cfg.get("industry_focus", "mixed")

        org_parts = []
        org_contacts = sel_cfg.get("organizer_contacts", {})
        if isinstance(org_contacts, dict):
            for org_name, org_info in org_contacts.items():
                p = org_name
                if isinstance(org_info, dict):
                    if org_info.get("email"):
                        p += " &middot; " + org_info["email"]
                    if org_info.get("phone"):
                        p += " &middot; " + org_info["phone"]
                org_parts.append(p)
        org_html = ""
        if org_parts:
            sep = " | "
            org_html = '<div style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.06);font-size:0.78rem;color:#6e7681">{} {}</div>'.format(t("organizer"), sep.join(org_parts))

        st.markdown(
            '<div class="event-card">'
            '<h4>{name} {seg}</h4>'
            '<div class="meta">'
            '<strong>{date}</strong> &middot; {loc} &middot; {price} &middot; {days}'
            '</div>'
            '<div style="margin-top:6px;font-size:0.85rem;color:#8b949e">{desc}</div>'
            '{org}'
            '</div>'.format(
                name=sel_cfg["event_name"], seg=segment_badge(seg),
                date=sel_cfg.get("event_date", "TBD"), loc=sel_cfg.get("location", "TBD"),
                price=sel_cfg.get("price", ""), days=ev_options[sel_ev_idx]["status"],
                desc=sel_cfg.get("description", "")[:200], org=org_html,
            ),
            unsafe_allow_html=True,
        )

        ev_row = next((dict(e) for e in event_list if dict(e)["event_name"] == sel_cfg["event_name"]), None)
        if ev_row:
            companies_raw = get_event_companies(event_id=ev_row["id"])
            companies_all_unfiltered = [dict(c) for c in companies_raw]

            show_rejected = st.checkbox(t("show_rejected"), value=False, key="ev_show_rejected")
            if show_rejected:
                companies_all = companies_all_unfiltered
            else:
                companies_all = [c for c in companies_all_unfiltered if c.get("entity_status") != "rejected"]

            rejected_count = sum(1 for c in companies_all_unfiltered if c.get("entity_status") == "rejected")
            clients_ev = [c for c in companies_all if is_user_client(c["company_name"], c.get("matched_account_id"))]
            new_mkt_ev = [c for c in companies_all if not is_user_client(c["company_name"], c.get("matched_account_id"))]
            _HISTORICAL_ROLES = {"attended_2025", "exhibited_2025", "past_attendee"}
            confirmed_ev = [c for c in companies_all if c.get("role", "") not in _HISTORICAL_ROLES]
            historical_ev = [c for c in companies_all if c.get("role", "") in _HISTORICAL_ROLES]

            m1, m2, m3, m4, m5, m6 = st.columns(6)
            m1.metric(t("companies_metric"), len(companies_all))
            m2.metric(t("confirmed_2026"), len(confirmed_ev))
            m3.metric(t("last_year"), len(historical_ev))
            m4.metric(t("your_clients"), len(clients_ev))
            scored_count = sum(1 for c in companies_all if ev_enrich_events.get(c["company_name"].lower().strip(), {}).get("opportunity_score"))
            m5.metric(t("scored"), scored_count)
            m6.metric(t("rejected"), rejected_count)

            st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

            ev_view = st.radio(t("view"), [t("view_all"), t("view_confirmed"), t("view_last_year"), t("view_clients"), t("view_new_market")], horizontal=True, key="ev_view_filter")

            if ev_view == t("view_clients"):
                show_companies = clients_ev
            elif ev_view == t("view_new_market"):
                show_companies = new_mkt_ev
            elif ev_view == t("view_confirmed"):
                show_companies = confirmed_ev
            elif ev_view == t("view_last_year"):
                show_companies = historical_ev
            else:
                show_companies = companies_all

            cc = t("col_company")
            ca = t("col_attendance")
            cs = t("col_status")
            ct_type = t("col_type")
            cos = t("col_opp_score")
            ced = t("col_est_deal")
            cem = t("col_employees")
            cseg = t("col_segment")
            cpr = t("col_products")
            cse = t("col_seats")
            cad = t("col_adsk_detected")
            ccompd = t("col_competitor_detected")
            clc = t("col_likely_cad")
            cdo = t("col_domain")
            csi = t("col_signals")

            table_rows = []
            for c in show_companies:
                cn = c["company_name"]
                ck = cn.lower().strip()
                enr = get_company_full_enrichment(cn, ev_enrich_events, acct_enrich_events)
                master = enr.get("_master", {})
                acct = enr.get("_acct_enrichment", {})

                signals = []
                if enr.get("hiring_signal"):
                    signals.append(t("sig_hiring").format(enr.get("hiring_intensity", "")))
                if enr.get("engineering_hiring"):
                    signals.append(t("sig_eng_hiring"))
                if enr.get("autodesk_tools_in_jobs"):
                    tools = enr["autodesk_tools_in_jobs"]
                    signals.append(t("sig_adsk_tools").format(", ".join(tools) if isinstance(tools, list) else str(tools)))
                if enr.get("competitor_tools_in_jobs"):
                    ctools = enr["competitor_tools_in_jobs"]
                    signals.append(t("sig_competitor").format(", ".join(ctools) if isinstance(ctools, list) else str(ctools)))
                if enr.get("leadership_change"):
                    signals.append(t("sig_leadership_change"))
                if enr.get("has_public_contracts"):
                    signals.append(t("sig_public_contracts"))

                entity_st = c.get("entity_status", "pending")
                if entity_st == "verified":
                    status_label = tval(ENTITY_STATUS_LABELS, "verified")
                elif entity_st == "rejected":
                    reason = c.get("reject_reason", "")
                    status_label = tval(ENTITY_STATUS_LABELS, "rejected") + (" ({})".format(reason) if reason else "")
                else:
                    status_label = tval(ENTITY_STATUS_LABELS, entity_st) or (entity_st.replace("_", " ").title() if entity_st else "")

                role = c.get("role", "")
                if role in _HISTORICAL_ROLES:
                    attendance = tval(ATTENDANCE_LABELS, "last_year")
                else:
                    attendance = tval(ATTENDANCE_LABELS, "confirmed")

                all_adsk = set(enr.get("autodesk_products_zi", []))
                all_adsk.update(enr.get("website_autodesk_mentions", []))
                all_adsk.update(enr.get("autodesk_tools_in_jobs", []))
                all_comp = set(enr.get("competitor_products_zi", []))
                all_comp.update(enr.get("website_competitor_mentions", []))
                all_comp.update(enr.get("competitor_tools_in_jobs", []))
                nace_inf = enr.get("nace_tech_inference", "")

                table_rows.append({
                    cc: cn,
                    ca: attendance,
                    cs: status_label,
                    ct_type: tval(TYPE_LABELS, "client") if is_user_client(cn, c.get("matched_account_id")) else tval(TYPE_LABELS, "new"),
                    cos: enr.get("opportunity_score", ""),
                    ced: enr.get("estimated_deal_eur", master.get("potential_acv_eur", "")),
                    cem: enr.get("employee_count", ""),
                    cseg: enr.get("ares_primary_segment", master.get("primary_segment", "")),
                    cpr: master.get("current_products", "---"),
                    cse: master.get("total_seats", ""),
                    cad: ", ".join(sorted(all_adsk)) if all_adsk else "",
                    ccompd: ", ".join(sorted(all_comp)) if all_comp else "",
                    clc: nace_inf,
                    cdo: enr.get("zi_domain", master.get("website", c.get("company_domain", ""))),
                    csi: " | ".join(signals) if signals else "",
                    "_enr": enr,
                    "_raw": c,
                })

            table_rows.sort(key=lambda x: -(x[cos] if isinstance(x[cos], (int, float)) else 0))

            if table_rows:
                df_ev = pd.DataFrame(table_rows)
                for _nc in [cos, ced, cem, cse]:
                    df_ev[_nc] = pd.to_numeric(df_ev[_nc], errors="coerce")
                display_ev_cols = [cc, ca, cs, ct_type, cos, ced, cem, cseg, cpr, cse, cad, ccompd, clc, cdo, csi]
                st.dataframe(df_ev[display_ev_cols], hide_index=True, width="stretch", height=450)

                st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
                st.markdown(f"##### {t('company_deep_dive')}")

                company_names_ev = [r[cc] for r in table_rows]
                sel_ev_company = st.selectbox(t("select_company"), company_names_ev, key="ev_company_sel")

                if sel_ev_company:
                    sel_row = next((r for r in table_rows if r[cc] == sel_ev_company), None)
                    if sel_row:
                        enr = sel_row["_enr"]
                        master = enr.get("_master", {})
                        acct = enr.get("_acct_enrichment", {})

                        col_id, col_sig = st.columns(2)

                        with col_id:
                            id_parts = ['<div class="detail-card"><h4>{}</h4>'.format(sel_ev_company)]
                            id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_type"), sel_row[ct_type]))
                            if enr.get("ico"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_ico"), enr["ico"]))
                            if enr.get("zi_domain") or master.get("website"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_domain"), enr.get("zi_domain", master.get("website", ""))))
                            if enr.get("ares_address"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_address"), enr["ares_address"]))
                            seg_val = enr.get("ares_primary_segment", master.get("primary_segment", ""))
                            if seg_val:
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_segment"), seg_val))
                            if enr.get("employee_count"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_employees"), enr["employee_count"]))
                            if enr.get("revenue"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">${}K</div>'.format(t("lbl_revenue_usd"), enr["revenue"]))
                            if enr.get("opportunity_score"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}/100</div>'.format(t("lbl_opp_score"), enr["opportunity_score"]))
                            if enr.get("estimated_deal_eur"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">EUR {}</div>'.format(t("lbl_est_deal"), enr["estimated_deal_eur"]))

                            # Autodesk relationship
                            if master.get("current_products"):
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{} ({} seats)</div>'.format(t("lbl_autodesk_products"), master["current_products"], master.get("total_seats", "?")))
                                if master.get("current_acv_eur"):
                                    id_parts.append('<div class="detail-label">{}</div><div class="detail-value">EUR {}</div>'.format(t("lbl_current_acv"), master["current_acv_eur"]))
                                if master.get("nearest_renewal"):
                                    id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{} ({} {})</div>'.format(t("lbl_next_renewal"), master["nearest_renewal"], master.get("days_to_renewal", "?"), t("days")))
                                if master.get("top_upsell"):
                                    id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_top_upsell"), master["top_upsell"]))
                                    if master.get("top_upsell_reason"):
                                        id_parts.append('<div style="font-size:0.78rem;color:#8b949e;padding:0 12px 4px">{}</div>'.format(master["top_upsell_reason"]))
                                if master.get("all_upsells"):
                                    id_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_all_upsells"), master["all_upsells"]))
                            else:
                                id_parts.append('<div class="detail-label">{}</div><div class="detail-value" style="color:#ffca28">{}</div>'.format(t("lbl_autodesk"), t("lbl_new_market_no_products")))

                            id_parts.append('</div>')
                            st.markdown("".join(id_parts), unsafe_allow_html=True)

                        with col_sig:
                            sig_parts = ['<div class="detail-card"><h4>{}</h4>'.format(t("lbl_signals_intelligence"))]

                            if enr.get("hiring_signal"):
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(
                                    t("lbl_hiring"),
                                    t("sig_intensity").format(enr.get("hiring_intensity", "unknown"), enr.get("total_jobs", 0))))
                                if enr.get("engineering_hiring"):
                                    sig_parts.append('<div style="font-size:0.78rem;color:#00e676;padding:0 12px 4px">{}</div>'.format(t("sig_eng_roles_detected")))
                            else:
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value" style="color:#546e7a">{}</div>'.format(t("lbl_hiring"), t("sig_no_hiring")))

                            adsk_tools = enr.get("autodesk_tools_in_jobs", [])
                            if adsk_tools:
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value" style="color:#00e676">{}</div>'.format(
                                    t("lbl_adsk_tools_in_jobs"),
                                    ", ".join(adsk_tools) if isinstance(adsk_tools, list) else str(adsk_tools)))

                            comp_tools = enr.get("competitor_tools_in_jobs", [])
                            if comp_tools:
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value" style="color:#ef5350">{}</div>'.format(
                                    t("lbl_competitor_tools_in_jobs"),
                                    ", ".join(comp_tools) if isinstance(comp_tools, list) else str(comp_tools)))

                            _all_adsk = set(enr.get("autodesk_products_zi", []))
                            _all_adsk.update(enr.get("website_autodesk_mentions", []))
                            _all_adsk.update(enr.get("autodesk_tools_in_jobs", []))
                            _all_comp = set(enr.get("competitor_products_zi", []))
                            _all_comp.update(enr.get("website_competitor_mentions", []))
                            _all_comp.update(enr.get("competitor_tools_in_jobs", []))
                            _cad_other = enr.get("cad_bim_other_zi", [])
                            _nace_inf = enr.get("nace_tech_inference", "")
                            _web_cad = enr.get("website_cad_mentions", [])

                            if _all_adsk or _all_comp or _cad_other or _nace_inf or _web_cad:
                                sig_parts.append('<div style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.06)"></div>')
                                sig_parts.append('<div class="detail-label">{}</div>'.format(t("lbl_tech_stack")))
                            if _all_adsk:
                                sig_parts.append('<div class="detail-value" style="color:#00e676">{} {}</div>'.format(t("lbl_autodesk_tech"), ", ".join(sorted(_all_adsk))))
                            if _all_comp:
                                sig_parts.append('<div class="detail-value" style="color:#ef5350">{} {}</div>'.format(t("lbl_competitors_tech"), ", ".join(sorted(_all_comp))))
                            if _cad_other:
                                sig_parts.append('<div class="detail-value" style="color:#8b949e">{} {}</div>'.format(t("lbl_other_cad"), ", ".join(_cad_other)))
                            if _nace_inf:
                                sig_parts.append('<div class="detail-value" style="color:#ffca28">{} {}</div>'.format(t("lbl_nace_inference"), _nace_inf))
                            if _web_cad:
                                sig_parts.append('<div class="detail-value" style="color:#8b949e">{} {}</div>'.format(t("lbl_website_mentions"), ", ".join(_web_cad)))

                            _tech_sources = []
                            if enr.get("autodesk_products_zi") or enr.get("competitor_products_zi"):
                                _tech_sources.append("ZoomInfo")
                            if enr.get("website_autodesk_mentions") or enr.get("website_competitor_mentions") or _web_cad:
                                _tech_sources.append("Website")
                            if enr.get("autodesk_tools_in_jobs") or enr.get("competitor_tools_in_jobs"):
                                _tech_sources.append("Jobs")
                            if _nace_inf:
                                _tech_sources.append("NACE")
                            if _tech_sources:
                                sig_parts.append('<div style="font-size:0.7rem;color:#546e7a;padding:2px 12px">{} {}</div>'.format(t("lbl_sources"), ", ".join(_tech_sources)))

                            if enr.get("leadership_change"):
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value" style="color:#ffca28">{}</div>'.format(t("lbl_leadership"), t("sig_recent_change")))
                            if enr.get("statutory_body"):
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_statutory_body"), enr["statutory_body"]))

                            if enr.get("has_public_contracts"):
                                _pc_cnt = enr.get("public_contracts_count", "?")
                                _pc_txt = t("sig_contracts_fmt").format(_pc_cnt)
                                if enr.get("public_contracts_value_czk"):
                                    _pc_txt += t("sig_contracts_czk").format(enr.get("public_contracts_value_czk"))
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(
                                    t("lbl_public_contracts"), _pc_txt))

                            if enr.get("revenue_growth"):
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}%</div>'.format(t("lbl_revenue_growth"), enr["revenue_growth"]))

                            nace = enr.get("nace_codes", [])
                            if nace:
                                nace_str = ", ".join(nace[:8]) if isinstance(nace, list) else str(nace)
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_nace_codes"), nace_str))

                            enr_sources = (acct.get("enrichment_sources") or []) if acct else []
                            if enr_sources:
                                sig_parts.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_data_sources"), ", ".join(enr_sources)))

                            sig_parts.append('</div>')
                            st.markdown("".join(sig_parts), unsafe_allow_html=True)

                        # Decision makers / contacts for this company
                        st.markdown(f"##### {t('decision_makers')}")
                        co_contacts = get_company_contacts(sel_ev_company)

                        if co_contacts:
                            for ct in co_contacts:
                                ct["_priority_score"] = score_contact_priority(ct, enr)
                                ct["_priority_label"] = _priority_label(ct["_priority_score"])
                            co_contacts.sort(key=lambda x: -x["_priority_score"])

                            ct_rows = []
                            for ct in co_contacts:
                                pl = ct["_priority_label"]
                                _em = ct.get("email", "")
                                if ct.get("email_guessed"):
                                    _em = "{} [guessed]".format(_em)
                                ct_rows.append({
                                    t("priority_filter"): "{} ({})".format(tval(PRIORITY_LABELS, pl), ct["_priority_score"]),
                                    t("col_name"): ct.get("name", ""),
                                    t("col_title"): ct.get("title", ""),
                                    t("lbl_email"): _em,
                                    t("lbl_phone"): ct.get("phone", ""),
                                    t("lbl_source"): ct.get("source", ""),
                                })
                            df_contacts_ev = pd.DataFrame(ct_rows)
                            st.dataframe(df_contacts_ev, hide_index=True, width="stretch")
                        else:
                            st.caption(t("no_contacts"))

                        # Contact Playbook
                        _playbook_data = _cached_contact_playbook()
                        _pb_key = sel_ev_company.lower().strip()
                        _pb_entry = _playbook_data.get(_pb_key, {})

                        if _pb_entry and _pb_entry.get("prioritized_contacts"):
                            st.markdown(f"##### {t('contact_playbook')}")

                            play_type = _pb_entry.get("play_type", "greenfield")
                            play_css = {"upsell": "play-upsell", "displacement": "play-displacement", "greenfield": "play-greenfield"}.get(play_type, "play-greenfield")
                            play_label = tval(PLAY_TYPE_LABELS, play_type) or play_type.title()

                            pb_header = ['<div class="playbook-card">']
                            pb_header.append('<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px">')
                            pb_header.append('<h4 style="margin:0">{}</h4>'.format(_pb_entry.get("company_name", sel_ev_company)))
                            pb_header.append('<span class="play-badge {}">{}</span>'.format(play_css, play_label))
                            pb_header.append('</div>')

                            pd_details = _pb_entry.get("play_details", {})
                            if pd_details.get("current_products"):
                                pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_current_products"), pd_details["current_products"]))
                            if pd_details.get("top_upsell"):
                                pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_top_upsell"), pd_details["top_upsell"]))
                            if pd_details.get("competitor_tools"):
                                pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_competitor_tools"), ", ".join(pd_details["competitor_tools"])))
                            if pd_details.get("signals"):
                                sig_html = " ".join('<span class="signal-tag">{}</span>'.format(s) for s in pd_details["signals"])
                                pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("col_signals"), sig_html))
                            if pd_details.get("nearest_renewal"):
                                pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{} ({} {})</div>'.format(
                                    t("lbl_renewal"), pd_details["nearest_renewal"], pd_details.get("days_to_renewal", "?"), t("days")))
                            _adv_sigs = pd_details.get("advanced_signals", [])
                            if _adv_sigs:
                                pb_header.append('<div style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.06)"></div>')
                                pb_header.append('<div class="detail-label">{}</div>'.format(t("lbl_advanced_intelligence").format(pd_details.get("advanced_signals_count", len(_adv_sigs)))))
                                for _as in _adv_sigs:
                                    pb_header.append('<div style="font-size:0.82rem;color:#c9d1d9;padding:3px 0 3px 12px;border-left:2px solid rgba(0,191,165,0.3);margin-bottom:3px">{}</div>'.format(_as))
                            if pd_details.get("certifications"):
                                cert_html = " ".join('<span class="product-tag">{}</span>'.format(c) for c in pd_details["certifications"][:4])
                                pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_certifications"), cert_html))
                            if pd_details.get("hiring_product_needs"):
                                hpn_html = " ".join('<span class="product-tag">{}</span>'.format(p) for p in pd_details["hiring_product_needs"][:5])
                                pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_hiring_product_needs"), hpn_html))

                            pb_header.append('</div>')
                            st.markdown("".join(pb_header), unsafe_allow_html=True)

                            # Expansion plays
                            exp_plays = _pb_entry.get("expansion_plays", [])
                            if exp_plays:
                                with st.expander(t("expansion_plays").format(len(exp_plays)), expanded=False):
                                    for ep in exp_plays:
                                        ep_html = ['<div class="play-detail">']
                                        ep_html.append('<div class="play-detail-title">{}</div>'.format(ep.get("name", "")))
                                        ep_html.append('<div class="play-detail-body"><strong>{}</strong> {}</div>'.format(t("sell"), ep.get("what_to_sell", "")))
                                        ep_html.append('<div class="play-detail-body"><strong>{}</strong> {}</div>'.format(t("why_now"), ep.get("why_now", "")))
                                        disc_q = ep.get("discovery_q", [])
                                        if disc_q:
                                            ep_html.append('<div class="play-detail-body" style="margin-top:4px"><strong>{}</strong></div>'.format(t("discovery")))
                                            for dq in disc_q:
                                                ep_html.append('<div class="play-detail-body" style="padding-left:12px">• {}</div>'.format(dq))
                                        ep_html.append('</div>')
                                        st.markdown("".join(ep_html), unsafe_allow_html=True)

                            # Prioritized contacts with personas
                            for pc in _pb_entry["prioritized_contacts"]:
                                prio = pc.get("priority", "low")
                                prio_css = {"high": "persona-high", "medium": "persona-medium", "low": "persona-low"}.get(prio, "persona-low")
                                pc_html = ['<div class="persona-card">']
                                pc_html.append('<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">')
                                pc_html.append('<span class="persona-name">{}</span>'.format(pc.get("name", "")))
                                pc_html.append('<span class="persona-label {}">{}</span>'.format(prio_css, tval(PRIORITY_LABELS, prio)))
                                pc_html.append('</div>')
                                pc_html.append('<div class="persona-title">{} &middot; {}</div>'.format(pc.get("title", ""), pc.get("persona_label", "")))

                                if pc.get("email"):
                                    pc_html.append('<div style="font-size:0.78rem;color:#4fc3f7;margin-top:2px">{}</div>'.format(pc["email"]))

                                pc_html.append('<div style="font-size:0.82rem;color:#c9d1d9;margin-top:6px">{}</div>'.format(pc.get("relevance", "")))

                                pain_pts = pc.get("pain_points", [])
                                if pain_pts:
                                    pc_html.append('<div style="margin-top:8px;font-size:0.78rem;color:#8b949e;font-weight:600">{}</div>'.format(t("lbl_pain_points")))
                                    for pp in pain_pts[:4]:
                                        pc_html.append('<div class="pain-point">{}</div>'.format(pp))

                                if pc.get("talk_track"):
                                    pc_html.append('<div class="talk-track">{}</div>'.format(pc["talk_track"]))

                                hooks = pc.get("signal_hooks", [])
                                if hooks:
                                    pc_html.append('<div style="margin-top:6px;font-size:0.78rem;color:#8b949e;font-weight:600">{}</div>'.format(t("lbl_signal_hooks")))
                                    for sh in hooks:
                                        pc_html.append('<div style="font-size:0.82rem;color:#c9d1d9;padding:2px 0 2px 12px;border-left:2px solid rgba(255,202,40,0.3);margin-bottom:4px">{}</div>'.format(sh))

                                prods = pc.get("recommended_products", [])
                                if prods:
                                    pc_html.append('<div style="margin-top:6px">')
                                    for pr in prods:
                                        pc_html.append('<span class="product-tag">{}</span>'.format(pr))
                                    pc_html.append('</div>')

                                pc_html.append('</div>')
                                st.markdown("".join(pc_html), unsafe_allow_html=True)

                        # Deep Research Report
                        _report_key = re.sub(r'[^\w\-.]', '_', sel_ev_company.lower().strip())[:80]
                        _report_path = Path(__file__).resolve().parent / "reports" / f"{_report_key}.md"
                        if _report_path.exists():
                            with st.expander(t("deep_research"), expanded=False):
                                _report_md = _report_path.read_text(encoding="utf-8")
                                st.markdown(_report_md, unsafe_allow_html=True)
                        else:
                            st.caption(t("no_research"))

    # ── Research Reports Browser ──
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
    st.markdown(f"### {t('research_reports')}")

    _reports_dir = Path(__file__).resolve().parent / "reports"
    _all_reports = sorted(_reports_dir.glob("*.md")) if _reports_dir.exists() else []
    if _all_reports:
        st.caption(t("reports_available").format(len(_all_reports)))
        _report_names = [p.stem.replace("_", " ").strip() for p in _all_reports]
        _sel_report_idx = st.selectbox(
            t("select_report"),
            range(len(_report_names)),
            format_func=lambda i: _report_names[i].title(),
            key="ev_report_browser",
        )
        if _sel_report_idx is not None:
            _sel_report_path = _all_reports[_sel_report_idx]
            with st.expander(t("view_report").format(_report_names[_sel_report_idx].title()), expanded=True):
                st.markdown(_sel_report_path.read_text(encoding="utf-8"), unsafe_allow_html=True)
    else:
        st.caption(t("no_reports"))


# =====================================================================
# TAB 3: COMPANIES
# =====================================================================
with tab_companies:
    st.markdown(f"## {t('companies_heading')}")

    all_companies = get_companies_with_events()
    ev_enrich_co = _cached_event_enrichment()

    if all_companies:
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            search_co = st.text_input(t("search_company"), key="co_search", placeholder=t("search_placeholder"))
        with f2:
            event_names = sorted({ev for row in all_companies for ev in (dict(row).get("events") or "").split(",") if ev.strip()})
            event_filter = st.selectbox(t("filter_by_event"), [t("status_all")] + event_names, key="co_ev_filter")
        with f3:
            client_filter = st.selectbox(t("status_filter"), [t("status_all"), t("status_clients"), t("status_non_clients")], key="co_cl_filter")
        with f4:
            min_opp = st.slider(t("min_opp_score"), 0, 100, 0, key="co_opp_slider")

        filtered = []
        for row in all_companies:
            rd = dict(row)
            key = rd.get("company_key", rd["company_name"].lower().strip())
            enrich = ev_enrich_co.get(key, {})
            opp_score = enrich.get("opportunity_score", 0)

            if search_co and search_co.lower() not in rd["company_name"].lower():
                continue
            if event_filter != t("status_all") and event_filter not in (rd.get("events") or ""):
                continue
            _is_cli = is_user_client(rd["company_name"], rd.get("matched_account_id"))
            if client_filter == t("status_clients") and not _is_cli:
                continue
            if client_filter == t("status_non_clients") and _is_cli:
                continue
            if opp_score < min_opp:
                continue

            rd["_opp_score"] = opp_score
            rd["_segment"] = enrich.get("primary_segment", enrich.get("ares_primary_segment", ""))
            rd["_deal_eur"] = enrich.get("estimated_deal_eur", 0)
            rd["_employees"] = enrich.get("employee_count", "")
            rd["_signals"] = len(enrich.get("timing_signals", []))
            filtered.append(rd)

        filtered.sort(key=lambda x: x["_opp_score"], reverse=True)
        st.caption(t("showing_of").format(len(filtered), len(all_companies)))

        if filtered:
            _cco = t("col_company")
            _cev = t("col_events")
            _cnev = t("col_num_events")
            _cclass = t("col_class")
            _ccli = t("col_client")
            _cos = t("col_opp_score")
            _cseg = t("col_segment")
            _ced = t("col_est_deal")
            _cem = t("col_employees")
            _csig = t("col_signals")

            display_data = []
            for rd in filtered[:300]:
                display_data.append({
                    _cco: rd["company_name"],
                    _cev: rd.get("events", ""),
                    _cnev: rd.get("event_count", 0),
                    _cclass: rd.get("lead_class", ""),
                    _ccli: tval(YES_NO, "yes") if is_user_client(rd["company_name"], rd.get("matched_account_id")) else tval(YES_NO, "no"),
                    _cos: rd["_opp_score"],
                    _cseg: rd["_segment"],
                    _ced: rd["_deal_eur"],
                    _cem: rd["_employees"],
                    _csig: rd["_signals"],
                })

            df_co = pd.DataFrame(display_data)
            for _nc in [_cos, _ced, _cem, _csig, _cnev]:
                if _nc in df_co.columns:
                    df_co[_nc] = pd.to_numeric(df_co[_nc], errors="coerce")
            st.dataframe(df_co, hide_index=True, width="stretch", height=400)

            # --- Company drill-down ---
            st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
            st.markdown(f"##### {t('company_profile')}")

            company_names = [rd[_cco] for rd in display_data]
            selected_company = st.selectbox(t("select_company"), company_names, key="company_detail_sel")

            if selected_company:
                co_row = next((rd for rd in filtered if rd["company_name"] == selected_company), {})
                key = co_row.get("company_key", selected_company.lower().strip())
                enrich = get_company_full_enrichment(selected_company, ev_enrich_co, _cached_account_enrichment())
                contacts = get_company_contacts(selected_company)

                col_info, col_signals = st.columns([1, 1])

                with col_info:
                    # Identity
                    status_badge = lead_class_badge(co_row.get("lead_class", ""))
                    opp = enrich.get("opportunity_score", 0)
                    lines = ['<div class="detail-card">']
                    lines.append('<h3>{} {}</h3>'.format(selected_company, status_badge))

                    if co_row.get("matched_account_id"):
                        acct_row = get_account(co_row["matched_account_id"])
                        acct = dict(acct_row) if acct_row else None
                        if acct:
                            if acct.get("autodesk_products"):
                                prods = ", ".join(normalize_products(acct["autodesk_products"]))
                                lines.append('<div class="detail-label">{}</div>'.format(t("lbl_current_autodesk_products")))
                                lines.append('<div class="detail-value">{}</div>'.format(prods))
                            if acct.get("industry"):
                                lines.append('<div class="detail-label">{}</div>'.format(t("lbl_industry")))
                                lines.append('<div class="detail-value">{}</div>'.format(acct["industry"]))
                            upsells = recommend_upsell(acct.get("autodesk_products", ""), industry_segment=detect_industry_segment(acct.get("industry", "")))
                            if upsells:
                                lines.append('<div class="detail-label">{}</div>'.format(t("lbl_whitespace_opportunities")))
                                upsell_items = []
                                for u in upsells[:5]:
                                    if isinstance(u, dict):
                                        upsell_items.append("{} - {}".format(u.get("product", ""), u.get("reason", "")))
                                    else:
                                        upsell_items.append(str(u))
                                lines.append('<div class="detail-value">{}</div>'.format("<br>".join(upsell_items)))

                    lines.append('<div class="detail-label">{}</div>'.format(t("lbl_opp_score")))
                    lines.append('<div class="detail-value">{}/100 {}</div>'.format(opp, _score_bar_html(opp)))

                    deal = enrich.get("estimated_deal_eur", 0)
                    if deal:
                        lines.append('<div class="detail-label">{}</div>'.format(t("lbl_estimated_deal_size")))
                        lines.append('<div class="detail-value">EUR {:,.0f}</div>'.format(deal))

                    seg = enrich.get("primary_segment", enrich.get("ares_primary_segment", ""))
                    if seg:
                        lines.append('<div class="detail-label">{}</div>'.format(t("lbl_primary_segment")))
                        lines.append('<div class="detail-value">{} {}</div>'.format(seg, segment_badge(seg)))

                    if co_row.get("company_domain"):
                        lines.append('<div class="detail-label">{}</div>'.format(t("lbl_domain")))
                        lines.append('<div class="detail-value">{}</div>'.format(co_row["company_domain"]))

                    lines.append('<div class="detail-label">{}</div>'.format(t("lbl_events_label")))
                    lines.append('<div class="detail-value">{}</div>'.format(co_row.get("events", "---")))

                    # ARES data
                    ares = enrich.get("ares_data", {})
                    if not ares and enrich.get("ico"):
                        ares = enrich
                    if ares and (ares.get("ico") or ares.get("success")):
                        lines.append('<div class="detail-label">{}</div>'.format(t("lbl_ares")))
                        ico = ares.get("ico", enrich.get("ico", ""))
                        official = ares.get("official_name", enrich.get("official_name", ""))
                        lf = ares.get("legal_form", enrich.get("legal_form", ""))
                        addr = ares.get("address", enrich.get("ares_address", ""))
                        ares_info = "ICO: {} | {} | {} {}".format(ico, official, t("legal_form"), lf)
                        if addr:
                            ares_info += "<br>" + addr
                        lines.append('<div class="detail-value">{}</div>'.format(ares_info))

                    lines.append('</div>')
                    st.markdown("".join(lines), unsafe_allow_html=True)

                with col_signals:
                    sig_lines = ['<div class="detail-card"><h4>{}</h4>'.format(t("lbl_signals_intelligence"))]

                    emp = enrich.get("employee_count")
                    rev = enrich.get("revenue")
                    if emp or rev:
                        sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_company_size")))
                        parts = []
                        if emp:
                            parts.append("{}: {:,}".format(t("lbl_employees"), emp) if isinstance(emp, (int, float)) else "{}: {}".format(t("lbl_employees"), emp))
                        if rev and isinstance(rev, (int, float)):
                            parts.append("Revenue: ${:,.0f}".format(rev))
                        sig_lines.append('<div class="detail-value">{}</div>'.format(" &middot; ".join(parts)))

                    zi = enrich.get("zi_company", {})
                    if zi:
                        loc_parts = [p for p in [zi.get("city", ""), zi.get("state", ""), zi.get("country", "")] if p]
                        if loc_parts:
                            sep = ", "
                            sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_location_zi")))
                            sig_lines.append('<div class="detail-value">{}</div>'.format(sep.join(loc_parts)))
                        if zi.get("industry"):
                            sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_industry")))
                            sub = zi.get("sub_industry", "")
                            ind_text = zi["industry"]
                            if sub:
                                ind_text += " > " + sub
                            sig_lines.append('<div class="detail-value">{}</div>'.format(ind_text))

                    timing = enrich.get("timing_signals", [])
                    if timing:
                        sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_timing_signals")))
                        tags = "".join('<span class="signal-tag">{}</span>'.format(s) for s in timing)
                        sig_lines.append('<div class="detail-value">{}</div>'.format(tags))

                    if enrich.get("hiring_signal"):
                        sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_hiring")))
                        hire_parts = [t("sig_total_jobs").format(enrich.get("total_jobs", 0))]
                        if enrich.get("engineering_hiring"):
                            hire_parts.append(t("sig_eng_roles_open"))
                        adsk_tools = enrich.get("autodesk_tools_in_jobs", [])
                        if adsk_tools:
                            hire_parts.append(t("sig_adsk_tools_postings") + ", ".join(adsk_tools))
                        comp_tools = enrich.get("competitor_tools_in_jobs", [])
                        if comp_tools:
                            hire_parts.append(t("sig_competitor_tools") + ", ".join(comp_tools))
                        sig_lines.append('<div class="detail-value">{}</div>'.format("<br>".join(hire_parts)))

                    if enrich.get("leadership_change"):
                        sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_leadership")))
                        sig_lines.append('<div class="detail-value">{}</div>'.format(t("sig_recent_change")))

                    if enrich.get("has_public_contracts"):
                        sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_public_contracts")))
                        cnt = enrich.get("public_contracts_count", 0)
                        val = enrich.get("public_contracts_value_czk", 0)
                        pc_text = t("sig_contracts_fmt").format(cnt)
                        if val:
                            pc_text += t("sig_contracts_czk").format(val)
                        sig_lines.append('<div class="detail-value">{}</div>'.format(pc_text))

                    if enrich.get("revenue_growth") is not None:
                        sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_revenue_growth")))
                        sig_lines.append('<div class="detail-value">{:+.1f}%</div>'.format(enrich["revenue_growth"]))

                    scores = enrich.get("component_scores", {})
                    if scores:
                        sig_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_score_breakdown")))
                        for comp, val in scores.items():
                            pct = int(val * 100)
                            sig_lines.append('<div style="font-size:0.8rem;color:#8b949e;margin-bottom:2px">{}: {}%</div>'.format(comp.replace("_", " ").title(), pct))

                    sig_lines.append('</div>')
                    st.markdown("".join(sig_lines), unsafe_allow_html=True)

                # Contacts section with priority scoring
                st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
                st.markdown(f"##### {t('contacts_at').format(selected_company, len(contacts))}")

                if contacts:
                    for ct in contacts:
                        ct["_priority_score"] = score_contact_priority(ct, enrich)
                        ct["_priority_label"] = _priority_label(ct["_priority_score"])
                    contacts.sort(key=lambda x: -x["_priority_score"])

                    for ct in contacts:
                        _ct_email = ct.get("email", "")
                        if ct.get("email_guessed"):
                            email_part = ' &middot; <span style="color:#ffca28" title="Guessed from domain pattern">{} [guessed]</span>'.format(_ct_email) if _ct_email else ""
                        else:
                            email_part = " &middot; " + _ct_email if _ct_email else ""
                        phone_part = " &middot; " + ct["phone"] if ct.get("phone") else ""
                        country_part = " &middot; " + ct.get("country", "") if ct.get("country") else ""
                        linkedin_part = ' &middot; <a href="{}" target="_blank">LinkedIn</a>'.format(ct["linkedin"]) if ct.get("linkedin") else ""
                        source_badge = '<span class="badge badge-aec">{}</span>'.format(ct.get("source", ""))
                        pl = ct["_priority_label"]
                        pri_color = _priority_color(pl)
                        _pl_disp = tval(PRIORITY_LABELS, pl)
                        pri_badge = '<span style="background:{};color:#0d1117;font-size:0.65rem;padding:2px 6px;border-radius:3px;font-weight:600;margin-left:6px">{} ({})</span>'.format(pri_color, _pl_disp, ct["_priority_score"])
                        st.markdown(
                            '<div class="contact-row">'
                            '<div class="contact-name">{name} {src}{pri}</div>'
                            '<div class="contact-title">{title}</div>'
                            '<div class="contact-detail">{email}{phone}{country}{li}</div>'
                            '</div>'.format(
                                name=ct["name"], src=source_badge, pri=pri_badge,
                                title=ct.get("title", ""),
                                email=email_part, phone=phone_part,
                                country=country_part, li=linkedin_part,
                            ),
                            unsafe_allow_html=True,
                        )

                    ct_data = [{t("col_name"): c["name"], t("col_title"): c.get("title", ""), t("lbl_email"): c.get("email", ""), t("lbl_phone"): c.get("phone", ""), t("lbl_country"): c.get("country", ""), t("lbl_source"): c.get("source", "")} for c in contacts]
                    csv_contacts = pd.DataFrame(ct_data).to_csv(index=False)
                    st.download_button(t("export_contacts_csv"), csv_contacts, "{}_contacts.csv".format(selected_company.replace(" ", "_")), "text/csv", key="dl_co_ct")
                else:
                    st.info(t("no_contacts_yet"))

                # Contact Playbook (Companies tab)
                _co_pb_data = _cached_contact_playbook()
                _co_pb_key = selected_company.lower().strip()
                _co_pb_entry = _co_pb_data.get(_co_pb_key, {})

                if _co_pb_entry and _co_pb_entry.get("prioritized_contacts"):
                    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
                    st.markdown(f"##### {t('contact_playbook')}")

                    co_play_type = _co_pb_entry.get("play_type", "greenfield")
                    co_play_css = {"upsell": "play-upsell", "displacement": "play-displacement", "greenfield": "play-greenfield"}.get(co_play_type, "play-greenfield")
                    co_play_label = tval(PLAY_TYPE_LABELS, co_play_type) or co_play_type.title()

                    co_pb_header = ['<div class="playbook-card">']
                    co_pb_header.append('<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px">')
                    co_pb_header.append('<h4 style="margin:0">{}</h4>'.format(_co_pb_entry.get("company_name", selected_company)))
                    co_pb_header.append('<span class="play-badge {}">{}</span>'.format(co_play_css, co_play_label))
                    co_pb_header.append('</div>')

                    co_pd = _co_pb_entry.get("play_details", {})
                    if co_pd.get("current_products"):
                        co_pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_current_products"), co_pd["current_products"]))
                    if co_pd.get("top_upsell"):
                        co_pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_top_upsell"), co_pd["top_upsell"]))
                    if co_pd.get("competitor_tools"):
                        co_pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_competitor_tools"), ", ".join(co_pd["competitor_tools"])))
                    if co_pd.get("signals"):
                        co_sig_html = " ".join('<span class="signal-tag">{}</span>'.format(s) for s in co_pd["signals"])
                        co_pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("col_signals"), co_sig_html))
                    if co_pd.get("nearest_renewal"):
                        co_pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{} ({} {})</div>'.format(
                            t("lbl_renewal"), co_pd["nearest_renewal"], co_pd.get("days_to_renewal", "?"), t("days")))
                    _co_adv = co_pd.get("advanced_signals", [])
                    if _co_adv:
                        co_pb_header.append('<div style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.06)"></div>')
                        co_pb_header.append('<div class="detail-label">{}</div>'.format(t("lbl_advanced_intelligence").format(co_pd.get("advanced_signals_count", len(_co_adv)))))
                        for _ca in _co_adv:
                            co_pb_header.append('<div style="font-size:0.82rem;color:#c9d1d9;padding:3px 0 3px 12px;border-left:2px solid rgba(0,191,165,0.3);margin-bottom:3px">{}</div>'.format(_ca))
                    if co_pd.get("certifications"):
                        co_cert_html = " ".join('<span class="product-tag">{}</span>'.format(c) for c in co_pd["certifications"][:4])
                        co_pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_certifications"), co_cert_html))
                    if co_pd.get("hiring_product_needs"):
                        co_hpn_html = " ".join('<span class="product-tag">{}</span>'.format(p) for p in co_pd["hiring_product_needs"][:5])
                        co_pb_header.append('<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_hiring_product_needs"), co_hpn_html))
                    co_pb_header.append('</div>')
                    st.markdown("".join(co_pb_header), unsafe_allow_html=True)

                    co_exp_plays = _co_pb_entry.get("expansion_plays", [])
                    if co_exp_plays:
                        with st.expander(t("expansion_plays").format(len(co_exp_plays)), expanded=False):
                            for co_ep in co_exp_plays:
                                co_ep_html = ['<div class="play-detail">']
                                co_ep_html.append('<div class="play-detail-title">{}</div>'.format(co_ep.get("name", "")))
                                co_ep_html.append('<div class="play-detail-body"><strong>{}</strong> {}</div>'.format(t("sell"), co_ep.get("what_to_sell", "")))
                                co_ep_html.append('<div class="play-detail-body"><strong>{}</strong> {}</div>'.format(t("why_now"), co_ep.get("why_now", "")))
                                co_disc_q = co_ep.get("discovery_q", [])
                                if co_disc_q:
                                    co_ep_html.append('<div class="play-detail-body" style="margin-top:4px"><strong>{}</strong></div>'.format(t("discovery")))
                                    for co_dq in co_disc_q:
                                        co_ep_html.append('<div class="play-detail-body" style="padding-left:12px">• {}</div>'.format(co_dq))
                                co_ep_html.append('</div>')
                                st.markdown("".join(co_ep_html), unsafe_allow_html=True)

                    for co_pc in _co_pb_entry["prioritized_contacts"]:
                        co_prio = co_pc.get("priority", "low")
                        co_prio_css = {"high": "persona-high", "medium": "persona-medium", "low": "persona-low"}.get(co_prio, "persona-low")
                        co_pc_html = ['<div class="persona-card">']
                        co_pc_html.append('<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">')
                        co_pc_html.append('<span class="persona-name">{}</span>'.format(co_pc.get("name", "")))
                        co_pc_html.append('<span class="persona-label {}">{}</span>'.format(co_prio_css, tval(PRIORITY_LABELS, co_prio)))
                        co_pc_html.append('</div>')
                        co_pc_html.append('<div class="persona-title">{} &middot; {}</div>'.format(co_pc.get("title", ""), co_pc.get("persona_label", "")))
                        if co_pc.get("email"):
                            co_pc_html.append('<div style="font-size:0.78rem;color:#4fc3f7;margin-top:2px">{}</div>'.format(co_pc["email"]))
                        co_pc_html.append('<div style="font-size:0.82rem;color:#c9d1d9;margin-top:6px">{}</div>'.format(co_pc.get("relevance", "")))
                        co_pain_pts = co_pc.get("pain_points", [])
                        if co_pain_pts:
                            co_pc_html.append('<div style="margin-top:8px;font-size:0.78rem;color:#8b949e;font-weight:600">{}</div>'.format(t("lbl_pain_points")))
                            for co_pp in co_pain_pts[:4]:
                                co_pc_html.append('<div class="pain-point">{}</div>'.format(co_pp))
                        if co_pc.get("talk_track"):
                            co_pc_html.append('<div class="talk-track">{}</div>'.format(co_pc["talk_track"]))
                        co_hooks = co_pc.get("signal_hooks", [])
                        if co_hooks:
                            co_pc_html.append('<div style="margin-top:6px;font-size:0.78rem;color:#8b949e;font-weight:600">{}</div>'.format(t("lbl_signal_hooks")))
                            for co_sh in co_hooks:
                                co_pc_html.append('<div style="font-size:0.82rem;color:#c9d1d9;padding:2px 0 2px 12px;border-left:2px solid rgba(255,202,40,0.3);margin-bottom:4px">{}</div>'.format(co_sh))
                        co_prods = co_pc.get("recommended_products", [])
                        if co_prods:
                            co_pc_html.append('<div style="margin-top:6px">')
                            for co_pr in co_prods:
                                co_pc_html.append('<span class="product-tag">{}</span>'.format(co_pr))
                            co_pc_html.append('</div>')
                        co_pc_html.append('</div>')
                        st.markdown("".join(co_pc_html), unsafe_allow_html=True)

                # Deep Research Report
                st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
                _co_report_key = re.sub(r'[^\w\-.]', '_', selected_company.lower().strip())[:80]
                _co_report_path = Path(__file__).resolve().parent / "reports" / f"{_co_report_key}.md"
                if _co_report_path.exists():
                    with st.expander(t("deep_research"), expanded=False):
                        _co_report_md = _co_report_path.read_text(encoding="utf-8")
                        st.markdown(_co_report_md, unsafe_allow_html=True)
                else:
                    st.caption(t("no_research"))

    else:
        st.info(t("no_company_data"))


# =====================================================================
# TAB 4: CONTACTS
# =====================================================================
with tab_contacts:
    st.markdown(f"## {t('contacts_heading')}")
    st.caption(t("contacts_caption"))

    acct_enrich_ct = _cached_account_enrichment()
    ev_enrich_ct = _cached_event_enrichment()

    conn_ct = _db()
    all_contacts_list = []
    _ct_dedup = set()

    def _add_ct(name, title, email, phone, country, company, source, linkedin="", persona_type="", adsk_persona="", **kw):
        key = (name.lower().strip(), company.lower().strip())
        if key in _ct_dedup or not name.strip():
            return
        _ct_dedup.add(key)
        all_contacts_list.append({
            "Company": company, "Name": name, "Title": title,
            "Email": email, "Phone": phone, "Country": country,
            "LinkedIn": linkedin, "Source": source,
            "persona_type": persona_type, "adsk_persona": adsk_persona,
        })

    # Source 1: Persona cache
    for pr in conn_ct.execute("SELECT result_json FROM enrichment_cache WHERE lookup_type='personas'").fetchall():
        d = json.loads(dict(pr)["result_json"])
        company = d.get("company_name", "")
        for c in d.get("contacts", []):
            _add_ct("{} {}".format(c.get("first_name", ""), c.get("last_name", "")).strip(),
                    c.get("title", ""), c.get("email", ""), c.get("phone", ""),
                    c.get("country", ""), company, "ZoomInfo", c.get("linkedin", ""),
                    c.get("persona_type", ""), c.get("adsk_persona", ""))

    # Source 2: Account enrichment contacts
    for csn, ae_data in acct_enrich_ct.items():
        company = ae_data.get("official_name") or ae_data.get("zi_company_name") or ""
        for c in ae_data.get("contacts", []):
            fn, ln = c.get("first_name", ""), c.get("last_name", "")
            name = "{} {}".format(fn, ln).strip()
            email = c.get("email", "")
            if not name and email:
                name = email.split("@")[0].replace(".", " ").title()
            _add_ct(name, c.get("title", ""), email, c.get("phone", ""), "",
                    company, c.get("source", "CRM").upper(), "",
                    c.get("persona_type", ""), c.get("adsk_persona", ""))

    # Source 3: Event contacts
    for r in conn_ct.execute(
        "SELECT ec.company_name, ec.person_name, ec.person_title, ec.person_linkedin "
        "FROM event_companies ec WHERE ec.person_name IS NOT NULL AND ec.person_name != '' "
        "AND COALESCE(ec.entity_status, 'pending') != 'rejected'"
    ).fetchall():
        rd = dict(r)
        _add_ct(rd["person_name"], rd.get("person_title", ""), "", "", "",
                rd["company_name"], "Event Data", rd.get("person_linkedin", ""))

    # Source 4: Accounts table CRM emails
    for acct_row in conn_ct.execute("SELECT company_name, notes FROM accounts WHERE notes LIKE '%Email:%'").fetchall():
        ad = dict(acct_row)
        email_match = re.search(r'Email:\s*(\S+@\S+)', ad.get("notes", ""))
        if email_match:
            email = email_match.group(1).rstrip(";,")
            name = email.split("@")[0].replace(".", " ").title()
            _add_ct(name, "Purchaser (CRM)", email, "", "", ad["company_name"], "CRM")
    conn_ct.close()

    # Score all contacts (lightweight: no per-company DB lookups for the table)
    for ct in all_contacts_list:
        ct["_priority_score"] = score_contact_priority(ct, None)
        ct["_priority_label"] = _priority_label(ct["_priority_score"])
        ct["Priority"] = "{} ({})".format(tval(PRIORITY_LABELS, ct["_priority_label"]), ct["_priority_score"])

    all_contacts_list.sort(key=lambda x: -x["_priority_score"])

    if all_contacts_list:
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric(t("total_contacts"), len(all_contacts_list))
        mc2.metric(t("high_priority"), sum(1 for c in all_contacts_list if c["_priority_label"] == "high"))
        mc3.metric(t("with_email"), sum(1 for c in all_contacts_list if c.get("Email")))
        mc4.metric(t("with_phone"), sum(1 for c in all_contacts_list if c.get("Phone")))

        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            ct_search = st.text_input(t("search_contacts"), key="ct_search", placeholder=t("search_ct_placeholder"))
        with fc2:
            ct_source = st.selectbox(t("source_filter"), [t("status_all"), "ZoomInfo", "ZOOMINFO", "CRM", "Event Data"], key="ct_source_filter")
        with fc3:
            _prio_rev = {tval(PRIORITY_LABELS, p): p for p in ["high", "medium", "low", "info"]}
            ct_priority = st.selectbox(
                t("priority_filter"),
                [t("status_all")] + [tval(PRIORITY_LABELS, p) for p in ["high", "medium", "low", "info"]],
                key="ct_priority_filter",
            )

        filtered_ct = all_contacts_list
        if ct_search:
            q = ct_search.lower()
            filtered_ct = [c for c in filtered_ct if q in c.get("Name", "").lower() or q in c.get("Company", "").lower() or q in c.get("Title", "").lower()]
        if ct_source != t("status_all"):
            filtered_ct = [c for c in filtered_ct if c.get("Source", "").upper() == ct_source.upper()]
        if ct_priority != t("status_all"):
            filtered_ct = [c for c in filtered_ct if c.get("_priority_label") == _prio_rev.get(ct_priority)]

        st.caption(t("showing_contacts").format(len(filtered_ct)))

        df_ct = pd.DataFrame(filtered_ct)
        _ct_col_map = ["Priority", "Name", "Company", "Title", "Email", "Phone", "Country", "Source"]
        display_cols = [c for c in _ct_col_map if c in df_ct.columns]
        _ct_rename = {
            "Priority": t("priority_filter"),
            "Name": t("col_name"),
            "Company": t("col_company"),
            "Title": t("col_title"),
            "Email": t("lbl_email"),
            "Phone": t("lbl_phone"),
            "Country": t("lbl_country"),
            "Source": t("lbl_source"),
        }
        st.dataframe(df_ct[display_cols].rename(columns=_ct_rename), hide_index=True, width="stretch", height=500)

        # Contact detail
        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        st.markdown(f"##### {t('contact_profile')}")

        contact_names = [c["Name"] for c in filtered_ct if c.get("Name")]
        if contact_names:
            selected_ct = st.selectbox(t("select_contact"), contact_names, key="ct_detail_sel")

            if selected_ct:
                ct_matches = [c for c in filtered_ct if c["Name"] == selected_ct]
                ct_info = ct_matches[0] if ct_matches else {}
                ct_company = ct_info.get("Company", "")

                col_profile, col_company_ctx = st.columns([1, 1])

                with col_profile:
                    li_html = ""
                    if ct_info.get("LinkedIn"):
                        li_html = '<div class="detail-label">{}</div><div class="detail-value"><a href="{}" target="_blank" style="color:#00bfa5">{}</a></div>'.format(t("lbl_linkedin"), ct_info["LinkedIn"], ct_info["LinkedIn"])
                    ev_html = ""
                    if ct_info.get("Event"):
                        ev_html = '<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_event_label"), ct_info["Event"])
                    acc_html = ""
                    if ct_info.get("accuracy"):
                        acc_html = '<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_accuracy_score"), ct_info["accuracy"])
                    country_html = ""
                    if ct_info.get("Country"):
                        country_html = '<div class="detail-label">{}</div><div class="detail-value">{}</div>'.format(t("lbl_country"), ct_info["Country"])

                    st.markdown(
                        '<div class="detail-card">'
                        '<h3>{}</h3>'
                        '<div class="detail-label">{}</div><div class="detail-value">{}</div>'
                        '<div class="detail-label">{}</div><div class="detail-value">{}</div>'
                        '<div class="detail-label">{}</div><div class="detail-value">{}</div>'
                        '<div class="detail-label">{}</div><div class="detail-value">{}</div>'
                        '{}{}{}{}'
                        '<div class="detail-label">{}</div><div class="detail-value">{}</div>'
                        '</div>'.format(
                            ct_info.get("Name", ""),
                            t("lbl_title"), ct_info.get("Title", "---"),
                            t("lbl_company_label"), ct_company or "---",
                            t("lbl_email"), ct_info.get("Email") or "---",
                            t("lbl_phone"), ct_info.get("Phone") or "---",
                            country_html, li_html, ev_html, acc_html,
                            t("lbl_source"), ct_info.get("Source", "---"),
                        ),
                        unsafe_allow_html=True,
                    )

                with col_company_ctx:
                    if ct_company:
                        enrich = get_company_full_enrichment(ct_company, _cached_event_enrichment(), _cached_account_enrichment())
                        company_lines = ['<div class="detail-card"><h4>{} {}</h4>'.format(t("company_out"), ct_company)]

                        opp = enrich.get("opportunity_score", 0)
                        if opp:
                            company_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_opp_score")))
                            company_lines.append('<div class="detail-value">{}/100 {}</div>'.format(opp, _score_bar_html(opp)))

                        emp = enrich.get("employee_count")
                        rev = enrich.get("revenue")
                        if emp or rev:
                            company_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_size")))
                            parts = []
                            if emp:
                                parts.append("{}: {:,}".format(t("lbl_employees"), emp) if isinstance(emp, (int, float)) else "{}: {}".format(t("lbl_employees"), emp))
                            if rev and isinstance(rev, (int, float)):
                                parts.append("Revenue: ${:,.0f}".format(rev))
                            sep_p = " &middot; "
                            company_lines.append('<div class="detail-value">{}</div>'.format(sep_p.join(parts)))

                        timing = enrich.get("timing_signals", [])
                        if timing:
                            company_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_timing_signals")))
                            tags = "".join('<span class="signal-tag">{}</span>'.format(s) for s in timing)
                            company_lines.append('<div class="detail-value">{}</div>'.format(tags))

                        conn_ev = _db()
                        ev_rows = conn_ev.execute(
                            "SELECT DISTINCT e.event_name FROM event_companies ec JOIN events e ON ec.event_id = e.id "
                            "WHERE LOWER(TRIM(ec.company_name)) = ?",
                            (ct_company.lower().strip(),),
                        ).fetchall()
                        conn_ev.close()
                        if ev_rows:
                            ev_names_list = [dict(r)["event_name"] for r in ev_rows]
                            sep_ev = ", "
                            company_lines.append('<div class="detail-label">{}</div>'.format(t("lbl_events_attending")))
                            company_lines.append('<div class="detail-value">{}</div>'.format(sep_ev.join(ev_names_list)))

                        company_lines.append('</div>')
                        st.markdown("".join(company_lines), unsafe_allow_html=True)

                if len(ct_matches) > 1:
                    st.markdown("**{}**".format(t("also_appears")))
                    for alt in ct_matches[1:]:
                        alt_event = " --- {} {}".format(t("lbl_event_label"), alt["Event"]) if alt.get("Event") else ""
                        st.markdown("- {} --- {} ({}){}".format(alt.get("Company", ""), alt.get("Title", ""), alt.get("Source", ""), alt_event))

        csv_all = pd.DataFrame(filtered_ct).to_csv(index=False)
        st.download_button(t("export_all_csv"), csv_all, "all_contacts.csv", "text/csv", key="dl_all_ct")

    else:
        st.info(t("no_contacts_discovered"))


# =====================================================================
# TAB 5: OUTREACH COMPOSER
# =====================================================================
with tab_outreach:
    st.markdown(f"## {t('outreach_heading')}")
    st.caption(t("outreach_caption"))

    ev_enrich_out = _cached_event_enrichment()
    acct_enrich_out = _cached_account_enrichment()

    # -------------------------------------------------------------------
    # Build comprehensive contact list from ALL sources
    # -------------------------------------------------------------------
    outreach_contacts = []
    _out_seen = set()

    def _add_out_contact(name, first_name, last_name, title, email, phone, company, country, source, accuracy=""):
        key = (name.lower().strip(), company.lower().strip())
        if key in _out_seen or not name.strip():
            return
        _out_seen.add(key)
        outreach_contacts.append({
            "name": name, "first_name": first_name, "last_name": last_name,
            "title": title, "email": email, "phone": phone,
            "company": company, "country": country, "source": source, "accuracy": accuracy,
        })

    # Source 1: Persona cache (ZoomInfo enriched contacts)
    conn_out = _db()
    for pr in conn_out.execute("SELECT result_json FROM enrichment_cache WHERE lookup_type='personas'").fetchall():
        d = json.loads(dict(pr)["result_json"])
        company = d.get("company_name", "")
        for c in d.get("contacts", []):
            fn, ln = c.get("first_name", ""), c.get("last_name", "")
            _add_out_contact("{} {}".format(fn, ln).strip(), fn, ln, c.get("title", ""),
                            c.get("email", ""), c.get("phone", ""), company,
                            c.get("country", ""), "ZoomInfo Persona", c.get("accuracy_score", ""))

    # Source 2: Account enrichment JSON contacts (CRM, ZoomInfo, etc.)
    for csn, ae_data in acct_enrich_out.items():
        company = ae_data.get("official_name") or ae_data.get("zi_company_name") or ""
        for c in ae_data.get("contacts", []):
            fn, ln = c.get("first_name", ""), c.get("last_name", "")
            name = "{} {}".format(fn, ln).strip()
            email = c.get("email", "")
            if not name and email:
                name = email.split("@")[0].replace(".", " ").title()
                fn = name.split()[0] if name.split() else ""
                ln = name.split()[-1] if len(name.split()) > 1 else ""
            _add_out_contact(name, fn, ln, c.get("title", ""), email,
                            c.get("phone", ""), company, "", c.get("source", "CRM").upper(),
                            c.get("confidence", ""))

    # Source 3: Event company contacts
    for ec_row in conn_out.execute(
        "SELECT ec.company_name, ec.person_name, ec.person_title, ec.person_linkedin "
        "FROM event_companies ec WHERE ec.person_name IS NOT NULL AND ec.person_name != '' "
        "AND COALESCE(ec.entity_status, 'pending') != 'rejected'"
    ).fetchall():
        d = dict(ec_row)
        name = d.get("person_name", "").strip()
        parts = name.split(None, 1)
        fn = parts[0] if parts else name
        ln = parts[1] if len(parts) > 1 else ""
        _add_out_contact(name, fn, ln, d.get("person_title", ""), "",
                        "", d["company_name"], "", "Event Data")

    # Source 4: Accounts table (CRM purchaser emails from notes)
    for acct_row in conn_out.execute("SELECT company_name, notes FROM accounts WHERE notes LIKE '%Email:%'").fetchall():
        ad = dict(acct_row)
        email_match = re.search(r'Email:\s*(\S+@\S+)', ad.get("notes", ""))
        if email_match:
            email = email_match.group(1).rstrip(";,")
            name = email.split("@")[0].replace(".", " ").title()
            parts = name.split(None, 1)
            _add_out_contact(name, parts[0] if parts else "", parts[1] if len(parts) > 1 else "",
                            "Purchaser (CRM)", email, "", ad["company_name"], "", "CRM")
    conn_out.close()

    outreach_contacts.sort(key=lambda x: (x["company"].lower(), x["name"].lower()))

    if not outreach_contacts:
        st.info(t("no_enriched_contacts"))
    else:
        from outreach.persona_templates import PERSONAS as PERSONA_PAIN_POINTS

        col_sel, col_opts = st.columns([1, 1])

        with col_sel:
            contact_options = ["{} --- {} ({})".format(c["name"], c["title"], c["company"]) for c in outreach_contacts]
            sel_idx = st.selectbox(t("select_contact_out"), range(len(contact_options)),
                                  format_func=lambda i: contact_options[i], key="out_contact")
            sel_contact = outreach_contacts[sel_idx] if sel_idx is not None else None

        with col_opts:
            plays = list_plays() if list_plays else ["cold_intro"]
            play_choice = st.selectbox(t("outreach_play"), plays, key="out_play")

        if sel_contact:
            ct_company = sel_contact["company"]
            enrich_out = get_company_full_enrichment(ct_company, ev_enrich_out, acct_enrich_out)
            persona_key = _match_persona_module(sel_contact["title"])
            persona = PERSONA_PAIN_POINTS.get(persona_key) if persona_key else None

            # Gather ALL enrichment fields into convenient variables
            master_data = enrich_out.get("_master", {})
            acct_data = enrich_out.get("_acct_enrichment", {})

            emp_count = enrich_out.get("employee_count", "")
            revenue_usd = enrich_out.get("revenue", "")
            revenue_czk = enrich_out.get("revenue_czk", "")
            profit_czk = enrich_out.get("profit_czk", "")
            rev_growth = enrich_out.get("revenue_growth", "")
            ico = enrich_out.get("ico", "")
            address = enrich_out.get("ares_address", "")
            domain = enrich_out.get("zi_domain", master_data.get("domain", ""))
            primary_seg = enrich_out.get("ares_primary_segment", enrich_out.get("primary_segment", master_data.get("primary_segment", "")))
            nace_codes = enrich_out.get("nace_codes", [])

            current_products = master_data.get("current_products", "")
            total_seats = master_data.get("total_seats", "")
            current_acv = master_data.get("current_acv_eur", "")
            potential_acv = master_data.get("potential_acv_eur", "")
            renewal_risk = master_data.get("renewal_risk", "")
            nearest_renewal = master_data.get("nearest_renewal", "")
            days_to_renewal = master_data.get("days_to_renewal", "")
            top_upsell = master_data.get("top_upsell", "")
            top_upsell_reason = master_data.get("top_upsell_reason", "")
            all_upsells = master_data.get("all_upsells", "")
            whitespace_products = master_data.get("whitespace_products", "")
            priority_score = master_data.get("priority_score", "")
            maturity_label = master_data.get("maturity_label", "")
            industry_segment = master_data.get("industry_segment", "")
            reseller = master_data.get("reseller", "")

            hiring_signal = enrich_out.get("hiring_signal", False)
            hiring_intensity = enrich_out.get("hiring_intensity", "")
            total_jobs = enrich_out.get("total_jobs", "")
            eng_hiring = enrich_out.get("engineering_hiring", False)
            adsk_tools = enrich_out.get("autodesk_tools_in_jobs", [])
            comp_tools = enrich_out.get("competitor_tools_in_jobs", [])
            leadership_change = enrich_out.get("leadership_change", False)
            statutory = enrich_out.get("statutory_body", "")
            has_contracts = enrich_out.get("has_public_contracts", False)
            contracts_count = enrich_out.get("public_contracts_count", "")
            contracts_val = enrich_out.get("public_contracts_value_czk", "")

            opp_score = enrich_out.get("opportunity_score", 0)
            est_deal = enrich_out.get("estimated_deal_eur", "")
            lead_class = enrich_out.get("lead_class", "")
            timing_signals = enrich_out.get("timing_signals", [])

            # Get events this company is attending
            conn_ev2 = _db()
            ev_rows2 = conn_ev2.execute(
                "SELECT DISTINCT e.event_name, e.event_date FROM event_companies ec JOIN events e ON ec.event_id = e.id "
                "WHERE LOWER(TRIM(ec.company_name)) = ?",
                (ct_company.lower().strip(),),
            ).fetchall()
            conn_ev2.close()
            event_names = [dict(r)["event_name"] for r in ev_rows2]

            # Also get other contacts at this company for awareness
            all_co_contacts = get_company_contacts(ct_company)

            # ----- Show intelligence context panel -----
            with st.expander(t("intelligence_context"), expanded=True):
                ctx_col1, ctx_col2 = st.columns(2)
                with ctx_col1:
                    st.markdown("**{}** {} --- {}".format(t("contact_label"), sel_contact["name"], sel_contact["title"]))
                    st.markdown("**{}** {}".format(t("company_out"), ct_company))
                    if sel_contact.get("email"):
                        st.markdown("**{}** {}".format(t("email_out"), sel_contact["email"]))
                    if sel_contact.get("phone"):
                        st.markdown("**{}** {}".format(t("phone_out"), sel_contact["phone"]))
                    if persona:
                        st.markdown("**{}** {}".format(t("persona_out"), persona["label"]))
                        st.markdown("**{}**".format(t("key_pain_points")))
                        for pp in persona["pain_points"][:3]:
                            st.markdown("- {}".format(pp))
                        st.markdown("**{}** {}".format(t("relevant_products"), ", ".join(persona["products"][:4])))

                        ind_hooks = get_industry_hooks(persona_key, primary_seg) if persona_key else []
                        if ind_hooks:
                            st.markdown("**{}**".format(t("industry_hooks")))
                            for h in ind_hooks[:2]:
                                st.markdown("- {}".format(h))

                        sig_hooks = build_signal_hooks(persona_key, enrich_out) if persona_key else []
                        if sig_hooks:
                            st.markdown("**{}**".format(t("signal_hooks_out")))
                            for h in sig_hooks:
                                st.markdown("- {}".format(h))

                with ctx_col2:
                    if opp_score:
                        st.markdown("**{}** {}/100".format(t("opp_score_out"), opp_score))
                    if emp_count:
                        st.markdown("**{}** {}".format(t("employees_out"), emp_count))
                    if current_products:
                        st.markdown("**{}** {} ({} seats)".format(t("adsk_products_out"), current_products, total_seats or "?"))
                    else:
                        st.markdown("**{}** {}".format(t("adsk_products_out"), t("adsk_products_new")))
                    if current_acv:
                        st.markdown("**{}** EUR {} (potential: EUR {})".format(t("acv_out"), current_acv, potential_acv or "?"))
                    if nearest_renewal:
                        st.markdown("**{}** {} ({} days)".format(t("renewal_out"), nearest_renewal, days_to_renewal or "?"))
                    if top_upsell:
                        st.markdown("**{}** {}".format(t("top_upsell_out"), top_upsell))
                    if hiring_signal:
                        st.markdown("**{}** {} ({} jobs)".format(t("hiring_out"), hiring_intensity, total_jobs))
                    if timing_signals:
                        st.markdown("**{}** {}".format(t("signals_out"), " | ".join(timing_signals)))
                    if event_names:
                        st.markdown("**{}** {}".format(t("events_out"), ", ".join(event_names)))

            custom_notes = st.text_area(
                t("additional_context"),
                placeholder=t("additional_placeholder"),
                key="out_notes",
            )

            # ---------------------------------------------------------------
            # Build FULL prompt context
            # ---------------------------------------------------------------
            ctx = []

            # SECTION 1: Role instruction
            ctx.append("# Outreach Draft Request")
            ctx.append("You are writing a personalized sales outreach email on behalf of Martin Valovic, "
                       "Account Manager at Autodesk for the Czechia territory. "
                       "The tone should be professional, consultative, and personalized. "
                       "Reference specific company data and signals to show genuine research. "
                       "Keep the email concise (150-250 words). Write a compelling subject line.")
            if play_choice:
                ctx.append("Outreach play/angle: {}".format(play_choice))

            # SECTION 2: Contact
            c_lines = ["## Contact Details"]
            c_lines.append("- Full Name: {}".format(sel_contact["name"]))
            c_lines.append("- First Name: {}".format(sel_contact.get("first_name", "")))
            c_lines.append("- Title: {}".format(sel_contact["title"]))
            c_lines.append("- Company: {}".format(ct_company))
            c_lines.append("- Email: {}".format(sel_contact.get("email", "") or "not available"))
            c_lines.append("- Phone: {}".format(sel_contact.get("phone", "") or "not available"))
            c_lines.append("- Country: {}".format(sel_contact.get("country", "") or "Czech Republic"))
            c_lines.append("- Source: {}".format(sel_contact.get("source", "")))
            ctx.append("\n".join(c_lines))

            # SECTION 3: Persona
            if persona:
                p_lines = ["## Persona & Messaging Angles"]
                p_lines.append("- Persona type: {}".format(persona["label"]))
                p_lines.append("- Their key pain points:")
                for pp in persona["pain_points"]:
                    p_lines.append("  - {}".format(pp))
                p_lines.append("- Autodesk products relevant to this persona: {}".format(", ".join(persona["products"])))

                ind_hooks = get_industry_hooks(persona_key, primary_seg) if persona_key else []
                if ind_hooks:
                    p_lines.append("- Industry-specific hooks:")
                    for h in ind_hooks:
                        p_lines.append("  - {}".format(h))
                sig_hooks = build_signal_hooks(persona_key, enrich_out) if persona_key else []
                if sig_hooks:
                    p_lines.append("- Signal-triggered conversation angles:")
                    for h in sig_hooks:
                        p_lines.append("  - {}".format(h))
                if event_names and persona_key:
                    ev_hook = build_event_hook(persona_key, event_names[0], ct_company)
                    if ev_hook:
                        p_lines.append("- Event hook: {}".format(ev_hook))
                ctx.append("\n".join(p_lines))

            # SECTION 4: Company Intelligence (the critical section)
            co = ["## Company Intelligence"]

            # Identity
            co.append("\n### Identity & Profile")
            co.append("- Official name: {}".format(enrich_out.get("official_name", ct_company)))
            if ico:
                co.append("- ICO (Czech business ID): {}".format(ico))
            if domain:
                co.append("- Website: {}".format(domain))
            if address:
                co.append("- Address: {}".format(address))
            if primary_seg:
                co.append("- Primary industry segment: {}".format(primary_seg))
            if nace_codes:
                nace_str = ", ".join(nace_codes[:6]) if isinstance(nace_codes, list) else str(nace_codes)
                co.append("- NACE codes: {}".format(nace_str))
            if lead_class:
                co.append("- Lead classification: {}".format(lead_class))

            # Size & Financials
            co.append("\n### Size & Financials")
            if emp_count:
                co.append("- Employee count: {}".format(emp_count))
            if revenue_usd:
                co.append("- Revenue (USD thousands): ${}K".format(revenue_usd))
            if revenue_czk:
                co.append("- Revenue (CZK): {} CZK".format(revenue_czk))
            if profit_czk:
                co.append("- Profit (CZK): {} CZK".format(profit_czk))
            if rev_growth:
                co.append("- Revenue growth: {}%".format(rev_growth))

            # Autodesk Relationship
            co.append("\n### Autodesk Relationship")
            if current_products:
                co.append("- Current Autodesk products: {}".format(current_products))
            else:
                co.append("- Current Autodesk products: None (new market / whitespace opportunity)")
            if total_seats:
                co.append("- Total seats: {}".format(total_seats))
            if maturity_label:
                co.append("- Maturity level: {}".format(maturity_label))
            if current_acv:
                co.append("- Current ACV: EUR {}".format(current_acv))
            if potential_acv:
                co.append("- Potential ACV: EUR {}".format(potential_acv))
            if nearest_renewal:
                renewal_note = "Nearest renewal: {}".format(nearest_renewal)
                if days_to_renewal:
                    renewal_note += " ({} days away)".format(days_to_renewal)
                co.append("- {}".format(renewal_note))
            if top_upsell:
                co.append("- Top upsell opportunity: {}".format(top_upsell))
            if top_upsell_reason:
                co.append("  - Why: {}".format(top_upsell_reason))
            if all_upsells:
                co.append("- All upsell candidates: {}".format(all_upsells))
            if whitespace_products:
                co.append("- Whitespace (new product) opportunities: {}".format(whitespace_products))
            if renewal_risk:
                co.append("- Renewal risk: {}".format(renewal_risk))
            if reseller:
                co.append("- Reseller/partner: {}".format(reseller))
            if opp_score:
                co.append("- Opportunity score: {}/100".format(opp_score))
            if est_deal:
                co.append("- Estimated deal size: EUR {}".format(est_deal))
            if industry_segment:
                co.append("- Industry sub-segment: {}".format(industry_segment))

            # Business Signals
            co.append("\n### Business Signals & Intent")
            signal_count = 0
            if hiring_signal:
                co.append("- HIRING: {} intensity, {} open positions".format(hiring_intensity, total_jobs))
                if eng_hiring:
                    co.append("  - Engineering/technical hiring detected")
                signal_count += 1
            if adsk_tools:
                co.append("- Autodesk tools mentioned in job postings: {}".format(", ".join(adsk_tools) if isinstance(adsk_tools, list) else str(adsk_tools)))
                signal_count += 1
            if comp_tools:
                co.append("- Competitor tools in job postings: {}".format(", ".join(comp_tools) if isinstance(comp_tools, list) else str(comp_tools)))
                signal_count += 1
            if leadership_change:
                co.append("- LEADERSHIP CHANGE detected")
                if statutory:
                    co.append("  - Statutory body: {}".format(statutory))
                signal_count += 1
            if has_contracts:
                co.append("- PUBLIC CONTRACTS: {} contracts".format(contracts_count))
                if contracts_val:
                    co.append("  - Total value: {} CZK".format(contracts_val))
                signal_count += 1
            if timing_signals:
                co.append("- Timing signals: {}".format("; ".join(timing_signals)))
                signal_count += 1
            if signal_count == 0:
                co.append("- No strong signals detected yet")

            # Events
            if event_names:
                co.append("\n### Events")
                for ev_name in event_names:
                    co.append("- Attending: {}".format(ev_name))

            ctx.append("\n".join(co))

            # SECTION 5: Other contacts at the company
            if len(all_co_contacts) > 1:
                oc_lines = ["## Other Known Contacts at {}".format(ct_company)]
                for oc in all_co_contacts:
                    if oc.get("name", "").lower() != sel_contact["name"].lower():
                        oc_lines.append("- {} | {} | {}".format(
                            oc.get("name", ""), oc.get("title", ""),
                            oc.get("email", "") or "no email"))
                if len(oc_lines) > 1:
                    ctx.append("\n".join(oc_lines))

            # SECTION 6: Custom notes
            if custom_notes:
                ctx.append("## Additional Notes from Sales Rep\n{}".format(custom_notes))

            # SECTION 7: Output instructions
            ctx.append("## Output Format\nPlease generate:\n1. **Subject line** (compelling, personalized)\n2. **Email body** (150-250 words, professional, consultative tone)\n\nUse the contact's first name. Reference specific company data points. Do NOT sound generic.")

            full_context = "\n\n".join(ctx)

            if st.button(t("generate_draft"), type="primary", key="gen_outreach"):
                if HAS_LLM_KEY:
                    result = craft_message(
                        play_name=play_choice,
                        contact={
                            "first_name": sel_contact["first_name"],
                            "last_name": sel_contact["last_name"],
                            "title": sel_contact["title"],
                            "email": sel_contact.get("email", ""),
                        },
                        account={"company_name": ct_company, "industry": primary_seg},
                        enrichment=enrich_out,
                        custom_notes=full_context,
                    )
                    if result.get("success"):
                        parsed = parse_ai_response(result["raw_response"])
                        st.session_state["draft_subject"] = parsed.get("subject", "")
                        st.session_state["draft_body"] = parsed.get("body", result["raw_response"])
                    else:
                        st.error(t("gen_failed").format(result.get("error", "")))
                else:
                    st.session_state["draft_context"] = full_context
                    st.session_state.pop("draft_subject", None)

            if st.session_state.get("draft_subject"):
                st.markdown("---")
                st.markdown("**{}** {}".format(t("subject_label"), st.session_state["draft_subject"]))
                st.text_area(t("email_body"), st.session_state.get("draft_body", ""), height=200, key="draft_body_edit")

            if st.session_state.get("draft_context") and not HAS_LLM_KEY:
                st.markdown("---")
                st.info(t("no_llm_key"))
                st.markdown("**{}**".format(t("full_prompt")))
                st.code(st.session_state["draft_context"], language="markdown")


# =====================================================================
# TAB 6: ACTIVITY LOG
# =====================================================================
with tab_log:
    st.markdown(f"## {t('activity_log')}")

    outreach_entries = get_outreach_log(limit=200)
    if outreach_entries:
        log_data = []
        status_counts = {}
        for entry in outreach_entries:
            ed = dict(entry)
            log_data.append({
                t("col_status_log"): ed.get("status", ""),
                t("col_play"): ed.get("play", ""),
                t("col_subject"): ed.get("subject", "")[:60],
                t("col_drafted"): ed.get("drafted_at", ""),
                t("col_sent"): ed.get("sent_at", ""),
            })
            s = ed.get("status", "unknown")
            status_counts[s] = status_counts.get(s, 0) + 1

        if status_counts:
            _st_col = t("col_status_log")
            _ct_col = "Count"  # plotly internal; chart axis — keep or translate? User didn't specify. Use English "Count" for plotly consistency or add t key. Briefly use literal "Count" for df column only.
            df_status = pd.DataFrame([{_st_col: k, _ct_col: v} for k, v in status_counts.items()])
            fig_status = px.bar(df_status, x=_st_col, y=_ct_col, color=_st_col,
                                color_discrete_map={"drafted": "#ffca28", "sent": "#00e676", "failed": "#ef5350"})
            plotly_dark(fig_status, 250)
            st.plotly_chart(fig_status)

        df_log = pd.DataFrame(log_data)
        st.dataframe(df_log, hide_index=True, width="stretch")
    else:
        st.info(t("no_activity"))
