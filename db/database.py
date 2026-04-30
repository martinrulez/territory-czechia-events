"""SQLite database layer for the Sales Prospecting Agent."""

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "prospects.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


@contextmanager
def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if they don't exist."""
    with open(SCHEMA_PATH, "r") as f:
        schema = f.read()
    with get_connection() as conn:
        conn.executescript(schema)
        _migrations = [
            "ALTER TABLE events ADD COLUMN event_type TEXT DEFAULT 'upcoming'",
            "ALTER TABLE event_companies ADD COLUMN entity_type TEXT DEFAULT 'unknown'",
            "ALTER TABLE event_companies ADD COLUMN entity_status TEXT DEFAULT 'pending'",
            "ALTER TABLE event_companies ADD COLUMN reject_reason TEXT",
        ]
        for sql in _migrations:
            try:
                conn.execute(sql)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------

def upsert_account(
    company_name: str,
    domain: str = None,
    industry: str = None,
    employee_count: str = None,
    autodesk_products: str = None,
    account_status: str = "prospect",
    notes: str = None,
) -> int:
    """Insert or update an account. Returns the account id."""
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM accounts WHERE LOWER(company_name) = LOWER(?)",
            (company_name,),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE accounts
                   SET domain=COALESCE(?,domain),
                       industry=COALESCE(?,industry),
                       employee_count=COALESCE(?,employee_count),
                       autodesk_products=COALESCE(?,autodesk_products),
                       account_status=COALESCE(?,account_status),
                       notes=COALESCE(?,notes),
                       updated_at=datetime('now')
                   WHERE id=?""",
                (domain, industry, employee_count, autodesk_products, account_status, notes, existing["id"]),
            )
            return existing["id"]
        cur = conn.execute(
            """INSERT INTO accounts
               (company_name,domain,industry,employee_count,autodesk_products,account_status,notes)
               VALUES (?,?,?,?,?,?,?)""",
            (company_name, domain, industry, employee_count, autodesk_products, account_status, notes),
        )
        return cur.lastrowid


def get_accounts(search: str = None):
    with get_connection() as conn:
        if search:
            return conn.execute(
                "SELECT * FROM accounts WHERE company_name LIKE ? ORDER BY updated_at DESC",
                (f"%{search}%",),
            ).fetchall()
        return conn.execute("SELECT * FROM accounts ORDER BY updated_at DESC").fetchall()


def get_account(account_id: int):
    with get_connection() as conn:
        return conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()


def delete_account(account_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM contacts WHERE account_id=?", (account_id,))
        conn.execute("DELETE FROM opportunities WHERE account_id=?", (account_id,))
        conn.execute("DELETE FROM outreach_log WHERE account_id=?", (account_id,))
        conn.execute("DELETE FROM accounts WHERE id=?", (account_id,))


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------

def upsert_contact(
    first_name: str,
    last_name: str,
    account_id: int = None,
    title: str = None,
    email: str = None,
    phone: str = None,
    linkedin_url: str = None,
    source: str = "manual",
    notes: str = None,
) -> int:
    with get_connection() as conn:
        existing = None
        if email:
            existing = conn.execute(
                "SELECT id FROM contacts WHERE LOWER(email) = LOWER(?)", (email,)
            ).fetchone()
        if not existing and first_name and last_name and account_id:
            existing = conn.execute(
                "SELECT id FROM contacts WHERE LOWER(first_name)=LOWER(?) AND LOWER(last_name)=LOWER(?) AND account_id=?",
                (first_name, last_name, account_id),
            ).fetchone()
        if existing:
            conn.execute(
                """UPDATE contacts
                   SET account_id=COALESCE(?,account_id),
                       title=COALESCE(?,title),
                       email=COALESCE(?,email),
                       phone=COALESCE(?,phone),
                       linkedin_url=COALESCE(?,linkedin_url),
                       source=COALESCE(?,source),
                       notes=COALESCE(?,notes),
                       updated_at=datetime('now')
                   WHERE id=?""",
                (account_id, title, email, phone, linkedin_url, source, notes, existing["id"]),
            )
            return existing["id"]
        cur = conn.execute(
            """INSERT INTO contacts
               (account_id,first_name,last_name,title,email,phone,linkedin_url,source,notes)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (account_id, first_name, last_name, title, email, phone, linkedin_url, source, notes),
        )
        return cur.lastrowid


def get_contacts(account_id: int = None, search: str = None):
    with get_connection() as conn:
        if account_id:
            return conn.execute(
                "SELECT * FROM contacts WHERE account_id=? ORDER BY updated_at DESC",
                (account_id,),
            ).fetchall()
        if search:
            return conn.execute(
                """SELECT * FROM contacts
                   WHERE first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR title LIKE ?
                   ORDER BY updated_at DESC""",
                (f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%"),
            ).fetchall()
        return conn.execute("SELECT * FROM contacts ORDER BY updated_at DESC").fetchall()


def get_contact(contact_id: int):
    with get_connection() as conn:
        return conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()


def delete_contact(contact_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM outreach_log WHERE contact_id=?", (contact_id,))
        conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,))


# ---------------------------------------------------------------------------
# Opportunities
# ---------------------------------------------------------------------------

def upsert_opportunity(
    account_id: int,
    opp_name: str,
    contact_id: int = None,
    stage: str = None,
    products: str = None,
    value: float = None,
    close_date: str = None,
    notes: str = None,
) -> int:
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM opportunities WHERE account_id=? AND LOWER(opp_name)=LOWER(?)",
            (account_id, opp_name),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE opportunities
                   SET contact_id=COALESCE(?,contact_id),
                       stage=COALESCE(?,stage),
                       products=COALESCE(?,products),
                       value=COALESCE(?,value),
                       close_date=COALESCE(?,close_date),
                       notes=COALESCE(?,notes),
                       updated_at=datetime('now')
                   WHERE id=?""",
                (contact_id, stage, products, value, close_date, notes, existing["id"]),
            )
            return existing["id"]
        cur = conn.execute(
            """INSERT INTO opportunities
               (account_id,contact_id,opp_name,stage,products,value,close_date,notes)
               VALUES (?,?,?,?,?,?,?,?)""",
            (account_id, contact_id, opp_name, stage, products, value, close_date, notes),
        )
        return cur.lastrowid


def get_opportunities(account_id: int = None):
    with get_connection() as conn:
        if account_id:
            return conn.execute(
                "SELECT * FROM opportunities WHERE account_id=? ORDER BY updated_at DESC",
                (account_id,),
            ).fetchall()
        return conn.execute("SELECT * FROM opportunities ORDER BY updated_at DESC").fetchall()


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def upsert_event(
    event_name: str,
    url: str,
    event_date: str = None,
    location: str = None,
    industry_focus: str = None,
    relevance_score: int = 5,
    notes: str = None,
    event_type: str = "upcoming",
) -> int:
    with get_connection() as conn:
        existing = conn.execute("SELECT id FROM events WHERE url=?", (url,)).fetchone()
        if existing:
            conn.execute(
                """UPDATE events SET event_name=?, event_date=COALESCE(?,event_date),
                   location=COALESCE(?,location), industry_focus=COALESCE(?,industry_focus),
                   relevance_score=COALESCE(?,relevance_score), notes=COALESCE(?,notes),
                   event_type=COALESCE(?,event_type)
                   WHERE id=?""",
                (event_name, event_date, location, industry_focus, relevance_score, notes, event_type, existing["id"]),
            )
            return existing["id"]
        cur = conn.execute(
            """INSERT INTO events (event_name,event_date,location,url,industry_focus,relevance_score,notes,event_type)
               VALUES (?,?,?,?,?,?,?,?)""",
            (event_name, event_date, location, url, industry_focus, relevance_score, notes, event_type),
        )
        return cur.lastrowid


def mark_event_scraped(event_id: int):
    with get_connection() as conn:
        conn.execute("UPDATE events SET scraped_at=datetime('now') WHERE id=?", (event_id,))


def get_events():
    with get_connection() as conn:
        return conn.execute("SELECT * FROM events ORDER BY event_date ASC").fetchall()


def get_event(event_id: int):
    with get_connection() as conn:
        return conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()


def delete_event(event_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM event_companies WHERE event_id=?", (event_id,))
        conn.execute("DELETE FROM events WHERE id=?", (event_id,))


# ---------------------------------------------------------------------------
# Event Companies
# ---------------------------------------------------------------------------

def insert_event_company(
    event_id: int,
    company_name: str,
    company_domain: str = None,
    role: str = None,
    person_name: str = None,
    person_title: str = None,
    person_linkedin: str = None,
    entity_type: str = "unknown",
    entity_status: str = "pending",
    reject_reason: str = None,
) -> int:
    with get_connection() as conn:
        existing = conn.execute(
            """SELECT id FROM event_companies
               WHERE event_id=? AND LOWER(company_name)=LOWER(?)
               AND COALESCE(person_name,'')=COALESCE(?,'')""",
            (event_id, company_name, person_name),
        ).fetchone()
        if existing:
            return existing["id"]
        cur = conn.execute(
            """INSERT INTO event_companies
               (event_id,company_name,company_domain,role,person_name,person_title,
                person_linkedin,entity_type,entity_status,reject_reason)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (event_id, company_name, company_domain, role, person_name, person_title,
             person_linkedin, entity_type, entity_status, reject_reason),
        )
        return cur.lastrowid


def get_event_companies(event_id: int = None, lead_class: str = None):
    with get_connection() as conn:
        query = "SELECT * FROM event_companies"
        params = []
        conditions = []
        if event_id:
            conditions.append("event_id=?")
            params.append(event_id)
        if lead_class:
            conditions.append("lead_class=?")
            params.append(lead_class)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY lead_score DESC NULLS LAST"
        return conn.execute(query, params).fetchall()


def update_event_company_match(ec_id: int, matched_account_id: int, match_confidence: float):
    with get_connection() as conn:
        conn.execute(
            "UPDATE event_companies SET matched_account_id=?, match_confidence=? WHERE id=?",
            (matched_account_id, match_confidence, ec_id),
        )


def update_event_company_score(ec_id: int, lead_score: int, lead_class: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE event_companies SET lead_score=?, lead_class=? WHERE id=?",
            (lead_score, lead_class, ec_id),
        )


def update_event_company_entity(
    ec_id: int,
    entity_type: str = None,
    entity_status: str = None,
    reject_reason: str = None,
):
    """Update the entity classification columns on an event_company row."""
    with get_connection() as conn:
        conn.execute(
            """UPDATE event_companies
               SET entity_type   = COALESCE(?, entity_type),
                   entity_status = COALESCE(?, entity_status),
                   reject_reason = COALESCE(?, reject_reason)
               WHERE id = ?""",
            (entity_type, entity_status, reject_reason, ec_id),
        )


def clear_event_companies(event_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM event_companies WHERE event_id=?", (event_id,))


def get_all_leads(min_score: int = 0, event_type: str = None, include_rejected: bool = False):
    """Get all scored event companies across all events."""
    with get_connection() as conn:
        query = """SELECT ec.*, e.event_name, e.event_date, e.industry_focus,
                          COALESCE(e.event_type, 'upcoming') as event_type
                   FROM event_companies ec
                   JOIN events e ON ec.event_id = e.id
                   WHERE ec.lead_score >= ?"""
        params = [min_score]
        if not include_rejected:
            query += " AND COALESCE(ec.entity_status, 'pending') != 'rejected'"
        if event_type:
            query += " AND COALESCE(e.event_type, 'upcoming') = ?"
            params.append(event_type)
        query += " ORDER BY ec.lead_score DESC"
        return conn.execute(query, params).fetchall()


def get_upcoming_event_leads(include_rejected: bool = False):
    """Get all event companies from upcoming events (not discovery sources)."""
    with get_connection() as conn:
        rejected_filter = "" if include_rejected else "AND COALESCE(ec.entity_status, 'pending') != 'rejected'"
        return conn.execute(
            """SELECT ec.*, e.event_name, e.event_date, e.industry_focus,
                      COALESCE(e.event_type, 'upcoming') as event_type
               FROM event_companies ec
               JOIN events e ON ec.event_id = e.id
               WHERE COALESCE(e.event_type, 'upcoming') = 'upcoming'
               {rejected_filter}
               ORDER BY ec.lead_score DESC NULLS LAST""".format(rejected_filter=rejected_filter),
        ).fetchall()


def get_company_event_count(company_name: str) -> int:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(DISTINCT event_id) as c FROM event_companies WHERE LOWER(company_name)=LOWER(?)",
            (company_name,),
        ).fetchone()
        return row["c"] if row else 0


def get_companies_with_events(include_rejected: bool = False) -> list:
    """Return one row per unique company with all associated event names aggregated."""
    with get_connection() as conn:
        where = "" if include_rejected else "WHERE COALESCE(ec.entity_status, 'pending') != 'rejected'"
        return conn.execute(
            """
            SELECT
                LOWER(TRIM(ec.company_name)) AS company_key,
                ec.company_name,
                ec.company_domain,
                GROUP_CONCAT(DISTINCT e.event_name) AS events,
                COUNT(DISTINCT e.id) AS event_count,
                GROUP_CONCAT(DISTINCT ec.role) AS roles,
                GROUP_CONCAT(DISTINCT e.industry_focus) AS industries,
                MAX(ec.lead_score) AS best_score,
                MAX(ec.lead_class) AS lead_class,
                MAX(ec.matched_account_id) AS matched_account_id,
                MAX(ec.match_confidence) AS match_confidence,
                MAX(ec.person_name) AS person_name,
                MAX(ec.person_title) AS person_title,
                MAX(ec.entity_type) AS entity_type,
                MAX(ec.entity_status) AS entity_status
            FROM event_companies ec
            JOIN events e ON ec.event_id = e.id
            {where}
            GROUP BY company_key
            ORDER BY event_count DESC, best_score DESC NULLS LAST
            """.format(where=where)
        ).fetchall()


# ---------------------------------------------------------------------------
# Enrichment Cache
# ---------------------------------------------------------------------------

def get_cached_enrichment(lookup_type: str, lookup_key: str):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT result_json FROM enrichment_cache WHERE lookup_type=? AND lookup_key=?",
            (lookup_type, lookup_key),
        ).fetchone()
        if row:
            return json.loads(row["result_json"])
        return None


def save_enrichment(lookup_type: str, lookup_key: str, result: dict):
    with get_connection() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO enrichment_cache (lookup_type, lookup_key, result_json)
               VALUES (?, ?, ?)""",
            (lookup_type, lookup_key, json.dumps(result)),
        )


# ---------------------------------------------------------------------------
# Outreach Log
# ---------------------------------------------------------------------------

def log_outreach(
    contact_id: int,
    account_id: int,
    play: str,
    subject: str,
    body: str,
    status: str = "drafted",
) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO outreach_log (contact_id, account_id, play, subject, body, status)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (contact_id, account_id, play, subject, body, status),
        )
        return cur.lastrowid


def update_outreach_status(outreach_id: int, status: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE outreach_log SET status=?, sent_at=CASE WHEN ?='sent' THEN datetime('now') ELSE sent_at END WHERE id=?",
            (status, status, outreach_id),
        )


def get_outreach_log(contact_id: int = None, limit: int = 100):
    with get_connection() as conn:
        if contact_id:
            return conn.execute(
                "SELECT * FROM outreach_log WHERE contact_id=? ORDER BY drafted_at DESC LIMIT ?",
                (contact_id, limit),
            ).fetchall()
        return conn.execute(
            "SELECT * FROM outreach_log ORDER BY drafted_at DESC LIMIT ?", (limit,)
        ).fetchall()


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def get_stats() -> dict:
    with get_connection() as conn:
        return {
            "accounts": conn.execute("SELECT COUNT(*) as c FROM accounts").fetchone()["c"],
            "contacts": conn.execute("SELECT COUNT(*) as c FROM contacts").fetchone()["c"],
            "opportunities": conn.execute("SELECT COUNT(*) as c FROM opportunities").fetchone()["c"],
            "outreach_drafted": conn.execute(
                "SELECT COUNT(*) as c FROM outreach_log WHERE status='drafted'"
            ).fetchone()["c"],
            "outreach_sent": conn.execute(
                "SELECT COUNT(*) as c FROM outreach_log WHERE status='sent'"
            ).fetchone()["c"],
            "events": conn.execute("SELECT COUNT(*) as c FROM events").fetchone()["c"],
            "event_leads": conn.execute(
                "SELECT COUNT(*) as c FROM event_companies WHERE lead_score IS NOT NULL"
            ).fetchone()["c"],
        }
