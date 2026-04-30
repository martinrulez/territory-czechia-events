"""Batch enrichment for event companies (new_market and whitespace).

Extends the account enrichment pipeline to cover companies discovered
at industry events that are NOT in the existing Autodesk client list.
Runs ARES + ZoomInfo company enrichment and stores results in a
separate JSON keyed by lowercase company name.

Usage:
    python -m scoring.event_enricher                          # all event companies
    python -m scoring.event_enricher --class new_market       # only non-clients
    python -m scoring.event_enricher --class whitespace       # only existing clients
    python -m scoring.event_enricher --ares-only              # skip ZoomInfo
    python -m scoring.event_enricher --top 100                # limit to top 100 by score
"""

import json
import sys
import time
from pathlib import Path

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from db.database import init_db, get_connection
from enrichment.zoominfo_client import enrich_company as zi_enrich
from scraper.ares_client import lookup_company as ares_lookup

ENRICHMENT_DIR = Path(__file__).resolve().parent.parent / "enrichment_data"
EVENT_ENRICHMENT_FILE = ENRICHMENT_DIR / "event_company_enrichment.json"


def load_event_companies(lead_class=None, top_n=None):
    """Load unique event companies from the database."""
    with get_connection() as conn:
        query = """
            SELECT
                LOWER(TRIM(ec.company_name)) AS company_key,
                ec.company_name,
                ec.company_domain,
                GROUP_CONCAT(DISTINCT e.event_name) AS events,
                COUNT(DISTINCT e.id) AS event_count,
                MAX(ec.lead_score) AS best_score,
                MAX(ec.lead_class) AS lead_class,
                MAX(ec.matched_account_id) AS matched_account_id,
                GROUP_CONCAT(DISTINCT e.industry_focus) AS segments
            FROM event_companies ec
            JOIN events e ON ec.event_id = e.id
            WHERE COALESCE(ec.entity_status, '') != 'rejected'
        """
        params = []
        if lead_class:
            query += " AND ec.lead_class = ?"
            params.append(lead_class)
        query += " GROUP BY company_key ORDER BY best_score DESC NULLS LAST"
        if top_n:
            query += " LIMIT ?"
            params.append(top_n)
        return [dict(r) for r in conn.execute(query, params).fetchall()]


def load_event_enrichment():
    """Load previously saved event company enrichment."""
    if EVENT_ENRICHMENT_FILE.exists():
        with open(EVENT_ENRICHMENT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_event_enrichment(data):
    """Save event company enrichment to JSON."""
    ENRICHMENT_DIR.mkdir(parents=True, exist_ok=True)
    with open(EVENT_ENRICHMENT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved enrichment for {len(data)} event companies")


def enrich_events_ares(companies, existing):
    """Run ARES enrichment on event companies."""
    print(f"\n--- ARES Enrichment for Event Companies ({len(companies)}) ---")
    success = 0
    cached = 0
    failed = 0

    for i, co in enumerate(companies):
        key = co["company_key"]
        name = co["company_name"]

        if key not in existing:
            existing[key] = {"company_name": name}

        if existing[key].get("ares_done"):
            cached += 1
            _log(i + 1, len(companies), name, "cached")
            continue

        result = ares_lookup(name)
        existing[key]["ares_done"] = True

        if result.get("success"):
            existing[key]["ico"] = result.get("ico", "")
            existing[key]["official_name"] = result.get("official_name", "")
            existing[key]["legal_form"] = result.get("legal_form", "")
            existing[key]["nace_codes"] = result.get("nace_codes", [])
            existing[key]["ares_segments"] = result.get("autodesk_segments", [])
            existing[key]["ares_primary_segment"] = result.get("primary_segment", "")
            existing[key]["ares_address"] = result.get("address", "")
            success += 1
        else:
            existing[key]["ares_error"] = result.get("error", "")
            failed += 1

        _log(i + 1, len(companies), name, "OK" if result.get("success") else "miss")

    print(f"\nARES: {success} found, {cached} cached, {failed} failed")
    return existing


def enrich_events_zoominfo(companies, existing):
    """Run ZoomInfo company enrichment on event companies."""
    print(f"\n--- ZoomInfo Enrichment for Event Companies ({len(companies)}) ---")
    success = 0
    cached = 0
    failed = 0

    for i, co in enumerate(companies):
        key = co["company_key"]
        name = co["company_name"]
        domain = co.get("company_domain", "")

        if key not in existing:
            existing[key] = {"company_name": name}

        if existing[key].get("zi_done"):
            cached += 1
            _log(i + 1, len(companies), name, "cached")
            continue

        result = zi_enrich(company_name=name, domain=domain if domain else None)
        existing[key]["zi_done"] = True

        if result.get("success"):
            existing[key]["employee_count"] = result.get("employee_count")
            existing[key]["revenue"] = result.get("revenue")
            existing[key]["zi_company_name"] = result.get("company_name", "")
            existing[key]["zi_domain"] = result.get("domain", "")
            existing[key]["zi_city"] = result.get("city", "")
            existing[key]["zi_country"] = result.get("country", "")
            existing[key]["zi_industry"] = result.get("industry", "")
            existing[key]["zi_sub_industry"] = result.get("sub_industry", "")
            success += 1
        else:
            existing[key]["zi_error"] = result.get("error", "")
            failed += 1

        _log(i + 1, len(companies), name, "OK" if result.get("success") else "miss")
        time.sleep(0.1)

    print(f"\nZoomInfo: {success} enriched, {cached} cached, {failed} failed")
    return existing


def enrich_events_signals(companies, existing, signals=("or", "kurzy", "jobs", "smlouvy")):
    """Run Czech-specific signal scrapers on event companies.

    Adapts the signal_aggregator logic to work with event_company_enrichment.json
    keyed by lowercase company name instead of CSN.
    """
    ico_companies = [c for c in companies if existing.get(c["company_key"], {}).get("ico")]
    print(f"\n=== Signal Collection for Event Companies ===")
    print(f"Total: {len(companies)}, With ICO: {len(ico_companies)}")

    if "or" in signals:
        existing = _collect_event_or(ico_companies, existing)
        save_event_enrichment(existing)

    if "kurzy" in signals:
        existing = _collect_event_kurzy(ico_companies, existing)
        save_event_enrichment(existing)

    if "jobs" in signals:
        existing = _collect_event_jobs(companies, existing)
        save_event_enrichment(existing)

    if "smlouvy" in signals:
        existing = _collect_event_smlouvy(companies, existing)
        save_event_enrichment(existing)

    return existing


def _collect_event_or(companies, enrichment):
    from scraper.or_client import check_leadership_changes

    print(f"\n--- OR / Leadership Signals ({len(companies)} companies) ---")
    found = 0
    for i, co in enumerate(companies):
        key = co["company_key"]
        name = co["company_name"]
        enrich = enrichment.get(key, {})
        ico = enrich.get("ico")

        if not ico or enrich.get("or_done"):
            _log(i + 1, len(companies), name, "skip" if not ico else "cached")
            continue

        result = check_leadership_changes(ico, months_back=12)
        enrich["or_done"] = True
        if result.get("change_detected"):
            enrich["leadership_change"] = True
            enrich["leadership_changes_count"] = result.get("changes_count", 0)
            found += 1
            _log(i + 1, len(companies), name, f"CHANGE ({result.get('changes_count', 0)})")
        else:
            enrich["leadership_change"] = False
            _log(i + 1, len(companies), name, "no change")
        enrichment[key] = enrich

    print(f"\nOR: {found} with leadership changes")
    return enrichment


def _collect_event_kurzy(companies, enrichment):
    from scraper.kurzy_client import get_financial_signal

    print(f"\n--- Kurzy.cz / Financial Signals ({len(companies)} companies) ---")
    found = 0
    for i, co in enumerate(companies):
        key = co["company_key"]
        name = co["company_name"]
        enrich = enrichment.get(key, {})
        ico = enrich.get("ico")

        if not ico or enrich.get("kurzy_done"):
            _log(i + 1, len(companies), name, "skip" if not ico else "cached")
            continue

        result = get_financial_signal(ico, name)
        enrich["kurzy_done"] = True
        if result.get("has_financials"):
            if result.get("employees") and not enrich.get("employee_count"):
                enrich["employee_count"] = result["employees"]
                enrich["employee_source"] = "kurzy"
            if result.get("revenue_czk"):
                enrich["revenue_czk"] = result["revenue_czk"]
            if result.get("revenue_growth_pct") is not None:
                enrich["revenue_growth"] = result["revenue_growth_pct"]
            found += 1
            _log(i + 1, len(companies), name, f"OK emp={result.get('employees', '-')}")
        else:
            _log(i + 1, len(companies), name, "no data")
        enrichment[key] = enrich

    print(f"\nKurzy: {found} with financial data")
    return enrichment


def _collect_event_jobs(companies, enrichment):
    from scraper.jobs_scraper import get_hiring_signal

    print(f"\n--- Jobs.cz / Hiring Signals ({len(companies)} companies) ---")
    found = 0
    for i, co in enumerate(companies):
        key = co["company_key"]
        name = co["company_name"]
        enrich = enrichment.get(key, {})

        if enrich.get("jobs_done"):
            _log(i + 1, len(companies), name, "cached")
            continue

        result = get_hiring_signal(name)
        enrich["jobs_done"] = True
        if result.get("hiring_signal"):
            enrich["hiring_signal"] = True
            enrich["total_jobs"] = result.get("total_jobs", 0)
            enrich["engineering_hiring"] = result.get("engineering_hiring", False)
            enrich["autodesk_tools_in_jobs"] = result.get("autodesk_tools", [])
            enrich["competitor_tools_in_jobs"] = result.get("competitor_tools", [])
            found += 1
            tools = result.get("autodesk_tools", []) + result.get("competitor_tools", [])
            _log(i + 1, len(companies), name, f"jobs={result.get('total_jobs', 0)} [{', '.join(tools[:3])}]")
        else:
            enrich["hiring_signal"] = False
            _log(i + 1, len(companies), name, "not hiring")
        enrichment[key] = enrich

    print(f"\nJobs: {found} actively hiring")
    return enrichment


def _collect_event_smlouvy(companies, enrichment):
    from scraper.smlouvy_client import get_procurement_signal

    print(f"\n--- Smlouvy.gov.cz / Contract Signals ({len(companies)} companies) ---")
    found = 0
    for i, co in enumerate(companies):
        key = co["company_key"]
        name = co["company_name"]
        enrich = enrichment.get(key, {})
        ico = enrich.get("ico", "")

        if enrich.get("smlouvy_done"):
            _log(i + 1, len(companies), name, "cached")
            continue

        result = get_procurement_signal(name, ico)
        enrich["smlouvy_done"] = True
        if result.get("has_signal"):
            enrich["has_public_contracts"] = True
            enrich["smlouvy_contracts_count"] = result.get("contracts", 0)
            enrich["smlouvy_value_czk"] = result.get("value_czk", 0)
            found += 1
            _log(i + 1, len(companies), name, f"contracts={result.get('contracts', 0)}")
        else:
            enrich["has_public_contracts"] = False
            _log(i + 1, len(companies), name, "none")
        enrichment[key] = enrich
        time.sleep(0.3)

    print(f"\nSmlouvy: {found} with public contracts")
    return enrichment


def _log(current, total, name, status):
    pct = current / total * 100 if total else 0
    print(f"  [{current}/{total} {pct:5.1f}%] {name[:50]:<50} {status}")


def print_summary(enrichment):
    """Print enrichment coverage summary."""
    total = len(enrichment)
    if not total:
        print("No event company enrichment data.")
        return

    with_ico = sum(1 for v in enrichment.values() if v.get("ico"))
    with_emp = sum(1 for v in enrichment.values()
                   if v.get("employee_count") and isinstance(v["employee_count"], (int, float)))
    with_rev = sum(1 for v in enrichment.values()
                   if v.get("revenue") and isinstance(v["revenue"], (int, float)))
    with_nace = sum(1 for v in enrichment.values()
                    if v.get("nace_codes") and len(v["nace_codes"]) > 0)

    print(f"\n{'=' * 50}")
    print("EVENT COMPANY ENRICHMENT COVERAGE")
    print(f"{'=' * 50}")
    print(f"Total companies:       {total}")
    print(f"With ICO (ARES):       {with_ico} ({with_ico / total * 100:.1f}%)")
    print(f"With NACE codes:       {with_nace} ({with_nace / total * 100:.1f}%)")
    print(f"With employee count:   {with_emp} ({with_emp / total * 100:.1f}%)")
    print(f"With revenue:          {with_rev} ({with_rev / total * 100:.1f}%)")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Event company enrichment")
    parser.add_argument("--class", dest="lead_class", default=None,
                        help="Filter by lead_class (new_market, whitespace, displacement)")
    parser.add_argument("--top", type=int, default=None, help="Limit to top N companies")
    parser.add_argument("--ares-only", action="store_true", help="Only run ARES")
    parser.add_argument("--zi-only", action="store_true", help="Only run ZoomInfo")
    parser.add_argument("--signals-only", action="store_true",
                        help="Only run signal collection (OR, kurzy, jobs, smlouvy)")
    parser.add_argument("--signals", default="or,kurzy,jobs,smlouvy",
                        help="Comma-separated signal sources")
    args = parser.parse_args()

    init_db()

    companies = load_event_companies(lead_class=args.lead_class, top_n=args.top)
    print(f"Loaded {len(companies)} event companies")
    if args.lead_class:
        print(f"  Filtered by lead_class = {args.lead_class}")

    enrichment = load_event_enrichment()
    print(f"Existing enrichment: {len(enrichment)} companies")

    if args.signals_only:
        selected = tuple(s.strip() for s in args.signals.split(",") if s.strip())
        print(f"Running signals: {', '.join(selected)}")
        enrichment = enrich_events_signals(companies, enrichment, signals=selected)
        save_event_enrichment(enrichment)
    else:
        if not args.zi_only:
            enrichment = enrich_events_ares(companies, enrichment)
            save_event_enrichment(enrichment)

        if not args.ares_only:
            enrichment = enrich_events_zoominfo(companies, enrichment)
            save_event_enrichment(enrichment)

    print_summary(enrichment)


if __name__ == "__main__":
    main()
