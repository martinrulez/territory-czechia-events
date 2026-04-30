"""Waterfall enrichment pipeline with fallback sources and cross-referencing.

For each data point, tries multiple sources in priority order and
validates results by cross-referencing where possible.

Usage:
    python -m scoring.waterfall_enricher                    # top 500 company data
    python -m scoring.waterfall_enricher --contacts --top 50  # + ZI contact search
    python -m scoring.waterfall_enricher --signals --top 200  # + Czech signals
    python -m scoring.waterfall_enricher --all --top 50       # everything
"""

import json
import sys
import time
from pathlib import Path

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from db.database import init_db
from scoring.batch_enricher import (
    ENRICHMENT_DIR,
    load_enrichment,
    load_prioritized,
    save_enrichment,
)

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent


def waterfall_enrich_company(accounts: list, enrichment: dict) -> dict:
    """Run waterfall company enrichment: ZoomInfo -> ARES -> Kurzy -> Kurzy PW -> Smlouvy."""
    from enrichment.zoominfo_client import enrich_company as zi_enrich
    from scraper.ares_client import lookup_company as ares_lookup

    print(f"\n{'='*60}")
    print(f"WATERFALL COMPANY ENRICHMENT ({len(accounts)} accounts)")
    print(f"{'='*60}")

    zi_ok = zi_miss = ares_ok = kurzy_ok = kurzy_pw_ok = smlouvy_ok = techno_ok = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        domain = acct.get("website", "")

        if csn not in enrichment:
            enrichment[csn] = {}
        e = enrichment[csn]

        # --- Source 1: ZoomInfo (employees, revenue, industry) ---
        if not e.get("zi_done"):
            result = zi_enrich(company_name=name, domain=domain or None)
            e["zi_done"] = True
            if result.get("success"):
                e["employee_count"] = result.get("employee_count")
                e["revenue"] = result.get("revenue")
                e["zi_company_name"] = result.get("company_name", "")
                e["zi_domain"] = result.get("domain", "")
                e["zi_city"] = result.get("city", "")
                e["zi_country"] = result.get("country", "")
                e["zi_industry"] = result.get("industry", "")
                e["zi_sub_industry"] = result.get("sub_industry", "")
                e["zi_sic_code"] = result.get("sic_code", "")
                e["zi_naics_code"] = result.get("naics_code", "")
                zi_ok += 1
            else:
                e["zi_error"] = result.get("error", "")
                zi_miss += 1
            time.sleep(0.1)

        # --- Source 2: ARES (ICO, NACE codes) ---
        if not e.get("ares_done") and name and len(name) >= 2:
            result = ares_lookup(name)
            e["ares_done"] = True
            if result.get("success"):
                e["ico"] = result.get("ico", "")
                e["official_name"] = result.get("official_name", "")
                e["legal_form"] = result.get("legal_form", "")
                e["nace_codes"] = result.get("nace_codes", [])
                e["ares_segments"] = result.get("autodesk_segments", [])
                e["ares_primary_segment"] = result.get("primary_segment", "")
                e["ares_address"] = result.get("address", "")
                ares_ok += 1
            else:
                e["ares_error"] = result.get("error", "")

        # --- Source 3: Kurzy.cz requests fallback ---
        ico = e.get("ico")
        if ico and not e.get("kurzy_done"):
            from scraper.kurzy_client import get_financial_signal
            result = get_financial_signal(ico, name)
            e["kurzy_done"] = True
            if result.get("has_financials"):
                if result.get("employees") and not e.get("employee_count"):
                    e["employee_count"] = result["employees"]
                    e["employee_source"] = "kurzy"
                if result.get("revenue_czk"):
                    e["revenue_czk"] = result["revenue_czk"]
                if result.get("profit_czk"):
                    e["profit_czk"] = result["profit_czk"]
                if result.get("revenue_growth_pct") is not None:
                    e["revenue_growth"] = result["revenue_growth_pct"]
                kurzy_ok += 1

        # --- Source 4: Kurzy.cz Playwright (headless browser bypass) ---
        if ico and not e.get("kurzy_pw_done") and not e.get("employee_count"):
            try:
                from scraper.kurzy_playwright import get_financial_signal_pw
                result = get_financial_signal_pw(ico, name)
                e["kurzy_pw_done"] = True
                if result.get("has_financials"):
                    if result.get("employees") and not e.get("employee_count"):
                        e["employee_count"] = result["employees"]
                        e["employee_source"] = "kurzy_pw"
                    if result.get("revenue_czk") and not e.get("revenue_czk"):
                        e["revenue_czk"] = result["revenue_czk"]
                    if result.get("profit_czk") and not e.get("profit_czk"):
                        e["profit_czk"] = result["profit_czk"]
                    if result.get("revenue_growth_pct") is not None and e.get("revenue_growth") is None:
                        e["revenue_growth"] = result["revenue_growth_pct"]
                    kurzy_pw_ok += 1
            except Exception:
                e["kurzy_pw_done"] = True

        # --- Source 5: ZoomInfo Technographics ---
        if not e.get("techno_done"):
            try:
                from enrichment.zoominfo_client import get_technographics
                result = get_technographics(company_name=name, domain=domain or None)
                e["techno_done"] = True
                if result.get("success"):
                    e["zi_tech_total"] = result.get("total_technologies", 0)
                    adsk = result.get("autodesk_products", [])
                    comp = result.get("competitor_products", [])
                    cad_other = result.get("cad_bim_other", [])
                    if adsk:
                        e["zi_autodesk_tech"] = adsk
                    if comp:
                        e["zi_competitor_tech"] = comp
                    if cad_other:
                        e["zi_cad_bim_tech"] = cad_other
                    techno_ok += 1
            except Exception:
                e["techno_done"] = True
            time.sleep(0.05)

        # --- Source 6: Smlouvy.gov.cz (public contracts) ---
        if ico and not e.get("smlouvy_done"):
            try:
                from scraper.smlouvy_client import get_procurement_signal
                result = get_procurement_signal(name, ico)
                e["smlouvy_done"] = True
                if result.get("has_signal"):
                    e["has_public_contracts"] = True
                    e["smlouvy_contracts_count"] = result.get("contracts", 0)
                    e["smlouvy_value_czk"] = result.get("value_czk", 0)
                    e["smlouvy_aec_contracts"] = result.get("aec_contracts", 0)
                    e["smlouvy_aec_value_czk"] = result.get("aec_value_czk", 0)
                    e["smlouvy_dm_contracts"] = result.get("dm_contracts", 0)
                    smlouvy_ok += 1
                else:
                    e["has_public_contracts"] = False
            except Exception:
                e["smlouvy_done"] = True

        _cross_reference(e, acct)

        _log(i + 1, len(accounts), name,
             f"emp={e.get('employee_count', '-')} rev={e.get('revenue', '-')}")

    print(f"\nZoomInfo: {zi_ok} OK, {zi_miss} miss")
    print(f"ZI Technographics: {techno_ok} with tech data")
    print(f"ARES: {ares_ok} matched")
    print(f"Kurzy (requests): {kurzy_ok} with financials")
    print(f"Kurzy (Playwright): {kurzy_pw_ok} with employee data")
    print(f"Smlouvy.gov.cz: {smlouvy_ok} with public contracts")
    return enrichment


def _cross_reference(e: dict, acct: dict):
    """Cross-reference data from multiple sources, flag inconsistencies."""
    flags = []

    zi_country = (e.get("zi_country") or "").lower()
    if zi_country and "czech" not in zi_country and "cz" not in zi_country:
        flags.append(f"zi_country_mismatch:{e.get('zi_country')}")
        zi_rev = e.get("revenue")
        kurzy_rev = e.get("revenue_czk")
        if zi_rev and kurzy_rev and zi_rev > kurzy_rev * 50:
            flags.append("zi_revenue_likely_parent")
            e["revenue_local"] = kurzy_rev
            e["revenue_parent"] = zi_rev

    crm_seg = acct.get("primary_segment", "")
    ares_seg = e.get("ares_primary_segment", "")
    if crm_seg and ares_seg and crm_seg != ares_seg and crm_seg != "unknown":
        flags.append(f"segment_mismatch:crm={crm_seg},ares={ares_seg}")

    if flags:
        e["validation_flags"] = flags

    sources = []
    if e.get("zi_done") and not e.get("zi_error"):
        sources.append("zoominfo")
    if e.get("ares_done") and e.get("ico"):
        sources.append("ares")
    if e.get("kurzy_done") and e.get("revenue_czk"):
        sources.append("kurzy")
    if e.get("kurzy_pw_done") and e.get("employee_source") == "kurzy_pw":
        sources.append("kurzy_pw")
    if e.get("techno_done") and e.get("zi_tech_total"):
        sources.append("zi_tech")
    if e.get("or_done"):
        sources.append("or")
    if e.get("jobs_done"):
        sources.append("jobs")
    if e.get("isvz_done"):
        sources.append("isvz")
    if e.get("smlouvy_done") and e.get("has_public_contracts"):
        sources.append("smlouvy")

    e["enrichment_sources"] = sources
    total_possible = 9
    e["enrichment_confidence"] = round(len(sources) / total_possible * 100, 0)


def waterfall_contacts(accounts: list, enrichment: dict) -> dict:
    """Find decision makers via ZoomInfo -> ARES VR -> Domain scrape -> CRM."""
    from enrichment.zoominfo_client import search_decision_makers
    from scraper.ares_vr_client import get_statutory_body
    from scraper.domain_contact_scraper import scrape_domain_contacts
    from scoring.persona_engine import classify_contact

    print(f"\n{'='*60}")
    print(f"WATERFALL CONTACT SEARCH ({len(accounts)} accounts)")
    print(f"{'='*60}")

    zi_found = vr_found = domain_found = crm_only = no_contacts = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        segment = acct.get("primary_segment", "")

        if csn not in enrichment:
            enrichment[csn] = {}
        e = enrichment[csn]

        if e.get("contacts_done"):
            _log(i + 1, len(accounts), name, "cached")
            continue

        contacts = []

        # --- Source 1: ZoomInfo contact search ---
        try:
            result = search_decision_makers(
                company_name=name,
                segment=segment if segment != "unknown" else None,
                max_results=5,
            )
            if result.get("success") and result.get("contacts"):
                for c in result["contacts"]:
                    contacts.append({
                        "first_name": c.get("first_name", ""),
                        "last_name": c.get("last_name", ""),
                        "title": c.get("title", ""),
                        "email": c.get("email", ""),
                        "phone": c.get("phone", ""),
                        "source": "zoominfo",
                        "confidence": "high" if c.get("accuracy_score", 0) and c["accuracy_score"] > 80 else "medium",
                    })
                zi_found += 1
        except Exception:
            pass
        time.sleep(0.2)

        # --- Source 2: ARES VR statutory body ---
        ico = e.get("ico", "")
        if not contacts and ico:
            try:
                vr = get_statutory_body(ico)
                if vr.get("success") and vr.get("persons"):
                    for p in vr["persons"][:8]:
                        contacts.append({
                            "first_name": p.get("first_name", ""),
                            "last_name": p.get("last_name", ""),
                            "full_name": p.get("full_name", ""),
                            "title": p.get("title", ""),
                            "email": "",
                            "phone": "",
                            "source": "ares_vr",
                            "confidence": "medium",
                            "organ": p.get("organ", ""),
                            "since": p.get("since", ""),
                        })
                    vr_found += 1
            except Exception:
                pass

        # --- Source 3: Domain contact scrape ---
        domain = e.get("zi_domain", "")
        if not contacts and domain:
            try:
                dr = scrape_domain_contacts(domain, name)
                if dr.get("success") and dr.get("persons"):
                    for p in dr["persons"][:8]:
                        contacts.append({
                            "first_name": p.get("first_name", ""),
                            "last_name": p.get("last_name", ""),
                            "full_name": p.get("full_name", ""),
                            "title": p.get("title", ""),
                            "email": p.get("email", ""),
                            "phone": p.get("phone", ""),
                            "source": "domain_scrape",
                            "confidence": "medium",
                        })
                    domain_found += 1
            except Exception:
                pass

        # --- Source 4: CRM purchaser email as last resort ---
        crm_email = acct.get("contact_email", "")
        if crm_email and not any(c.get("email") for c in contacts):
            for em in crm_email.split("|"):
                em = em.strip()
                if em and "@" in em:
                    contacts.append({
                        "first_name": "",
                        "last_name": "",
                        "title": "Purchaser (CRM)",
                        "email": em,
                        "phone": "",
                        "source": "crm",
                        "confidence": "low",
                    })
            if not any(c["source"] != "crm" for c in contacts):
                crm_only += 1

        if not contacts:
            no_contacts += 1
            linkedin_url = (
                f"https://www.linkedin.com/search/results/people/"
                f"?keywords={name.replace(' ', '%20')}"
                f"&origin=SWITCH_SEARCH_VERTICAL"
            )
            e["linkedin_search_url"] = linkedin_url

        for contact in contacts:
            if not contact.get("full_name"):
                fn = contact.get("first_name", "")
                ln = contact.get("last_name", "")
                contact["full_name"] = f"{fn} {ln}".strip()
            contact["persona_type"] = classify_contact(
                contact.get("title", ""), segment
            )

        e["contacts"] = contacts
        e["contacts_count"] = len(contacts)
        e["contacts_done"] = True

        src = contacts[0]["source"] if contacts else "none"
        status = f"{len(contacts)} contacts ({src})" if contacts else "none"
        _log(i + 1, len(accounts), name, status)

    print(f"\nZoomInfo contacts: {zi_found} companies")
    print(f"ARES VR statutory: {vr_found} companies")
    print(f"Domain scrape: {domain_found} companies")
    print(f"CRM only: {crm_only} companies")
    print(f"No contacts: {no_contacts} companies")
    return enrichment


def waterfall_signals(accounts: list, enrichment: dict) -> dict:
    """Run Czech signal scrapers (OR, Kurzy, ISVZ, Jobs)."""
    from scoring.signal_aggregator import collect_signals

    print(f"\n{'='*60}")
    print(f"SIGNAL COLLECTION ({len(accounts)} accounts)")
    print(f"{'='*60}")

    enrichment = collect_signals(accounts, enrichment)

    for csn in enrichment:
        _cross_reference(enrichment[csn], {"primary_segment": ""})

    return enrichment


def _log(current: int, total: int, name: str, status: str):
    pct = current / total * 100 if total else 0
    print(f"  [{current}/{total} {pct:5.1f}%] {name[:50]:<50} {status}")


def print_waterfall_summary(enrichment: dict):
    """Print comprehensive enrichment coverage summary."""
    total = len(enrichment)
    if not total:
        return

    with_emp = sum(1 for v in enrichment.values() if v.get("employee_count"))
    with_rev = sum(1 for v in enrichment.values() if v.get("revenue"))
    with_ico = sum(1 for v in enrichment.values() if v.get("ico"))
    with_contacts = sum(1 for v in enrichment.values() if v.get("contacts"))
    with_hiring = sum(1 for v in enrichment.values() if v.get("hiring_signal"))
    with_growth = sum(1 for v in enrichment.values() if v.get("revenue_growth") is not None)
    with_flags = sum(1 for v in enrichment.values() if v.get("validation_flags"))

    avg_conf = sum(
        v.get("enrichment_confidence", 0) for v in enrichment.values()
    ) / total

    print(f"\n{'='*60}")
    print("WATERFALL ENRICHMENT SUMMARY")
    print(f"{'='*60}")
    print(f"Total accounts:        {total}")
    print(f"Employee count:        {with_emp} ({with_emp/total*100:.0f}%)")
    print(f"Revenue:               {with_rev} ({with_rev/total*100:.0f}%)")
    print(f"ICO (ARES):            {with_ico} ({with_ico/total*100:.0f}%)")
    print(f"Contacts found:        {with_contacts} ({with_contacts/total*100:.0f}%)")
    print(f"Hiring signal:         {with_hiring} ({with_hiring/total*100:.0f}%)")
    print(f"Revenue growth:        {with_growth} ({with_growth/total*100:.0f}%)")
    print(f"Validation flags:      {with_flags}")
    print(f"Avg confidence:        {avg_conf:.0f}%")

    contact_accounts = [v for v in enrichment.values() if v.get("contacts")]
    if contact_accounts:
        total_contacts = sum(len(v["contacts"]) for v in contact_accounts)
        with_email = sum(
            sum(1 for c in v["contacts"] if c.get("email"))
            for v in contact_accounts
        )
        with_phone = sum(
            sum(1 for c in v["contacts"] if c.get("phone"))
            for v in contact_accounts
        )
        print(f"\nContact details:")
        print(f"  Total contacts:      {total_contacts}")
        print(f"  With email:          {with_email}")
        print(f"  With phone:          {with_phone}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Waterfall enrichment pipeline")
    parser.add_argument("--top", type=int, default=500)
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--contacts", action="store_true",
                        help="Run ZoomInfo contact search")
    parser.add_argument("--contacts-top", type=int, default=50,
                        help="Contact search for top N accounts")
    parser.add_argument("--signals", action="store_true",
                        help="Run Czech signal scrapers")
    parser.add_argument("--all", action="store_true",
                        help="Run everything")
    parser.add_argument("--rescore", action="store_true")
    args = parser.parse_args()

    init_db()

    input_csv = args.input or str(WORKSPACE_ROOT / "prioritized_accounts.csv")
    enrichment_path = args.output or str(ENRICHMENT_DIR / "account_enrichment.json")

    if not Path(input_csv).exists():
        print(f"Error: {input_csv} not found")
        sys.exit(1)

    print(f"Loading top {args.top} accounts ...")
    accounts = load_prioritized(input_csv, top_n=args.top)
    print(f"Loaded {len(accounts)} accounts")

    enrichment = load_enrichment(enrichment_path)
    print(f"Existing enrichment: {len(enrichment)} accounts")

    enrichment = waterfall_enrich_company(accounts, enrichment)
    save_enrichment(enrichment, enrichment_path)

    if args.contacts or args.all:
        contact_accounts = accounts[:args.contacts_top]
        enrichment = waterfall_contacts(contact_accounts, enrichment)
        save_enrichment(enrichment, enrichment_path)

    if args.signals or args.all:
        enrichment = waterfall_signals(accounts, enrichment)
        save_enrichment(enrichment, enrichment_path)

    print_waterfall_summary(enrichment)

    if args.rescore:
        print("\n--- Re-scoring with enrichment data ---")
        from scoring.territory_scorer import (
            load_and_aggregate, score_all, write_results, print_summary,
        )
        csv_source = str(WORKSPACE_ROOT / "Martin Valovic FY27 1 copy.csv")
        accts = load_and_aggregate(csv_source)
        results = score_all(accts, enrichment=enrichment)
        output_csv = str(WORKSPACE_ROOT / "prioritized_accounts_enriched.csv")
        write_results(results, output_csv)
        print_summary(results)


if __name__ == "__main__":
    main()
