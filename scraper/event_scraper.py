"""Main event scraper dispatcher.

Orchestrates scraping for all configured events, combining live scraping
results with fallback data to ensure completeness. Also dispatches
extended discovery sources (BVV catalog, archives, associations).
"""

from db.database import (
    upsert_event,
    insert_event_company,
    clear_event_companies,
    mark_event_scraped,
    get_event_companies,
    update_event_company_entity,
)
from scraper.company_classifier import classify_with_icp
from scraper.site_configs import EVENTS
from scraper.static_scraper import SITE_SCRAPERS


def _deduplicate(records: list[dict]) -> list[dict]:
    """Remove duplicate companies, keeping the richest record per company+person."""
    seen = {}
    for rec in records:
        name = (rec.get("company_name") or "").strip().lower()
        person = (rec.get("person_name") or "").strip().lower()
        key = f"{name}|{person}"
        if not name or name in ("", "logo", "image"):
            continue
        if key not in seen:
            seen[key] = rec
        else:
            existing = seen[key]
            for k, v in rec.items():
                if v and not existing.get(k):
                    existing[k] = v
    return list(seen.values())


def scrape_event(event_key: str) -> dict:
    """Scrape a single event and store results in the database.

    Returns a summary dict with counts.
    """
    if event_key not in EVENTS:
        return {"success": False, "error": f"Unknown event key: {event_key}"}

    config = EVENTS[event_key]

    event_id = upsert_event(
        event_name=config["event_name"],
        url=config["url"],
        event_date=config.get("event_date"),
        location=config.get("location"),
        industry_focus=config.get("industry_focus"),
        relevance_score=config.get("relevance_score", 5),
    )

    clear_event_companies(event_id)

    scraped = []
    if config["scraper_type"] == "static" and event_key in SITE_SCRAPERS:
        try:
            scraped = SITE_SCRAPERS[event_key](config)
        except Exception as e:
            scraped = []

    fallback_companies = config.get("fallback_companies", [])
    fallback_speakers = config.get("fallback_speakers", [])

    if config.get("fallback_json"):
        import json
        from pathlib import Path
        json_path = Path(__file__).resolve().parent.parent / config["fallback_json"]
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                fallback_companies = fallback_companies + json.load(f)

    all_records = scraped + fallback_companies + fallback_speakers
    deduped = _deduplicate(all_records)

    imported = 0
    for rec in deduped:
        company_name = (rec.get("company_name") or "").strip()
        if not company_name or len(company_name) < 2:
            continue
        classification = classify_with_icp(company_name)
        insert_event_company(
            event_id=event_id,
            company_name=company_name,
            company_domain=rec.get("company_domain"),
            role=rec.get("role", "unknown"),
            person_name=rec.get("person_name"),
            person_title=rec.get("person_title"),
            person_linkedin=rec.get("person_linkedin"),
            entity_type=classification["entity_type"],
            entity_status=classification["entity_status"],
            reject_reason=classification["reject_reason"],
        )
        imported += 1

    mark_event_scraped(event_id)

    return {
        "success": True,
        "event_name": config["event_name"],
        "event_id": event_id,
        "scraped_live": len(scraped),
        "fallback_used": len(fallback_companies) + len(fallback_speakers),
        "total_imported": imported,
    }


def scrape_all_events() -> list[dict]:
    """Scrape all configured events."""
    results = []
    for key in EVENTS:
        result = scrape_event(key)
        results.append(result)
    return results


def import_manual_companies(event_key: str, companies: list[dict]) -> dict:
    """Manually import companies for events that can't be scraped (b2match, etc.)."""
    if event_key not in EVENTS:
        return {"success": False, "error": f"Unknown event key: {event_key}"}

    config = EVENTS[event_key]
    event_id = upsert_event(
        event_name=config["event_name"],
        url=config["url"],
        event_date=config.get("event_date"),
        location=config.get("location"),
        industry_focus=config.get("industry_focus"),
        relevance_score=config.get("relevance_score", 5),
    )

    imported = 0
    for rec in companies:
        company_name = (rec.get("company_name") or "").strip()
        if not company_name:
            continue
        classification = classify_with_icp(company_name)
        insert_event_company(
            event_id=event_id,
            company_name=company_name,
            company_domain=rec.get("company_domain"),
            role=rec.get("role", "attendee"),
            person_name=rec.get("person_name"),
            person_title=rec.get("person_title"),
            person_linkedin=rec.get("person_linkedin"),
            entity_type=classification["entity_type"],
            entity_status=classification["entity_status"],
            reject_reason=classification["reject_reason"],
        )
        imported += 1

    mark_event_scraped(event_id)
    return {"success": True, "event_id": event_id, "imported": imported}


def list_event_keys() -> list[dict]:
    """Return all configured event keys with basic info."""
    return [
        {
            "key": key,
            "event_name": cfg["event_name"],
            "event_date": cfg.get("event_date", "TBD"),
            "industry_focus": cfg.get("industry_focus", "mixed"),
            "scraper_type": cfg["scraper_type"],
        }
        for key, cfg in EVENTS.items()
    ]


# ---------------------------------------------------------------------------
# Extended Discovery Sources
# ---------------------------------------------------------------------------

def scrape_bvv_catalog(priority_only: bool = True) -> dict:
    """Scrape the BVV/MSV exhibitor catalog and import into the Digital Factory event."""
    from scraper.bvv_catalog_scraper import scrape_bvv_catalog as _scrape

    event_id = upsert_event(
        event_name="MSV 2025 Exhibitor Catalog",
        url="https://ikatalog.bvv.cz/msv",
        event_date="2025-10-07",
        location="Brno, Czech Republic",
        industry_focus="D&M",
        relevance_score=9,
        event_type="discovery",
    )

    companies = _scrape(priority_only=priority_only)
    imported = 0
    for rec in companies:
        name = (rec.get("company_name") or "").strip()
        if not name or len(name) < 2:
            continue
        classification = classify_with_icp(name)
        insert_event_company(
            event_id=event_id,
            company_name=name,
            company_domain=rec.get("company_domain"),
            role=rec.get("role", "exhibitor"),
            person_name=rec.get("person_name"),
            person_title=rec.get("person_title"),
            entity_type=classification["entity_type"],
            entity_status=classification["entity_status"],
            reject_reason=classification["reject_reason"],
        )
        imported += 1

    mark_event_scraped(event_id)
    return {
        "success": True,
        "event_name": "MSV 2025 Exhibitor Catalog",
        "event_id": event_id,
        "total_imported": imported,
    }


def scrape_past_events() -> dict:
    """Scrape past event archives and add companies to their respective events."""
    from scraper.archive_scraper import scrape_archives

    result = scrape_archives()
    if not result.get("success"):
        return result

    total_imported = 0
    event_results = []

    for event_key, companies in result["results"].items():
        if event_key not in EVENTS:
            continue

        config = EVENTS[event_key]
        event_id = upsert_event(
            event_name=config["event_name"],
            url=config["url"],
            event_date=config.get("event_date"),
            location=config.get("location"),
            industry_focus=config.get("industry_focus"),
            relevance_score=config.get("relevance_score", 5),
        )

        existing = get_event_companies(event_id=event_id)
        existing_names = {(ec["company_name"] or "").lower().strip() for ec in existing}

        imported = 0
        for rec in companies:
            name = (rec.get("company_name") or "").strip()
            if not name or len(name) < 2:
                continue
            if name.lower().strip() in existing_names:
                continue
            classification = classify_with_icp(name)
            insert_event_company(
                event_id=event_id,
                company_name=name,
                role="past_attendee",
                entity_type=classification["entity_type"],
                entity_status=classification["entity_status"],
                reject_reason=classification["reject_reason"],
            )
            imported += 1

        total_imported += imported
        event_results.append({
            "event_name": config["event_name"],
            "new_companies": imported,
        })

    return {
        "success": True,
        "total_imported": total_imported,
        "events": event_results,
    }


def scrape_associations() -> dict:
    """Scrape Czech industry association member directories."""
    from scraper.association_scraper import scrape_all_associations

    assoc_data = scrape_all_associations()
    total_imported = 0
    results = {}

    source_configs = [
        ("sps", "SPS Construction Members", "AEC",
         "https://www.sps.cz/o-sps/seznam-clenu/"),
        ("spcr", "SPCR Industry Members", "D&M",
         "https://www.spcr.cz/en/membership/membership-base"),
        ("slevaren", "Czech Foundry Association Members", "D&M",
         "https://www.svazslevaren.cz/slevarny"),
        ("czgbc", "Czech Green Building Council Members", "AEC",
         "http://czgbc.org/en/members/list"),
        ("aviation", "Moravian Aviation Cluster Members", "D&M",
         "https://www.czech-aerospace.cz/f-nasi-clenove"),
        ("hkcr", "HK CR Brno Chamber Members", "mixed",
         "https://ohkbv.cz/clenstvi/seznam-clenu/"),
    ]

    for source_key, source_name, industry, url in source_configs:
        source = assoc_data.get(source_key, {})
        companies = source.get("companies", [])
        if not companies:
            results[source_key] = 0
            continue

        event_id = upsert_event(
            event_name=source_name,
            url=url,
            event_date="2026-01-01",
            location="Czech Republic",
            industry_focus=industry,
            relevance_score=7,
            event_type="discovery",
        )

        imported = 0
        for rec in companies:
            name = (rec.get("company_name") or "").strip()
            if not name or len(name) < 3:
                continue
            classification = classify_with_icp(name)
            insert_event_company(
                event_id=event_id,
                company_name=name,
                company_domain=rec.get("company_domain"),
                role="association_member",
                entity_type=classification["entity_type"],
                entity_status=classification["entity_status"],
                reject_reason=classification["reject_reason"],
            )
            imported += 1

        mark_event_scraped(event_id)
        total_imported += imported
        results[source_key] = imported

    return {
        "success": True,
        "total_imported": total_imported,
        "breakdown": results,
    }


def scrape_msv_exhibitors(cz_only: bool = False) -> dict:
    """Parse the MSV 2025 visitor guide PDF for the full exhibitor list.

    This downloads or reads the cached PDF text and extracts all exhibitors.
    If no local file exists, it fetches the PDF from bvv.cz.
    """
    import os
    from scraper.msv_pdf_parser import parse_msv_exhibitor_text

    cache_path = os.path.join(
        os.path.expanduser("~"),
        ".cursor/projects/Users-valovim-Desktop-ADSK-List-building/agent-tools",
        "54833ac7-aeb8-4472-b911-f8e6c6075b88.txt"
    )

    text = ""
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8", errors="ignore") as f:
            text = f.read()
    else:
        import requests
        try:
            resp = requests.get(
                "https://www.bvv.cz/veletrhy/MSV/2025/pdf/pruvodce_navstevnika_msv.pdf",
                timeout=30,
            )
            if resp.status_code == 200:
                text = resp.text
        except Exception:
            pass

    if not text:
        return {"success": False, "error": "Could not load MSV visitor guide text"}

    companies = parse_msv_exhibitor_text(text, cz_only=cz_only)

    event_id = upsert_event(
        event_name="MSV 2025 Full Exhibitor List",
        url="https://www.bvv.cz/veletrhy/MSV/2025/pdf/pruvodce_navstevnika_msv.pdf",
        event_date="2025-10-07",
        location="Brno, Czech Republic",
        industry_focus="D&M",
        relevance_score=9,
        event_type="discovery",
    )

    clear_event_companies(event_id)

    imported = 0
    for rec in companies:
        name = (rec.get("company_name") or "").strip()
        if not name or len(name) < 2:
            continue
        classification = classify_with_icp(name)
        insert_event_company(
            event_id=event_id,
            company_name=name,
            company_domain=rec.get("company_domain"),
            role="exhibitor",
            entity_type=classification["entity_type"],
            entity_status=classification["entity_status"],
            reject_reason=classification["reject_reason"],
        )
        imported += 1

    mark_event_scraped(event_id)
    return {
        "success": True,
        "event_name": "MSV 2025 Full Exhibitor List",
        "event_id": event_id,
        "total_parsed": len(companies),
        "total_imported": imported,
        "cz_only": cz_only,
    }


def scrape_urbis_catalog() -> dict:
    """Scrape full URBIS Smart Cities exhibitor catalog from ikatalog.bvv.cz."""
    from scraper.bvv_catalog_scraper import scrape_urbis_catalog as _scrape

    event_id = upsert_event(
        event_name="URBIS Smart Cities 2026",
        url="https://www.smartcityfair.cz/en/",
        event_date="2026-06-02",
        location="Brno, Czech Republic",
        industry_focus="AEC",
        relevance_score=8,
    )

    companies = _scrape()
    existing = get_event_companies(event_id=event_id)
    existing_names = {(ec["company_name"] or "").lower().strip() for ec in existing}

    imported = 0
    for rec in companies:
        name = (rec.get("company_name") or "").strip()
        if not name or len(name) < 2:
            continue
        if name.lower().strip() in existing_names:
            continue
        classification = classify_with_icp(name)
        insert_event_company(
            event_id=event_id,
            company_name=name,
            company_domain=rec.get("company_domain"),
            role=rec.get("role", "exhibitor"),
            person_title=rec.get("person_title"),
            entity_type=classification["entity_type"],
            entity_status=classification["entity_status"],
            reject_reason=classification["reject_reason"],
        )
        imported += 1

    mark_event_scraped(event_id)
    return {
        "success": True,
        "event_name": "URBIS Smart Cities 2026",
        "event_id": event_id,
        "new_imported": imported,
        "previously_existing": len(existing_names),
    }


def scrape_forarch_catalog() -> dict:
    """Scrape FOR ARCH exhibitor catalog from katalogy.abf.cz."""
    from scraper.bvv_catalog_scraper import scrape_abf_catalog as _scrape

    event_id = upsert_event(
        event_name="FOR ARCH 2026",
        url="https://forarch.cz/",
        event_date="2026-09-16",
        location="Prague, Czech Republic",
        industry_focus="AEC",
        relevance_score=9,
    )

    companies = _scrape()
    existing = get_event_companies(event_id=event_id)
    existing_names = {(ec["company_name"] or "").lower().strip() for ec in existing}

    imported = 0
    for rec in companies:
        name = (rec.get("company_name") or "").strip()
        if not name or len(name) < 2:
            continue
        if name.lower().strip() in existing_names:
            continue
        classification = classify_with_icp(name)
        insert_event_company(
            event_id=event_id,
            company_name=name,
            company_domain=rec.get("company_domain"),
            role=rec.get("role", "exhibitor"),
            entity_type=classification["entity_type"],
            entity_status=classification["entity_status"],
            reject_reason=classification["reject_reason"],
        )
        imported += 1

    mark_event_scraped(event_id)
    return {
        "success": True,
        "event_name": "FOR ARCH 2026",
        "event_id": event_id,
        "new_imported": imported,
        "previously_existing": len(existing_names),
    }


def ares_validate_companies(event_id: int = None) -> dict:
    """Validate event companies via ARES Czech business registry.

    Looks up each company; if found, marks entity_status='verified' and
    entity_type='company'.  Already-rejected entries (individuals, food
    trucks, etc.) are skipped — their status stays 'rejected'.
    """
    from scraper.ares_client import lookup_company

    companies = get_event_companies(event_id=event_id)
    validated = 0
    not_found = 0
    errors = 0
    cached = 0
    skipped_rejected = 0

    for ec in companies:
        ec_dict = dict(ec)
        name = (ec_dict["company_name"] or "").strip()
        if not name or len(name) < 3:
            continue

        if ec_dict.get("entity_status") == "rejected":
            skipped_rejected += 1
            continue

        result = lookup_company(name)
        found = False
        if result.get("from_cache"):
            cached += 1
            found = result.get("success", False)
        elif result.get("success"):
            found = True
        elif "Not found" in result.get("error", ""):
            not_found += 1
        else:
            errors += 1

        if found:
            validated += 1
            update_event_company_entity(
                ec_id=ec_dict["id"],
                entity_type="company",
                entity_status="verified",
            )
        elif not result.get("success") and "Not found" in result.get("error", ""):
            update_event_company_entity(
                ec_id=ec_dict["id"],
                entity_status="unverified",
            )

    return {
        "success": True,
        "total": len(companies),
        "validated": validated,
        "not_found": not_found,
        "errors": errors,
        "from_cache": cached,
        "skipped_rejected": skipped_rejected,
    }
