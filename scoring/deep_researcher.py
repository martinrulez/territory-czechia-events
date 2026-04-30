"""Deep research data collector for event companies.

Gathers raw research data from multiple sources for each company:
- Existing enrichment (ARES, ZoomInfo firmographics, signals)
- Company website content (homepage + about + contact pages)
- ZoomInfo technographics (installed software)
- ZoomInfo decision-maker contacts

Saves per-company JSON to research_data/ for the report generator.

Usage:
    python -m scoring.deep_researcher --top 200
    python -m scoring.deep_researcher --top 50 --skip-website
    python -m scoring.deep_researcher --top 200 --skip-contacts
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from db.database import init_db
from scoring.event_enricher import load_event_companies, load_event_enrichment

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESEARCH_DIR = PROJECT_ROOT / "research_data"
ENRICHMENT_DIR = PROJECT_ROOT / "enrichment_data"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.5",
})

ABOUT_PATHS = ["/o-nas", "/about", "/about-us", "/o-spolecnosti", "/spolecnost", "/firma"]
CONTACT_PATHS = ["/kontakt", "/kontakty", "/contact", "/contacts"]
PRODUCT_PATHS = ["/produkty", "/sluzby", "/products", "/services", "/nabidka"]


def _resolve_domain(enrichment: dict, company_row: dict) -> str:
    """Get the best domain URL for a company."""
    domain = enrichment.get("zi_domain", "")
    if not domain:
        domain = enrichment.get("ml_domain", "")
    if not domain:
        domain = company_row.get("company_domain", "")
    if not domain:
        return ""
    domain = domain.strip().rstrip("/")
    if not domain.startswith("http"):
        domain = f"https://{domain}"
    return domain


def _fetch_page(url: str, timeout: int = 8) -> Optional[str]:
    """Fetch a page and return its text content, or None on failure."""
    try:
        resp = SESSION.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code >= 400:
            return None
        return resp.text
    except Exception:
        return None


def _extract_text(html: str, max_chars: int = 5000) -> str:
    """Extract clean text from HTML, stripping nav/footer/script."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.select("script, style, nav, footer, header, noscript, iframe"):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    lines = [l.strip() for l in text.split("\n") if l.strip() and len(l.strip()) > 2]
    return "\n".join(lines)[:max_chars]


def _extract_meta(html: str) -> dict:
    """Extract meta description and title from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    if soup.title:
        title = soup.title.get_text(strip=True)
    description = ""
    meta = soup.find("meta", attrs={"name": "description"})
    if meta:
        description = meta.get("content", "").strip()
    if not description:
        meta = soup.find("meta", attrs={"property": "og:description"})
        if meta:
            description = meta.get("content", "").strip()
    return {"title": title, "description": description}


def scrape_website(domain_url: str) -> dict:
    """Scrape company website for about/products/contact content."""
    if not domain_url:
        return {"success": False, "error": "no domain"}

    result = {
        "success": False,
        "domain": domain_url,
        "homepage": {},
        "about": {},
        "products": {},
        "contact_page": {},
    }

    homepage_html = _fetch_page(domain_url)
    if homepage_html:
        result["success"] = True
        meta = _extract_meta(homepage_html)
        result["homepage"] = {
            "title": meta["title"],
            "description": meta["description"],
            "text_excerpt": _extract_text(homepage_html, 3000),
        }

    for path in ABOUT_PATHS:
        html = _fetch_page(domain_url.rstrip("/") + path)
        if html and len(html) > 1000:
            result["about"] = {
                "url": domain_url.rstrip("/") + path,
                "text": _extract_text(html, 4000),
            }
            result["success"] = True
            break
        time.sleep(0.3)

    for path in PRODUCT_PATHS:
        html = _fetch_page(domain_url.rstrip("/") + path)
        if html and len(html) > 1000:
            result["products"] = {
                "url": domain_url.rstrip("/") + path,
                "text": _extract_text(html, 3000),
            }
            result["success"] = True
            break
        time.sleep(0.3)

    for path in CONTACT_PATHS:
        html = _fetch_page(domain_url.rstrip("/") + path)
        if html and len(html) > 500:
            result["contact_page"] = {
                "url": domain_url.rstrip("/") + path,
                "text": _extract_text(html, 2000),
            }
            result["success"] = True
            break
        time.sleep(0.3)

    return result


def get_technographics_safe(company_name: str, domain: str = "") -> dict:
    """Get ZoomInfo technographics with error handling."""
    try:
        from enrichment.zoominfo_client import get_technographics
        return get_technographics(company_name=company_name, domain=domain or None)
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_contacts_safe(company_name: str, segment: str = "", domain: str = "") -> dict:
    """Get ZoomInfo decision-maker contacts with error handling."""
    try:
        from enrichment.zoominfo_client import search_decision_makers
        return search_decision_makers(
            company_name=company_name,
            segment=segment or None,
            max_results=5,
            domain=domain or None,
        )
    except Exception as e:
        return {"success": False, "error": str(e), "contacts": []}


def research_company(
    company_row: dict,
    enrichment: dict,
    skip_website: bool = False,
    skip_techno: bool = False,
    skip_contacts: bool = False,
) -> dict:
    """Collect all research data for a single company."""
    key = company_row["company_key"]
    name = company_row["company_name"]
    enr = enrichment.get(key, {})
    domain_url = _resolve_domain(enr, company_row)
    domain_short = domain_url.replace("https://", "").replace("http://", "").rstrip("/")

    research = {
        "company_key": key,
        "company_name": name,
        "domain": domain_short,
        "domain_url": domain_url,
        "events": company_row.get("events", ""),
        "event_count": company_row.get("event_count", 1),
        "lead_class": company_row.get("lead_class", ""),
        "matched_account_id": company_row.get("matched_account_id"),
        "enrichment": enr,
        "website_data": {},
        "technographics": {},
        "contacts": {},
        "research_timestamp": time.strftime("%Y-%m-%d %H:%M"),
    }

    if not skip_website and domain_url:
        research["website_data"] = scrape_website(domain_url)

    if not skip_techno and (domain_short or name):
        research["technographics"] = get_technographics_safe(name, domain_short)
        time.sleep(0.1)

    if not skip_contacts and name:
        segment = enr.get("ares_primary_segment", enr.get("primary_segment", ""))
        research["contacts"] = get_contacts_safe(name, segment, domain_short)
        time.sleep(0.1)

    return research


def save_research(research: dict):
    """Save a single company's research data to JSON."""
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    key = research["company_key"]
    safe_name = re.sub(r'[^\w\-.]', '_', key)[:80]
    path = RESEARCH_DIR / f"{safe_name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(research, f, ensure_ascii=False, indent=2)
    return path


def _log(current: int, total: int, name: str, status: str):
    pct = current / total * 100 if total else 0
    print(f"  [{current}/{total} {pct:5.1f}%] {name[:50]:<50} {status}")


def run_research(
    top_n: int = 200,
    skip_website: bool = False,
    skip_techno: bool = False,
    skip_contacts: bool = False,
    force: bool = False,
):
    """Run deep research on the top N event companies."""
    init_db()
    companies = load_event_companies(top_n=top_n)
    enrichment = load_event_enrichment()

    companies.sort(
        key=lambda c: enrichment.get(c["company_key"], {}).get("opportunity_score", 0),
        reverse=True,
    )
    companies = companies[:top_n]

    print(f"=== Deep Research: {len(companies)} companies ===")
    print(f"  Website: {'skip' if skip_website else 'enabled'}")
    print(f"  Technographics: {'skip' if skip_techno else 'enabled'}")
    print(f"  Contacts: {'skip' if skip_contacts else 'enabled'}")
    print()

    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    done = 0
    skipped = 0

    for i, co in enumerate(companies):
        key = co["company_key"]
        name = co["company_name"]
        safe_name = re.sub(r'[^\w\-.]', '_', key)[:80]
        out_path = RESEARCH_DIR / f"{safe_name}.json"

        if out_path.exists() and not force:
            skipped += 1
            _log(i + 1, len(companies), name, "cached")
            continue

        research = research_company(
            co, enrichment,
            skip_website=skip_website,
            skip_techno=skip_techno,
            skip_contacts=skip_contacts,
        )
        save_research(research)
        done += 1

        web_ok = "web" if research["website_data"].get("success") else ""
        tech_ok = "tech" if research["technographics"].get("success") else ""
        cont_ok = f"contacts={research['contacts'].get('contacts_found', 0)}" if research["contacts"].get("success") else ""
        parts = [p for p in [web_ok, tech_ok, cont_ok] if p]
        _log(i + 1, len(companies), name, " ".join(parts) or "minimal")

        time.sleep(0.3)

    print(f"\nDone: {done} researched, {skipped} cached")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Deep research data collector")
    parser.add_argument("--top", type=int, default=200, help="Top N companies by opp score")
    parser.add_argument("--skip-website", action="store_true", help="Skip website scraping")
    parser.add_argument("--skip-techno", action="store_true", help="Skip ZI technographics")
    parser.add_argument("--skip-contacts", action="store_true", help="Skip ZI contact search")
    parser.add_argument("--force", action="store_true", help="Re-research even if cached")
    args = parser.parse_args()

    run_research(
        top_n=args.top,
        skip_website=args.skip_website,
        skip_techno=args.skip_techno,
        skip_contacts=args.skip_contacts,
        force=args.force,
    )


if __name__ == "__main__":
    main()
