"""Czech Obchodni rejstrik (OR) / Justice.cz client.

Queries the Czech business registry for:
- Financial statements (revenue, profit, employee count from annual reports)
- Statutory body changes (leadership changes -- new jednatel, reditel, etc.)
- Ownership changes (M&A signals)

Uses the ARES ICO number to look up companies in the OR.
"""

import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from db.database import get_cached_enrichment, save_enrichment

OR_BASE = "https://or.justice.cz"
OR_SEARCH_URL = f"{OR_BASE}/ias/ui/rejstrik-\$telerik"
OR_DETAIL_URL = f"{OR_BASE}/ias/ui/rejstrik-firma.vysledky"
JUSTICE_API_URL = "https://or.justice.cz/ias/ui/rejstrik-firma.vysledky?ico={ico}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) SalesAgent/1.0",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "cs,en;q=0.5",
}

REQUEST_DELAY = 0.5


def lookup_by_ico(ico: str) -> dict:
    """Look up a company in the OR by ICO and extract key signals.

    Returns:
        Dict with financial data, statutory body info, and change signals.
    """
    if not ico or len(str(ico).strip()) < 2:
        return {"success": False, "error": "ICO required"}

    ico = str(ico).strip().lstrip("0")
    cache_key = f"or:{ico}"
    cached = get_cached_enrichment("or_justice", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    time.sleep(REQUEST_DELAY)

    try:
        url = f"https://or.justice.cz/ias/ui/rejstrik-firma.vysledky?subjektId=&typ=PLATNY&nazev=&ic={ico}&obec=&ulice=&justice="
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"success": False, "error": f"OR request failed: {e}"}

    soup = BeautifulSoup(resp.text, "html.parser")

    result = {
        "success": True,
        "ico": ico,
        "company_name": "",
        "legal_form": "",
        "registered_address": "",
        "date_of_registration": "",
        "statutory_body": [],
        "recent_changes": [],
        "from_cache": False,
    }

    company_link = soup.select_one("a.result-name")
    if company_link:
        result["company_name"] = company_link.get_text(strip=True)

    detail_url = None
    if company_link and company_link.get("href"):
        detail_url = OR_BASE + company_link["href"]

    if detail_url:
        time.sleep(REQUEST_DELAY)
        try:
            detail_resp = requests.get(detail_url, headers=HEADERS, timeout=15)
            detail_resp.raise_for_status()
            detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
            result = _parse_detail_page(detail_soup, result)
        except requests.RequestException:
            pass

    save_enrichment("or_justice", cache_key, result)
    return result


def _parse_detail_page(soup: BeautifulSoup, result: dict) -> dict:
    """Parse the OR detail page for statutory body and registration info."""
    sections = soup.select("div.aunp-content")

    for section in sections:
        header = section.find_previous("h2")
        if not header:
            continue
        header_text = header.get_text(strip=True).lower()

        if "statutární orgán" in header_text or "jednatel" in header_text:
            persons = []
            rows = section.select("div.div-row, li")
            for row in rows:
                text = row.get_text(strip=True)
                if text and len(text) > 3:
                    person_info = _extract_person(text)
                    if person_info:
                        persons.append(person_info)
            if persons:
                result["statutory_body"] = persons

    change_entries = soup.select("div.podrizeny-objekt, div.zmeny-entry")
    for entry in change_entries:
        text = entry.get_text(strip=True)
        date_match = re.search(r"(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})", text)
        if date_match:
            result["recent_changes"].append({
                "date": date_match.group(1),
                "text": text[:200],
            })

    return result


def _extract_person(text: str) -> dict:
    """Extract a person's name and role from OR text."""
    name_match = re.match(
        r"^([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+\s+"
        r"[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+)",
        text,
    )
    if not name_match:
        return None

    name = name_match.group(1).strip()
    date_match = re.search(r"(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})", text)
    effective_date = date_match.group(1) if date_match else ""

    return {
        "name": name,
        "effective_date": effective_date,
        "raw_text": text[:150],
    }


def check_leadership_changes(ico: str, months_back: int = 12) -> dict:
    """Check if there were recent leadership changes at a company.

    Returns a signal dict with change_detected flag and details.
    """
    or_data = lookup_by_ico(ico)
    if not or_data.get("success"):
        return {"change_detected": False, "error": or_data.get("error")}

    cutoff = datetime.now().replace(
        year=datetime.now().year - (1 if months_back >= 12 else 0),
        month=max(1, datetime.now().month - months_back % 12),
    )

    recent_changes = []
    for change in or_data.get("recent_changes", []):
        try:
            d = change.get("date", "")
            parts = [p.strip() for p in d.split(".")]
            if len(parts) == 3:
                change_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                if change_date >= cutoff:
                    recent_changes.append(change)
        except (ValueError, IndexError):
            pass

    return {
        "change_detected": len(recent_changes) > 0,
        "changes_count": len(recent_changes),
        "changes": recent_changes[:5],
        "statutory_body": or_data.get("statutory_body", []),
        "company_name": or_data.get("company_name", ""),
    }


def batch_check_leadership(ico_list: list, months_back: int = 12) -> list:
    """Check leadership changes for multiple companies."""
    results = []
    for ico in ico_list:
        result = check_leadership_changes(ico, months_back)
        result["ico"] = ico
        results.append(result)
    return results
