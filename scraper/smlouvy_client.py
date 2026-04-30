"""Czech public contract registry client (smlouvy.gov.cz).

Scrapes the search results page for contracts associated with a company
(by ICO). Identifies AEC-relevant contracts for scoring.

Source: https://smlouvy.gov.cz/vyhledavani
"""

import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from db.database import get_cached_enrichment, save_enrichment

SMLOUVY_SEARCH = "https://smlouvy.gov.cz/vyhledavani"
REQUEST_DELAY = 0.6

AEC_KEYWORDS = {
    "stavba", "stavební", "projekt", "rekonstrukce", "infrastruktur",
    "komunikace", "silnice", "kanalizace", "vodovod", "most",
    "architekton", "územní plán", "geodet", "revitalizac",
    "zateplení", "fasáda", "výstavba", "demolice", "inženýrsk",
    "dopravní", "železnic", "tunel", "dálnic", "oprav",
}

DM_KEYWORDS = {
    "strojírens", "výrob", "stroj", "obráběc", "sváře",
    "montáž", "automob", "součást", "nástroj", "formy",
}


def search_contracts(
    ico: str = "",
    company_name: str = "",
    limit: int = 50,
) -> dict:
    """Search public contracts for a company by ICO.

    Returns contract count, total value, and AEC/D&M relevance scores.
    """
    if not ico and not company_name:
        return {"success": False, "error": "Provide ICO or company name"}

    cache_key = f"smlouvy:{ico or company_name.lower().strip()}"
    cached = get_cached_enrichment("smlouvy", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    result = {
        "success": False,
        "ico": ico,
        "company_name": company_name,
        "contracts_count": 0,
        "total_value_czk": 0,
        "aec_contracts_count": 0,
        "aec_value_czk": 0,
        "dm_contracts_count": 0,
        "recent_contracts": [],
        "has_signal": False,
        "from_cache": False,
    }

    time.sleep(REQUEST_DELAY)

    try:
        params = {"searchResultList-limit": min(limit, 500)}
        if ico:
            params["party_idnum"] = ico
        else:
            params["party_name"] = company_name

        resp = requests.get(
            SMLOUVY_SEARCH,
            params=params,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "cs-CZ,cs;q=0.9",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            save_enrichment("smlouvy", cache_key, result)
            return result

        soup = BeautifulSoup(resp.text, "html.parser")
        _parse_results(soup, result)

    except requests.RequestException:
        pass

    save_enrichment("smlouvy", cache_key, result)
    return result


def _parse_results(soup: BeautifulSoup, result: dict):
    """Parse the smlouvy.gov.cz search results table."""
    table = soup.select_one("table")
    if not table:
        return

    rows = table.select("tr")
    if len(rows) < 2:
        return

    contracts = []
    for row in rows[1:]:
        cells = row.select("td")
        if len(cells) < 5:
            continue

        publisher = cells[0].get_text(strip=True)
        subject = cells[1].get_text(strip=True)
        date = cells[3].get_text(strip=True)
        value_text = cells[4].get_text(strip=True)
        counterparty = cells[5].get_text(strip=True) if len(cells) > 5 else ""

        value = _parse_contract_value(value_text)

        contracts.append({
            "publisher": publisher,
            "subject": subject,
            "date": date,
            "value_czk": value,
            "counterparty": counterparty,
        })

    if not contracts:
        return

    total_value = 0
    aec_count = 0
    aec_value = 0
    dm_count = 0
    recent = []

    for c in contracts:
        subject_lower = c["subject"].lower()
        value = c["value_czk"] or 0
        total_value += value

        is_aec = any(kw in subject_lower for kw in AEC_KEYWORDS)
        is_dm = any(kw in subject_lower for kw in DM_KEYWORDS)

        if is_aec:
            aec_count += 1
            aec_value += value
        if is_dm:
            dm_count += 1

        if len(recent) < 5:
            recent.append({
                "subject": c["subject"][:200],
                "value_czk": c["value_czk"],
                "date": c["date"],
                "is_aec": is_aec,
                "counterparty": c["counterparty"][:100],
            })

    result["contracts_count"] = len(contracts)
    result["total_value_czk"] = total_value
    result["aec_contracts_count"] = aec_count
    result["aec_value_czk"] = aec_value
    result["dm_contracts_count"] = dm_count
    result["recent_contracts"] = recent
    result["has_signal"] = len(contracts) > 0
    result["success"] = True


def _parse_contract_value(text: str) -> Optional[float]:
    """Parse a contract value from the results table."""
    if not text or "neuvedeno" in text.lower():
        return None
    cleaned = re.sub(r"[^\d.,\-]", "", text)
    cleaned = cleaned.replace(",", ".")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def get_procurement_signal(
    company_name: str,
    ico: str = "",
) -> dict:
    """Simplified procurement signal for the scoring engine."""
    data = search_contracts(ico=ico, company_name=company_name)
    if not data.get("success") or not data.get("has_signal"):
        return {"has_signal": False, "contracts": 0}

    return {
        "has_signal": True,
        "contracts": data["contracts_count"],
        "value_czk": data["total_value_czk"],
        "aec_contracts": data["aec_contracts_count"],
        "aec_value_czk": data["aec_value_czk"],
        "dm_contracts": data.get("dm_contracts_count", 0),
    }
