"""Czech ISVZ (Informacni system verejnych zakazek) client.

Queries the Czech public procurement portal to find companies that have
won government contracts -- a strong buying signal for AEC accounts.

Uses multiple fallback approaches:
1. ISVZ vestnik HTML search (with SSL verify disabled for cert issues)
2. Smlouvy.gov.cz contract registry API
3. NEN (Narodni Elektronicky Nastroj) search
"""

import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

from db.database import get_cached_enrichment, save_enrichment

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ISVZ_SEARCH_URL = "https://www.vestnikverejnychzakazek.cz/SearchForm/Search"
SMLOUVY_API_URL = "https://smlouvy.gov.cz/vyhledavani"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_DELAY = 0.5

AEC_CPV_PREFIXES = [
    "45",  # Construction work
    "71",  # Architectural, engineering, and planning services
    "72",  # IT services (partial overlap)
    "44",  # Construction structures and materials
    "50",  # Repair and maintenance
]

AEC_KEYWORDS = [
    "stavb", "projekt", "architekton", "inženýr", "infrastruktur",
    "rekonstrukc", "výstavb", "silnic", "most", "budov", "vodovod",
    "kanalizac", "komunikac", "doprav",
]


def search_contracts_by_company(company_name: str, ico: str = None) -> dict:
    """Search for public contracts awarded to or involving a company."""
    if not company_name and not ico:
        return {"success": False, "error": "company_name or ico required"}

    cache_key = f"isvz:{ico or company_name.lower().strip()}"
    cached = get_cached_enrichment("isvz", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    contracts = _search_vestnik(ico or company_name)

    if not contracts and ico:
        contracts = _search_smlouvy(ico)

    aec_relevant = sum(1 for c in contracts if c.get("is_aec_relevant"))
    total_value = sum(c.get("value_czk", 0) for c in contracts)

    result = {
        "success": True,
        "company_name": company_name,
        "ico": ico or "",
        "contracts_found": len(contracts),
        "aec_relevant_count": aec_relevant,
        "total_value_czk": total_value,
        "contracts": contracts[:20],
        "has_active_projects": len(contracts) > 0,
        "from_cache": False,
    }

    save_enrichment("isvz", cache_key, result)
    return result


def _search_vestnik(search_term: str) -> list:
    """Query the vestnik search with SSL verification disabled."""
    time.sleep(REQUEST_DELAY)
    contracts = []

    try:
        params = {
            "searchText": search_term,
            "isZakazka": "true",
            "pageSize": "50",
        }
        resp = requests.get(
            "https://www.vestnikverejnychzakazek.cz/SearchForm/SearchResult",
            params=params,
            headers=HEADERS,
            timeout=15,
            verify=False,
        )
        if resp.status_code != 200:
            return contracts

        soup = BeautifulSoup(resp.text, "html.parser")

        rows = soup.select(
            "tr.search-result-row, div.search-result-item, "
            "div.result-item, tr[data-id], .vvz-result-item, "
            "div.card, article.result"
        )

        for row in rows:
            contract = _parse_contract_row(row)
            if contract:
                contracts.append(contract)

        if not contracts:
            for link in soup.select("a[href]"):
                href = link.get("href", "")
                text = link.get_text(strip=True)
                if ("/zakazk" in href or "/zakaz" in href) and len(text) > 10:
                    is_aec = any(kw in text.lower() for kw in AEC_KEYWORDS)
                    contracts.append({
                        "title": text[:200],
                        "value_czk": 0,
                        "date": "",
                        "cpv": "",
                        "is_aec_relevant": is_aec,
                        "source": "vestnik",
                    })
                    if len(contracts) >= 20:
                        break

    except requests.RequestException:
        pass

    return contracts


def _search_smlouvy(ico: str) -> list:
    """Fallback: search smlouvy.gov.cz (Czech contract registry) by ICO."""
    contracts = []
    time.sleep(REQUEST_DELAY)

    try:
        resp = requests.get(
            "https://smlouvy.gov.cz/vyhledavani",
            params={"subject_idnum": ico, "page": "1"},
            headers=HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            return contracts

        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select(".record, .smlouva, tr.item, div.result-item"):
            text = item.get_text(strip=True)
            if len(text) < 15:
                continue

            value = 0
            value_match = re.search(r"([\d\s,.]+)\s*(?:Kč|CZK)", text)
            if value_match:
                raw = value_match.group(1).replace(" ", "").replace(",", ".")
                try:
                    value = float(raw)
                except ValueError:
                    pass

            is_aec = any(kw in text.lower() for kw in AEC_KEYWORDS)
            contracts.append({
                "title": text[:200],
                "value_czk": value,
                "date": "",
                "cpv": "",
                "is_aec_relevant": is_aec,
                "source": "smlouvy",
            })

    except requests.RequestException:
        pass

    return contracts


def _parse_contract_row(element) -> dict:
    """Parse a single contract result."""
    text = element.get_text(strip=True)
    if not text or len(text) < 10:
        return None

    title_el = element.select_one("a, .result-title, td:first-child")
    title = title_el.get_text(strip=True) if title_el else text[:100]

    value = 0
    value_match = re.search(r"(\d[\d\s,.]+)\s*(Kč|CZK|mil\.?)", text)
    if value_match:
        raw = value_match.group(1).replace(" ", "").replace(",", ".")
        try:
            value = float(raw)
            if "mil" in (value_match.group(2) or ""):
                value *= 1_000_000
        except ValueError:
            value = 0

    date_match = re.search(r"(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})", text)
    date_str = date_match.group(1) if date_match else ""

    cpv_match = re.search(r"(\d{8}-\d)", text)
    cpv = cpv_match.group(1) if cpv_match else ""
    is_aec = any(cpv.startswith(p) for p in AEC_CPV_PREFIXES) if cpv else False
    if not is_aec:
        is_aec = any(kw in text.lower() for kw in AEC_KEYWORDS)

    return {
        "title": title[:200],
        "value_czk": value,
        "date": date_str,
        "cpv": cpv,
        "is_aec_relevant": is_aec,
        "source": "vestnik",
    }


def get_procurement_signal(company_name: str, ico: str = None) -> dict:
    """High-level signal for the scoring engine."""
    data = search_contracts_by_company(company_name, ico)
    if not data.get("success") or not data.get("has_active_projects"):
        return {
            "has_signal": False,
            "contracts": 0,
            "value_czk": 0,
        }

    return {
        "has_signal": True,
        "contracts": data["contracts_found"],
        "aec_contracts": data["aec_relevant_count"],
        "value_czk": data["total_value_czk"],
        "latest_contract": (
            data["contracts"][0]["title"] if data["contracts"] else ""
        ),
    }


def batch_procurement_check(companies: list) -> list:
    """Check public procurement for multiple companies."""
    results = []
    for c in companies:
        signal = get_procurement_signal(
            c.get("company_name", ""),
            c.get("ico"),
        )
        signal["company_name"] = c.get("company_name", "")
        results.append(signal)
    return results
