"""Czech financial data client using Kurzy.cz / public registry APIs.

Retrieves financial data for Czech companies:
- Revenue and profit from annual reports
- Revenue growth rate (year-over-year)
- Employee count from financial filings
- Basic credit/financial health indicators

Uses ICO to query Kurzy.cz (with bot-protection bypass via session cookies)
and falls back to the ARES extended REST API for basic company data.
"""

import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from db.database import get_cached_enrichment, save_enrichment

KURZY_COMPANY_URL = "https://rejstrik-firem.kurzy.cz/{ico}/"
KURZY_STATS_URL = "https://rejstrik-firem.kurzy.cz/{ico}/{slug}/statisticky-urad/"
ARES_EXTENDED_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/{ico}"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}
REQUEST_DELAY = 0.8

_session = None


def _get_session() -> requests.Session:
    """Return a reusable session with browser-like headers."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(BROWSER_HEADERS)
    return _session


def _warmup_kurzy(session: requests.Session, ico: str):
    """First request to Kurzy.cz sets the 'jetoklient' bot-bypass cookie."""
    try:
        session.get(KURZY_COMPANY_URL.format(ico=ico), timeout=10)
        time.sleep(0.6)
    except requests.RequestException:
        pass


def lookup_financials(ico: str, company_name: str = "") -> dict:
    """Look up financial data for a company by ICO.

    Tries Kurzy.cz first (with session cookie), then falls back
    to the ARES extended API for employee/size estimates.
    """
    if not ico or len(str(ico).strip()) < 2:
        return {"success": False, "error": "ICO required"}

    ico = str(ico).strip()
    cache_key = f"kurzy:{ico}"
    cached = get_cached_enrichment("kurzy", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    result = {
        "success": False,
        "ico": ico,
        "company_name": company_name,
        "revenue_czk": None,
        "profit_czk": None,
        "employees": None,
        "revenue_history": [],
        "revenue_growth_pct": None,
        "from_cache": False,
    }

    session = _get_session()
    if "jetoklient" not in session.cookies:
        _warmup_kurzy(session, ico)

    time.sleep(REQUEST_DELAY)

    main_soup = None
    try:
        url = KURZY_COMPANY_URL.format(ico=ico)
        resp = session.get(url, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 3000:
            main_soup = BeautifulSoup(resp.text, "html.parser")
            result = _parse_kurzy_page(main_soup, result)
            _extract_employees_from_text(main_soup, result)
    except requests.RequestException:
        pass

    if not result.get("employees"):
        _try_kurzy_stats_page(session, ico, result)

    if not result.get("success"):
        _try_ares_extended(ico, result)

    save_enrichment("kurzy", cache_key, result)
    return result


def _try_kurzy_stats_page(session: requests.Session, ico: str, result: dict):
    """Try the statistics sub-page on Kurzy for employee/financial data."""
    try:
        main_url = KURZY_COMPANY_URL.format(ico=ico)
        resp = session.get(main_url, timeout=10)
        if resp.status_code != 200:
            return

        soup = BeautifulSoup(resp.text, "html.parser")

        stats_link = None
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "statisticky-urad" in href:
                stats_link = href
                break

        if not stats_link:
            return

        if stats_link.startswith("/"):
            stats_link = "https://rejstrik-firem.kurzy.cz" + stats_link

        time.sleep(0.5)
        resp2 = session.get(stats_link, timeout=10)
        if resp2.status_code == 200:
            soup2 = BeautifulSoup(resp2.text, "html.parser")
            _parse_kurzy_page(soup2, result)
            _extract_employees_from_text(soup2, result)
    except requests.RequestException:
        pass


def _extract_employees_from_text(soup: BeautifulSoup, result: dict):
    """Extract employee count from page text (stats pages often have it outside tables)."""
    if result.get("employees"):
        return
    text = soup.get_text()
    patterns = [
        r"[Pp]očet\s*zaměstnanců\s*[:\s]*(\d[\d\s]*\d|\d+)",
        r"[Zz]aměstnanc\w*\s*[:\s]*(\d[\d\s]*\d|\d+)",
        r"[Pp]racovník\w*\s*[:\s]*(\d[\d\s]*\d|\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            emp = _parse_number(m.group(1))
            if emp and 0 < emp < 1_000_000:
                result["employees"] = emp
                result["success"] = True
                return


def _try_ares_extended(ico: str, result: dict):
    """Fallback: fetch basic data from ARES REST API."""
    try:
        resp = requests.get(
            ARES_EXTENDED_URL.format(ico=ico),
            headers={"Accept": "application/json", "User-Agent": "SalesAgent/1.0"},
            timeout=10,
        )
        if resp.status_code != 200:
            return

        data = resp.json()
        name = data.get("obchodniJmeno", "")
        if name and not result.get("company_name"):
            result["company_name"] = name

        legal_form = data.get("pravniForma", "")
        nace = data.get("czNace2008") or data.get("czNace") or []

        if legal_form and nace:
            result["ares_legal_form"] = legal_form
            result["ares_nace"] = nace[:5]
            result["success"] = True
    except (requests.RequestException, ValueError):
        pass


def _parse_kurzy_page(soup: BeautifulSoup, result: dict) -> dict:
    """Parse the kurzy.cz company page for financial data."""
    name_el = soup.select_one("h1, .company-name, .rejstrik-firma-nazev")
    if name_el and not result.get("company_name"):
        result["company_name"] = name_el.get_text(strip=True)

    tables = soup.select("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.select("th")]

        has_revenue = any(
            "tržby" in h or "obrat" in h or "výnosy" in h for h in headers
        )
        has_finance = any(
            "výsledek" in h or "zisk" in h or "hospodaření" in h for h in headers
        )

        if has_revenue or has_finance:
            rows = table.select("tr")
            for row in rows:
                cells = row.select("td, th")
                if len(cells) < 2:
                    continue

                label = cells[0].get_text(strip=True).lower()
                value_text = cells[-1].get_text(strip=True)

                if "tržby" in label or "obrat" in label or "výnosy celkem" in label:
                    val = _parse_money(value_text)
                    if val is not None:
                        result["revenue_czk"] = val
                        result["success"] = True

                if "výsledek hospodaření" in label or "zisk" in label:
                    val = _parse_money(value_text)
                    if val is not None:
                        result["profit_czk"] = val
                        result["success"] = True

                if "zaměstnanc" in label or "pracovník" in label:
                    emp = _parse_number(value_text)
                    if emp is not None and 0 < emp < 1_000_000:
                        result["employees"] = emp
                        result["success"] = True

    revenue_items = _extract_revenue_history(soup)
    if revenue_items:
        result["revenue_history"] = revenue_items
        result["success"] = True
        if len(revenue_items) >= 2:
            latest = revenue_items[0]["value"]
            previous = revenue_items[1]["value"]
            if previous and previous > 0:
                growth = (latest - previous) / previous * 100
                result["revenue_growth_pct"] = round(growth, 1)

    text = soup.get_text()
    emp_match = re.search(
        r"(?:počet\s+zaměstnanců|zaměstnanci)[:\s]*(\d[\d\s]*\d|\d+)",
        text,
        re.IGNORECASE,
    )
    if emp_match and not result.get("employees"):
        emp = _parse_number(emp_match.group(1))
        if emp and 0 < emp < 1_000_000:
            result["employees"] = emp
            result["success"] = True

    return result


def _extract_revenue_history(soup: BeautifulSoup) -> list:
    """Try to extract multi-year revenue history from tables or charts."""
    history = []
    tables = soup.select("table")

    for table in tables:
        rows = table.select("tr")
        for row in rows:
            cells = [c.get_text(strip=True) for c in row.select("td, th")]
            if len(cells) >= 2:
                year_match = re.search(r"(20\d{2})", cells[0])
                if year_match:
                    val = _parse_money(cells[1]) or _parse_money(cells[-1])
                    if val is not None:
                        history.append(
                            {"year": int(year_match.group(1)), "value": val}
                        )

    history.sort(key=lambda x: x["year"], reverse=True)
    seen_years = set()
    deduped = []
    for item in history:
        if item["year"] not in seen_years:
            seen_years.add(item["year"])
            deduped.append(item)
    return deduped[:5]


def _parse_money(text: str):
    """Parse a Czech-formatted money value (tis. Kč, mil. Kč, etc.)."""
    if not text:
        return None
    text = text.strip()
    text = re.sub(r"\s+", "", text)

    multiplier = 1
    if "mil" in text.lower():
        multiplier = 1_000_000
    elif "tis" in text.lower():
        multiplier = 1_000

    number_match = re.search(r"[-+]?[\d.,]+", text)
    if not number_match:
        return None

    num_str = number_match.group()
    num_str = num_str.replace(",", ".")
    if num_str.count(".") > 1:
        parts = num_str.split(".")
        num_str = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(num_str) * multiplier
    except ValueError:
        return None


def _parse_number(text: str):
    """Parse a plain number, ignoring spaces and non-digits."""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def get_financial_signal(ico: str, company_name: str = "") -> dict:
    """Simplified financial signal for the scoring engine."""
    data = lookup_financials(ico, company_name)
    if not data.get("success"):
        return {
            "has_financials": False,
            "revenue_growth": None,
            "employee_count": None,
        }

    return {
        "has_financials": True,
        "revenue_czk": data.get("revenue_czk"),
        "profit_czk": data.get("profit_czk"),
        "employees": data.get("employees"),
        "revenue_growth_pct": data.get("revenue_growth_pct"),
        "revenue_growth": data.get("revenue_growth_pct"),
    }


def batch_financial_check(companies: list) -> list:
    """Check financials for multiple companies."""
    results = []
    for c in companies:
        signal = get_financial_signal(c.get("ico", ""), c.get("company_name", ""))
        signal["company_name"] = c.get("company_name", "")
        signal["ico"] = c.get("ico", "")
        results.append(signal)
    return results
