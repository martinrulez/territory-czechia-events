"""Kurzy.cz financial data scraper using Playwright headless browser.

Bypasses JavaScript bot protection (jetoklient cookie) by running a real
Chromium instance. Extracts revenue, profit, employee count, and
year-over-year revenue growth from the company registry pages.
"""

import re
import time
from typing import Optional

from db.database import get_cached_enrichment, save_enrichment

_browser = None
_context = None


def _ensure_browser():
    """Lazily launch a Playwright Chromium browser (reused across calls)."""
    global _browser, _context
    if _browser is not None:
        return _context

    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    _browser = pw.chromium.launch(headless=True)
    _context = _browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="cs-CZ",
    )
    return _context


def lookup_financials_pw(ico: str, company_name: str = "") -> dict:
    """Scrape financial data from Kurzy.cz using Playwright.

    Returns revenue_czk, profit_czk, employees, revenue_growth_pct,
    and multi-year revenue_history when available.
    """
    if not ico or len(str(ico).strip()) < 2:
        return {"success": False, "error": "ICO required"}

    ico = str(ico).strip()
    cache_key = f"kurzy_pw:{ico}"
    cached = get_cached_enrichment("kurzy_pw", cache_key)
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

    try:
        ctx = _ensure_browser()
        page = ctx.new_page()

        url = f"https://rejstrik-firem.kurzy.cz/{ico}/"
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(2000)

        html = page.content()

        if len(html) < 2000:
            page.close()
            save_enrichment("kurzy_pw", cache_key, result)
            return result

        _parse_html(html, result)

        if not result.get("revenue_czk") or not result.get("employees"):
            final_url = page.url
            if final_url and "/rejstrik-firem.kurzy.cz/" in final_url or "kurzy.cz" in final_url:
                slug = final_url.rstrip("/").split("/")[-1] if "/" in final_url else ""
                if slug and slug != ico:
                    stats_url = f"https://rejstrik-firem.kurzy.cz/{ico}/{slug}/statisticky-urad/"
                else:
                    stats_url = f"https://rejstrik-firem.kurzy.cz/{ico}/"
                    resp = page.goto(stats_url, wait_until="domcontentloaded", timeout=15000)
                    page.wait_for_timeout(1500)
                    final_url = page.url
                    slug = final_url.rstrip("/").split("/")[-1]
                    if slug and slug != ico:
                        stats_url = f"https://rejstrik-firem.kurzy.cz/{ico}/{slug}/statisticky-urad/"
                    else:
                        stats_url = None

                if stats_url:
                    try:
                        page.goto(stats_url, wait_until="domcontentloaded", timeout=15000)
                        page.wait_for_timeout(2000)
                        stats_html = page.content()
                        if len(stats_html) > 2000:
                            _parse_html(stats_html, result)
                    except Exception:
                        pass

        page.close()

    except Exception:
        pass

    if result.get("success"):
        save_enrichment("kurzy_pw", cache_key, result)
    else:
        result["success"] = False
        save_enrichment("kurzy_pw", cache_key, result)

    return result


def _parse_html(html: str, result: dict):
    """Parse financial data from the Kurzy.cz HTML content."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    name_el = soup.select_one("h1, .company-name, .rejstrik-firma-nazev")
    if name_el and not result.get("company_name"):
        result["company_name"] = name_el.get_text(strip=True)

    tables = soup.select("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.select("th")]

        has_revenue = any(
            kw in h for h in headers
            for kw in ("tržby", "obrat", "výnosy", "revenue")
        )
        has_finance = any(
            kw in h for h in headers
            for kw in ("výsledek", "zisk", "hospodaření", "profit")
        )
        has_employees = any(
            kw in h for h in headers
            for kw in ("zaměstnanc", "pracovník", "employee")
        )

        if has_revenue or has_finance or has_employees:
            _parse_financial_table(table, result)

    _extract_revenue_history(soup, result)

    text = soup.get_text()

    emp_range_match = re.search(
        r"počet\s+zaměstnanců[:\s]*([\d]+)\s*[-–]\s*([\d]+)",
        text,
        re.IGNORECASE,
    )
    if emp_range_match:
        low = int(emp_range_match.group(1))
        high = int(emp_range_match.group(2))
        result["employees"] = (low + high) // 2
        result["employee_range"] = f"{low}-{high}"
        result["success"] = True

    if not result.get("employees"):
        emp_match = re.search(
            r"(?:počet\s+zaměstnanců|zaměstnanci|employees?)[:\s]*(\d[\d\s]*\d|\d+)",
            text,
            re.IGNORECASE,
        )
        if emp_match:
            emp = _parse_number(emp_match.group(1))
            if emp and 0 < emp < 1_000_000:
                result["employees"] = emp
                result["success"] = True

    rev_match = re.search(
        r"(?:tržby|obrat|výnosy)[:\s]*([\d\s.,]+)\s*(?:tis\.?\s*)?(?:Kč|CZK)",
        text,
        re.IGNORECASE,
    )
    if rev_match and not result.get("revenue_czk"):
        val = _parse_money_from_text(rev_match.group(0))
        if val:
            result["revenue_czk"] = val
            result["success"] = True


def _parse_financial_table(table, result: dict):
    """Extract values from a financial data table."""
    rows = table.select("tr")
    for row in rows:
        cells = row.select("td, th")
        if len(cells) < 2:
            continue

        label = cells[0].get_text(strip=True).lower()
        value_text = cells[-1].get_text(strip=True)

        if any(kw in label for kw in ("tržby", "obrat", "výnosy celkem")):
            val = _parse_money(value_text)
            if val is not None:
                result["revenue_czk"] = val
                result["success"] = True

        if any(kw in label for kw in ("výsledek hospodaření", "zisk", "čistý zisk")):
            val = _parse_money(value_text)
            if val is not None:
                result["profit_czk"] = val
                result["success"] = True

        if any(kw in label for kw in ("zaměstnanc", "pracovník")):
            emp = _parse_number(value_text)
            if emp is not None and 0 < emp < 1_000_000:
                result["employees"] = emp
                result["success"] = True


def _extract_revenue_history(soup, result: dict):
    """Extract multi-year revenue from tables to compute YoY growth."""
    history = []
    for table in soup.select("table"):
        rows = table.select("tr")
        for row in rows:
            cells = [c.get_text(strip=True) for c in row.select("td, th")]
            if len(cells) >= 2:
                year_match = re.search(r"(20\d{2})", cells[0])
                if year_match:
                    val = _parse_money(cells[1]) or _parse_money(cells[-1])
                    if val is not None:
                        history.append({"year": int(year_match.group(1)), "value": val})

    history.sort(key=lambda x: x["year"], reverse=True)
    seen = set()
    deduped = []
    for item in history:
        if item["year"] not in seen:
            seen.add(item["year"])
            deduped.append(item)

    if deduped:
        result["revenue_history"] = deduped[:5]
        result["success"] = True
        if len(deduped) >= 2:
            latest = deduped[0]["value"]
            previous = deduped[1]["value"]
            if previous and previous > 0:
                growth = (latest - previous) / previous * 100
                result["revenue_growth_pct"] = round(growth, 1)


def _parse_money(text: str) -> Optional[float]:
    """Parse Czech-formatted money values."""
    if not text:
        return None
    text = text.strip()
    cleaned = re.sub(r"\s+", "", text)

    multiplier = 1
    if "mil" in cleaned.lower():
        multiplier = 1_000_000
    elif "tis" in cleaned.lower():
        multiplier = 1_000

    number_match = re.search(r"[-+]?[\d.,]+", cleaned)
    if not number_match:
        return None

    num_str = number_match.group().replace(",", ".")
    if num_str.count(".") > 1:
        parts = num_str.split(".")
        num_str = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(num_str) * multiplier
    except ValueError:
        return None


def _parse_money_from_text(text: str) -> Optional[float]:
    """Parse money from a broader text match."""
    return _parse_money(text)


def _parse_number(text: str) -> Optional[int]:
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


def get_financial_signal_pw(ico: str, company_name: str = "") -> dict:
    """Simplified signal for the scoring engine."""
    data = lookup_financials_pw(ico, company_name)
    if not data.get("success"):
        return {"has_financials": False}

    return {
        "has_financials": True,
        "revenue_czk": data.get("revenue_czk"),
        "profit_czk": data.get("profit_czk"),
        "employees": data.get("employees"),
        "revenue_growth_pct": data.get("revenue_growth_pct"),
    }


def close_browser():
    """Shut down the Playwright browser cleanly."""
    global _browser, _context
    if _browser:
        try:
            _browser.close()
        except Exception:
            pass
        _browser = None
        _context = None
