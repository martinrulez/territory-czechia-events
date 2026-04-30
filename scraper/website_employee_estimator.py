"""Website-based employee count estimator.

Scrapes company websites for signals that indicate company size:
- Explicit employee counts on About/O nás pages
- Team page member counts
- Office location counts
- Subsidiary/branch mentions
- Footer/header employee count mentions

Uses Playwright for JS-rendered pages, with requests fallback.
Results cached in SQLite.
"""

import re
import time
from typing import Optional, List, Dict

from db.database import get_cached_enrichment, save_enrichment

ABOUT_PATHS = [
    "/o-nas", "/o-spolecnosti", "/o-firme", "/about", "/about-us",
    "/spolecnost", "/firma", "/nase-firma", "/profil",
    "/vedeni", "/management", "/team", "/nas-tym", "/tym",
    "/kariera", "/kariera", "/career", "/careers", "/jobs",
    "/kontakt", "/kontakty", "/contact",
    "/",
]

EMPLOYEE_PATTERNS = [
    re.compile(r"(\d[\d\s,.]*)\s*(?:zaměstnanc[ůeí]|pracovník[ůeí]|lidí|koleg[ůůy])", re.I),
    re.compile(r"(?:zaměstnáváme|máme|tým|team)\s+(?:více\s+než\s+|přes\s+|nad\s+)?(\d[\d\s,.]*)\s*(?:zaměstnanc|pracovník|lidí|koleg|osob|člověk)", re.I),
    re.compile(r"(\d[\d\s,.]*)\s*(?:employees|team\s+members|people|colleagues|staff)", re.I),
    re.compile(r"(?:we\s+have|we\s+employ|team\s+of|staff\s+of|more\s+than)\s+(\d[\d\s,.]*)\s*(?:employees|people|members|professionals)", re.I),
    re.compile(r"(?:over|more\s+than|přes|více\s+než)\s+(\d[\d\s,.]*)\s*(?:zaměstnanc|employees|people|lidí|koleg)", re.I),
    re.compile(r"(\d{2,5})\+?\s*(?:zaměstnanc|employees)", re.I),
]

REVENUE_PATTERNS = [
    re.compile(r"(?:obrat|tržby|revenue|turnover)[:\s]+(\d[\d\s,.]*)\s*(?:mil|mld|mln|tis|CZK|Kč|EUR|€|\$|USD)", re.I),
    re.compile(r"(\d[\d\s,.]*)\s*(?:mil|mld)\s*(?:Kč|CZK|korun)", re.I),
]

BRANCH_PATTERNS = [
    re.compile(r"(\d+)\s*(?:poboč[eky]|provozoven|závodů|výrobních?\s+závodů?|factories|plants|offices|locations|branches)", re.I),
]

TEAM_MEMBER_SELECTORS = [
    "div.team-member", "div.member", "div.person", "div.employee",
    "div.tym-clen", "div.clen-tymu",
    ".team-grid > div", ".team-list > div",
    "article.person", "article.team-member",
    ".card.person", ".card.team",
]


def _parse_number(s: str) -> Optional[int]:
    """Parse a number string like '1 200', '1.200', '1,200'."""
    if not s:
        return None
    clean = re.sub(r"[\s,.]", "", s.strip())
    try:
        n = int(clean)
        return n if 1 <= n <= 500_000 else None
    except ValueError:
        return None


def _extract_signals_from_html(html: str, url: str = "") -> dict:
    """Extract employee/size signals from raw HTML text."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)

    signals = {
        "employee_count": None,
        "employee_source_detail": None,
        "revenue_mention": None,
        "branch_count": None,
        "team_page_members": 0,
        "has_career_page": False,
        "page_url": url,
    }

    for pat in EMPLOYEE_PATTERNS:
        m = pat.search(text)
        if m:
            n = _parse_number(m.group(1))
            if n and n >= 2:
                signals["employee_count"] = n
                signals["employee_source_detail"] = f"website_text:{m.group(0)[:80]}"
                break

    for pat in REVENUE_PATTERNS:
        m = pat.search(text)
        if m:
            signals["revenue_mention"] = m.group(0)[:100]
            break

    for pat in BRANCH_PATTERNS:
        m = pat.search(text)
        if m:
            n = _parse_number(m.group(1))
            if n:
                signals["branch_count"] = n
            break

    career_kws = ["kariéra", "kariera", "career", "volná místa", "volné pozice",
                  "nabídka práce", "hledáme", "join us", "we are hiring"]
    text_lower = text.lower()
    if any(kw in text_lower for kw in career_kws):
        signals["has_career_page"] = True

    for selector in TEAM_MEMBER_SELECTORS:
        members = soup.select(selector)
        if len(members) >= 2:
            signals["team_page_members"] = max(signals["team_page_members"], len(members))

    return signals


def estimate_employees_from_website(domain: str, company_name: str = "") -> dict:
    """Scrape a company website to estimate employee count.

    Uses Playwright to load pages and extract employee signals.
    Falls back to requests if Playwright is unavailable.
    """
    cache_key = f"emp_est:{domain.lower().strip()}"
    cached = get_cached_enrichment("website_emp", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    base_url = f"https://{domain.replace('www.', '')}"
    if not domain.replace("www.", "").strip():
        return {"employee_count": None, "method": "skip", "signals": []}

    all_signals = []
    best_emp = None
    best_source = None
    revenue_mention = None
    branch_count = None
    team_members = 0
    has_career = False

    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()
            page.set_default_timeout(12000)

            for path in ABOUT_PATHS:
                url = base_url.rstrip("/") + path
                try:
                    resp = page.goto(url, wait_until="domcontentloaded", timeout=10000)
                    if not resp or resp.status >= 400:
                        continue
                    page.wait_for_timeout(1500)
                    html = page.content()
                    sigs = _extract_signals_from_html(html, url)
                    all_signals.append(sigs)

                    if sigs["employee_count"] and (best_emp is None or sigs["employee_count"] > best_emp):
                        best_emp = sigs["employee_count"]
                        best_source = sigs["employee_source_detail"]

                    if sigs["revenue_mention"] and not revenue_mention:
                        revenue_mention = sigs["revenue_mention"]
                    if sigs["branch_count"] and (branch_count is None or sigs["branch_count"] > branch_count):
                        branch_count = sigs["branch_count"]
                    if sigs["team_page_members"] > team_members:
                        team_members = sigs["team_page_members"]
                    if sigs["has_career_page"]:
                        has_career = True

                    if best_emp and best_emp >= 10:
                        break

                except Exception:
                    continue

            browser.close()

    except ImportError:
        import requests as req
        from bs4 import BeautifulSoup

        for path in ABOUT_PATHS[:6]:
            url = base_url.rstrip("/") + path
            try:
                r = req.get(url, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; SalesAgent/1.0)",
                }, timeout=8, allow_redirects=True)
                if r.status_code < 400:
                    sigs = _extract_signals_from_html(r.text, url)
                    all_signals.append(sigs)
                    if sigs["employee_count"] and (best_emp is None or sigs["employee_count"] > best_emp):
                        best_emp = sigs["employee_count"]
                        best_source = sigs["employee_source_detail"]
                    if sigs["revenue_mention"] and not revenue_mention:
                        revenue_mention = sigs["revenue_mention"]
                time.sleep(0.5)
            except Exception:
                continue

    if not best_emp and team_members >= 3:
        best_emp = team_members
        best_source = f"team_page_count:{team_members}"

    result = {
        "employee_count": best_emp,
        "employee_source": best_source or "not_found",
        "revenue_mention": revenue_mention,
        "branch_count": branch_count,
        "team_page_members": team_members,
        "has_career_page": has_career,
        "pages_checked": len(all_signals),
        "method": "playwright" if best_emp else "not_found",
    }

    save_enrichment("website_emp", cache_key, result)
    return result


def batch_estimate_employees(accounts: List[Dict], max_accounts: int = None) -> Dict:
    """Estimate employees for a batch of accounts from their websites.

    Args:
        accounts: list of {"csn", "domain", "company_name"} dicts
        max_accounts: limit

    Returns: {csn: result_dict}
    """
    results = {}
    total = min(len(accounts), max_accounts) if max_accounts else len(accounts)
    found = 0
    cached = 0
    with_revenue = 0

    for i, acct in enumerate(accounts[:total]):
        csn = acct["csn"]
        domain = acct.get("domain", "")
        name = acct.get("company_name", "")

        if not domain:
            results[csn] = {"employee_count": None, "method": "no_domain"}
            continue

        result = estimate_employees_from_website(domain, company_name=name)
        results[csn] = result

        if result.get("from_cache"):
            cached += 1
        if result.get("employee_count"):
            found += 1
        if result.get("revenue_mention"):
            with_revenue += 1

        if (i + 1) % 10 == 0 or i == total - 1:
            print(f"  [{i+1}/{total}] Employees found: {found}, Revenue mentions: {with_revenue}, Cached: {cached}")

    print(f"Employee estimator: {found}/{total} counts found ({cached} from cache)")
    return results
