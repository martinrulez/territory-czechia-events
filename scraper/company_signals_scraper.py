"""Company website signals scraper using Playwright.

Scrapes company websites for news/aktuality pages to extract
recent business signals (new projects, awards, hiring, expansion,
investments, leadership changes, technology adoption).

Targets common Czech corporate site patterns:
  /aktuality, /novinky, /news, /blog, /o-nas, /reference
"""

import re
import time
from datetime import datetime

from db.database import get_cached_enrichment, save_enrichment

NEWS_PATHS = [
    "/aktuality", "/aktuality/", "/novinky", "/novinky/",
    "/news", "/news/", "/blog", "/blog/",
    "/aktuality?page=1", "/novinky?page=1",
    "/o-nas", "/o-spolecnosti", "/about", "/about-us",
    "/reference", "/reference/", "/projekty", "/projects",
    "/kariera", "/career", "/kariéra",
]

SIGNAL_KEYWORDS = {
    "expansion": [
        "rozšíření", "expanze", "nová pobočka", "nový závod", "investice",
        "expansion", "new facility", "investment", "nová hala", "rozšiřujeme",
        "nová výrobní", "nový provoz",
    ],
    "hiring": [
        "hledáme", "nabídka práce", "volné místo", "přijmeme", "hiring",
        "career", "kariéra", "nábor", "pracovní pozice",
    ],
    "technology": [
        "digitalizace", "automatizace", "BIM", "CAD", "CNC", "robot",
        "3D tisk", "3D print", "laser", "IoT", "Industry 4.0",
        "Průmysl 4.0", "software", "ERP", "MES", "PLM",
        "digitální", "digital", "innovation", "inovace",
    ],
    "project": [
        "projekt", "zakázka", "stavba", "realizace", "dokončení",
        "project", "completion", "contract", "award", "dodávka",
    ],
    "award": [
        "ocenění", "cena", "award", "certifikace", "ISO",
        "vítěz", "winner", "nominace", "nominated",
    ],
    "leadership": [
        "nový ředitel", "nový jednatel", "jmenování", "změna vedení",
        "new CEO", "new director", "appointment", "management change",
    ],
    "sustainability": [
        "udržitelnost", "ESG", "sustainability", "green", "zelená",
        "uhlíková", "carbon", "solar", "fotovoltaika",
    ],
}

DATE_PATTERNS = [
    re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(202[3-6])"),
    re.compile(r"(202[3-6])-(\d{2})-(\d{2})"),
    re.compile(r"(\d{1,2})\.\s*(ledna|února|března|dubna|května|června|"
               r"července|srpna|září|října|listopadu|prosince)\s*(202[3-6])", re.IGNORECASE),
]

MONTH_MAP = {
    "ledna": 1, "února": 2, "března": 3, "dubna": 4, "května": 5,
    "června": 6, "července": 7, "srpna": 8, "září": 9,
    "října": 10, "listopadu": 11, "prosince": 12,
}


def _extract_date(text):
    """Try to extract a date from text."""
    for pattern in DATE_PATTERNS:
        m = pattern.search(text)
        if m:
            groups = m.groups()
            if len(groups) == 3:
                if groups[1] in MONTH_MAP:
                    return f"{groups[2]}-{MONTH_MAP[groups[1]]:02d}-{int(groups[0]):02d}"
                try:
                    if int(groups[0]) > 31:
                        return f"{groups[0]}-{groups[1]}-{groups[2]}"
                    return f"{groups[2]}-{int(groups[1]):02d}-{int(groups[0]):02d}"
                except (ValueError, IndexError):
                    pass
    return ""


def _classify_signal(text):
    """Classify a text snippet by signal type."""
    text_lower = text.lower()
    categories = []
    for category, keywords in SIGNAL_KEYWORDS.items():
        if any(kw.lower() in text_lower for kw in keywords):
            categories.append(category)
    return categories or ["general"]


def scrape_company_signals(domain, company_name="", max_signals=15):
    """Scrape a company website for recent business signals.

    Returns dict with signals list and metadata.
    """
    if not domain:
        return {"success": False, "error": "No domain", "signals": []}

    domain = domain.strip().rstrip("/")
    if domain.startswith("http"):
        base_url = domain
    else:
        base_url = f"https://{domain}"

    cache_key = f"web_signals:{domain.lower()}"
    cached = get_cached_enrichment("web_signals", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "error": "Playwright not installed", "signals": []}

    signals = []
    pages_tried = 0
    pages_loaded = 0

    pw = sync_playwright().start()
    try:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="cs-CZ",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        for path in NEWS_PATHS:
            if len(signals) >= max_signals:
                break

            url = base_url.rstrip("/") + path
            pages_tried += 1

            try:
                page = ctx.new_page()
                resp = page.goto(url, wait_until="domcontentloaded", timeout=12000)
                if resp and resp.status >= 400:
                    page.close()
                    continue
                page.wait_for_timeout(1500)
                html = page.content()
                page.close()
            except Exception:
                try:
                    page.close()
                except Exception:
                    pass
                continue

            if len(html) < 500:
                continue

            pages_loaded += 1
            page_signals = _extract_signals_from_html(html, url)
            for s in page_signals:
                if not _is_dup_signal(s, signals):
                    signals.append(s)

        browser.close()
    except Exception as exc:
        signals.append({"headline": f"Scraping error: {str(exc)[:80]}", "categories": ["error"]})
    finally:
        try:
            pw.stop()
        except Exception:
            pass

    signals = signals[:max_signals]

    result = {
        "success": len(signals) > 0,
        "signals": signals,
        "total": len(signals),
        "pages_tried": pages_tried,
        "pages_loaded": pages_loaded,
        "domain": domain,
        "scraped_at": datetime.now().strftime("%Y-%m-%d"),
        "from_cache": False,
    }
    save_enrichment("web_signals", cache_key, result)
    return result


def _extract_signals_from_html(html, source_url):
    """Extract business signals from an HTML page."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.select("script, style, nav, footer, header, aside"):
        tag.decompose()

    signals = []

    article_selectors = [
        "article", ".news-item", ".aktualita", ".novinka", ".blog-post",
        ".post-item", ".news-card", ".card", ".list-item",
        "[class*='news']", "[class*='aktual']", "[class*='novin']",
        "[class*='blog']", "[class*='article']",
    ]

    articles = []
    for sel in article_selectors:
        articles.extend(soup.select(sel))
        if len(articles) >= 20:
            break

    if articles:
        seen_texts = set()
        for art in articles[:20]:
            text = art.get_text(separator=" ", strip=True)
            if len(text) < 20 or len(text) > 2000:
                continue

            text_key = text[:80].lower()
            if text_key in seen_texts:
                continue
            seen_texts.add(text_key)

            headline = ""
            for h_tag in ["h2", "h3", "h4", "a", "strong", ".title", ".headline"]:
                h_el = art.select_one(h_tag)
                if h_el:
                    h_text = h_el.get_text(strip=True)
                    if 10 < len(h_text) < 200:
                        headline = h_text
                        break

            if not headline:
                headline = text[:120].strip()
                if len(headline) < 15:
                    continue

            date = _extract_date(text)
            categories = _classify_signal(text)

            if categories == ["general"] and not date:
                continue

            link = ""
            a_tag = art.select_one("a[href]")
            if a_tag:
                href = a_tag.get("href", "")
                if href.startswith("/"):
                    from urllib.parse import urljoin
                    link = urljoin(source_url, href)
                elif href.startswith("http"):
                    link = href

            signals.append({
                "headline": headline[:200],
                "date": date,
                "categories": categories,
                "source_url": link or source_url,
                "snippet": text[:300] if len(text) > len(headline) + 20 else "",
            })
    else:
        text = soup.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in text.split("\n") if len(l.strip()) > 20]
        for line in lines[:30]:
            categories = _classify_signal(line)
            if categories != ["general"]:
                date = _extract_date(line)
                signals.append({
                    "headline": line[:200],
                    "date": date,
                    "categories": categories,
                    "source_url": source_url,
                    "snippet": "",
                })

    return signals


def _is_dup_signal(signal, existing):
    """Check if a signal is a duplicate."""
    h = signal.get("headline", "").lower()[:60]
    for s in existing:
        if h and h in s.get("headline", "").lower():
            return True
    return False


def batch_scrape_signals(accounts, max_per_account=10):
    """Scrape signals for multiple accounts.

    Args:
        accounts: list of dicts with 'csn', 'website', 'company_name', 'rank'
    """
    results = {}
    total = len(accounts)
    success = 0
    cached = 0

    for i, acct in enumerate(accounts):
        domain = acct.get("website", "")
        company = acct.get("company_name", "")
        csn = acct.get("csn", "")
        rank = acct.get("rank", 0)

        if not domain:
            continue

        print(f"  [{i+1}/{total}] #{rank} {company} ({domain})...", end=" ", flush=True)

        result = scrape_company_signals(domain, company, max_signals=max_per_account)

        if result.get("from_cache"):
            cached += 1
            print(f"cached ({result.get('total', 0)} signals)")
        elif result.get("success"):
            success += 1
            print(f"found {result.get('total', 0)} signals")
        else:
            print(f"no signals ({result.get('error', 'none found')})")

        results[csn] = result
        time.sleep(0.3)

    return results, success, cached
