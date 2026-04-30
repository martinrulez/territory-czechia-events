"""Domain finder for Czech companies missing website data.

Uses multiple strategies:
1. ARES detail API (sometimes includes website)
2. Construct likely domain from company name
3. Google Custom Search API (if configured)
4. Common Czech domain patterns (.cz TLD probing)

Results are cached in SQLite to avoid repeated lookups.
"""

import re
import time
import socket
from typing import Optional, List, Dict

import requests

from db.database import get_cached_enrichment, save_enrichment

ARES_DETAIL_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/{ico}"
REQUEST_DELAY = 0.3

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}

STRIP_LEGAL = re.compile(
    r"\s*[,.]?\s*(?:s\.r\.o\.|s\. r\. o\.|spol\.\s*s\s*r\.?\s*o\.?|a\.s\.|a\. s\.|"
    r"s\.p\.|k\.s\.|v\.o\.s\.|z\.s\.|z\.ú\.|o\.p\.s\.|SE|se)\s*\.?\s*$",
    re.IGNORECASE,
)

STRIP_EXTRA = re.compile(
    r"\s*[-–—]\s*(czech|czechia|čechy|morava|cz|eu|europe)\s*$",
    re.IGNORECASE,
)


def _normalize_for_domain(name: str) -> str:
    """Convert company name to a plausible domain slug."""
    name = STRIP_LEGAL.sub("", name).strip()
    name = STRIP_EXTRA.sub("", name).strip()
    name = re.sub(r"\s*\(.*?\)\s*", " ", name).strip()

    trans = str.maketrans("áčďéěíňóřšťúůýžÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ",
                          "acdeeinorstuuyzACDEEINORSTUUYZ")
    slug = name.translate(trans).lower()
    slug = re.sub(r"[^a-z0-9]+", "", slug)
    return slug


def _probe_domain(domain: str, timeout: float = 4.0) -> bool:
    """Check if a domain resolves and has a live HTTP server."""
    if not domain or len(domain) > 253 or ".." in domain:
        return False
    if any(len(part) > 63 for part in domain.split(".")):
        return False

    try:
        socket.setdefaulttimeout(timeout)
        socket.getaddrinfo(domain, 443)
    except (socket.gaierror, OSError, UnicodeError):
        return False

    for scheme in ("https", "http"):
        try:
            r = requests.head(
                f"{scheme}://{domain}",
                headers=BROWSER_HEADERS,
                timeout=timeout,
                allow_redirects=True,
            )
            if r.status_code < 500:
                return True
        except (requests.RequestException, OSError, UnicodeError):
            continue
    return False


def _try_ares_website(ico: str) -> Optional[str]:
    """Check ARES detail API for a website field."""
    if not ico:
        return None

    cache_key = f"ares_web:{ico}"
    cached = get_cached_enrichment("domain_finder", cache_key)
    if cached:
        return cached.get("domain")

    try:
        url = ARES_DETAIL_URL.format(ico=ico)
        r = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
        time.sleep(REQUEST_DELAY)
        if r.status_code == 200:
            data = r.json()
            sid = data.get("sidlo", {})
            kontakty = data.get("czNace", [])

            for src in [data.get("datoveSchranky", [])]:
                pass

            web = None
            if isinstance(data.get("www"), str) and data["www"].strip():
                web = data["www"].strip()

            save_enrichment("domain_finder", cache_key, {"domain": web})
            return web
    except (requests.RequestException, ValueError):
        pass

    save_enrichment("domain_finder", cache_key, {"domain": None})
    return None


def _generate_candidates(name: str) -> List[str]:
    """Generate plausible domain candidates from company name."""
    slug = _normalize_for_domain(name)
    if not slug or len(slug) < 3:
        return []

    candidates = []
    candidates.append(f"www.{slug}.cz")
    candidates.append(f"{slug}.cz")
    candidates.append(f"www.{slug}.com")
    candidates.append(f"{slug}.eu")

    name_clean = STRIP_LEGAL.sub("", name).strip()
    words = re.split(r"[\s&,.-]+", name_clean)
    words = [w for w in words if len(w) > 1]

    if len(words) >= 2:
        trans = str.maketrans("áčďéěíňóřšťúůýžÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ",
                              "acdeeinorstuuyzACDEEINORSTUUYZ")
        short = "".join(w[0] for w in words[:4]).lower().translate(trans)
        if len(short) >= 2:
            candidates.append(f"www.{short}.cz")

        first_word = words[0].lower().translate(trans)
        first_word = re.sub(r"[^a-z0-9]", "", first_word)
        if first_word and first_word != slug:
            candidates.append(f"www.{first_word}.cz")
            candidates.append(f"{first_word}.cz")

    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def find_domain(company_name: str, ico: str = None) -> dict:
    """Find the domain for a company using multiple strategies.

    Returns: {"domain": "example.cz", "method": "ares"|"probe"|None, "candidates_tried": N}
    """
    cache_key = f"domain_found:{(company_name or '').lower().strip()}:{ico or ''}"
    cached = get_cached_enrichment("domain_finder", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    ares_web = _try_ares_website(ico) if ico else None
    if ares_web:
        clean = ares_web.replace("http://", "").replace("https://", "").rstrip("/")
        result = {"domain": clean, "method": "ares", "candidates_tried": 0}
        save_enrichment("domain_finder", cache_key, result)
        return result

    candidates = _generate_candidates(company_name)
    for idx, cand in enumerate(candidates):
        host = cand.replace("www.", "") if cand.startswith("www.") else cand
        try:
            if _probe_domain(host):
                result = {
                    "domain": cand,
                    "method": "probe",
                    "candidates_tried": idx + 1,
                }
                save_enrichment("domain_finder", cache_key, result)
                return result
        except Exception:
            continue
        time.sleep(0.2)

    result = {"domain": None, "method": None, "candidates_tried": len(candidates)}
    save_enrichment("domain_finder", cache_key, result)
    return result


def batch_find_domains(accounts: List[Dict], max_accounts: int = None) -> Dict:
    """Find domains for a batch of accounts.

    Args:
        accounts: list of {"csn", "company_name", "ico"} dicts
        max_accounts: limit processing count

    Returns: {csn: {"domain": ..., "method": ...}}
    """
    results = {}
    total = min(len(accounts), max_accounts) if max_accounts else len(accounts)
    found = 0
    cached = 0

    for i, acct in enumerate(accounts[:total]):
        csn = acct["csn"]
        name = acct.get("company_name", "")
        ico = acct.get("ico", "")

        result = find_domain(name, ico=ico)
        results[csn] = result

        if result.get("from_cache"):
            cached += 1
            if result.get("domain"):
                found += 1
        elif result.get("domain"):
            found += 1

        if (i + 1) % 25 == 0 or i == total - 1:
            print(f"  [{i+1}/{total}] Found: {found}, Cached: {cached}")

    print(f"Domain finder: {found}/{total} domains found ({cached} from cache)")
    return results
