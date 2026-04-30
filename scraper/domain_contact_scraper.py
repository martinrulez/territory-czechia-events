"""Domain contact scraper using Playwright.

Scrapes company websites for contact/team pages and extracts
person names, roles, emails, and phone numbers.

Targets common page patterns in Czech company sites:
  /kontakt, /kontakty, /o-nas, /vedeni, /team, /nase-firma, etc.
"""

import re
import time

from db.database import get_cached_enrichment, save_enrichment

CONTACT_PATHS = [
    "/kontakt", "/kontakty", "/kontakty/", "/contact", "/contacts",
    "/o-nas", "/o-nas/vedeni", "/o-spolecnosti",
    "/vedeni", "/vedeni-spolecnosti", "/management",
    "/team", "/nas-tym", "/tym",
    "/about", "/about-us", "/about/team",
    "/spolecnost", "/firma", "/nase-firma",
]

CZ_NAME_PATTERN = re.compile(
    r"(?:(?:Ing|Mgr|Bc|JUDr|MUDr|PhDr|RNDr|doc|prof|PhD|CSc|MBA|MSc|DiS)\.?\s*,?\s*)*"
    r"([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+)"
    r"\s+"
    r"([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+)"
    r"(?:\s*,?\s*(?:Ph\.?D|CSc|MBA|MSc|DiS|ACCA|M\.Sc)\.?)?"
)

ROLE_PATTERNS = [
    r"(?:generální\s+)?ředitel(?:ka)?",
    r"jednatel(?:ka)?",
    r"(?:obchodní|finanční|technický|výrobní|provozní)\s+ředitel(?:ka)?",
    r"(?:vedoucí|head)\s+\w+",
    r"(?:CEO|CFO|CTO|COO|CIO)",
    r"(?:director|manager|manažer)",
    r"prokurista",
    r"(?:předseda|člen)\s+(?:představenstva|dozorčí\s+rady)",
    r"(?:projektant|konstruktér|architekt)",
    r"(?:sales|obchod)\s*(?:manager|manažer|ředitel)",
    r"(?:IT|BIM|CAD)\s*(?:manager|manažer|správce|specialist)",
]

ROLE_RE = re.compile("|".join(ROLE_PATTERNS), re.IGNORECASE)

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
)

PHONE_RE = re.compile(
    r"(?:\+420\s?)?(?:\d{3}\s?){3}"
)


def scrape_domain_contacts(domain: str, company_name: str = "") -> dict:
    """Scrape a company website for contact information.

    Args:
        domain: The company domain (e.g. www.example.cz or example.cz).
        company_name: Company name for context.

    Returns:
        Dict with persons list and metadata.
    """
    if not domain:
        return {"success": False, "error": "No domain", "persons": []}

    domain = domain.strip().rstrip("/")
    if domain.startswith("http"):
        base_url = domain
    else:
        base_url = f"https://{domain}"

    cache_key = f"domain_contacts:{domain.lower()}"
    cached = get_cached_enrichment("domain_contacts", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "error": "Playwright not installed", "persons": []}

    persons = []
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

        for path in CONTACT_PATHS:
            url = base_url.rstrip("/") + path
            pages_tried += 1

            try:
                page = ctx.new_page()
                resp = page.goto(url, wait_until="domcontentloaded", timeout=10000)
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
            page_persons = _extract_persons(html, base_url)
            for p in page_persons:
                p["source_url"] = url
                if not _is_duplicate(p, persons):
                    persons.append(p)

            if len(persons) >= 10:
                break

        browser.close()
    except Exception:
        pass
    finally:
        try:
            pw.stop()
        except Exception:
            pass

    result = {
        "success": len(persons) > 0,
        "persons": persons,
        "total": len(persons),
        "pages_tried": pages_tried,
        "pages_loaded": pages_loaded,
        "domain": domain,
        "from_cache": False,
    }
    save_enrichment("domain_contacts", cache_key, result)
    return result


def _extract_persons(html: str, base_url: str) -> list:
    """Extract person info from HTML content."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.select("script, style, nav, footer, header"):
        tag.decompose()

    persons = []
    all_emails = EMAIL_RE.findall(soup.get_text())
    skip_emails = {
        e for e in all_emails
        if any(p in e.lower() for p in ["info@", "office@", "obchod@", "sales@", "marketing@", "support@", "recepce@", "podatelna@", "faktura"])
    }

    contact_blocks = soup.select(
        ".team-member, .person, .contact-person, .vedeni, "
        ".member, .staff, .employee, .card, .vcard, "
        "[class*='team'], [class*='person'], [class*='contact'], "
        "[class*='vedeni'], [class*='member']"
    )

    if contact_blocks:
        for block in contact_blocks:
            person = _parse_block(block, skip_emails)
            if person and person.get("first_name"):
                persons.append(person)
    else:
        text = soup.get_text()
        persons = _extract_from_text(text, skip_emails)

    return persons


def _parse_block(block, skip_emails: set) -> dict:
    """Extract person data from a structured HTML block."""
    text = block.get_text(separator=" ", strip=True)

    name_match = CZ_NAME_PATTERN.search(text)
    if not name_match:
        return {}

    first_name = name_match.group(1)
    last_name = name_match.group(2)

    if _is_common_word(first_name) or _is_common_word(last_name):
        return {}

    role = ""
    role_match = ROLE_RE.search(text)
    if role_match:
        role = role_match.group(0).strip()

    email = ""
    email_matches = EMAIL_RE.findall(text)
    for em in email_matches:
        if em not in skip_emails:
            email = em
            break

    phone = ""
    phone_match = PHONE_RE.search(text)
    if phone_match:
        phone = phone_match.group(0).strip()

    full_prefix = text[:name_match.start()].strip()
    title_prefix = ""
    title_match = re.search(
        r"((?:Ing|Mgr|Bc|JUDr|MUDr|PhDr|RNDr|doc|prof)\.?\s*,?\s*)+",
        full_prefix,
    )
    if title_match:
        title_prefix = title_match.group(0).strip().rstrip(",").strip()

    full_name = " ".join(p for p in [title_prefix, first_name, last_name] if p)

    return {
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "title": role,
        "email": email,
        "phone": phone,
    }


def _extract_from_text(text: str, skip_emails: set) -> list:
    """Fallback: extract persons from unstructured text near role keywords."""
    persons = []
    lines = text.split("\n")

    for i, line in enumerate(lines):
        line = line.strip()
        if not line or len(line) < 5:
            continue

        role_match = ROLE_RE.search(line)
        if not role_match:
            continue

        context = line
        if i > 0:
            context = lines[i - 1].strip() + " " + context
        if i < len(lines) - 1:
            context = context + " " + lines[i + 1].strip()

        name_match = CZ_NAME_PATTERN.search(context)
        if not name_match:
            continue

        first_name = name_match.group(1)
        last_name = name_match.group(2)

        if _is_common_word(first_name) or _is_common_word(last_name):
            continue

        email = ""
        email_matches = EMAIL_RE.findall(context)
        for em in email_matches:
            if em not in skip_emails:
                email = em
                break

        phone = ""
        phone_match = PHONE_RE.search(context)
        if phone_match:
            phone = phone_match.group(0).strip()

        persons.append({
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "title": role_match.group(0).strip(),
            "email": email,
            "phone": phone,
        })

    return persons


def _is_duplicate(person: dict, existing: list) -> bool:
    """Check if person is already in the list."""
    key = f"{person.get('first_name', '').lower()}|{person.get('last_name', '').lower()}"
    for p in existing:
        existing_key = f"{p.get('first_name', '').lower()}|{p.get('last_name', '').lower()}"
        if key == existing_key:
            return True
    return False


COMMON_WORDS = {
    "kontakt", "kontakty", "adresa", "telefon", "email", "fax",
    "obchod", "firma", "informace", "pobočka", "czech", "republika",
    "profil", "stránka", "služby", "produkty", "navigace", "reference",
    "popis", "hlavní", "další", "naše", "nová", "nové",
}


def _is_common_word(word: str) -> bool:
    """Check if a word is a common Czech word, not a name."""
    return word.lower() in COMMON_WORDS
