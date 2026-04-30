"""BVV exhibitor catalog scrapers for ikatalog.bvv.cz.

Scrapes online exhibitor catalogs for:
- MSV Brno (1500+ companies across 17 industry categories)
- URBIS Smart Cities (140+ companies across 10 categories)
- FOR ARCH via ABF catalog (525 exhibitors, letter-paginated)
"""

import re
import time

import requests
from bs4 import BeautifulSoup

from scraper.static_scraper import HEADERS, fetch_page

CATALOG_BASE = "https://ikatalog.bvv.cz"
CATALOG_MAIN = f"{CATALOG_BASE}/msv/_fexa10466&lang=A"

PRIORITY_CATEGORIES = {
    "17": "Industry 4.0 / Digital Factory",
    "11": "Metal-working and forming machines",
    "15": "Plastics, Rubber and Composites",
    "07": "Electronics, automation and measuring",
    "09": "Research, development, transfer of technologies",
    "02": "Materials and components for mechanical engineering",
    "03": "Drives, hydraulics and pneumatics",
    "06": "Power engineering",
}


def _extract_companies_from_page(soup: BeautifulSoup) -> list[dict]:
    """Extract company names from a catalog page."""
    companies = []
    for h2 in soup.find_all("h2"):
        name = h2.get_text(strip=True)
        if not name or len(name) < 2:
            continue
        name = re.sub(r"\s+", " ", name).strip()
        skip_patterns = (
            "msv", "filter", "favourites", "visited", "about",
            "exhibitors by", "product categorie", "trade fair",
        )
        if any(p in name.lower() for p in skip_patterns):
            continue
        booth = ""
        next_sib = h2.find_next_sibling()
        if next_sib and next_sib.name == "p":
            booth_text = next_sib.get_text(strip=True)
            if "PAV" in booth_text or "Open" in booth_text:
                booth = booth_text

        description = ""
        for sib in h2.find_next_siblings():
            if sib.name == "h2":
                break
            if sib.name == "p" and sib.get_text(strip=True) != booth:
                desc_text = sib.get_text(strip=True)
                if desc_text and "PAV" not in desc_text and "Open air" not in desc_text:
                    description = desc_text
                    break

        companies.append({
            "company_name": name,
            "role": "exhibitor",
            "person_title": booth,
            "company_domain": "",
        })
    return companies


def _get_category_urls(soup: BeautifulSoup) -> list[dict]:
    """Extract category links from the main catalog page."""
    categories = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "nom=" in href and "fexa" in href:
            text = a_tag.get_text(strip=True)
            if text and len(text) > 3:
                cat_match = re.match(r"^(\d{2})", text)
                cat_id = cat_match.group(1) if cat_match else ""
                full_url = href if href.startswith("http") else f"{CATALOG_BASE}{href}"
                categories.append({
                    "id": cat_id,
                    "name": text,
                    "url": full_url,
                })
    return categories


def scrape_bvv_catalog(priority_only: bool = True, delay: float = 0.5) -> list[dict]:
    """Scrape the BVV/MSV exhibitor catalog.

    Args:
        priority_only: If True, only scrape high-priority categories relevant to Autodesk.
        delay: Seconds to wait between page requests.

    Returns:
        List of company dicts ready for import.
    """
    all_companies = []

    try:
        main_soup = fetch_page(CATALOG_MAIN)
    except Exception:
        return all_companies

    main_companies = _extract_companies_from_page(main_soup)
    all_companies.extend(main_companies)

    categories = _get_category_urls(main_soup)

    for cat in categories:
        if priority_only and cat["id"] and cat["id"] not in PRIORITY_CATEGORIES:
            continue

        try:
            time.sleep(delay)
            cat_soup = fetch_page(cat["url"])
            cat_companies = _extract_companies_from_page(cat_soup)
            all_companies.extend(cat_companies)
        except Exception:
            continue

    seen = set()
    deduped = []
    for c in all_companies:
        key = c["company_name"].lower().strip()
        if key not in seen and len(key) >= 2:
            seen.add(key)
            deduped.append(c)

    return deduped


# ---------------------------------------------------------------------------
# URBIS Smart Cities catalog
# ---------------------------------------------------------------------------

URBIS_CATALOG_MAIN = f"{CATALOG_BASE}/urbis/_fexa10468&lang=A"

URBIS_SKIP_PATTERNS = (
    "urbis", "filter", "favourites", "visited", "about",
    "product categorie", "trade fair", "all countries",
    "all areas", "all fairs", "list of firm", "contacts",
    "veletrhy brno",
)


def _extract_urbis_companies(soup: BeautifulSoup) -> list[dict]:
    """Extract company names from a URBIS catalog page."""
    companies = []
    for h2 in soup.find_all("h2"):
        name = h2.get_text(strip=True)
        if not name or len(name) < 2:
            continue
        name = re.sub(r"\s+", " ", name).strip()
        if any(p in name.lower() for p in URBIS_SKIP_PATTERNS):
            continue
        if len(name) > 120:
            continue

        booth = ""
        next_sib = h2.find_next_sibling()
        if next_sib and next_sib.name in ("p", "div"):
            booth_text = next_sib.get_text(strip=True)
            if "PAV" in booth_text or "VP" in booth_text or "Open" in booth_text:
                booth = booth_text

        companies.append({
            "company_name": name,
            "role": "exhibitor",
            "person_title": booth,
            "company_domain": "",
        })
    return companies


def _get_urbis_category_urls(soup: BeautifulSoup) -> list[dict]:
    """Extract category links from the URBIS catalog main page."""
    categories = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if ("nom=" in href or "fbda" in href) and ("urbis" in href or "10468" in href):
            text = a_tag.get_text(strip=True)
            if text and len(text) > 3:
                cat_match = re.match(r"^(\d{2})", text)
                cat_id = cat_match.group(1) if cat_match else ""
                full_url = href if href.startswith("http") else f"{CATALOG_BASE}{href}"
                if full_url not in [c["url"] for c in categories]:
                    categories.append({
                        "id": cat_id,
                        "name": text,
                        "url": full_url,
                    })
    return categories


def scrape_urbis_catalog(delay: float = 0.5) -> list[dict]:
    """Scrape the full URBIS Smart Cities exhibitor catalog.

    Crawls all 10 categories: Digital Transformation, Green Deal, Mobility,
    Energy, Government, Waste, Economic Development, Social Innovation,
    Education, Consulting.

    Returns:
        List of company dicts ready for import.
    """
    all_companies = []

    try:
        main_soup = fetch_page(URBIS_CATALOG_MAIN)
    except Exception:
        return all_companies

    main_companies = _extract_urbis_companies(main_soup)
    all_companies.extend(main_companies)

    categories = _get_urbis_category_urls(main_soup)

    for cat in categories:
        try:
            time.sleep(delay)
            cat_soup = fetch_page(cat["url"])
            cat_companies = _extract_urbis_companies(cat_soup)
            all_companies.extend(cat_companies)

            sub_cats = _get_urbis_category_urls(cat_soup)
            for sub in sub_cats:
                if sub["url"] != cat["url"]:
                    try:
                        time.sleep(delay)
                        sub_soup = fetch_page(sub["url"])
                        sub_companies = _extract_urbis_companies(sub_soup)
                        all_companies.extend(sub_companies)
                    except Exception:
                        continue
        except Exception:
            continue

    seen = set()
    deduped = []
    for c in all_companies:
        key = c["company_name"].lower().strip()
        if key not in seen and len(key) >= 2:
            seen.add(key)
            deduped.append(c)

    return deduped


# ---------------------------------------------------------------------------
# ABF / FOR ARCH exhibitor catalog
# ---------------------------------------------------------------------------

ABF_CATALOG_BASE = "https://katalogy.abf.cz"
ABF_FORARCH_URL = f"{ABF_CATALOG_BASE}/exhibitors&cat=326&lang=1"

ABF_SKIP_PATTERNS = (
    "for® arch", "for arch", "katalog", "catalogue", "filter",
    "exhibitors by", "vystavovatele", "contact", "login", "register",
    "home", "search", "about", "abf, a.s", "cs katalogy",
    "holder of the event", "catalogue and databases",
)


def _extract_abf_companies(soup: BeautifulSoup) -> list[dict]:
    """Extract company names from an ABF catalog page.

    ABF uses div.content__item-name for company names.
    """
    companies = []

    for name_div in soup.find_all("div", class_="content__item-name"):
        name = name_div.get_text(strip=True)
        if not name or len(name) < 2 or len(name) > 120:
            continue
        name = re.sub(r"\s+", " ", name).strip()
        if any(p in name.lower() for p in ABF_SKIP_PATTERNS):
            continue

        booth = ""
        loc_div = name_div.find_next_sibling("div", class_="content__item-location")
        if loc_div:
            booth = loc_div.get_text(strip=True)

        companies.append({
            "company_name": name,
            "role": "exhibitor",
            "person_title": booth,
            "company_domain": "",
        })

    return companies


def scrape_abf_catalog(delay: float = 0.5) -> list[dict]:
    """Scrape the ABF/FOR ARCH exhibitor catalog.

    The catalog is paginated by letter (A-Z, CH). Each letter page lists
    exhibitors whose names start with that letter.

    Returns:
        List of company dicts ready for import.
    """
    all_companies = []

    try:
        main_soup = fetch_page(ABF_FORARCH_URL)
    except Exception:
        return all_companies

    main_companies = _extract_abf_companies(main_soup)
    all_companies.extend(main_companies)

    for letter_link in main_soup.find_all("a", href=True):
        href = letter_link["href"]
        text = letter_link.get_text(strip=True)
        if "let=" in href and len(text) <= 2 and text.isalpha():
            full_url = f"{ABF_CATALOG_BASE}/{href}"
            try:
                time.sleep(delay)
                letter_soup = fetch_page(full_url)
                letter_companies = _extract_abf_companies(letter_soup)
                all_companies.extend(letter_companies)
            except Exception:
                continue

    seen = set()
    deduped = []
    for c in all_companies:
        key = c["company_name"].lower().strip()
        if key not in seen and len(key) >= 2:
            seen.add(key)
            deduped.append(c)

    return deduped
