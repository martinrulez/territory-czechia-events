"""Parse the MSV Visitor Guide PDF (OCR'd text) for the complete exhibitor list.

The BVV visitor guide contains a comprehensive list of 1000+ exhibitors with
company names, cities, country codes, and booth locations. This parser
extracts Czech companies from that text, filtering out Chinese/Asian companies
that are irrelevant to the Czechia territory.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional


COUNTRY_CODES = {"CZ", "SK", "DE", "AT", "PL", "HU", "CH", "IT", "FR",
                 "GB", "SE", "FI", "DK", "NL", "BE", "ES", "PT", "US",
                 "CA", "JP", "TW", "CN", "KR", "TR", "IN", "UA", "IL"}

BOOTH_PATTERN = re.compile(
    r"(?:PAV|VP)\s+[A-Z]\d?\s+\d{3}", re.IGNORECASE
)

SKIP_PREFIXES = (
    "www.", "http", "seznam vystavovatelů", "list of exhibitors",
    "plánková část", "plans of hall", "doprovodný program",
    "supporting programme", "veletržní informace", "fair information",
    "pavilon ", "pavilonu", "pavilion", "hall ", "termín", "datum",
    "umístění", "location", "organizer", "pořadatel", "provozní",
    "opening hours", "for exhibitors", "for visitors", "česká repub",
    "direction:", "vstup", "entry", "expoparking", "bus ",
    "změny", "changes", "vydavatel", "editor", "obálka",
    "uzávěrka", "tisk", "grafická", "francouzský pavilon",
)

SKIP_COUNTRIES = {"CN", "TW", "KR", "JP", "IN"}


def _extract_company_line(line: str) -> Optional[dict]:
    """Try to extract a company name and country from one line of the exhibitor list."""
    stripped = line.strip()
    if not stripped or len(stripped) < 4:
        return None

    lower = stripped.lower()
    if any(lower.startswith(p) for p in SKIP_PREFIXES):
        return None
    if stripped.isdigit():
        return None
    if not BOOTH_PATTERN.search(stripped) and "PAV" not in stripped and "VP " not in stripped:
        return None

    name_part = re.split(r"\.{2,}|PAV\s|VP\s", stripped)[0].strip()
    name_part = re.sub(r"\s*,\s*$", "", name_part)

    country = None
    country_match = re.search(r",\s*([A-Z]{2})\s*$", name_part)
    if country_match:
        country = country_match.group(1)
        name_part = name_part[:country_match.start()].strip().rstrip(",").strip()

    city = None
    city_match = re.search(r",\s*([^,]+)\s*$", name_part)
    if city_match:
        candidate = city_match.group(1).strip()
        if not re.search(r"(s\.r\.o|a\.s|spol|GmbH|Ltd|Inc|SA|AG|SAS|S\.r\.l)", candidate):
            city = candidate
            name_part = name_part[:city_match.start()].strip().rstrip(",").strip()

    if not name_part or len(name_part) < 2:
        return None

    return {
        "company_name": name_part,
        "city": city,
        "country": country,
    }


def parse_msv_exhibitor_text(text: str, cz_only: bool = False) -> list[dict]:
    """Parse the MSV visitor guide text and extract exhibitors.

    Args:
        text: Full OCR'd text from the MSV visitor guide PDF.
        cz_only: If True, only return Czech/Slovak companies.

    Returns:
        List of dicts with company_name, city, country, role, company_domain.
    """
    lines = text.split("\n")
    results = []
    pending_name = None

    for i, line in enumerate(lines):
        stripped = line.strip()

        if not stripped or stripped.isdigit():
            continue

        lower = stripped.lower()
        if any(lower.startswith(p) for p in SKIP_PREFIXES):
            pending_name = None
            continue

        if stripped.startswith("www.") or stripped.startswith("http"):
            if results:
                domain = stripped.split(",")[0].strip()
                if domain.startswith("www."):
                    domain = domain[4:]
                results[-1]["company_domain"] = domain
            continue

        if pending_name and (BOOTH_PATTERN.search(stripped) or "PAV" in stripped or "VP " in stripped):
            full_line = pending_name + " " + stripped
            parsed = _extract_company_line(full_line)
            if parsed:
                parsed["role"] = "exhibitor"
                parsed["company_domain"] = ""
                results.append(parsed)
            pending_name = None
            continue

        parsed = _extract_company_line(stripped)
        if parsed:
            parsed["role"] = "exhibitor"
            parsed["company_domain"] = ""
            results.append(parsed)
            pending_name = None
        elif stripped.endswith(",") and not lower.startswith("www"):
            pending_name = stripped
        else:
            pending_name = None

    if cz_only:
        results = [r for r in results if r.get("country") in (None, "CZ", "SK") and
                   r.get("country") not in SKIP_COUNTRIES]
    else:
        results = [r for r in results if r.get("country") not in SKIP_COUNTRIES]

    seen = set()
    deduped = []
    for r in results:
        key = r["company_name"].lower().strip()
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    return deduped


def parse_msv_from_file(filepath: str, cz_only: bool = False) -> list[dict]:
    """Parse MSV exhibitors from a text file (OCR'd PDF)."""
    path = Path(filepath)
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8", errors="ignore")
    return parse_msv_exhibitor_text(text, cz_only=cz_only)
