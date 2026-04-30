"""ARES Czech Business Registry client.

Queries the official Czech ARES REST API to validate company names,
get ICO numbers, legal forms, addresses, and NACE industry codes.
"""

import json
import time

import requests

from db.database import get_cached_enrichment, save_enrichment

ARES_SEARCH_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/vyhledat"
ARES_DETAIL_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty"
ARES_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "SalesAgent/1.0",
}

REQUEST_DELAY = 0.25

NACE_TO_SEGMENT = {
    "41": "AEC", "42": "AEC", "43": "AEC",
    "71": "AEC",
    "68": "AEC",
    "24": "D&M", "25": "D&M", "26": "D&M", "27": "D&M",
    "28": "D&M", "29": "D&M", "30": "D&M",
    "22": "D&M",
    "33": "D&M",
    "58": "M&E", "59": "M&E", "60": "M&E",
    "62": "M&E", "63": "M&E",
    "90": "M&E",
}


def nace_to_autodesk_segment(nace_code: str) -> str:
    """Map a NACE code (first 2 digits) to Autodesk segment."""
    if not nace_code:
        return "unknown"
    prefix = str(nace_code).strip()[:2]
    return NACE_TO_SEGMENT.get(prefix, "unknown")


def lookup_company(company_name: str) -> dict:
    """Look up a company in ARES by name.

    Returns enrichment data with official name, ICO, legal form, NACE codes.
    Results are cached in the enrichment_cache table.
    """
    if not company_name or len(company_name) < 2:
        return {"success": False, "error": "Company name too short"}

    cache_key = f"ares:{company_name.lower().strip()}"
    cached = get_cached_enrichment("ares", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    try:
        time.sleep(REQUEST_DELAY)
        resp = requests.post(
            ARES_SEARCH_URL,
            headers=ARES_HEADERS,
            json={
                "obchodniJmeno": company_name,
                "pocet": 3,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        return {"success": False, "error": f"ARES API error: {e}"}

    subjects = data.get("ekonomickeSubjekty", [])
    if not subjects:
        result = {"success": False, "error": "Not found in ARES", "query": company_name}
        save_enrichment("ares", cache_key, result)
        return result

    best = subjects[0]
    nace_codes = []
    for nace_field in ("czNace", "czNace2008"):
        cinnosti = best.get(nace_field, [])
        if isinstance(cinnosti, list):
            for item in cinnosti:
                if isinstance(item, str):
                    nace_codes.append(item)
                elif isinstance(item, dict):
                    code = item.get("kod") or item.get("code") or ""
                    if code:
                        nace_codes.append(str(code))

    segments = set()
    for nc in nace_codes:
        seg = nace_to_autodesk_segment(nc)
        if seg != "unknown":
            segments.add(seg)

    address_parts = []
    sidlo = best.get("sidlo", {})
    if isinstance(sidlo, dict):
        for field in ("textovaAdresa", "nazevObce", "nazevUlice", "psc"):
            val = sidlo.get(field)
            if val:
                address_parts.append(str(val))

    ico_raw = best.get("ico") or best.get("icoId") or ""
    ico_value = str(ico_raw)
    if ico_value.startswith("ARES_"):
        ico_value = ico_value[5:]

    legal_form = best.get("pravniForma", "")
    if isinstance(legal_form, dict):
        legal_form = legal_form.get("nazev", "")
    else:
        legal_form = str(legal_form)

    result = {
        "success": True,
        "official_name": best.get("obchodniJmeno", ""),
        "ico": str(ico_value),
        "legal_form": legal_form,
        "address": ", ".join(address_parts) if address_parts else "",
        "nace_codes": nace_codes,
        "autodesk_segments": list(segments),
        "primary_segment": list(segments)[0] if segments else "unknown",
        "from_cache": False,
    }

    save_enrichment("ares", cache_key, result)
    return result


def batch_ares_lookup(company_names: list, delay: float = None) -> dict:
    """Look up multiple companies in ARES.

    Args:
        company_names: List of company name strings.
        delay: Override the per-request delay.

    Returns:
        Summary dict with results.
    """
    if delay is not None:
        global REQUEST_DELAY
        REQUEST_DELAY = delay

    found = 0
    not_found = 0
    errors = 0
    results = []

    for name in company_names:
        result = lookup_company(name)
        results.append(result)
        if result.get("success"):
            found += 1
        elif result.get("error", "").startswith("Not found"):
            not_found += 1
        else:
            errors += 1

    return {
        "success": True,
        "total": len(company_names),
        "found": found,
        "not_found": not_found,
        "errors": errors,
        "results": results,
    }
