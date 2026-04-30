"""ARES VR (Veřejný rejstřík) client for statutory body extraction.

Queries the ARES VR REST endpoint to get statutory body members
(jednatelé, představenstvo, dozorčí rada, prokuristé) for Czech companies.

This is distinct from the basic ARES endpoint: it provides full
person-level data from the business registry (Obchodní rejstřík).
"""

import time

import requests

from db.database import get_cached_enrichment, save_enrichment

ARES_VR_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty-vr"
ARES_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "SalesAgent/1.0",
}
REQUEST_DELAY = 0.3

ORGAN_ROLE_MAP = {
    "STATUTARNI_ORGAN_CLEN": "Člen statutárního orgánu",
    "STATUTARNI_ORGAN_PREDSEDA": "Předseda",
    "JEDNATEL": "Jednatel",
    "DOZORCI_RADA_CLEN": "Člen dozorčí rady",
    "DOZORCI_RADA_PREDSEDA": "Předseda dozorčí rady",
    "PROKURA_OSOBA": "Prokurista",
}


def get_statutory_body(ico: str) -> dict:
    """Fetch statutory body members from ARES VR by ICO.

    Returns dict with success flag, list of active persons,
    and organ type info.
    """
    if not ico or len(str(ico).strip()) < 2:
        return {"success": False, "error": "ICO required", "persons": []}

    ico = str(ico).strip()
    cache_key = f"ares_vr:{ico}"
    cached = get_cached_enrichment("ares_vr", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    time.sleep(REQUEST_DELAY)

    url = f"{ARES_VR_URL}/{ico}"
    try:
        resp = requests.get(url, headers=ARES_HEADERS, timeout=15)
        if resp.status_code == 404:
            result = {
                "success": False,
                "error": "Not found in VR",
                "persons": [],
                "from_cache": False,
            }
            save_enrichment("ares_vr", cache_key, result)
            return result
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"success": False, "error": str(e), "persons": []}

    data = resp.json()
    persons = []
    seen_names = set()

    for zaznam in data.get("zaznamy", []):
        for organ_list_key in ("statutarniOrgany", "ostatniOrgany"):
            for organ in zaznam.get(organ_list_key, []):
                organ_name = organ.get("nazevOrganu", "")
                for clen in organ.get("clenoveOrganu", []):
                    if clen.get("datumVymazu"):
                        continue

                    osoba = clen.get("fyzickaOsoba", {})
                    if not osoba:
                        continue

                    jmeno = osoba.get("jmeno", "")
                    prijmeni = osoba.get("prijmeni", "")
                    if not jmeno and not prijmeni:
                        continue

                    titul_pred = osoba.get("titulPred", "")
                    titul_za = osoba.get("titulZa", "")

                    full_name = " ".join(
                        p for p in [titul_pred, jmeno, prijmeni, titul_za] if p
                    ).strip()
                    name_key = f"{jmeno}|{prijmeni}".lower()
                    if name_key in seen_names:
                        continue
                    seen_names.add(name_key)

                    funkce = ""
                    clenstvi_data = clen.get("clenstvi", {})
                    if isinstance(clenstvi_data, dict):
                        fc = clenstvi_data.get("funkce", {})
                        if isinstance(fc, dict):
                            funkce = fc.get("nazev", "")
                        clenstvi_inner = clenstvi_data.get("clenstvi", {})
                    else:
                        clenstvi_inner = {}

                    since = ""
                    if isinstance(clenstvi_inner, dict):
                        since = clenstvi_inner.get("vznikClenstvi", "")

                    typ = clen.get("typAngazma", "")
                    role = funkce or ORGAN_ROLE_MAP.get(typ, clen.get("nazevAngazma", ""))

                    persons.append({
                        "first_name": jmeno,
                        "last_name": prijmeni,
                        "full_name": full_name,
                        "title": role,
                        "organ": organ_name,
                        "type": typ,
                        "since": since,
                    })

    result = {
        "success": len(persons) > 0,
        "persons": persons,
        "total": len(persons),
        "from_cache": False,
    }
    save_enrichment("ares_vr", cache_key, result)
    return result


def batch_get_statutory(ico_list: list) -> list:
    """Get statutory body for multiple companies."""
    results = []
    for ico in ico_list:
        r = get_statutory_body(ico)
        r["ico"] = ico
        results.append(r)
    return results
