"""Past event archive scraper.

Scrapes previous editions (2024/2025) of the same events to find
companies that attended in prior years -- a strong signal they'll return.
"""

import re

from scraper.static_scraper import fetch_page, HEADERS

ARCHIVE_URLS = {
    "3dise": [
        "https://3dise.com/2025/",
        "https://3dise.com/2024/",
        "https://3dise.com/exhibitors-and-partners/",
    ],
    "connected_construction": [
        "https://connectedconstructiondays.cz/2025/",
    ],
    "game_access": [
        "https://game-access.com/conference/2025/",
    ],
    "lean_summit": [
        "https://beexcellent.cz/lean-summit-2025/",
        "https://beexcellent.cz/lean-summit-czechoslovakia/previous-year-2024/",
    ],
    "brno_industry40": [
        "https://www.b2match.com/e/bi40-2024",
        "https://www.b2match.com/e/bi40-2025",
    ],
    "for_arch": [
        "https://forarch.cz/historie-veletrhu/2025/",
        "https://forarch.cz/historie-veletrhu/2024/",
    ],
    "bim_day": [
        "https://www.bimday.cz/2025-2/",
    ],
    "architect_at_work": [
        "https://www.architectatwork.com/en/events/a@w-prague",
    ],
    "cadforum_2026": [
        "https://konference.cadforum.cz/",
    ],
    "designblok": [
        "https://www.designblok.cz/en/katalog",
    ],
}

LEAN_SUMMIT_2024_COMPANIES = [
    {"company_name": "Tatra banka", "person_name": "Natália Major", "person_title": "Member of the Board"},
    {"company_name": "New Generation Hospital Michalovce", "person_name": "Marián Haviernik", "person_title": "Hospital CEO"},
    {"company_name": "ČSOB banka", "person_name": "Branislav Pelech", "person_title": "Agile project lead"},
    {"company_name": "U. S. Steel Košice", "person_name": "Jana Darašová", "person_title": "Deputy GM Process Excellence"},
    {"company_name": "Beko Europe", "person_name": "Michal Major", "person_title": "Site Director"},
    {"company_name": "FRÄNKISCHE Industrial Pipes", "person_name": "Tomáš Šilhán", "person_title": "Ředitel závodu"},
    {"company_name": "Essity TORK", "person_name": "Zdeněk Havran", "person_title": "Key Account Manager"},
    {"company_name": "DEVELOR Czech", "person_name": "Martin Kunc", "person_title": "Managing Director"},
    {"company_name": "Berlin Brands Group", "person_name": "Richard Durech", "person_title": "Director of Operations CEE"},
    {"company_name": "O2 Slovakia", "person_name": "Eva Slobodová", "person_title": "Head of Growth"},
    {"company_name": "Up Slovensko", "person_name": "Štefan Kováč", "person_title": "Business Project Director"},
    {"company_name": "Syráreň Bel", "person_name": "Ján Česlák", "person_title": "Operations Manager"},
    {"company_name": "CommScope", "person_name": "Jaromír Doležel", "person_title": "Manager Continuous Improvement"},
    {"company_name": "Fores Technology Group", "person_name": "Peter Dzurjanin", "person_title": "CEO"},
    {"company_name": "SLOVNAFT a.s.", "person_name": "Janko Červenski", "person_title": "Project Manager Business Excellence"},
    {"company_name": "FasterFish s.r.o.", "person_name": "Tomáš Kisela", "person_title": "Co-founder, CEO"},
    {"company_name": "Raiffeisenbank a.s.", "person_name": "David Čunta", "person_title": "Lean Navigator"},
    {"company_name": "Edwards, s.r.o.", "person_name": "Jiří Waszek", "person_title": "Business Process Improvement Program Manager"},
    {"company_name": "Vaillant Group", "person_name": "Jakub Hutta", "person_title": "VPS Group Coach"},
    {"company_name": "4industry consulting", "person_name": "Roman Bače", "person_title": "Senior Business Consultant"},
    {"company_name": "Sewio", "person_name": "Šimon Chudoba", "person_title": "Regional Sales Manager"},
    {"company_name": "Alliance Laundry Systems LLC", "person_name": "Petr Opavský", "person_title": "Engineering Manager"},
    {"company_name": "Ennvea", "person_name": "Luděk Cigánek", "person_title": "CEO"},
    {"company_name": "Slovenské cukrovary, s.r.o", "person_name": "Ružena Brádňanová", "person_title": "Managing Director, CFO"},
]

TDISE_2025_PARTNERS = [
    {"company_name": "Leica Geosystems", "role": "partner", "company_domain": "leica-geosystems.com"},
    {"company_name": "RealityCapture", "role": "partner"},
    {"company_name": "XGRIDS", "role": "partner"},
    {"company_name": "Nira.app", "role": "partner"},
    {"company_name": "NUBIGON", "role": "partner"},
    {"company_name": "Twinzo", "role": "partner", "company_domain": "twinzo.eu"},
    {"company_name": "Scan 3D", "role": "partner"},
    {"company_name": "Ingos 3D", "role": "partner"},
    {"company_name": "Overhead4D", "role": "partner", "company_domain": "overhead4d.com"},
    {"company_name": "Tiki3D", "role": "partner"},
    {"company_name": "Everypoint", "role": "partner"},
    {"company_name": "ŠKODA AUTO", "role": "partner", "company_domain": "skoda-auto.com"},
    {"company_name": "NDN Tech", "role": "partner"},
    {"company_name": "Norriv", "role": "partner"},
    {"company_name": "Insta360", "role": "partner", "company_domain": "insta360.com"},
    {"company_name": "3DScanLA", "role": "partner"},
    {"company_name": "Drontex", "role": "partner"},
    {"company_name": "UAVA", "role": "partner"},
]

DIGITAL_FACTORY_2024_COMPANIES = [
    {"company_name": "National Centre for Industry 4.0", "role": "partner"},
    {"company_name": "RICAIP Testbed Prague", "role": "partner"},
    {"company_name": "KUKA", "role": "exhibitor", "company_domain": "kuka.com"},
    {"company_name": "Siemens", "role": "exhibitor", "company_domain": "siemens.com"},
    {"company_name": "Česká spořitelna", "role": "exhibitor"},
    {"company_name": "Smart Informatics", "role": "exhibitor"},
    {"company_name": "T-Mobile", "role": "exhibitor", "company_domain": "t-mobile.cz"},
    {"company_name": "Deprag", "role": "exhibitor", "company_domain": "deprag.com"},
    {"company_name": "DEL", "role": "exhibitor"},
    {"company_name": "Asseco CEIT", "role": "exhibitor"},
    {"company_name": "Bartech", "role": "exhibitor"},
    {"company_name": "MICROSYS", "role": "exhibitor"},
    {"company_name": "EUROTRONIC", "role": "exhibitor"},
    {"company_name": "Gedis Distribution", "role": "exhibitor"},
]


def _scrape_generic_logos(url: str) -> list[dict]:
    """Generic scraper: extract company names from img alt tags and headings."""
    results = []
    try:
        soup = fetch_page(url)

        for img in soup.find_all("img"):
            alt = (img.get("alt") or "").strip()
            if alt and len(alt) > 2:
                alt = re.sub(r"\s*(logo|Logo|LOGO|_logo)\s*", "", alt).strip()
                skip = ("icon", "arrow", "banner", "hero", "background", "bg",
                        "favicon", "pixel", "spacer", "loading", "placeholder")
                if alt and not any(s in alt.lower() for s in skip) and len(alt) < 80:
                    results.append({
                        "company_name": alt,
                        "role": "past_attendee",
                    })

        for section in soup.find_all(["div", "section"]):
            heading = section.find(["h2", "h3"])
            if not heading:
                continue
            heading_text = heading.get_text(strip=True).lower()
            if any(kw in heading_text for kw in ("speaker", "partner", "sponsor",
                                                   "exhibitor", "company")):
                for card in section.find_all("div", recursive=True):
                    h3 = card.find("h3") or card.find("h4")
                    if not h3:
                        continue
                    name = re.sub(r"\s+", " ", h3.get_text(strip=True))
                    if name and len(name) > 2:
                        results.append({
                            "company_name": name,
                            "role": "past_attendee",
                        })
    except Exception:
        pass

    return results


DESIGNBLOK_2025_COMPANIES = [
    {"company_name": "Bang & Olufsen Praha", "role": "exhibitor", "company_domain": "bang-olufsen.com"},
    {"company_name": "ČEZ / Svět energie", "role": "exhibitor", "company_domain": "cez.cz"},
    {"company_name": "GROHE", "role": "exhibitor", "company_domain": "grohe.cz"},
    {"company_name": "Nowy Styl", "role": "exhibitor", "company_domain": "nowystyl.com"},
    {"company_name": "Smart Acoustic", "role": "exhibitor"},
    {"company_name": "Česká spořitelna & VISA", "role": "exhibitor"},
    {"company_name": "LEGO", "role": "exhibitor", "company_domain": "lego.com"},
    {"company_name": "CAMP", "role": "exhibitor"},
    {"company_name": "Ceraflow", "role": "exhibitor"},
    {"company_name": "CULTO", "role": "exhibitor"},
    {"company_name": "PROFIL NÁBYTEK", "role": "exhibitor"},
    {"company_name": "RAKETA", "role": "exhibitor"},
    {"company_name": "Oido FlexCo", "role": "exhibitor"},
    {"company_name": "MOZA company", "role": "exhibitor"},
    {"company_name": "Merkur Toys", "role": "exhibitor"},
    {"company_name": "ELLE & ELLE Decoration", "role": "media_partner"},
    {"company_name": "Vitra", "role": "exhibitor", "company_domain": "vitra.com"},
    {"company_name": "Kettal", "role": "exhibitor", "company_domain": "kettal.com"},
    {"company_name": "Konsepti", "role": "exhibitor", "company_domain": "konsepti.com"},
    {"company_name": "PLOOM", "role": "exhibitor"},
    {"company_name": "Plastenco design", "role": "exhibitor"},
    {"company_name": "LUKRU design", "role": "exhibitor"},
    {"company_name": "LUMILOGY", "role": "exhibitor"},
    {"company_name": "Florentine Cosmetics", "role": "exhibitor"},
    {"company_name": "MANU TILES", "role": "exhibitor"},
    {"company_name": "United Spaces", "role": "exhibitor"},
    {"company_name": "Wontek", "role": "exhibitor"},
    {"company_name": "Geometrikon", "role": "exhibitor"},
    {"company_name": "Studio LAVISH", "role": "exhibitor"},
    {"company_name": "Rare Places", "role": "exhibitor"},
    {"company_name": "SIN Gallery", "role": "exhibitor"},
]

BIM_DAY_2025_SPEAKERS = [
    {"company_name": "buildingSMART International", "role": "speaker", "company_domain": "buildingsmart.org"},
    {"company_name": "EU BIM Task Group", "role": "speaker"},
    {"company_name": "buildingSMART Poland", "role": "speaker"},
    {"company_name": "Institute for BIM Italy (IBIMI)", "role": "speaker"},
    {"company_name": "BIMaS Slovakia", "role": "speaker"},
    {"company_name": "SDENG Engineering Bureau", "role": "speaker"},
    {"company_name": "Centralny Port Komunikacyjny", "role": "speaker"},
    {"company_name": "Hamburger Hafen und Logistik", "role": "speaker"},
]

CADFORUM_2025_COMPANIES = [
    {"company_name": "Arkance Systems CZ", "role": "organizer", "company_domain": "arkance.world"},
    {"company_name": "CAD Studio", "role": "organizer", "company_domain": "cadstudio.cz"},
    {"company_name": "Autodesk", "role": "partner", "company_domain": "autodesk.com"},
]

HARDCODED_ARCHIVES = {
    "lean_summit": LEAN_SUMMIT_2024_COMPANIES,
    "3dise": TDISE_2025_PARTNERS,
    "digital_factory": DIGITAL_FACTORY_2024_COMPANIES,
    "designblok": DESIGNBLOK_2025_COMPANIES,
    "bim_day": BIM_DAY_2025_SPEAKERS,
    "cadforum_2026": CADFORUM_2025_COMPANIES,
}


def scrape_archives(event_key: str = None) -> dict:
    """Scrape past event archives and merge with hardcoded data.

    Args:
        event_key: If given, only scrape archives for this event.
                   If None, scrape all available archives.

    Returns:
        Dict with results per event key.
    """
    targets = {}
    if event_key:
        if event_key in ARCHIVE_URLS:
            targets[event_key] = ARCHIVE_URLS[event_key]
        elif event_key in HARDCODED_ARCHIVES:
            targets[event_key] = []
        else:
            return {"success": True, "results": {}, "message": "No archives configured for this event"}
    else:
        targets = dict(ARCHIVE_URLS)
        for k in HARDCODED_ARCHIVES:
            if k not in targets:
                targets[k] = []

    all_results = {}
    for key, urls in targets.items():
        companies = []
        for url in urls:
            scraped = _scrape_generic_logos(url)
            companies.extend(scraped)

        hardcoded = HARDCODED_ARCHIVES.get(key, [])
        for rec in hardcoded:
            entry = dict(rec)
            entry.setdefault("role", "past_attendee")
            companies.append(entry)

        seen = set()
        deduped = []
        for c in companies:
            ckey = c["company_name"].lower().strip()
            if ckey not in seen and len(ckey) >= 2:
                seen.add(ckey)
                deduped.append(c)

        all_results[key] = deduped

    return {"success": True, "results": all_results}
