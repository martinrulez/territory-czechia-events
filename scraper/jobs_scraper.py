"""Czech job posting scraper for hiring and tech stack signals.

Scrapes jobs.cz to find:
1. Companies that are actively hiring (growth signal)
2. Job postings mentioning specific CAD/BIM/PLM tools (tech stack intel)
3. Engineering/design/IT roles that indicate Autodesk relevance

Enhanced with full detail-page parsing for deeper tool detection.
"""

import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from db.database import get_cached_enrichment, save_enrichment

JOBS_CZ_SEARCH = "https://www.jobs.cz/prace/"
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
}
REQUEST_DELAY = 1.0
DETAIL_DELAY = 1.2

CAD_TOOLS = {
    "autocad": "AutoCAD",
    "revit": "Revit",
    "inventor": "Inventor",
    "fusion 360": "Fusion",
    "fusion360": "Fusion",
    "civil 3d": "Civil 3D",
    "civil3d": "Civil 3D",
    "3ds max": "3ds Max",
    "3dsmax": "3ds Max",
    "maya": "Maya",
    "navisworks": "Navisworks",
    "bim": "BIM",
    "archicad": "ArchiCAD",
    "solidworks": "SolidWorks",
    "catia": "CATIA",
    "siemens nx": "Siemens NX",
    "solid edge": "Solid Edge",
    "creo": "Creo",
    "tekla": "Tekla",
    "allplan": "Allplan",
    "microstation": "MicroStation",
    "vectorworks": "Vectorworks",
    "sketchup": "SketchUp",
    "rhino": "Rhino",
    "blender": "Blender",
    "cinema 4d": "Cinema 4D",
    "houdini": "Houdini",
    "nuke": "Nuke",
    "mastercam": "Mastercam",
    "edgecam": "EDGECAM",
    "powermill": "PowerMill",
    "featurecam": "FeatureCAM",
    "moldflow": "Moldflow",
    "ansys": "Ansys",
    "abaqus": "Abaqus",
    "teamcenter": "Teamcenter",
    "windchill": "Windchill",
    "vault": "Vault",
    "pdm": "PDM",
    "plm": "PLM",
    "cad": "CAD",
    "cam": "CAM",
    "cnc": "CNC",
    "bentley": "Bentley",
    "infraworks": "InfraWorks",
    "plant 3d": "Plant 3D",
    "recap": "ReCap",
    "formit": "FormIt",
    "acc": "Autodesk Construction Cloud",
    "bim 360": "BIM 360",
    "bim360": "BIM 360",
    "shotgrid": "ShotGrid",
    "flow production": "Flow Production Tracking",
    "twinmotion": "Twinmotion",
    "enscape": "Enscape",
    "lumion": "Lumion",
}

AUTODESK_TOOLS = {
    "autocad", "revit", "inventor", "fusion 360", "fusion360", "civil 3d",
    "civil3d", "3ds max", "3dsmax", "maya", "navisworks", "bim",
    "powermill", "featurecam", "moldflow", "vault", "infraworks",
    "plant 3d", "recap", "formit", "acc", "bim 360", "bim360",
    "shotgrid", "flow production",
}

COMPETITOR_TOOLS = {
    "archicad", "solidworks", "catia", "siemens nx", "solid edge", "creo",
    "tekla", "allplan", "microstation", "vectorworks", "mastercam", "edgecam",
    "teamcenter", "windchill", "bentley",
}

RELEVANT_ROLES = {
    "konstruktér", "konstrukter", "projektant", "architekt",
    "engineer", "inženýr", "inzenyr",
    "cad", "bim", "cam", "cnc",
    "technolog", "designer", "návrhář",
    "it manager", "it ředitel", "cto",
    "vfx", "3d", "animátor", "animator",
}

ROLE_CLASSIFICATIONS = {
    "designer": ["konstruktér", "konstrukter", "designer", "návrhář"],
    "engineer": ["inženýr", "inzenyr", "engineer"],
    "architect": ["architekt", "architect"],
    "it": ["it manager", "it ředitel", "správce", "admin"],
    "manager": ["vedoucí", "manager", "manažer", "ředitel", "director", "head"],
    "technologist": ["technolog"],
    "bim_specialist": ["bim", "bim koordinátor", "bim manažer"],
    "cad_operator": ["cad", "kreslič"],
    "vfx_artist": ["vfx", "3d", "animátor", "animator"],
    "cnc_programmer": ["cnc", "programátor cnc", "seřízeč"],
}

SENIORITY_PATTERNS = {
    "director": r"\b(?:ředitel|director|head of|vedoucí úsek)\b",
    "lead": r"\b(?:vedoucí|lead|senior|hlavní|chief)\b",
    "senior": r"\b(?:senior|zkušen|sr\.)\b",
    "mid": r"\b(?:mid|střed)\b",
    "junior": r"\b(?:junior|jr\.|začínaj|absolvent)\b",
}


def search_company_jobs(company_name: str, deep: bool = True) -> dict:
    """Search for current job postings from a specific company.

    Args:
        company_name: Company to search for.
        deep: If True, follow detail page links for full tool detection.
    """
    if not company_name or len(company_name) < 2:
        return {"success": False, "error": "Company name too short"}

    cache_key = f"jobs:{company_name.lower().strip()}"
    cached = get_cached_enrichment("jobs_cz", cache_key)
    if cached:
        if cached.get("deep_scraped") or not deep:
            cached["from_cache"] = True
            return cached

    jobs = _scrape_jobs_cz(company_name, deep=deep)

    tool_mentions = {}
    autodesk_mentions = set()
    competitor_mentions = set()
    relevant_roles_found = []

    for job in jobs:
        text = (
            job.get("title", "") + " " +
            job.get("description", "") + " " +
            job.get("full_requirements", "")
        ).lower()

        for tool_key, tool_name in CAD_TOOLS.items():
            if tool_key in text:
                tool_mentions[tool_name] = tool_mentions.get(tool_name, 0) + 1
                if tool_key in AUTODESK_TOOLS:
                    autodesk_mentions.add(tool_name)
                elif tool_key in COMPETITOR_TOOLS:
                    competitor_mentions.add(tool_name)

        for role_kw in RELEVANT_ROLES:
            if role_kw in text:
                relevant_roles_found.append(job.get("title", ""))
                break

    result = {
        "success": True,
        "company_name": company_name,
        "total_jobs": len(jobs),
        "relevant_jobs": len(relevant_roles_found),
        "tool_mentions": tool_mentions,
        "autodesk_tools_mentioned": sorted(autodesk_mentions),
        "competitor_tools_mentioned": sorted(competitor_mentions),
        "relevant_roles": relevant_roles_found[:10],
        "hiring_signal": len(jobs) > 0,
        "engineering_hiring": len(relevant_roles_found) > 0,
        "jobs": jobs[:15],
        "deep_scraped": deep,
        "from_cache": False,
    }

    save_enrichment("jobs_cz", cache_key, result)
    return result


def _scrape_jobs_cz(company_name: str, deep: bool = True) -> list:
    """Scrape jobs.cz for postings from a company."""
    time.sleep(REQUEST_DELAY)
    jobs = []

    try:
        search_url = (
            f"https://www.jobs.cz/prace/?q={requests.utils.quote(company_name)}"
        )
        resp = requests.get(
            search_url, headers=BROWSER_HEADERS, timeout=15, allow_redirects=True
        )
        if resp.status_code != 200:
            return jobs

        soup = BeautifulSoup(resp.text, "html.parser")

        articles = soup.select(
            "article, div[data-test='serp-item'], "
            "div.standalone-search-item, li.search-list__item"
        )

        for article in articles:
            job = _parse_job_listing(article)
            if job:
                jobs.append(job)

        if not jobs:
            generic = soup.select("a[href*='/rpd/'], a[href*='/pd/']")
            for link in generic[:20]:
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if title and len(title) > 5:
                    if href and not href.startswith("http"):
                        href = "https://www.jobs.cz" + href
                    jobs.append({
                        "title": title,
                        "url": href,
                        "company": company_name,
                        "description": "",
                    })

        if deep:
            _enrich_with_detail_pages(jobs)

    except requests.RequestException:
        pass

    return jobs


def _enrich_with_detail_pages(jobs: list, max_details: int = 8):
    """Follow detail page URLs to get full job descriptions and requirements."""
    enriched_count = 0
    for job in jobs:
        if enriched_count >= max_details:
            break

        url = job.get("url", "")
        if not url or not url.startswith("http"):
            continue

        detail = _scrape_job_detail(url)
        if detail:
            job["full_requirements"] = detail.get("requirements", "")
            job["full_description"] = detail.get("description", "")
            job["location"] = detail.get("location", "")
            job["tools_mentioned"] = detail.get("tools_mentioned", [])
            job["role_type"] = detail.get("role_type", "")
            job["seniority"] = detail.get("seniority", "")
            enriched_count += 1


def _scrape_job_detail(url: str) -> dict:
    """Fetch and parse a single job detail page."""
    time.sleep(DETAIL_DELAY)
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        desc_el = soup.select_one(
            "[data-test='job-description'], .job-detail__body, "
            "article, .job-description, main"
        )
        full_text = desc_el.get_text(separator="\n", strip=True) if desc_el else ""

        requirements = ""
        req_section = _find_section(soup, [
            "požadujeme", "požadavky", "requirements", "požadovaná",
            "hledáme", "očekáváme", "co byste měl", "co potřebujete",
        ])
        if req_section:
            requirements = req_section

        location = ""
        loc_el = soup.select_one(
            "[data-test='job-location'], .job-detail__location, "
            ".location, [itemprop='jobLocation']"
        )
        if loc_el:
            location = loc_el.get_text(strip=True)

        text_lower = full_text.lower()
        tools_found = []
        for tool_key, tool_name in CAD_TOOLS.items():
            if tool_key in text_lower:
                tools_found.append(tool_name)

        role_type = _classify_role(full_text.lower())
        seniority = _classify_seniority(full_text.lower())

        return {
            "description": full_text[:2000],
            "requirements": requirements[:1500],
            "location": location,
            "tools_mentioned": sorted(set(tools_found)),
            "role_type": role_type,
            "seniority": seniority,
        }

    except requests.RequestException:
        return None


def _find_section(soup: BeautifulSoup, keywords: list) -> str:
    """Find a named section (e.g. 'Requirements') in the job page."""
    for el in soup.select("h2, h3, h4, strong, b, p"):
        text = el.get_text(strip=True).lower()
        if any(kw in text for kw in keywords):
            section_parts = []
            sibling = el.find_next_sibling()
            while sibling and sibling.name not in ("h2", "h3", "h4"):
                section_parts.append(sibling.get_text(separator="\n", strip=True))
                sibling = sibling.find_next_sibling()
            if section_parts:
                return "\n".join(section_parts)

            parent_text = ""
            parent = el.parent
            if parent:
                next_els = []
                for s in parent.find_next_siblings()[:5]:
                    next_els.append(s.get_text(separator="\n", strip=True))
                parent_text = "\n".join(next_els)
            return parent_text

    return ""


def _classify_role(text: str) -> str:
    """Classify job posting into a role type."""
    for role_type, keywords in ROLE_CLASSIFICATIONS.items():
        if any(kw in text for kw in keywords):
            return role_type
    return "other"


def _classify_seniority(text: str) -> str:
    """Detect seniority level from job text."""
    for level, pattern in SENIORITY_PATTERNS.items():
        if re.search(pattern, text, re.IGNORECASE):
            return level
    return "mid"


def _parse_job_listing(element) -> dict:
    """Parse a single job listing element from search results."""
    title_el = element.select_one(
        "h2 a, a.standalone-search-item__title, "
        "a[data-test='link'], .search-list__main-info a"
    )
    if not title_el:
        title_el = element.select_one("a")
    if not title_el:
        return None

    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    url = title_el.get("href", "")
    if url and not url.startswith("http"):
        url = "https://www.jobs.cz" + url

    company_el = element.select_one(
        "span.company, .standalone-search-item__company, "
        "[data-test='company-name']"
    )
    company = company_el.get_text(strip=True) if company_el else ""

    desc_el = element.select_one(
        "p, .standalone-search-item__body, [data-test='text-body']"
    )
    description = desc_el.get_text(strip=True) if desc_el else ""

    return {
        "title": title,
        "url": url,
        "company": company,
        "description": description[:500],
    }


def get_hiring_signal(company_name: str) -> dict:
    """Simplified hiring signal for the scoring engine."""
    data = search_company_jobs(company_name)
    if not data.get("success"):
        return {"hiring_signal": False}

    return {
        "hiring_signal": data["hiring_signal"],
        "engineering_hiring": data["engineering_hiring"],
        "total_jobs": data["total_jobs"],
        "relevant_jobs": data["relevant_jobs"],
        "autodesk_tools": data["autodesk_tools_mentioned"],
        "competitor_tools": data["competitor_tools_mentioned"],
    }


def batch_hiring_check(company_names: list) -> list:
    """Check hiring signals for multiple companies."""
    results = []
    for name in company_names:
        signal = get_hiring_signal(name)
        signal["company_name"] = name
        results.append(signal)
    return results
