"""ZoomInfo API client for contact and company enrichment.

Supports PKI authentication (preferred) and username/password fallback.
Field availability depends on your ZoomInfo plan.
Czech Republic territory-focused: persona searches default to CZ contacts.
"""

from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

from db.database import get_cached_enrichment, save_enrichment

load_dotenv()

BASE_URL = "https://api.zoominfo.com"
_token_cache = {"token": None, "expires_at": 0}
_master_list_cache = None

COMPANY_OUTPUT_FIELDS = [
    # Core identifiers
    "id", "name", "website", "domainList", "phone",
    # Location
    "city", "state", "country", "continent", "zipCode", "street",
    "locationCount",
    # Size & financials
    "employeeCount", "employeeRange", "revenue", "revenueRange",
    "employeeGrowth", "employeeCountByDepartment",
    "departmentBudgets",
    # Industry classification
    "primaryIndustry", "primaryIndustryCode", "primarySubIndustryCode",
    "industries", "industryCodes", "sicCodes", "naicsCodes",
    # Corporate structure
    "type", "parentId", "parentName",
    "ultimateParentId", "ultimateParentName",
    "ultimateParentRevenue", "ultimateParentEmployees",
    "subUnitType", "subUnitIndustries",
    # Company lifecycle
    "foundedYear", "companyStatus", "isDefunct", "certified",
    "businessModel",
    # Competitive & social
    "competitors", "socialMediaUrls",
    "numberOfContactsInZoomInfo",
    # Funding
    "companyFunding", "recentFundingAmount", "recentFundingDate",
    "totalFundingAmount",
]

CONTACT_SEARCH_FIELDS = [
    "firstName", "lastName", "jobTitle", "companyName",
    "contactAccuracyScore", "hasEmail", "hasDirectPhone", "hasMobilePhone",
    "validDate", "lastUpdatedDate",
]

CONTACT_ENRICH_FIELDS = [
    "firstName", "lastName", "jobTitle", "email", "phone",
    "companyName", "city", "state", "country",
]

TARGET_COUNTRY = "Czech Republic"
TARGET_COUNTRY_VARIANTS = {"Czech Republic", "Czechia", "CZ"}


def _load_master_list():
    """Load the prioritized accounts CSV as a lookup dict keyed by lowercase company name."""
    global _master_list_cache
    if _master_list_cache is not None:
        return _master_list_cache

    _master_list_cache = {}
    csv_paths = [
        Path(__file__).resolve().parent.parent.parent / "prioritized_accounts_enriched.csv",
        Path(__file__).resolve().parent.parent.parent / "prioritized_accounts.csv",
    ]
    for csv_path in csv_paths:
        if csv_path.exists():
            try:
                import pandas as pd
                df = pd.read_csv(str(csv_path), encoding="utf-8-sig", dtype=str, keep_default_na=False)
                for _, row in df.iterrows():
                    name = row.get("company_name", "").strip()
                    if name:
                        _master_list_cache[name.lower()] = {
                            "company_name": name,
                            "csn": row.get("csn", ""),
                            "domain": row.get("website", ""),
                            "city": row.get("city", ""),
                            "industry_segment": row.get("industry_segment", ""),
                            "primary_segment": row.get("primary_segment", ""),
                            "current_products": row.get("current_products", ""),
                            "contact_email": row.get("contact_email", ""),
                        }
            except Exception:
                pass
            break
    return _master_list_cache

AUTODESK_TARGET_TITLES = {
    "AEC": [
        "BIM Manager", "BIM Director", "BIM Coordinator",
        "CAD Manager", "Design Technology Manager",
        "Director of Design Technology", "VP of Design",
        "Chief Technology Officer", "CTO",
        "Director of Engineering", "VP Engineering",
        "Head of Digital Construction", "Digital Transformation",
        "Director of Architecture", "Head of Architecture",
        "Construction Technology Manager",
        "Director of IT", "IT Manager",
        "VP of Operations", "Director of Operations",
    ],
    "D&M": [
        "CAD Manager", "Engineering Manager",
        "Director of Engineering", "VP Engineering",
        "Chief Technology Officer", "CTO",
        "Director of R&D", "Head of R&D",
        "Product Engineering Manager", "Design Engineering Manager",
        "Manufacturing Engineering Manager",
        "Director of Product Development",
        "VP of Product Development",
        "IT Director", "IT Manager",
        "Director of Operations", "VP Manufacturing",
        "Head of Digital Transformation",
        "PLM Manager", "PDM Manager",
    ],
    "M&E": [
        "VFX Supervisor", "Head of Production",
        "Technical Director", "CTO",
        "Studio Manager", "Pipeline Director",
        "Head of 3D", "Director of Animation",
        "IT Director",
    ],
}

FALLBACK_TITLES = [
    "CTO", "Chief Technology Officer",
    "VP Engineering", "Director of Engineering",
    "IT Director", "IT Manager",
    "CAD Manager", "Engineering Manager",
    "Director of Operations",
    "Head of Digital Transformation",
]


def _format_pem_key(raw: str) -> str:
    """Reformat a PEM private key that may be stored as a single line."""
    raw = raw.strip()
    if "\n" in raw:
        return raw
    body = raw.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").strip()
    lines = [body[i:i + 64] for i in range(0, len(body), 64)]
    return "-----BEGIN PRIVATE KEY-----\n" + "\n".join(lines) + "\n-----END PRIVATE KEY-----"


def _authenticate() -> str:
    """Get a JWT access token from ZoomInfo using PKI or username/password."""
    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now:
        return _token_cache["token"]

    client_id = os.getenv("ZOOMINFO_API_CLIENT_ID")
    private_key_raw = os.getenv("ZOOMINFO_API_KEY")
    username = os.getenv("ZOOMINFO_API_USERNAME")

    if client_id and private_key_raw and username:
        private_key = _format_pem_key(private_key_raw)
        try:
            import zi_api_auth_client
            token = zi_api_auth_client.pki_authentication(username, client_id, private_key)
            _token_cache["token"] = token
            _token_cache["expires_at"] = now + 3500
            return token
        except Exception as e:
            raise ValueError(f"ZoomInfo PKI authentication failed: {e}")

    password = os.getenv("ZOOMINFO_API_PASSWORD")
    if username and password:
        resp = requests.post(
            f"{BASE_URL}/authenticate",
            json={"username": username, "password": password},
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json().get("jwt")
        if not token:
            raise ValueError("ZoomInfo auth response missing JWT token")
        _token_cache["token"] = token
        _token_cache["expires_at"] = now + 3500
        return token

    raise ValueError(
        "Set ZOOMINFO_API_USERNAME + ZOOMINFO_API_CLIENT_ID + ZOOMINFO_API_KEY (PKI) "
        "or ZOOMINFO_API_USERNAME + ZOOMINFO_API_PASSWORD in .env"
    )


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_authenticate()}",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Company enrichment
# ---------------------------------------------------------------------------

def enrich_company(company_name: str = None, domain: str = None) -> dict:
    """Look up a company by name or domain via the enrich endpoint."""
    lookup_key = domain or company_name
    if not lookup_key:
        return {"success": False, "error": "Provide company_name or domain"}

    cached = get_cached_enrichment("company", lookup_key.lower())
    if cached:
        cached["from_cache"] = True
        return cached

    match_input = {}
    if domain:
        match_input["companyWebsite"] = domain
    else:
        match_input["companyName"] = company_name

    try:
        resp = requests.post(
            f"{BASE_URL}/enrich/company",
            headers=_headers(),
            json={
                "matchCompanyInput": [match_input],
                "outputFields": COMPANY_OUTPUT_FIELDS,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        return {"success": False, "error": f"ZoomInfo API error: {e}"}

    results = data.get("data", {}).get("result", [])
    if not results or not results[0].get("data"):
        return {"success": False, "error": "No company found"}

    company = results[0]["data"][0]

    # Extract employee growth data
    emp_growth_raw = company.get("employeeGrowth") or {}
    emp_growth = {
        "one_year_growth_rate": emp_growth_raw.get("oneYearGrowthRate"),
        "two_year_growth_rate": emp_growth_raw.get("twoYearGrowthRate"),
        "data_points": [
            {"label": dp.get("label"), "count": dp.get("employeeCount")}
            for dp in (emp_growth_raw.get("employeeGrowthDataPoints") or [])
        ],
    }

    # Extract employee count by department
    dept_raw = company.get("employeeCountByDepartment") or {}
    emp_by_dept = {
        "c_suite": dept_raw.get("cSuite"),
        "engineering": dept_raw.get("engineeringAndTechnical"),
        "it": dept_raw.get("informationTechnology"),
        "sales": dept_raw.get("sales"),
        "operations": dept_raw.get("operations"),
        "finance": dept_raw.get("finance"),
        "hr": dept_raw.get("humanResources"),
        "marketing": dept_raw.get("marketing"),
        "legal": dept_raw.get("legal"),
        "medical": dept_raw.get("medicalAndHealth"),
    }

    # Extract department budgets
    budgets_raw = company.get("departmentBudgets") or {}
    dept_budgets = {
        "marketing": budgets_raw.get("marketingBudget"),
        "finance": budgets_raw.get("financeBudget"),
        "it": budgets_raw.get("itBudget"),
        "hr": budgets_raw.get("hrBudget"),
    }

    # Extract competitors
    competitors_raw = company.get("competitors") or []
    competitors = [
        {
            "name": comp.get("name"),
            "website": comp.get("website"),
            "employee_count": comp.get("employeeCount"),
            "rank": comp.get("rank"),
        }
        for comp in competitors_raw[:10]
    ]

    # Extract social media URLs
    social_raw = company.get("socialMediaUrls") or []
    social_urls = {s.get("type", "").lower(): s.get("url", "") for s in social_raw}
    linkedin_url = social_urls.get("linkedin", "")

    # Extract industry codes
    sic_codes = [
        {"code": s.get("code"), "name": s.get("name")}
        for s in (company.get("sicCodes") or [])
    ]
    naics_codes = [
        {"code": n.get("code"), "name": n.get("name")}
        for n in (company.get("naicsCodes") or [])
    ]

    # Extract funding
    funding_rounds = [
        {
            "date": f.get("fundingDate"),
            "type": f.get("fundingType"),
            "amount": f.get("fundingAmount"),
        }
        for f in (company.get("companyFunding") or [])
    ]

    result = {
        "success": True,
        # Core
        "zi_company_id": company.get("id"),
        "company_name": company.get("name"),
        "domain": company.get("website"),
        "domain_list": company.get("domainList") or [],
        "phone": company.get("phone"),
        # Location
        "street": company.get("street"),
        "city": company.get("city"),
        "state": company.get("state"),
        "zip_code": company.get("zipCode"),
        "country": company.get("country"),
        "continent": company.get("continent"),
        "location_count": company.get("locationCount"),
        # Size & financials
        "employee_count": company.get("employeeCount"),
        "employee_range": company.get("employeeRange"),
        "revenue": company.get("revenue"),
        "revenue_range": company.get("revenueRange"),
        "employee_growth": emp_growth,
        "employee_by_department": emp_by_dept,
        "department_budgets": dept_budgets,
        # Industry
        "primary_industry": (company.get("primaryIndustry") or [None])[0],
        "primary_industry_code": company.get("primaryIndustryCode"),
        "primary_sub_industry_code": company.get("primarySubIndustryCode"),
        "industries": company.get("industries") or [],
        "sic_codes": sic_codes,
        "naics_codes": naics_codes,
        # Corporate structure
        "company_type": company.get("type"),
        "parent_id": company.get("parentId"),
        "parent_name": company.get("parentName"),
        "ultimate_parent_id": company.get("ultimateParentId"),
        "ultimate_parent_name": company.get("ultimateParentName"),
        "ultimate_parent_revenue": company.get("ultimateParentRevenue"),
        "ultimate_parent_employees": company.get("ultimateParentEmployees"),
        "sub_unit_type": company.get("subUnitType"),
        "sub_unit_industries": company.get("subUnitIndustries") or [],
        # Lifecycle
        "founded_year": company.get("foundedYear"),
        "company_status": company.get("companyStatus"),
        "is_defunct": company.get("isDefunct"),
        "certified": company.get("certified"),
        "business_model": company.get("businessModel") or [],
        # Competitive & social
        "competitors": competitors,
        "linkedin_url": linkedin_url,
        "social_urls": social_urls,
        "zi_contacts_available": company.get("numberOfContactsInZoomInfo"),
        # Funding
        "funding_rounds": funding_rounds,
        "recent_funding_amount": company.get("recentFundingAmount"),
        "recent_funding_date": company.get("recentFundingDate"),
        "total_funding_amount": company.get("totalFundingAmount"),
        #
        "from_cache": False,
    }

    save_enrichment("company", lookup_key.lower(), result)
    return result


# ---------------------------------------------------------------------------
# Technographics — installed software per company
# ---------------------------------------------------------------------------

TECHNO_OUTPUT_FIELDS = [
    "category", "vendor", "product", "subCategory",
]

AUTODESK_PRODUCTS = {
    "autocad", "revit", "inventor", "fusion 360", "civil 3d", "3ds max",
    "maya", "navisworks", "bim 360", "vault", "infraworks", "plant 3d",
    "recap", "formit", "powermill", "moldflow", "featurecam", "shotgrid",
    "autodesk construction cloud", "autodesk acc",
}

AUTODESK_VENDORS = {"autodesk", "autodesk, inc", "autodesk inc"}

COMPETITOR_PRODUCTS = {
    "solidworks", "catia", "siemens nx", "solid edge", "creo",
    "archicad", "tekla", "allplan", "microstation", "vectorworks",
    "mastercam", "edgecam", "teamcenter", "windchill",
    "sketchup", "pro/engineer", "mathcad",
}

COMPETITOR_VENDORS = {
    "bentley", "trimble", "hexagon", "dassault", "ptc",
    "siemens", "nemetschek", "graphisoft",
}

TECH_FALSE_POSITIVES = {
    "recaptcha", "captcha", "peoplenet",
}

CAD_BIM_CATEGORIES = {
    "cad", "bim", "plm", "cam", "cae", "design", "engineering",
    "architecture", "construction", "manufacturing", "3d modeling",
    "product lifecycle", "computer aided",
}


def get_technographics(
    company_name: str = None,
    domain: str = None,
) -> dict:
    """Fetch installed technology stack for a company via ZoomInfo company enrich.

    Uses the techAttributes output field on the /enrich/company endpoint.
    Returns categorized lists of Autodesk products, competitor products,
    and other relevant CAD/BIM/PLM tools detected.
    """
    lookup_key = domain or company_name
    if not lookup_key:
        return {"success": False, "error": "Provide company_name or domain"}

    cache_key = f"techno:{lookup_key.lower().strip()}"
    cached = get_cached_enrichment("technographics", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    match_input = {}
    if domain:
        match_input["companyWebsite"] = domain
    else:
        match_input["companyName"] = company_name

    try:
        resp = requests.post(
            f"{BASE_URL}/enrich/company",
            headers=_headers(),
            json={
                "matchCompanyInput": [match_input],
                "outputFields": ["id", "name", "techAttributes"],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        return {"success": False, "error": f"ZoomInfo API error: {e}"}

    results = data.get("data", {}).get("result", [])
    if not results or not results[0].get("data"):
        result = {"success": False, "error": "No company found"}
        save_enrichment("technographics", cache_key, result)
        return result

    company_data = results[0]["data"][0]
    techs = company_data.get("techAttributes") or []

    if not techs:
        result = {"success": False, "error": "No technographics found"}
        save_enrichment("technographics", cache_key, result)
        return result

    autodesk_found = []
    competitor_found = []
    cad_bim_other = []
    all_tech = []

    for tech in techs:
        product = (tech.get("product") or "").strip()
        vendor = (tech.get("vendor") or "").strip()
        category = (tech.get("category") or "").strip()
        cat_parent = (tech.get("categoryParent") or "").strip()

        product_lower = product.lower()
        vendor_lower = vendor.lower()
        cat_lower = (category + " " + cat_parent).lower()

        all_tech.append({
            "product": product,
            "vendor": vendor,
            "category": category,
            "categoryParent": cat_parent,
        })

        if any(fp in product_lower for fp in TECH_FALSE_POSITIVES):
            continue

        is_adsk = (
            any(ap in product_lower for ap in AUTODESK_PRODUCTS)
            or any(av == vendor_lower for av in AUTODESK_VENDORS)
        )
        is_comp = (
            any(cp in product_lower for cp in COMPETITOR_PRODUCTS)
            or any(cv in vendor_lower for cv in COMPETITOR_VENDORS)
        )

        if is_adsk:
            autodesk_found.append(product)
        elif is_comp:
            competitor_found.append(product)
        elif any(cc in cat_lower for cc in CAD_BIM_CATEGORIES):
            cad_bim_other.append(product)

    result = {
        "success": True,
        "company": company_name or domain,
        "autodesk_products": sorted(set(autodesk_found)),
        "competitor_products": sorted(set(competitor_found)),
        "cad_bim_other": sorted(set(cad_bim_other)),
        "total_technologies": len(all_tech),
        "all_technologies": all_tech[:50],
        "from_cache": False,
    }

    save_enrichment("technographics", cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Contact search + enrich (two-step: search finds names, enrich gets emails)
# ---------------------------------------------------------------------------

def search_decision_makers(
    company_name: str,
    segment: str = None,
    max_results: int = 5,
    country: str = TARGET_COUNTRY,
    domain: str = None,
) -> dict:
    """Find decision makers at a company in the target country.

    Uses domain anchoring from the master list when available to ensure
    we hit the correct Czech subsidiary, not a global namesake.

    Step 1: Search by company + target titles + country filter.
    Step 2: Enrich each found contact to get email/phone + verify country.
    """
    cache_key = f"personas:{company_name.lower().strip()}:{segment or 'all'}"
    cached = get_cached_enrichment("personas", cache_key)
    if cached:
        cached["from_cache"] = True
        return cached

    master = _load_master_list()
    master_entry = master.get(company_name.lower().strip())
    if not domain and master_entry:
        domain = master_entry.get("domain", "")

    all_contacts = []

    # Strategy 1: Broad search by company name only (no title/country filter)
    # -- ZoomInfo Czech coverage is sparse; strict filters return nothing
    search_body = {
        "rpp": max_results,
        "outputFields": CONTACT_SEARCH_FIELDS,
    }
    if domain:
        search_body["companyWebsite"] = domain
    else:
        search_body["companyName"] = company_name

    try:
        resp = requests.post(
            f"{BASE_URL}/search/contact",
            headers=_headers(),
            json=search_body,
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            for c in data.get("data", []):
                name_key = f"{c.get('firstName', '')}_{c.get('lastName', '')}".lower()
                if any(name_key == f"{x.get('first_name', '')}_{x.get('last_name', '')}".lower() for x in all_contacts):
                    continue
                all_contacts.append({
                    "first_name": c.get("firstName", ""),
                    "last_name": c.get("lastName", ""),
                    "title": c.get("jobTitle", ""),
                    "company": c.get("companyName") or (c.get("company", {}) or {}).get("name", ""),
                    "has_email": c.get("hasEmail", False),
                    "has_phone": c.get("hasDirectPhone", False) or c.get("hasMobilePhone", False),
                    "accuracy_score": c.get("contactAccuracyScore"),
                })
                if len(all_contacts) >= max_results:
                    break
    except requests.RequestException:
        pass

    # Strategy 2: If broad search missed, try per-title searches without country
    if not all_contacts:
        target_titles = AUTODESK_TARGET_TITLES.get(segment, FALLBACK_TITLES)
        for title in target_titles[:5]:
            if len(all_contacts) >= max_results:
                break

            search_body = {
                "rpp": 3,
                "jobTitle": title,
                "outputFields": CONTACT_SEARCH_FIELDS,
            }
            if domain:
                search_body["companyWebsite"] = domain
            else:
                search_body["companyName"] = company_name

            try:
                resp = requests.post(
                    f"{BASE_URL}/search/contact",
                    headers=_headers(),
                    json=search_body,
                    timeout=30,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
            except requests.RequestException:
                continue

            for c in data.get("data", []):
                name_key = f"{c.get('firstName', '')}_{c.get('lastName', '')}".lower()
                if any(name_key == f"{x.get('first_name', '')}_{x.get('last_name', '')}".lower() for x in all_contacts):
                    continue
                all_contacts.append({
                    "first_name": c.get("firstName", ""),
                    "last_name": c.get("lastName", ""),
                    "title": c.get("jobTitle", ""),
                    "company": c.get("companyName") or (c.get("company", {}) or {}).get("name", ""),
                    "has_email": c.get("hasEmail", False),
                    "has_phone": c.get("hasDirectPhone", False) or c.get("hasMobilePhone", False),
                    "accuracy_score": c.get("contactAccuracyScore"),
                })
                if len(all_contacts) >= max_results:
                    break

    enriched_contacts = []
    for contact in all_contacts:
        email = ""
        phone = ""
        contact_country = country or ""
        if contact.get("has_email"):
            try:
                enrich_resp = requests.post(
                    f"{BASE_URL}/enrich/contact",
                    headers=_headers(),
                    json={
                        "matchPersonInput": [{
                            "firstName": contact["first_name"],
                            "lastName": contact["last_name"],
                            "companyName": contact["company"] or company_name,
                        }],
                        "outputFields": CONTACT_ENRICH_FIELDS,
                    },
                    timeout=30,
                )
                if enrich_resp.status_code == 200:
                    enrich_data = enrich_resp.json()
                    enrich_results = enrich_data.get("data", {}).get("result", [])
                    if enrich_results and enrich_results[0].get("data"):
                        person = enrich_results[0]["data"][0]
                        email = person.get("email", "")
                        phone = person.get("phone", "")
                        contact_country = person.get("country", country or "")
            except requests.RequestException:
                pass

        enriched_contacts.append({
            "first_name": contact["first_name"],
            "last_name": contact["last_name"],
            "title": contact["title"],
            "email": email,
            "phone": phone,
            "company": contact["company"],
            "country": contact_country,
            "has_email": contact["has_email"],
            "has_phone": contact["has_phone"],
            "accuracy_score": contact.get("accuracy_score"),
        })

    result = {
        "success": len(enriched_contacts) > 0,
        "company_name": company_name,
        "segment": segment,
        "country": country,
        "contacts_found": len(enriched_contacts),
        "contacts": enriched_contacts,
        "from_cache": False,
    }

    if enriched_contacts:
        save_enrichment("personas", cache_key, result)

    return result


def enrich_contact(
    first_name: str = None,
    last_name: str = None,
    company_name: str = None,
    email: str = None,
) -> dict:
    """Enrich a single contact by name+company or email."""
    lookup_key = email or f"{first_name}_{last_name}_{company_name}".lower()
    if lookup_key == "__":
        return {"success": False, "error": "Provide at least name+company or email"}

    cached = get_cached_enrichment("contact", lookup_key.lower())
    if cached:
        cached["from_cache"] = True
        return cached

    match_input = {}
    if email:
        match_input["emailAddress"] = email
    else:
        if first_name:
            match_input["firstName"] = first_name
        if last_name:
            match_input["lastName"] = last_name
        if company_name:
            match_input["companyName"] = company_name

    try:
        resp = requests.post(
            f"{BASE_URL}/enrich/contact",
            headers=_headers(),
            json={
                "matchPersonInput": [match_input],
                "outputFields": CONTACT_ENRICH_FIELDS,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        return {"success": False, "error": f"ZoomInfo API error: {e}"}

    results = data.get("data", {}).get("result", [])
    if not results or not results[0].get("data"):
        return {"success": False, "error": "No contact found"}

    contact = results[0]["data"][0]
    company_data = contact.get("company", {}) or {}
    result = {
        "success": True,
        "first_name": contact.get("firstName"),
        "last_name": contact.get("lastName"),
        "email": contact.get("email"),
        "phone": contact.get("phone"),
        "title": contact.get("jobTitle"),
        "company": company_data.get("name") or contact.get("companyName"),
        "from_cache": False,
    }

    save_enrichment("contact", lookup_key.lower(), result)
    return result


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------

def batch_enrich_companies(companies: list, progress_callback=None) -> dict:
    """Enrich a list of companies."""
    results = []
    success_count = 0
    cached_count = 0

    for i, c in enumerate(companies):
        name = c.get("company_name") or c.get("name", "")
        domain = c.get("company_domain") or c.get("domain")

        result = enrich_company(company_name=name, domain=domain)
        result["original_name"] = name

        if result.get("success"):
            success_count += 1
        if result.get("from_cache"):
            cached_count += 1

        results.append(result)

        if progress_callback:
            progress_callback(i + 1, len(companies), name, result.get("success", False))

    return {
        "total": len(companies),
        "enriched": success_count,
        "cached": cached_count,
        "failed": len(companies) - success_count,
        "results": results,
    }


def flush_stale_personas():
    """Remove non-Czech persona cache entries so they can be re-fetched with country filter."""
    import sqlite3
    db_path = Path(__file__).resolve().parent.parent / "prospects.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, lookup_key, result_json FROM enrichment_cache WHERE lookup_type='personas'"
    ).fetchall()

    removed = 0
    for row in rows:
        data = json.loads(row["result_json"])
        if data.get("country") in TARGET_COUNTRY_VARIANTS:
            continue
        contacts = data.get("contacts", [])
        cz_count = sum(1 for c in contacts if _is_czech_contact(c))
        if cz_count == 0:
            conn.execute("DELETE FROM enrichment_cache WHERE id=?", (row["id"],))
            removed += 1

    conn.commit()
    conn.close()
    return removed


def _is_czech_contact(contact: dict) -> bool:
    """Heuristic check if a contact is likely Czech-based."""
    country = (contact.get("country") or "").lower()
    if country in {"czech republic", "czechia", "cz"}:
        return True
    phone = contact.get("phone", "")
    if phone and phone.startswith("+420"):
        return True
    email = contact.get("email", "")
    if email and email.endswith(".cz"):
        return True
    return False


def batch_find_personas(
    companies: list,
    segment_map: Optional[dict] = None,
    max_contacts_per_company: int = 3,
    country: str = TARGET_COUNTRY,
    progress_callback=None,
) -> dict:
    """Find decision makers at multiple companies, filtered by country."""
    results = []
    total_contacts = 0
    success_count = 0
    cached_count = 0

    for i, c in enumerate(companies):
        name = c.get("company_name") or c.get("name", "")
        segment = c.get("segment") or (segment_map or {}).get(name.lower())
        domain = c.get("domain") or c.get("company_domain") or c.get("website", "")

        result = search_decision_makers(
            company_name=name,
            segment=segment,
            max_results=max_contacts_per_company,
            country=country,
            domain=domain if domain else None,
        )
        result["original_name"] = name

        if result.get("success"):
            success_count += 1
            total_contacts += result.get("contacts_found", 0)
        if result.get("from_cache"):
            cached_count += 1

        results.append(result)

        if progress_callback:
            progress_callback(i + 1, len(companies), name, result.get("success", False))

    return {
        "total_companies": len(companies),
        "companies_with_contacts": success_count,
        "cached": cached_count,
        "total_contacts_found": total_contacts,
        "results": results,
    }


if __name__ == "__main__":
    import argparse
    import sys

    _PARENT = str(Path(__file__).resolve().parent.parent)
    if _PARENT not in sys.path:
        sys.path.insert(0, _PARENT)

    parser = argparse.ArgumentParser(description="ZoomInfo persona operations")
    parser.add_argument("--flush-personas", action="store_true",
                        help="Remove non-Czech persona cache entries")
    parser.add_argument("--stats", action="store_true",
                        help="Show persona cache statistics")
    args = parser.parse_args()

    if args.flush_personas:
        removed = flush_stale_personas()
        print(f"Removed {removed} non-Czech persona cache entries")
    elif args.stats:
        import sqlite3
        db_path = Path(__file__).resolve().parent.parent / "prospects.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT result_json FROM enrichment_cache WHERE lookup_type='personas'"
        ).fetchall()
        total_entries = len(rows)
        total_contacts = 0
        cz_contacts = 0
        for row in rows:
            data = json.loads(row["result_json"])
            for c in data.get("contacts", []):
                total_contacts += 1
                if _is_czech_contact(c):
                    cz_contacts += 1
        conn.close()
        print(f"Persona cache: {total_entries} entries, {total_contacts} contacts")
        print(f"  Czech contacts: {cz_contacts}")
        print(f"  Non-Czech contacts: {total_contacts - cz_contacts}")
    else:
        parser.print_help()
