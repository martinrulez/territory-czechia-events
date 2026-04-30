"""Advanced enrichment signals for Tier 1-3 accounts.

Extracts deeper intelligence from already-cached data sources plus
targeted new scraping. Covers:

1. Public tender depth — map contract types to Autodesk products
2. EU grant program classification — link programs to product opportunities
3. Facility expansion — detect from OR changes, news, building keywords
4. M&A activity — detect ownership changes from OR data
5. Certification & compliance — ISO 19650, BIM mandate, IATF, ISO 9001
6. Website content signals — digital transformation, case studies, tech stack
7. Sustainability / ESG — CSRD, green certs, sustainability roles
8. Event engagement depth — multi-event, exhibitor status, repeat attendance
"""

import json
import re
import sys
from pathlib import Path

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from db.database import get_cached_enrichment


# =====================================================================
# 1. PUBLIC TENDER DEPTH — map contract types to products
# =====================================================================

TENDER_PRODUCT_MAP = {
    "infrastructure": {
        "keywords": [
            "silnic", "dálnic", "komunikac", "doprav", "most",
            "tunel", "železnic", "tramvaj", "metro", "cyklostezk",
            "vodovod", "kanalizac", "inženýrsk", "hydrotech",
            "meliorac", "přehrad",
        ],
        "products": ["Civil 3D", "InfraWorks", "AEC Collection"],
        "label": "Infrastructure project",
    },
    "building": {
        "keywords": [
            "budov", "stavba obyt", "bytov", "administrativn",
            "nemocnic", "škol", "školsk", "zateplení", "fasád",
            "rekonstrukce budov", "výstavba budov", "obchodní centr",
            "nákupní centr", "hotel",
        ],
        "products": ["Revit", "AEC Collection", "BIM Collaborate Pro"],
        "label": "Building construction",
    },
    "architecture": {
        "keywords": [
            "architekton", "územní plán", "urbanist",
            "studie staveb", "návrh staveb", "projekt staveb",
            "projektová dokumentac",
        ],
        "products": ["Revit", "Forma", "AEC Collection"],
        "label": "Architectural services",
    },
    "geodesy": {
        "keywords": [
            "geodet", "geometrick", "katastr", "měření", "zaměření",
            "mapování", "3d skenování", "lidar",
        ],
        "products": ["ReCap", "Civil 3D", "AEC Collection"],
        "label": "Geodesy & surveying",
    },
    "construction_mgmt": {
        "keywords": [
            "stavební dozor", "koordinát", "řízení staveb",
            "technický dozor", "autorský dozor", "site management",
        ],
        "products": ["ACC Build", "BIM Collaborate Pro", "Navisworks"],
        "label": "Construction management",
    },
    "manufacturing": {
        "keywords": [
            "strojírens", "výrob", "stroj", "obráběc", "sváře",
            "montáž", "automob", "součást", "nástroj", "formy",
            "cnc", "lisov", "technolog",
        ],
        "products": ["PDMC", "Fusion Mfg Ext", "Inventor"],
        "label": "Manufacturing / machinery",
    },
}


def analyze_tender_depth(enrichment: dict) -> dict:
    """Classify public contracts by type and map to products."""
    result = {
        "tender_types": [],
        "tender_products": [],
        "tender_value_trend": None,
        "first_time_tender": False,
        "tender_depth_summary": "",
    }

    ico = enrichment.get("ico", "")
    official_name = enrichment.get("official_name", "")

    all_contracts = []

    smlouvy_cache_key = f"smlouvy:{ico}" if ico else None
    if smlouvy_cache_key:
        cached = get_cached_enrichment("smlouvy", smlouvy_cache_key)
        if cached and cached.get("recent_contracts"):
            all_contracts.extend(cached["recent_contracts"])

    isvz_cache_key = f"isvz:{ico}" if ico else None
    if isvz_cache_key:
        cached = get_cached_enrichment("isvz", isvz_cache_key)
        if cached and cached.get("contracts"):
            all_contracts.extend(cached["contracts"])

    if not all_contracts:
        return result

    type_counts = {}
    product_set = set()

    for contract in all_contracts:
        subject = (contract.get("subject", "") or contract.get("title", "")).lower()
        for ttype, config in TENDER_PRODUCT_MAP.items():
            if any(kw in subject for kw in config["keywords"]):
                type_counts[ttype] = type_counts.get(ttype, 0) + 1
                product_set.update(config["products"])

    if type_counts:
        result["tender_types"] = [
            {"type": t, "count": c, "label": TENDER_PRODUCT_MAP[t]["label"]}
            for t, c in sorted(type_counts.items(), key=lambda x: -x[1])
        ]
        result["tender_products"] = sorted(product_set)

    total_contracts = enrichment.get("smlouvy_contracts_count", 0) or 0
    try:
        total_contracts = int(total_contracts)
    except (ValueError, TypeError):
        total_contracts = 0

    if total_contracts > 0 and total_contracts <= 3:
        result["first_time_tender"] = True

    parts = []
    if result["tender_types"]:
        top_types = [t["label"] for t in result["tender_types"][:3]]
        parts.append(f"Contract types: {', '.join(top_types)}")
    if result["tender_products"]:
        parts.append(f"Product fit: {', '.join(result['tender_products'][:4])}")
    if result["first_time_tender"]:
        parts.append("New to public procurement — BIM mandate readiness opportunity")
    result["tender_depth_summary"] = "; ".join(parts)

    return result


# =====================================================================
# 2. EU GRANT PROGRAM CLASSIFICATION
# =====================================================================

GRANT_PROGRAM_MAP = {
    "optak": {
        "keywords": ["optak", "op tak", "technologie a aplikace", "operační program technolog"],
        "products": ["PDMC", "Fusion", "Inventor", "Vault"],
        "label": "OPTAK — Industry 4.0 / Digitalization",
    },
    "trend": {
        "keywords": ["trend", "technologická agentura", "tačr"],
        "products": ["Fusion Sim Ext", "Moldflow", "Inventor"],
        "label": "TREND — R&D and Innovation",
    },
    "modernizacni_fond": {
        "keywords": ["modernizační fond", "modernizacni fond", "mod. fond"],
        "products": ["Forma", "Revit"],
        "label": "Modernizační fond — Energy & Sustainability",
    },
    "horizon": {
        "keywords": ["horizon", "h2020", "erasmus", "fp7"],
        "products": ["Fusion", "Inventor"],
        "label": "Horizon Europe — EU R&D",
    },
    "irop": {
        "keywords": ["irop", "integrovaný regionální"],
        "products": ["Revit", "Civil 3D", "AEC Collection"],
        "label": "IROP — Regional Development",
    },
    "opz": {
        "keywords": ["opz", "zaměstnanost", "lidské zdroje"],
        "products": [],
        "label": "OPZ — Employment (indirect)",
    },
    "oppik": {
        "keywords": ["oppik", "op pik", "podnikání a inovace"],
        "products": ["PDMC", "Fusion", "Inventor", "Vault"],
        "label": "OPPIK — Enterprise & Innovation",
    },
    "digitalization": {
        "keywords": [
            "digitalizace", "digital", "průmysl 4.0", "industry 4.0",
            "automatizace", "robotizace", "smart factory", "smart",
            "informační systém", "erp", "mes", "plm", "cad", "bim",
        ],
        "products": ["PDMC", "Fusion", "AEC Collection"],
        "label": "Digitalization / Industry 4.0",
    },
    "construction": {
        "keywords": [
            "staveb", "výstavb", "rekonstrukc", "revitalizac",
            "infrastruktur", "budov",
        ],
        "products": ["Revit", "Civil 3D", "AEC Collection", "ACC Build"],
        "label": "Construction & Infrastructure",
    },
}


def classify_grants(enrichment: dict) -> dict:
    """Classify EU grants by program type and map to products."""
    result = {
        "grant_programs": [],
        "grant_products": [],
        "has_digi_budget": False,
        "grant_program_summary": "",
    }

    recent = enrichment.get("eu_recent_grants", [])
    digi = enrichment.get("eu_digi_grants", [])
    all_grants = recent + digi

    if not all_grants and not enrichment.get("has_eu_grants"):
        return result

    ico = enrichment.get("ico", "")
    if ico:
        dotace_projects_path = Path(__file__).resolve().parent.parent / "enrichment_data" / "dotace_projects.json"
        try:
            with open(dotace_projects_path) as f:
                dotace_projects = json.load(f)
            projects = dotace_projects.get(ico, [])
            for p in projects:
                name = p.get("nazev", "") or p.get("name", "")
                if name and not any(
                    g.get("name") == name or g.get("nazev") == name for g in all_grants
                ):
                    all_grants.append({"name": name, "year": p.get("year", "")})
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    program_counts = {}
    product_set = set()

    for grant in all_grants:
        text = (grant.get("name", "") + " " + grant.get("program", "")).lower()
        for prog_key, config in GRANT_PROGRAM_MAP.items():
            if any(kw in text for kw in config["keywords"]):
                program_counts[prog_key] = program_counts.get(prog_key, 0) + 1
                product_set.update(config["products"])

    if program_counts:
        result["grant_programs"] = [
            {"program": GRANT_PROGRAM_MAP[p]["label"], "count": c}
            for p, c in sorted(program_counts.items(), key=lambda x: -x[1])
        ]
        result["grant_products"] = sorted(product_set)

    digi_programs = {"optak", "trend", "oppik", "digitalization"}
    result["has_digi_budget"] = bool(digi_programs & set(program_counts.keys()))

    parts = []
    if result["grant_programs"]:
        progs = [g["program"] for g in result["grant_programs"][:3]]
        parts.append(f"Programs: {', '.join(progs)}")
    if result["grant_products"]:
        parts.append(f"Product fit: {', '.join(result['grant_products'][:4])}")
    if result["has_digi_budget"]:
        parts.append("Has earmarked digitalization budget")
    result["grant_program_summary"] = "; ".join(parts)

    return result


# =====================================================================
# 3. FACILITY EXPANSION
# =====================================================================

EXPANSION_KEYWORDS = {
    "new_facility": [
        "nová výrobní hala", "nový závod", "nová továrna", "nová pobočka",
        "nový provoz", "rozšíření výroby", "nová provozovna", "nový areál",
        "výstavba haly", "výstavba závodu", "nové sídlo",
        "new factory", "new plant", "new facility", "new branch",
        "expansion", "rozšíření kapacit",
    ],
    "construction_permit": [
        "stavební povolení", "územní rozhodnutí", "kolaudac",
        "building permit", "zkušební provoz",
    ],
    "relocation": [
        "přesídlení", "přestěhování", "stěhování", "nové sídlo",
        "nová adresa", "relocation",
    ],
}


def detect_facility_expansion(enrichment: dict) -> dict:
    """Detect facility expansion signals from cached data sources."""
    result = {
        "facility_expansion": False,
        "expansion_type": None,
        "expansion_evidence": [],
        "expansion_summary": "",
    }

    evidence = []

    or_changes = enrichment.get("recent_changes", [])
    if not or_changes:
        ico = enrichment.get("ico", "")
        if ico:
            cached_or = get_cached_enrichment("or_justice", f"or:{ico}")
            if cached_or:
                or_changes = cached_or.get("recent_changes", [])

    for change in or_changes:
        text = change.get("text", "").lower()
        for exp_type, keywords in EXPANSION_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                evidence.append({
                    "source": "or_justice",
                    "type": exp_type,
                    "detail": change.get("text", "")[:150],
                    "date": change.get("date", ""),
                })
                break

    domain = enrichment.get("domain", "")
    if domain:
        web_data = get_cached_enrichment("web_signals", f"web_signals:{domain}")
        if web_data and web_data.get("signals"):
            for signal in web_data.get("signals", []):
                headline = (signal.get("headline", "") + " " + signal.get("snippet", "")).lower()
                for exp_type, keywords in EXPANSION_KEYWORDS.items():
                    if any(kw in headline for kw in keywords):
                        evidence.append({
                            "source": "company_website",
                            "type": exp_type,
                            "detail": signal.get("headline", "")[:150],
                            "date": signal.get("date", ""),
                        })
                        break

    digi_grants = enrichment.get("eu_digi_grants", [])
    recent_grants = enrichment.get("eu_recent_grants", [])
    for grant in digi_grants + recent_grants:
        name = (grant.get("name", "")).lower()
        for exp_type, keywords in EXPANSION_KEYWORDS.items():
            if any(kw in name for kw in keywords):
                evidence.append({
                    "source": "eu_grants",
                    "type": exp_type,
                    "detail": grant.get("name", "")[:150],
                    "date": grant.get("year", ""),
                })
                break

    emp_growth = (enrichment.get("zi_employee_growth") or {}).get("one_year_growth_rate")
    try:
        if emp_growth is not None and float(emp_growth) > 25:
            evidence.append({
                "source": "zoominfo",
                "type": "rapid_growth",
                "detail": f"Employee growth {emp_growth}% YoY — likely expanding facilities",
                "date": "",
            })
    except (ValueError, TypeError):
        pass

    if evidence:
        result["facility_expansion"] = True
        result["expansion_type"] = evidence[0]["type"]
        result["expansion_evidence"] = evidence[:5]
        parts = [e["detail"][:80] for e in evidence[:2]]
        result["expansion_summary"] = "; ".join(parts)

    return result


# =====================================================================
# 4. M&A ACTIVITY
# =====================================================================

MA_KEYWORDS = [
    "fúze", "sloučení", "splynutí", "převod jmění", "rozdělení",
    "přeměna", "převzetí", "akvizice", "merger", "acquisition",
    "změna společníka", "změna jednatele", "vstup společníka",
    "nový společník", "převod obchodního podílu", "prodej podílu",
    "zvýšení základního kapitálu", "snížení základního kapitálu",
    "změna firmy", "přejmenování",
]


def detect_ma_activity(enrichment: dict) -> dict:
    """Detect M&A signals from Justice.cz OR changes."""
    result = {
        "ma_detected": False,
        "ma_type": None,
        "ma_evidence": [],
        "ma_summary": "",
    }

    or_changes = enrichment.get("recent_changes", [])
    if not or_changes:
        ico = enrichment.get("ico", "")
        if ico:
            cached_or = get_cached_enrichment("or_justice", f"or:{ico}")
            if cached_or:
                or_changes = cached_or.get("recent_changes", [])

    evidence = []
    for change in or_changes:
        text = change.get("text", "").lower()
        for kw in MA_KEYWORDS:
            if kw in text:
                ma_type = "merger" if any(w in kw for w in ["fúze", "sloučení", "merger"]) \
                    else "acquisition" if any(w in kw for w in ["převzetí", "akvizice", "acquisition"]) \
                    else "ownership_change" if any(w in kw for w in ["společník", "podíl"]) \
                    else "restructuring"

                evidence.append({
                    "type": ma_type,
                    "detail": change.get("text", "")[:150],
                    "date": change.get("date", ""),
                })
                break

    zi_parent = enrichment.get("zi_ultimate_parent_name") or enrichment.get("zi_parent_name", "")
    if zi_parent:
        evidence.append({
            "type": "subsidiary",
            "detail": f"Part of {zi_parent} group — potential standardization target",
            "date": "",
        })

    if evidence:
        result["ma_detected"] = True
        result["ma_type"] = evidence[0]["type"]
        result["ma_evidence"] = evidence[:5]
        parts = [e["detail"][:80] for e in evidence[:2]]
        result["ma_summary"] = "; ".join(parts)

    return result


# =====================================================================
# 5. CERTIFICATION & COMPLIANCE TRIGGERS
# =====================================================================

CERTIFICATION_SIGNALS = {
    "bim_mandate": {
        "keywords": [
            "bim", "iso 19650", "nda 40/2025", "bim mandát",
            "bim koordin", "bim manažer", "bim specialist",
            "informační modelování staveb", "information modelling",
            "openb", "ifc", "cde", "společné datové prostředí",
        ],
        "products": ["Revit", "BIM Collaborate Pro", "ACC", "AEC Collection", "Navisworks"],
        "label": "BIM mandate / ISO 19650 readiness",
    },
    "quality_mgmt": {
        "keywords": [
            "iso 9001", "iso9001", "quality management",
            "řízení kvality", "systém kvality", "qms",
        ],
        "products": ["Vault", "Fusion Manage"],
        "label": "ISO 9001 quality management",
    },
    "automotive_quality": {
        "keywords": [
            "iatf 16949", "iatf16949", "automotive quality",
            "vda 6", "ppap", "apqp", "fmea",
        ],
        "products": ["Vault", "Fusion Manage", "PDMC"],
        "label": "IATF 16949 automotive quality",
    },
    "ce_marking": {
        "keywords": [
            "ce marking", "ce značení", "machine directive",
            "strojní směrnice", "směrnice o strojních zařízeních",
            "technická dokumentace strojů",
        ],
        "products": ["Inventor", "PDMC", "Vault"],
        "label": "CE marking / Machine Directive compliance",
    },
    "environmental": {
        "keywords": [
            "iso 14001", "iso14001", "emas", "environmentální management",
            "environmental management",
        ],
        "products": ["Forma"],
        "label": "ISO 14001 environmental management",
    },
}


def detect_certification_signals(enrichment: dict) -> dict:
    """Detect certification/compliance triggers from jobs, website, grants."""
    result = {
        "certifications_detected": [],
        "certification_products": [],
        "bim_mandate_relevant": False,
        "certification_summary": "",
    }

    all_text_sources = []

    ico = enrichment.get("ico", "")
    official_name = enrichment.get("official_name", "")

    for name_field in ["official_name", "zi_company_name"]:
        company = enrichment.get(name_field, "")
        if company:
            cache_key = f"jobs:{company.lower().strip()}"
            cached = get_cached_enrichment("jobs_cz", cache_key)
            if cached and cached.get("jobs"):
                for job in cached["jobs"]:
                    text = " ".join([
                        job.get("title", ""),
                        job.get("description", ""),
                        job.get("full_requirements", ""),
                        job.get("full_description", ""),
                    ])
                    all_text_sources.append(text)
                break

    domain = enrichment.get("domain", "")
    if domain:
        web_data = get_cached_enrichment("web_signals", f"web_signals:{domain}")
        if web_data and web_data.get("signals"):
            for signal in web_data["signals"]:
                all_text_sources.append(
                    signal.get("headline", "") + " " + signal.get("snippet", "")
                )

        emp_data = get_cached_enrichment("website_emp", f"emp_est:{domain}")
        if emp_data:
            all_text_sources.append(str(emp_data))

    combined_text = " ".join(all_text_sources).lower()

    detected = {}
    product_set = set()

    for cert_key, config in CERTIFICATION_SIGNALS.items():
        for kw in config["keywords"]:
            if kw in combined_text:
                detected[cert_key] = config["label"]
                product_set.update(config["products"])
                break

    if detected:
        result["certifications_detected"] = [
            {"type": k, "label": v} for k, v in detected.items()
        ]
        result["certification_products"] = sorted(product_set)
        result["bim_mandate_relevant"] = "bim_mandate" in detected

        parts = [v for v in detected.values()]
        result["certification_summary"] = "; ".join(parts)

    return result


# =====================================================================
# 6 + 7. WEBSITE CONTENT & TECH SIGNALS + ESG
# =====================================================================

DIGITAL_TRANSFORMATION_KEYWORDS = [
    "digitální transformace", "digital transformation", "digitalizace",
    "průmysl 4.0", "industry 4.0", "smart factory", "smart manufacturing",
    "automatizace", "automation", "robotizace", "digitální dvojče",
    "digital twin", "iot", "internet věcí", "prediktivní údržba",
    "predictive maintenance", "ai", "umělá inteligence",
    "machine learning", "strojové učení", "cloud", "saas",
]

CASE_STUDY_KEYWORDS = [
    "případová studie", "case study", "reference", "realizace",
    "portfolio", "naše práce", "our work", "projects", "projekty",
]

TECH_STACK_KEYWORDS = {
    "erp": ["sap", "microsoft dynamics", "oracle", "helios", "karat", "abra"],
    "plm_pdm": ["windchill", "teamcenter", "enovia", "aras", "plm", "pdm"],
    "cad_competitors": [
        "solidworks", "catia", "siemens nx", "creo", "solid edge",
        "archicad", "allplan", "tekla", "microstation", "vectorworks",
    ],
    "cad_autodesk": [
        "autocad", "revit", "inventor", "fusion", "civil 3d",
        "navisworks", "3ds max", "maya",
    ],
    "cloud_platforms": ["aws", "azure", "google cloud", "gcp"],
    "project_mgmt": ["procore", "aconex", "primavera", "ms project"],
}

ESG_KEYWORDS = [
    "esg", "csrd", "sustainability", "udržitelnost", "udržitelný",
    "carbon footprint", "uhlíková stopa", "carbon neutral",
    "net zero", "co2", "emise", "lca", "lifecycle assessment",
    "breeam", "leed", "well", "green building",
    "certifikace budov", "pasivní dům", "nzeb",
    "sustainability officer", "sustainability manager",
    "manažer udržitelnosti", "esg report", "zpráva o udržitelnosti",
    "nefinanční reporting", "taxonomie eu",
]


def analyze_website_content(enrichment: dict) -> dict:
    """Analyze cached website content for transformation, tech stack, ESG signals."""
    result = {
        "digital_transformation": False,
        "dt_evidence": [],
        "tech_stack_detected": {},
        "has_case_studies": False,
        "esg_signals": False,
        "esg_evidence": [],
        "website_content_summary": "",
    }

    domain = enrichment.get("domain", "")
    all_text = []

    if domain:
        web_data = get_cached_enrichment("web_signals", f"web_signals:{domain}")
        if web_data and web_data.get("signals"):
            for signal in web_data["signals"]:
                all_text.append(
                    signal.get("headline", "") + " " + signal.get("snippet", "")
                )

        emp_data = get_cached_enrichment("website_emp", f"emp_est:{domain}")
        if emp_data and isinstance(emp_data, dict):
            rev = emp_data.get("revenue_mention", "")
            if rev:
                all_text.append(str(rev))

    for name_field in ["official_name", "zi_company_name"]:
        company = enrichment.get(name_field, "")
        if company:
            cache_key = f"jobs:{company.lower().strip()}"
            cached = get_cached_enrichment("jobs_cz", cache_key)
            if cached and cached.get("jobs"):
                for job in cached["jobs"]:
                    all_text.append(" ".join([
                        job.get("title", ""),
                        job.get("description", ""),
                        job.get("full_requirements", ""),
                        job.get("full_description", ""),
                    ]))
                break

    combined = " ".join(all_text).lower()

    if not combined:
        return result

    dt_evidence = []
    for kw in DIGITAL_TRANSFORMATION_KEYWORDS:
        if kw in combined:
            dt_evidence.append(kw)
    if dt_evidence:
        result["digital_transformation"] = True
        result["dt_evidence"] = dt_evidence[:5]

    for kw in CASE_STUDY_KEYWORDS:
        if kw in combined:
            result["has_case_studies"] = True
            break

    tech = {}
    for category, keywords in TECH_STACK_KEYWORDS.items():
        found = [kw for kw in keywords if kw in combined]
        if found:
            tech[category] = found
    result["tech_stack_detected"] = tech

    esg_evidence = []
    for kw in ESG_KEYWORDS:
        if kw in combined:
            esg_evidence.append(kw)
    if esg_evidence:
        result["esg_signals"] = True
        result["esg_evidence"] = esg_evidence[:5]

    parts = []
    if result["digital_transformation"]:
        parts.append(f"Digital transformation mentions: {', '.join(dt_evidence[:3])}")
    if result["esg_signals"]:
        parts.append(f"ESG/sustainability signals: {', '.join(esg_evidence[:3])}")
    if tech.get("plm_pdm"):
        parts.append(f"PLM/PDM: {', '.join(tech['plm_pdm'])}")
    if tech.get("cad_competitors"):
        parts.append(f"Competitor CAD: {', '.join(tech['cad_competitors'][:3])}")
    if tech.get("erp"):
        parts.append(f"ERP: {', '.join(tech['erp'][:2])}")
    if result["has_case_studies"]:
        parts.append("Has case studies / portfolio section")
    result["website_content_summary"] = "; ".join(parts)

    return result


# =====================================================================
# 8. EVENT ENGAGEMENT DEPTH
# =====================================================================

def analyze_event_engagement(enrichment: dict, event_data: dict = None) -> dict:
    """Analyze event engagement depth — multi-event, exhibitor, repeat."""
    result = {
        "events_attended": 0,
        "is_exhibitor": False,
        "event_years": [],
        "multi_year_attendee": False,
        "event_types": [],
        "engagement_level": "none",
        "event_summary": "",
    }

    if event_data is None:
        try:
            path = Path(__file__).resolve().parent.parent / "enrichment_data" / "event_company_enrichment.json"
            with open(path) as f:
                event_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return result

    company_name = (enrichment.get("official_name", "") or "").lower().strip()
    if not company_name:
        return result

    company_events = event_data.get(company_name, {})
    if not company_events:
        for k, v in event_data.items():
            if company_name in k.lower() or k.lower() in company_name:
                company_events = v
                break

    if not company_events or not isinstance(company_events, dict):
        return result

    events = company_events.get("timing_signals", [])
    fit_score = company_events.get("fit_score", 0)
    component_scores = company_events.get("component_scores", {})

    event_count = 0
    exhibitor = False
    years = set()
    event_types = set()

    if isinstance(events, list):
        for event in events:
            if isinstance(event, dict):
                event_count += 1
                role = event.get("role", "").lower()
                if "exhibitor" in role or "vystavovatel" in role:
                    exhibitor = True
                year = event.get("year", "")
                if year:
                    years.add(str(year))
                etype = event.get("event", "") or event.get("type", "")
                if etype:
                    event_types.add(etype)
            elif isinstance(event, str):
                event_count += 1
                if "exhibitor" in event.lower() or "vystavovatel" in event.lower():
                    exhibitor = True

    if event_count == 0 and company_events.get("opportunity_score", 0) > 0:
        event_count = 1

    result["events_attended"] = event_count
    result["is_exhibitor"] = exhibitor
    result["event_years"] = sorted(years)
    result["multi_year_attendee"] = len(years) >= 2
    result["event_types"] = sorted(event_types)

    if exhibitor and len(years) >= 2:
        result["engagement_level"] = "high"
    elif exhibitor or len(years) >= 2 or event_count >= 3:
        result["engagement_level"] = "medium"
    elif event_count >= 1:
        result["engagement_level"] = "low"

    parts = []
    if event_count > 0:
        parts.append(f"{event_count} event(s)")
    if exhibitor:
        parts.append("exhibitor")
    if result["multi_year_attendee"]:
        parts.append(f"multi-year ({', '.join(sorted(years))})")
    if event_types:
        parts.append(f"events: {', '.join(sorted(event_types)[:3])}")
    result["event_summary"] = "; ".join(parts)

    return result


# =====================================================================
# MASTER ENRICHMENT FUNCTION
# =====================================================================

def enrich_advanced_signals(enrichment: dict, tier_accounts: list) -> dict:
    """Run all advanced signal analyses for Tier 1-3 accounts.

    Args:
        enrichment: Full enrichment dict keyed by CSN
        tier_accounts: List of account dicts with at least 'csn' key

    Returns:
        Updated enrichment dict
    """
    print(f"\n{'='*60}")
    print(f"ADVANCED SIGNAL ENRICHMENT — {len(tier_accounts)} accounts")
    print(f"{'='*60}")

    event_data = None
    try:
        path = Path(__file__).resolve().parent.parent / "enrichment_data" / "event_company_enrichment.json"
        with open(path) as f:
            event_data = json.load(f)
        print(f"  Loaded event data: {len(event_data)} companies")
    except (FileNotFoundError, json.JSONDecodeError):
        print("  No event data found")

    counts = {
        "tender_depth": 0, "grant_class": 0, "facility_exp": 0,
        "ma_activity": 0, "certification": 0, "dt_signals": 0,
        "esg_signals": 0, "event_engagement": 0,
    }

    for i, acct in enumerate(tier_accounts):
        csn = acct["csn"]
        e = enrichment.get(csn, {})

        tender = analyze_tender_depth(e)
        e["tender_types"] = tender["tender_types"]
        e["tender_products"] = tender["tender_products"]
        e["first_time_tender"] = tender["first_time_tender"]
        e["tender_depth_summary"] = tender["tender_depth_summary"]
        if tender["tender_types"]:
            counts["tender_depth"] += 1

        grants = classify_grants(e)
        e["grant_programs"] = grants["grant_programs"]
        e["grant_products"] = grants["grant_products"]
        e["has_digi_budget"] = grants["has_digi_budget"]
        e["grant_program_summary"] = grants["grant_program_summary"]
        if grants["grant_programs"]:
            counts["grant_class"] += 1

        expansion = detect_facility_expansion(e)
        e["facility_expansion"] = expansion["facility_expansion"]
        e["expansion_type"] = expansion["expansion_type"]
        e["expansion_evidence"] = expansion["expansion_evidence"]
        e["expansion_summary"] = expansion["expansion_summary"]
        if expansion["facility_expansion"]:
            counts["facility_exp"] += 1

        ma = detect_ma_activity(e)
        e["ma_detected"] = ma["ma_detected"]
        e["ma_type"] = ma["ma_type"]
        e["ma_evidence"] = ma["ma_evidence"]
        e["ma_summary"] = ma["ma_summary"]
        if ma["ma_detected"]:
            counts["ma_activity"] += 1

        cert = detect_certification_signals(e)
        e["certifications_detected"] = cert["certifications_detected"]
        e["certification_products"] = cert["certification_products"]
        e["bim_mandate_relevant"] = cert["bim_mandate_relevant"]
        e["certification_summary"] = cert["certification_summary"]
        if cert["certifications_detected"]:
            counts["certification"] += 1

        web = analyze_website_content(e)
        e["digital_transformation"] = web["digital_transformation"]
        e["dt_evidence"] = web["dt_evidence"]
        e["tech_stack_detected"] = web["tech_stack_detected"]
        e["has_case_studies"] = web["has_case_studies"]
        e["esg_signals"] = web["esg_signals"]
        e["esg_evidence"] = web["esg_evidence"]
        e["website_content_summary"] = web["website_content_summary"]
        if web["digital_transformation"]:
            counts["dt_signals"] += 1
        if web["esg_signals"]:
            counts["esg_signals"] += 1

        events = analyze_event_engagement(e, event_data)
        e["events_attended"] = events["events_attended"]
        e["is_exhibitor"] = events["is_exhibitor"]
        e["multi_year_attendee"] = events["multi_year_attendee"]
        e["engagement_level"] = events["engagement_level"]
        e["event_summary"] = events["event_summary"]
        if events["events_attended"] > 0:
            counts["event_engagement"] += 1

        enrichment[csn] = e

        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(tier_accounts)}] processed")

    print(f"\n  Results:")
    print(f"  {'Tender product mapping:':<30} {counts['tender_depth']}/{len(tier_accounts)}")
    print(f"  {'Grant program classified:':<30} {counts['grant_class']}/{len(tier_accounts)}")
    print(f"  {'Facility expansion:':<30} {counts['facility_exp']}/{len(tier_accounts)}")
    print(f"  {'M&A activity:':<30} {counts['ma_activity']}/{len(tier_accounts)}")
    print(f"  {'Certification signals:':<30} {counts['certification']}/{len(tier_accounts)}")
    print(f"  {'Digital transformation:':<30} {counts['dt_signals']}/{len(tier_accounts)}")
    print(f"  {'ESG / sustainability:':<30} {counts['esg_signals']}/{len(tier_accounts)}")
    print(f"  {'Event engagement:':<30} {counts['event_engagement']}/{len(tier_accounts)}")

    return enrichment
