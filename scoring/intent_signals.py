"""Advanced intent signals for account prioritization.

Three signal types:
1. Upsell-aligned hiring intent — matches job postings to top upsell product
2. EU grant awards (dotace) — searches dotaceeu.cz for earmarked budget
3. Revenue-per-employee trend — labels productivity vs capacity investment
"""

# ---------------------------------------------------------------------------
# 1. UPSELL-ALIGNED HIRING INTENT
# ---------------------------------------------------------------------------

UPSELL_JOB_SIGNALS = {
    "PDMC": {
        "titles": [
            "konstruktér", "konstrukter", "mechanical engineer",
            "strojní inženýr", "strojní technik",
            "správce dat", "vault", "data manager",
            "vedoucí konstrukce", "head of engineering",
        ],
        "tools": {"Inventor", "Vault", "AutoCAD", "Fusion", "CAD", "PDM", "PLM"},
    },
    "AEC Collection": {
        "titles": [
            "bim koordinátor", "bim manažer", "bim manager",
            "bim specialista", "revit", "projektant",
            "stavební inženýr", "civil engineer",
            "architekt", "architect",
            "statik", "structural engineer",
        ],
        "tools": {"Revit", "Civil 3D", "Navisworks", "BIM", "AutoCAD"},
    },
    "Forma": {
        "titles": [
            "bim koordinátor", "bim manažer", "bim manager",
            "architekt", "architect", "urban planner",
            "sustainability", "udržitelnost",
        ],
        "tools": {"BIM", "Revit", "FormIt"},
    },
    "BIM Collaborate Pro": {
        "titles": [
            "bim koordinátor", "bim manažer", "bim manager",
            "projektový manažer", "project manager",
            "construction manager",
        ],
        "tools": {"BIM", "BIM 360", "Autodesk Construction Cloud", "Navisworks"},
    },
    "Fusion Mfg Ext": {
        "titles": [
            "cnc programátor", "cnc programmer", "cam programátor",
            "seřízeč", "obráběč", "machinist",
            "technolog výroby", "manufacturing engineer",
            "programátor cnc", "nástrojař",
        ],
        "tools": {"Fusion", "CAM", "CNC", "PowerMill", "FeatureCAM", "Mastercam"},
    },
    "Fusion Sim Ext": {
        "titles": [
            "simulační inženýr", "simulation engineer",
            "fea", "pevnostní výpočtář", "stress engineer",
            "výpočtář", "analyst",
        ],
        "tools": {"Fusion", "Ansys", "Abaqus"},
    },
    "Inventor": {
        "titles": [
            "konstruktér", "konstrukter", "mechanical designer",
            "strojní inženýr", "3d modelář",
        ],
        "tools": {"Inventor", "CAD", "SolidWorks", "Creo"},
    },
    "Fusion": {
        "titles": [
            "konstruktér", "konstrukter", "product designer",
            "průmyslový designér", "industrial designer",
        ],
        "tools": {"Fusion", "CAD", "CAM"},
    },
    "AutoCAD (full)": {
        "titles": [
            "kreslič", "cad operator", "cad technik",
            "projektant", "technický kreslič",
        ],
        "tools": {"AutoCAD", "CAD"},
    },
    "M&E Collection": {
        "titles": [
            "3d grafik", "3d artist", "vfx artist",
            "animátor", "animator", "vizualizér",
            "motion designer", "character artist",
        ],
        "tools": {"Maya", "3ds Max", "Blender", "Cinema 4D", "Houdini", "Nuke"},
    },
}


def analyze_hiring_intent(enrichment: dict, top_upsell: str) -> dict:
    """Check if a company's job postings signal intent for the top upsell product.

    Returns dict with:
        upsell_hiring_intent: bool
        intent_strength: 'strong' | 'moderate' | 'weak' | None
        matching_titles: list of job titles that match
        matching_tools: list of tools in postings that align with upsell
        intent_summary: human-readable summary
    """
    if not top_upsell or not enrichment.get("hiring_signal"):
        return {
            "upsell_hiring_intent": False,
            "intent_strength": None,
            "matching_titles": [],
            "matching_tools": [],
            "intent_summary": "",
        }

    signal_def = UPSELL_JOB_SIGNALS.get(top_upsell)
    if not signal_def:
        for key, val in UPSELL_JOB_SIGNALS.items():
            if key.lower() in top_upsell.lower():
                signal_def = val
                break
    if not signal_def:
        return {
            "upsell_hiring_intent": False,
            "intent_strength": None,
            "matching_titles": [],
            "matching_tools": [],
            "intent_summary": "",
        }

    target_titles = [t.lower() for t in signal_def["titles"]]
    target_tools = signal_def["tools"]

    autodesk_in_jobs = set(enrichment.get("autodesk_tools_in_jobs") or [])
    competitor_in_jobs = set(enrichment.get("competitor_tools_in_jobs") or [])
    all_tools_in_jobs = autodesk_in_jobs | competitor_in_jobs

    matching_tools = sorted(all_tools_in_jobs & target_tools)

    matching_titles = []
    jobs = enrichment.get("jobs_raw", [])
    if not jobs:
        relevant_roles = enrichment.get("relevant_roles", [])
        if relevant_roles:
            jobs = [{"title": r} for r in relevant_roles]

    for job in jobs:
        title = (job.get("title") or "").lower()
        for kw in target_titles:
            if kw in title:
                matching_titles.append(job.get("title", ""))
                break

    title_score = min(len(matching_titles), 3)
    tool_score = min(len(matching_tools), 3)
    total = title_score + tool_score

    if total >= 4:
        strength = "strong"
    elif total >= 2:
        strength = "moderate"
    elif total >= 1:
        strength = "weak"
    else:
        strength = None

    has_intent = strength is not None

    summary = ""
    if has_intent:
        parts = []
        if matching_titles:
            parts.append(f"hiring {matching_titles[0]}")
        if matching_tools:
            parts.append(f"uses {', '.join(matching_tools[:2])} in job postings")
        summary = f"Upsell intent ({strength}): {'; '.join(parts)} — aligns with {top_upsell}"

    return {
        "upsell_hiring_intent": has_intent,
        "intent_strength": strength,
        "matching_titles": matching_titles[:5],
        "matching_tools": matching_tools,
        "intent_summary": summary,
    }


# ---------------------------------------------------------------------------
# 2. REVENUE PER EMPLOYEE TREND ANALYSIS
# ---------------------------------------------------------------------------

def analyze_rev_per_employee(enrichment: dict) -> dict:
    """Compute revenue-per-employee and label investment type.

    Returns dict with:
        rev_per_employee: float or None (EUR)
        investment_label: 'productivity' | 'capacity' | 'stable' | None
        investment_detail: human-readable explanation
    """
    emp = enrichment.get("employee_count")
    rev = enrichment.get("revenue")
    rev_source = enrichment.get("revenue_source", "")

    if rev_source == "employee_benchmark":
        return {
            "rev_per_employee": None,
            "investment_label": None,
            "investment_detail": "",
        }

    if not emp or not rev:
        return {
            "rev_per_employee": None,
            "investment_label": None,
            "investment_detail": "",
        }

    try:
        emp_n = int(emp)
        rev_n = float(rev)
    except (ValueError, TypeError):
        return {
            "rev_per_employee": None,
            "investment_label": None,
            "investment_detail": "",
        }

    if emp_n <= 0:
        return {
            "rev_per_employee": None,
            "investment_label": None,
            "investment_detail": "",
        }

    # Revenue from Czech sources (kurzy.cz, ARES) is typically in CZK.
    # Heuristic: if rev/employee > 500k, it's likely CZK, convert at ~25 CZK/EUR.
    rpe = rev_n / emp_n
    if rpe > 500_000:
        rpe = rpe / 25.0

    rev_growth = enrichment.get("revenue_growth")
    zi_growth = enrichment.get("zi_employee_growth") or {}
    emp_growth_rate = zi_growth.get("one_year_growth_rate")

    label = None
    detail = ""

    if rev_growth is not None and emp_growth_rate is not None:
        try:
            rg = float(rev_growth)
            eg = float(emp_growth_rate)

            if rg > eg + 5:
                label = "productivity"
                detail = (
                    f"Revenue growing faster ({rg:+.1f}%) than headcount ({eg:+.1f}%) "
                    f"— investing in productivity tools, not just more people. "
                    f"Revenue per employee: EUR {rpe:,.0f}."
                )
            elif eg > rg + 5:
                label = "capacity"
                detail = (
                    f"Headcount growing faster ({eg:+.1f}%) than revenue ({rg:+.1f}%) "
                    f"— expanding capacity, needs more seats and workflows. "
                    f"Revenue per employee: EUR {rpe:,.0f}."
                )
            else:
                label = "stable"
                detail = (
                    f"Revenue ({rg:+.1f}%) and headcount ({eg:+.1f}%) growing in sync. "
                    f"Revenue per employee: EUR {rpe:,.0f}."
                )
        except (ValueError, TypeError):
            pass

    if label is None:
        if rpe > 150_000:
            label = "high-value"
            detail = f"High revenue per employee (EUR {rpe:,.0f}) — knowledge-intensive, tool ROI is clear."
        elif rpe > 50_000:
            label = "mid-range"
            detail = f"Revenue per employee: EUR {rpe:,.0f}."
        else:
            label = "labor-intensive"
            detail = f"Lower revenue per employee (EUR {rpe:,.0f}) — cost-conscious buyer, lead with efficiency."

    return {
        "rev_per_employee": round(rpe, 0),
        "investment_label": label,
        "investment_detail": detail,
    }


# ---------------------------------------------------------------------------
# 3. EU GRANT AWARDS (dotace)
# ---------------------------------------------------------------------------
# Grant data is pre-downloaded from the Czech IS ReD (Centrální registr dotací)
# bulk CSV exports and stored in enrichment_data/dotace_recipients.json and
# enrichment_data/dotace_projects.json. The pre-processing happens via a
# one-time bulk download script (see enrichment pipeline). The enrichment
# data already contains has_eu_grants, eu_grants_count, eu_grant_summary,
# eu_recent_grants, and eu_digi_grants fields populated from this data.


# ---------------------------------------------------------------------------
# BATCH ENRICHMENT FUNCTION
# ---------------------------------------------------------------------------

def enrich_intent_signals(enrichment: dict, accounts: list) -> dict:
    """Run hiring intent and rev/employee analyses, merge into enrichment.

    EU grant data is pre-loaded from bulk CSV exports (dotace_recipients.json,
    dotace_projects.json). This function handles hiring intent and
    revenue-per-employee analysis which use existing enrichment fields.
    """
    print(f"\n{'='*60}")
    print(f"INTENT SIGNAL ANALYSIS — {len(accounts)} accounts")
    print(f"{'='*60}")

    hiring_intent_count = 0
    grant_count = 0
    rpe_count = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        enrich = enrichment.get(csn, {})
        top_upsell = acct.get("top_upsell", "")

        intent = analyze_hiring_intent(enrich, top_upsell)
        enrich["upsell_hiring_intent"] = intent["upsell_hiring_intent"]
        enrich["intent_strength"] = intent["intent_strength"]
        enrich["intent_matching_titles"] = intent["matching_titles"]
        enrich["intent_matching_tools"] = intent["matching_tools"]
        enrich["intent_summary"] = intent["intent_summary"]
        if intent["upsell_hiring_intent"]:
            hiring_intent_count += 1

        rpe = analyze_rev_per_employee(enrich)
        enrich["rev_per_employee"] = rpe["rev_per_employee"]
        enrich["investment_label"] = rpe["investment_label"]
        enrich["investment_detail"] = rpe["investment_detail"]
        if rpe["rev_per_employee"] is not None:
            rpe_count += 1

        if enrich.get("has_eu_grants"):
            grant_count += 1

        enrichment[csn] = enrich

    print(f"\n  Upsell hiring intent:  {hiring_intent_count}/{len(accounts)}")
    print(f"  EU grants found:       {grant_count}/{len(accounts)}")
    print(f"  Rev/employee computed: {rpe_count}/{len(accounts)}")

    return enrichment
