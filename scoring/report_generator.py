"""Markdown report generator for deep-researched event companies.

Reads per-company JSON from research_data/ and generates structured
Markdown reports matching the deep research format:
  - Executive Summary
  - Company Snapshot
  - Recent Signals Timeline
  - Buying Committee
  - Tech Stack Evidence
  - Key Pain Points & Expansion Plays
  - Talk Tracks
  - Manual Input Section

Usage:
    python -m scoring.report_generator                    # all researched
    python -m scoring.report_generator --top 50           # top 50 only
    python -m scoring.report_generator --company "skanska" # single company
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESEARCH_DIR = PROJECT_ROOT / "research_data"
REPORTS_DIR = PROJECT_ROOT / "reports"

TECHNO_FALSE_POSITIVES = {
    "recaptcha", "captcha", "peoplenet", "vault payment solutions",
    "veritas enterprise vault", "veritas enterprise vault (ev)",
    "commvault",
}

NACE_SEGMENT_MAP = {
    "41": "AEC", "42": "AEC", "43": "AEC", "71": "AEC", "68": "AEC",
    "24": "D&M", "25": "D&M", "26": "D&M", "27": "D&M",
    "28": "D&M", "29": "D&M", "30": "D&M", "22": "D&M", "33": "D&M",
    "58": "M&E", "59": "M&E", "60": "M&E",
    "62": "M&E", "63": "M&E", "90": "M&E",
}

SEGMENT_LABELS = {
    "AEC": "Architecture, Engineering & Construction",
    "D&M": "Design & Manufacturing",
    "M&E": "Media & Entertainment",
}

NACE_DESCRIPTIONS = {
    "41": "Building construction", "42": "Civil engineering",
    "43": "Specialized construction", "71": "Architecture & engineering",
    "68": "Real estate", "24": "Basic metals", "25": "Fabricated metal",
    "26": "Electronics", "27": "Electrical equipment",
    "28": "Machinery & equipment", "29": "Motor vehicles",
    "30": "Other transport equipment", "22": "Rubber & plastics",
    "33": "Repair & installation of machinery",
    "58": "Publishing", "59": "Film & TV production",
    "60": "Broadcasting", "62": "Computer programming",
    "63": "Information services", "90": "Creative arts",
}

# ── Expansion Plays Library ──────────────────────────────────────────

EXPANSION_PLAYS = [
    {
        "id": "acc_foundation",
        "name": "Autodesk Construction Cloud Foundation",
        "segment": "AEC",
        "portfolio": "AEC",
        "condition": lambda e, t: e.get("ares_primary_segment") == "AEC",
        "what_to_sell": "ACC Build + Docs for centralized document management, RFIs, and issue tracking across projects.",
        "why_now": "Company operates in AEC with active projects and event presence — coordinated data and drawings across sites is a natural pain point.",
        "value_hypothesis": "Reduced rework from outdated drawings, faster RFI resolution, single source of truth for field + office.",
        "sponsor_title": "BIM Manager / Head of Design / Project Director",
        "blockers": "Existing CDE investment, reluctance to change mid-project, perception ACC is only for large contractors.",
        "disarm": "Start with one pilot project; show ROI on drawing revision cycle time and RFI closure speed.",
        "discovery_q": ["How do field teams currently access the latest drawings?", "What's the average RFI cycle time today?", "How many projects run concurrently, and is document management consistent across them?"],
    },
    {
        "id": "revit_bim_authoring",
        "name": "BIM Authoring & Quality (Revit + AEC Collection)",
        "segment": "AEC",
        "portfolio": "AEC",
        "condition": lambda e, t: e.get("ares_primary_segment") == "AEC" and _size_tier(e) in ("mid", "large"),
        "what_to_sell": "AEC Collection with Revit, Civil 3D, Navisworks for full multi-discipline BIM workflow.",
        "why_now": "Growing regulatory pressure for BIM adoption in Czech public projects; company's size supports Collection economics.",
        "value_hypothesis": "Multi-discipline coordination without file-exchange chaos, clash detection before construction, better project margins.",
        "sponsor_title": "BIM Manager / Technical Director / Head of Architecture",
        "blockers": "Legacy AutoCAD workflows, training investment, ArchiCAD entrenchment.",
        "disarm": "Phase from AutoCAD → Revit on new projects only; show clash-detection savings as the first win.",
        "discovery_q": ["Which disciplines do you coordinate in-house vs. externally?", "Do you use model-based clash detection today?", "What's the biggest coordination bottleneck between design and construction?"],
    },
    {
        "id": "pdmc_vault",
        "name": "Product Design Collection + Vault/PDM Pilot",
        "segment": "D&M",
        "portfolio": "D&M",
        "condition": lambda e, t: e.get("ares_primary_segment") == "D&M",
        "what_to_sell": "PD&M Collection (Inventor, AutoCAD Mechanical, Vault) for design-to-manufacturing with revision control.",
        "why_now": "Manufacturing company with product engineering — revision discipline and BOM accuracy are fundamental to production quality.",
        "value_hypothesis": "Reduced ECO cycle time, less rework from wrong revisions, faster engineering-to-production handoff.",
        "sponsor_title": "Engineering Manager / CTO / Head of R&D",
        "blockers": "Existing SolidWorks/Creo investment, internal homebrew PDM, price sensitivity.",
        "disarm": "Start with a 2-day assessment of current release flow; pilot Vault on one product family before broader rollout.",
        "discovery_q": ["How do you manage design revisions and BOMs today?", "What's the typical ECO cycle time?", "How many product variants do you maintain actively?"],
    },
    {
        "id": "displacement_solidworks",
        "name": "Competitive Displacement (SolidWorks / Creo / Solid Edge)",
        "segment": "D&M",
        "portfolio": "D&M",
        "condition": lambda e, t: bool(t.get("competitor_products")),
        "what_to_sell": "Migration to Inventor or Fusion 360 from competitive CAD, leveraging interoperability and data import tools.",
        "why_now": "Company is actively using competitor CAD tools — renewal cycles and dissatisfaction create natural switching windows.",
        "value_hypothesis": "Better integration with Autodesk ecosystem, cloud collaboration via Fusion, potential cost savings on bundled Collection.",
        "sponsor_title": "Engineering Manager / CTO / IT Director",
        "blockers": "Switching cost, retraining, legacy IP in competitor formats, supplier/customer format requirements.",
        "disarm": "Propose a parallel pilot with data interop test; quantify time lost to format conversions and manual workarounds.",
        "discovery_q": ["When is your next CAD renewal cycle?", "What are the biggest frustrations with your current CAD platform?", "How much time do you spend on file conversions for suppliers or customers?"],
    },
    {
        "id": "fusion_cloud",
        "name": "Fusion 360 Cloud Engineering",
        "segment": "D&M",
        "portfolio": "D&M",
        "condition": lambda e, t: e.get("ares_primary_segment") == "D&M" and _size_tier(e) == "small",
        "what_to_sell": "Fusion 360 for integrated CAD/CAM/CAE with cloud collaboration — ideal for smaller engineering teams.",
        "why_now": "Smaller D&M company where a single integrated tool reduces complexity vs. managing separate CAD + CAM + simulation licenses.",
        "value_hypothesis": "One tool from concept to manufacturing; cloud access for remote/distributed teams; lower total cost than separate tools.",
        "sponsor_title": "Owner / CTO / Lead Engineer",
        "blockers": "Cloud skepticism, perceived immaturity vs. Inventor/SolidWorks, niche manufacturing needs.",
        "disarm": "Free trial with a real part from their production; show CAM toolpath generation speed.",
        "discovery_q": ["How many engineers do design + manufacturing programming?", "Do you use separate tools for CAD and CAM today?", "Would cloud access for review/approval be valuable?"],
    },
    {
        "id": "autodesk_tools_upsell",
        "name": "Expand Existing Autodesk Footprint",
        "segment": "any",
        "portfolio": "Cross",
        "condition": lambda e, t: bool(t.get("autodesk_products")),
        "what_to_sell": "Upgrade existing Autodesk tools to Collection or add complementary products (e.g., Vault to existing Inventor, Navisworks to existing Revit).",
        "why_now": "Company already uses Autodesk products — expansion is lower-friction than net-new and addresses gaps in their current workflow.",
        "value_hypothesis": "Fill workflow gaps with tools that natively integrate with what they already own; Collection economics vs. individual SKUs.",
        "sponsor_title": "IT Director / CAD Manager / Engineering Manager",
        "blockers": "Budget pressure, perception that current tools are 'enough', subscription fatigue.",
        "disarm": "Show Collection price vs. sum of individual licenses; identify one workflow gap that costs them measurable time.",
        "discovery_q": ["Which Autodesk tools are you using today and on what versions?", "Where do you feel the biggest gaps in your current workflow?", "Are you on individual licenses or a Collection?"],
    },
    {
        "id": "hiring_growth",
        "name": "Growth Acceleration — Engineering Capacity Play",
        "segment": "any",
        "portfolio": "Cross",
        "condition": lambda e, t: e.get("engineering_hiring"),
        "what_to_sell": "Scalable licensing + onboarding package for growing engineering teams, with Flex tokens or named-user subscriptions.",
        "why_now": "Company is actively hiring engineers — new hires need tools on day one; this is the easiest budget conversation.",
        "value_hypothesis": "Faster new-hire productivity, standardized tool stack across the team, volume licensing economics.",
        "sponsor_title": "HR Director / Engineering Manager / CTO",
        "blockers": "Budget not yet approved for new seats, preference for free/open tools for junior hires.",
        "disarm": "Align with their hiring timeline; offer trial licenses for the onboarding period.",
        "discovery_q": ["How many engineering hires are planned this year?", "What's the current onboarding time for a new engineer to be productive?", "Do new hires bring their own tool preferences, or do you standardize?"],
    },
    {
        "id": "public_contracts_cde",
        "name": "Public Sector / BIM Mandate Readiness",
        "segment": "AEC",
        "portfolio": "AEC",
        "condition": lambda e, t: e.get("has_public_contracts") and e.get("ares_primary_segment") == "AEC",
        "what_to_sell": "ACC + BIM Collaborate Pro for compliance with Czech/EU BIM mandates on public infrastructure projects.",
        "why_now": "Company has active public contracts — upcoming BIM mandates for public procurement create urgency.",
        "value_hypothesis": "Win more public tenders by demonstrating BIM capability; avoid being locked out of growing public project pipeline.",
        "sponsor_title": "Project Director / BIM Manager / CEO",
        "blockers": "Mandate timeline perceived as distant, cost of BIM transition, existing 2D workflow is 'good enough'.",
        "disarm": "Show competitor firms already adopting BIM for public tenders; quantify the pipeline at risk.",
        "discovery_q": ["What percentage of your revenue comes from public projects?", "Have you encountered BIM requirements in recent tenders?", "What's your current BIM maturity — modeling, coordination, or full CDE?"],
    },
    {
        "id": "leadership_change_play",
        "name": "New Leadership — Strategic Technology Review",
        "segment": "any",
        "portfolio": "Cross",
        "condition": lambda e, t: e.get("leadership_change"),
        "what_to_sell": "Executive briefing on technology modernization aligned with the new leadership's strategic agenda.",
        "why_now": "Recent leadership change creates a window for process optimization and new investment evaluation.",
        "value_hypothesis": "New leaders want quick wins — technology modernization is a visible, measurable improvement area.",
        "sponsor_title": "New CEO / Managing Director / CTO",
        "blockers": "New leader may be focused on other priorities first, or may have existing vendor relationships.",
        "disarm": "Position as a 'landscape briefing', not a sales pitch; offer industry benchmarking data.",
        "discovery_q": ["What are the new leadership's top 3 priorities for this year?", "Is there a mandate to review technology investments?", "Where does engineering/design efficiency rank on the strategic agenda?"],
    },
    {
        "id": "me_pipeline",
        "name": "M&E Production Pipeline (3ds Max / Maya / ShotGrid)",
        "segment": "M&E",
        "portfolio": "M&E",
        "condition": lambda e, t: e.get("ares_primary_segment") == "M&E" or e.get("primary_segment") == "M&E",
        "what_to_sell": "3ds Max, Maya, or ShotGrid for VFX, animation, or visualization production pipelines.",
        "why_now": "Company operates in media/entertainment or visualization — production tools are core to their delivery capability.",
        "value_hypothesis": "Faster rendering, better asset management, streamlined review workflows.",
        "sponsor_title": "VFX Supervisor / Head of Production / Technical Director",
        "blockers": "Existing Blender/Houdini/Cinema 4D investment, freelancer ecosystem preferences.",
        "disarm": "Focus on pipeline integration and rendering performance rather than tool replacement.",
        "discovery_q": ["What's your primary 3D tool for production?", "How do you manage production tracking and reviews?", "What's your biggest bottleneck — asset creation, rendering, or review cycles?"],
    },
]


# ── Talk Track Templates ─────────────────────────────────────────────

def _build_talk_tracks(enr: dict, techno: dict, website: dict) -> list:
    """Generate 3-5 contextual talk tracks based on company profile."""
    tracks = []
    name = enr.get("company_name", enr.get("official_name", "your company"))
    segment = enr.get("ares_primary_segment", enr.get("primary_segment", ""))
    emp = enr.get("employee_count")
    events = enr.get("_events", "")

    if enr.get("hiring_signal") and enr.get("engineering_hiring"):
        tracks.append(
            f"You're hiring engineers right now — that usually means design capacity is the bottleneck, "
            f"not headcount. What if new hires were productive in week one instead of month three?"
        )

    if techno.get("competitor_products"):
        tools = ", ".join(techno["competitor_products"][:3])
        tracks.append(
            f"I noticed your team works with {tools}. Many Czech companies in your segment are "
            f"evaluating whether a unified Autodesk platform could reduce the time lost to format "
            f"conversions and fragmented data."
        )

    if techno.get("autodesk_products"):
        tools = ", ".join(techno["autodesk_products"][:3])
        tracks.append(
            f"You already use {tools} — the question isn't whether Autodesk fits, "
            f"but whether you're getting full value from what you own. Collections often cost less "
            f"than the individual licenses they replace."
        )

    if enr.get("leadership_change"):
        tracks.append(
            f"With recent changes in your leadership team, this is often the moment when companies "
            f"reassess their technology investments. We can offer a no-commitment landscape briefing "
            f"to help the new team understand where the industry is heading."
        )

    if enr.get("has_public_contracts") and segment == "AEC":
        tracks.append(
            f"Public procurement in Czech Republic is moving toward BIM mandates. With your portfolio "
            f"of public contracts, getting ahead of this requirement isn't just compliance — "
            f"it's a competitive advantage in winning the next tender."
        )

    if emp and isinstance(emp, (int, float)) and emp >= 200:
        tracks.append(
            f"At {int(emp)} employees, you're at the scale where engineering data governance becomes "
            f"critical. The cost of a wrong revision reaching production or a customer grows exponentially "
            f"with team size."
        )

    if enr.get("revenue_growth") and enr["revenue_growth"] > 10:
        tracks.append(
            f"Your revenue is growing at {enr['revenue_growth']:+.0f}% — that kind of growth "
            f"typically strains existing design workflows. We can help you scale your engineering "
            f"capacity to match your commercial momentum."
        )

    about = website.get("about", {}).get("text", "")
    if about and len(about) > 100:
        if any(kw in about.lower() for kw in ["export", "international", "zahranič", "worldwide", "global"]):
            tracks.append(
                f"Your international footprint means your engineering data needs to work across "
                f"borders, languages, and supply chains. That's where cloud-connected workflows "
                f"and standardized data formats make the biggest difference."
            )

    return tracks[:5]


# ── Report Generation ────────────────────────────────────────────────

def _size_tier(enr: dict) -> str:
    emp = enr.get("employee_count")
    if not emp or not isinstance(emp, (int, float)):
        return "unknown"
    if emp >= 500:
        return "large"
    if emp >= 50:
        return "mid"
    return "small"


def _format_number(n) -> str:
    if not n or not isinstance(n, (int, float)):
        return "N/A"
    if n >= 1_000_000:
        return f"{n/1_000_000:,.1f}M"
    if n >= 1_000:
        return f"{n/1_000:,.0f}K"
    return f"{n:,.0f}"


def _select_plays(enr: dict, techno: dict) -> list:
    """Select applicable expansion plays for this company."""
    selected = []
    for play in EXPANSION_PLAYS:
        try:
            if play["condition"](enr, techno):
                selected.append(play)
        except Exception:
            continue
    return selected[:4]


def generate_report(research: dict) -> str:
    """Generate a full Markdown report from research data."""
    enr = research.get("enrichment", {})
    techno = research.get("technographics", {})
    contacts = research.get("contacts", {})
    website = research.get("website_data", {})
    name = research.get("company_name", "Unknown Company")
    domain = research.get("domain", "")
    key = research.get("company_key", "")

    segment = enr.get("ares_primary_segment", enr.get("primary_segment", "unknown"))
    segment_label = SEGMENT_LABELS.get(segment, segment)
    emp = enr.get("employee_count")
    revenue = enr.get("revenue")
    opp_score = enr.get("opportunity_score", 0)
    deal_eur = enr.get("estimated_deal_eur", 0)
    is_client = enr.get("is_client", False)

    enr["_events"] = research.get("events", "")

    lines = []

    # ── Header ──
    lines.append(f"# {name} — Account Intelligence for Autodesk\n")

    # ── Executive Summary ──
    lines.append("## Executive Summary\n")
    if segment in SEGMENT_LABELS:
        lines.append(f"- **{segment}-led account.** Primary Autodesk motion is {segment_label}.")
    else:
        lines.append(f"- **Segment: {segment}.** Requires further classification for Autodesk motion.")

    if is_client:
        products = enr.get("current_products", "")
        seats = enr.get("total_seats", "")
        lines.append(f"- **Existing Autodesk client.** Currently uses {products} ({seats} seats). Focus on expansion and deepening adoption.")
    else:
        lines.append(f"- **New market opportunity.** No current Autodesk relationship detected — greenfield entry.")

    if emp and isinstance(emp, (int, float)):
        tier = _size_tier(enr)
        lines.append(f"- **Company scale: {tier} ({int(emp)} employees).** {'Enterprise-grade opportunity with multi-department potential.' if tier == 'large' else 'Mid-market opportunity with growth potential.' if tier == 'mid' else 'SMB opportunity — focus on efficiency and ROI.'}")

    if enr.get("hiring_signal"):
        eng = " with engineering/technical roles" if enr.get("engineering_hiring") else ""
        lines.append(f"- **Active hiring{eng}.** {enr.get('total_jobs', 0)} open positions detected — signals growth and capacity expansion.")

    adsk_tech = [t for t in techno.get("autodesk_products", []) if t.lower() not in TECHNO_FALSE_POSITIVES]
    if adsk_tech:
        tools = ", ".join(adsk_tech[:5])
        lines.append(f"- **Autodesk technology detected:** {tools}. Expansion and upsell opportunity.")

    if techno.get("competitor_products"):
        tools = ", ".join(techno["competitor_products"][:5])
        lines.append(f"- **Competitor technology detected:** {tools}. Displacement and consolidation opportunity.")

    if enr.get("leadership_change"):
        lines.append(f"- **Recent leadership change.** New leadership often opens windows for technology investment review.")

    if enr.get("has_public_contracts"):
        lines.append(f"- **Public sector contracts.** Active public procurement activity — BIM mandate readiness is relevant.")

    if opp_score:
        lines.append(f"- **Opportunity score: {opp_score}/100.** Estimated initial deal: EUR {_format_number(deal_eur)}.")

    events = research.get("events", "")
    if events:
        lines.append(f"- **Event presence:** {events}. Demonstrates industry engagement and accessibility.")

    lines.append("")

    # ── Company Snapshot ──
    lines.append("## Company Snapshot\n")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| **Company** | {name} |")
    if enr.get("official_name") and enr["official_name"].lower() != name.lower():
        lines.append(f"| **Official name** | {enr['official_name']} |")
    if enr.get("ico"):
        lines.append(f"| **ICO** | {enr['ico']} |")
    if domain:
        lines.append(f"| **Website** | {domain} |")
    if enr.get("ares_address"):
        lines.append(f"| **HQ** | {enr['ares_address']} |")
    if emp:
        lines.append(f"| **Employees** | {_format_number(emp)} |")
    if revenue:
        lines.append(f"| **Revenue (USD K)** | ${_format_number(revenue)} |")
    if enr.get("revenue_czk"):
        lines.append(f"| **Revenue (CZK)** | {_format_number(enr['revenue_czk'])} CZK |")
    if segment in SEGMENT_LABELS:
        lines.append(f"| **Primary segment** | {segment} — {segment_label} |")

    nace = enr.get("nace_codes", [])
    if nace:
        unique_prefixes = []
        seen = set()
        for code in nace:
            prefix = str(code).strip()[:2]
            if prefix not in seen and prefix in NACE_DESCRIPTIONS:
                seen.add(prefix)
                unique_prefixes.append(f"{prefix} ({NACE_DESCRIPTIONS[prefix]})")
        if unique_prefixes:
            lines.append(f"| **NACE codes** | {', '.join(unique_prefixes[:5])} |")

    if enr.get("legal_form"):
        lines.append(f"| **Legal form** | {enr['legal_form']} |")
    ev_count = research.get("event_count", 0)
    if ev_count:
        lines.append(f"| **Events attending** | {ev_count} |")
    if is_client:
        lines.append(f"| **Autodesk status** | Existing client |")
        if enr.get("current_products"):
            lines.append(f"| **Current products** | {enr['current_products']} |")
        if enr.get("total_seats"):
            lines.append(f"| **Total seats** | {enr['total_seats']} |")
        if enr.get("current_acv_eur"):
            lines.append(f"| **Current ACV** | EUR {_format_number(enr['current_acv_eur'])} |")
        if enr.get("nearest_renewal"):
            lines.append(f"| **Next renewal** | {enr['nearest_renewal']} ({enr.get('days_to_renewal', '?')} days) |")
        if enr.get("reseller"):
            lines.append(f"| **Reseller** | {enr['reseller']} |")

    homepage_desc = website.get("homepage", {}).get("description", "")
    if homepage_desc:
        lines.append(f"| **Self-description** | {homepage_desc[:200]} |")
    lines.append("")

    # ── Website Intelligence ──
    about_text = website.get("about", {}).get("text", "")
    if about_text and len(about_text) > 50:
        lines.append("## Website Intelligence\n")
        excerpt = about_text[:1500]
        if len(about_text) > 1500:
            excerpt += "..."
        lines.append(f"> {excerpt}\n")
        source_url = website.get("about", {}).get("url", "")
        if source_url:
            lines.append(f"*Source: {source_url}*\n")

    # ── Recent Signals Timeline ──
    lines.append("## Recent Signals Timeline\n")
    signals_found = False

    if enr.get("hiring_signal"):
        signals_found = True
        total = enr.get("total_jobs", 0)
        eng = " (including engineering/technical roles)" if enr.get("engineering_hiring") else ""
        lines.append(f"- **Hiring activity:** {total} open positions detected{eng}.")
        lines.append(f"  - *Why it matters:* Growing teams need tools and standardized workflows on day one.")
        adsk_tools = enr.get("autodesk_tools_in_jobs", [])
        if adsk_tools:
            lines.append(f"  - **Autodesk tools mentioned in jobs:** {', '.join(adsk_tools)}")
        comp_tools = enr.get("competitor_tools_in_jobs", [])
        if comp_tools:
            lines.append(f"  - **Competitor tools mentioned in jobs:** {', '.join(comp_tools)}")

    if enr.get("leadership_change"):
        signals_found = True
        count = enr.get("leadership_changes_count", 0)
        lines.append(f"- **Leadership change:** {count} recent change(s) in statutory body detected.")
        lines.append(f"  - *Why it matters:* New leadership often triggers technology and process review.")

    if enr.get("has_public_contracts"):
        signals_found = True
        count = enr.get("smlouvy_contracts_count", enr.get("public_contracts_count", 0))
        value = enr.get("smlouvy_value_czk", enr.get("public_contracts_value_czk", 0))
        lines.append(f"- **Public contracts:** {count} contracts found (value: {_format_number(value)} CZK).")
        lines.append(f"  - *Why it matters:* Public procurement increasingly requires BIM and digital workflows.")

    if enr.get("revenue_growth") and enr["revenue_growth"] != 0:
        signals_found = True
        lines.append(f"- **Revenue growth:** {enr['revenue_growth']:+.1f}% year-over-year.")
        lines.append(f"  - *Why it matters:* {'Rapid growth strains existing design capacity and processes.' if enr['revenue_growth'] > 5 else 'Stable or declining revenue may increase focus on efficiency and cost savings.'}")

    if not signals_found:
        lines.append("- No significant signals detected from available data sources. Manual research recommended.")
    lines.append("")

    # ── Buying Committee ──
    lines.append("## Buying Committee\n")
    contact_list = contacts.get("contacts", [])
    if contact_list:
        lines.append("| Name | Title | Email | Phone | Hypothesized Role |")
        lines.append("|---|---|---|---|---|")
        for c in contact_list[:7]:
            fname = c.get("first_name", "")
            lname = c.get("last_name", "")
            full_name = f"{fname} {lname}".strip() or c.get("full_name", "")
            title = c.get("title", "")
            email = c.get("email", "") or "—"
            phone = c.get("phone", "") or "—"
            role = _hypothesize_role(title)
            lines.append(f"| {full_name} | {title} | {email} | {phone} | {role} |")
        lines.append("")
        lines.append(f"*Source: ZoomInfo ({contacts.get('contacts_found', 0)} contacts found)*\n")
    else:
        lines.append("No decision-maker contacts found via ZoomInfo. Consider:")
        lines.append("- Checking the company website contact/team page")
        lines.append("- LinkedIn search for key titles (CTO, Engineering Manager, BIM Manager)")
        lines.append("- Asking your reseller partner for existing relationships\n")

    # ── Tech Stack Evidence ──
    lines.append("## Tech Stack Evidence\n")
    if techno.get("success"):
        clean_adsk = [t for t in techno.get("autodesk_products", []) if t.lower() not in TECHNO_FALSE_POSITIVES]
        clean_comp = [t for t in techno.get("competitor_products", []) if t.lower() not in TECHNO_FALSE_POSITIVES]
        clean_other = [t for t in techno.get("cad_bim_other", []) if t.lower() not in TECHNO_FALSE_POSITIVES]

        if clean_adsk:
            lines.append("### Autodesk Products Detected\n")
            for tool in sorted(set(clean_adsk)):
                lines.append(f"- {tool}")
            lines.append("")

        if clean_comp:
            lines.append("### Competitor Products Detected\n")
            for tool in sorted(set(clean_comp)):
                lines.append(f"- {tool}")
            lines.append("")

        if clean_other:
            lines.append("### Other CAD/BIM/Engineering Tools\n")
            for tool in sorted(set(clean_other))[:15]:
                lines.append(f"- {tool}")
            lines.append("")

        lines.append(f"*Total technologies detected: {techno.get('total_technologies', 0)} (via ZoomInfo)*\n")
    else:
        lines.append("No technographics data available from ZoomInfo. This could mean:")
        lines.append("- Company is below ZoomInfo's coverage threshold")
        lines.append("- Czech subsidiary is not tracked separately from parent")
        lines.append("- Discovery call should explore current tool stack\n")

    # Job-based tool signals
    adsk_in_jobs = enr.get("autodesk_tools_in_jobs", [])
    comp_in_jobs = enr.get("competitor_tools_in_jobs", [])
    if adsk_in_jobs or comp_in_jobs:
        lines.append("### Tools Mentioned in Job Postings\n")
        if adsk_in_jobs:
            lines.append(f"- **Autodesk:** {', '.join(adsk_in_jobs)}")
        if comp_in_jobs:
            lines.append(f"- **Competitor:** {', '.join(comp_in_jobs)}")
        lines.append("")

    # ── Key Pain Points & Expansion Plays ──
    lines.append("## Key Pain Points & Autodesk Expansion Plays\n")
    plays = _select_plays(enr, techno)
    if plays:
        for idx, play in enumerate(plays, 1):
            lines.append(f"### Play {idx}: {play['name']}\n")
            lines.append(f"**Portfolio:** {play['portfolio']} | **Segment fit:** {play['segment']}\n")
            lines.append(f"**What to sell:** {play['what_to_sell']}\n")
            lines.append(f"**Why now:** {play['why_now']}\n")
            lines.append(f"**Value hypothesis:** {play['value_hypothesis']}\n")
            lines.append(f"**Primary sponsor persona:** {play['sponsor_title']}\n")
            lines.append(f"**Likely blockers:** {play['blockers']}\n")
            lines.append(f"**How to disarm:** {play['disarm']}\n")
            lines.append("**Discovery questions:**\n")
            for q in play["discovery_q"]:
                lines.append(f"- {q}")
            lines.append("")
    else:
        lines.append("No specific expansion plays matched this company's profile. Manual assessment recommended.\n")

    # ── Talk Tracks ──
    lines.append("## Talk Tracks\n")
    tracks = _build_talk_tracks(enr, techno, website)
    if tracks:
        for idx, track in enumerate(tracks, 1):
            lines.append(f'{idx}. "{track}"\n')
    else:
        lines.append("No talk tracks generated — insufficient data for contextual outreach angles.\n")

    # ── Manual Input Section ──
    lines.append("## Manual Input Section\n")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append("| **Known Autodesk tools with seat counts** | |")
    lines.append("| **Partner involved** | |")
    lines.append("| **Current opportunity notes** | |")
    lines.append("| **Internal deal notes** | |")
    lines.append("")

    # ── Source List ──
    lines.append("## Sources\n")
    lines.append(f"- ARES (Czech Business Registry) — ICO, NACE codes, legal form, address")
    lines.append(f"- ZoomInfo — Company firmographics, technographics, decision-maker contacts")
    if website.get("success"):
        lines.append(f"- Company website: {domain}")
    lines.append(f"- Jobs.cz — Hiring signals, tool mentions in job postings")
    lines.append(f"- Kurzy.cz — Czech financial data")
    lines.append(f"- OR (Obchodní rejstřík) — Leadership change detection")
    lines.append(f"- Smlouvy.gov.cz — Public contract registry")
    lines.append(f"\n*Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")

    return "\n".join(lines)


def _hypothesize_role(title: str) -> str:
    """Guess the decision role from a job title."""
    t = title.lower()
    if any(kw in t for kw in ["ceo", "cto", "managing director", "ředitel", "jednatel", "owner"]):
        return "Decision maker / Budget owner"
    if any(kw in t for kw in ["vp", "vice president", "director", "head of"]):
        return "Executive sponsor"
    if any(kw in t for kw in ["manager", "manažer", "vedoucí", "lead"]):
        return "Technical evaluator / Influencer"
    if any(kw in t for kw in ["engineer", "architect", "konstruktér", "projektant"]):
        return "End user / Champion"
    if any(kw in t for kw in ["it ", "admin", "správce"]):
        return "IT gatekeeper"
    return "To be determined"


def generate_all(top_n: Optional[int] = None, company_filter: str = ""):
    """Generate reports for all researched companies."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    research_files = sorted(RESEARCH_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not research_files:
        print("No research data found. Run deep_researcher.py first.")
        return

    generated = 0
    for path in research_files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                research = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        if company_filter and company_filter.lower() not in research.get("company_key", ""):
            continue

        report_md = generate_report(research)
        safe_name = re.sub(r'[^\w\-.]', '_', research.get("company_key", "unknown"))[:80]
        out_path = REPORTS_DIR / f"{safe_name}.md"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(report_md)

        generated += 1
        if top_n and generated >= top_n:
            break

    print(f"Generated {generated} reports in {REPORTS_DIR}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate deep research Markdown reports")
    parser.add_argument("--top", type=int, default=None, help="Generate for top N only")
    parser.add_argument("--company", default="", help="Filter by company key substring")
    args = parser.parse_args()

    generate_all(top_n=args.top, company_filter=args.company)


if __name__ == "__main__":
    main()
