"""Persona-based outreach engine for Autodesk sales.

Maps contact titles to buyer personas, then generates tailored outreach
materials (briefs, discovery questions, emails, LinkedIn messages) based
on persona type, industry segment, current Autodesk products, and
enrichment signals.

Usage:
    from scoring.persona_engine import classify_contact, generate_outreach

    persona = classify_contact("BIM Manager", "AEC")
    outreach = generate_outreach(persona, account_context)
"""

import re

# ─────────────────────────────────────────────────────────────────────
# Title → Persona mapping
# ─────────────────────────────────────────────────────────────────────

PERSONA_TITLE_PATTERNS = {
    "champion": [
        r"bim\s*(?:manager|koordin|manažer|specialist|lead)",
        r"cad\s*(?:manager|admin|koordin|manažer|specialist)",
        r"design\s*(?:tech|technology)\s*(?:manager|lead)",
        r"plm\s*(?:manager|admin)",
        r"cam\s*(?:manager|admin|programátor)",
        r"application\s*(?:engineer|specialist)",
        r"design\s*office",
        r"konstrukční\s*(?:kancelář|oddělení)",
        r"leitung\s*(?:konstruktion|design)",
    ],
    "economic_buyer": [
        r"(?:cto|chief\s*technology)",
        r"(?:cso|chief\s*sales)",
        r"(?:coo|chief\s*operating)",
        r"vp\s*(?:of\s*)?(?:engineering|design|operations|manufacturing)",
        r"(?:vice\s*president|vp).*(?:eng|tech|design|oper)",
        r"director\s*(?:of\s*)?(?:engineering|design|R&D|technology|výroby|projekce|commercial)",
        r"ředitel\s*(?:technic|výrob|projekc|vývoj|obchod)",
        r"technický\s*ředitel",
        r"obchodní\s*ředitel",
        r"executive\s*director",
        r"geschäftsführ",
        r"geschftsfhr",
        r"leitung\s*(?:einkauf|vertrieb|technik|produktion)",
    ],
    "technical_influencer": [
        r"it\s*(?:director|manager|ředitel|manažer|lead)",
        r"(?:head|vedoucí)\s*(?:of\s*)?it",
        r"plm\s*(?:manager|director|lead)",
        r"infrastructure\s*(?:manager|lead)",
        r"systems?\s*(?:admin|architect|engineer)",
        r"správce\s*(?:sítě|it|systém)",
        r"(?:head|vedoucí)\s*(?:of\s*)?(?:production|výrob)",
        r"supply\s*chain",
    ],
    "executive_sponsor": [
        r"\bceo\b",
        r"\bcoo\b",
        r"\bcfo\b",
        r"(?:generální|general)\s*(?:ředitel|director|manager)",
        r"jednatel",
        r"(?:managing|executive)\s*director",
        r"\bpresident\b",
        r"majitel|owner|spolumajitel",
        r"prokurista",
        r"předseda\s*(?:představenstva|správní)",
        r"chief\s*executive",
        r"chief\s*strategy",
        r"\bpartner\b",
        r"co-?founder|spoluzakladatel",
        r"board\s*member|člen\s*(?:představenstva|dozorčí)",
        r"výkonný\s*ředitel",
        r"\březitel\b$",
        r"ředitel\s*(?:divis|společnost|závod|pobočk|firmy)",
    ],
    "end_user_leader": [
        r"(?:vedoucí|vedouci|head|lead)\s*(?:projekt|design|konstruk|archit|výrob|středis|oddělení|obchod|zahranič|elektr|strojírn|provo|technick|kvality|nástrojárn|logist|údržb|expedic)",
        r"(?:engineering|design)\s*(?:manager|lead|vedoucí)",
        r"(?:lead|senior|hlavní)\s*(?:architect|engineer|konstruktér|projektant)",
        r"(?:team|group)\s*lead",
        r"hlavní\s*(?:inženýr|projektant|konstruktér|architekt)",
        r"mistr\b",
        r"(?:sales|area|project)\s*manager",
        r"manager.*(?:project|design|sales|production|commercial|r&d|research|quality|technical|information\s*tech)",
        r"stavební\s*(?:manažer|manager)",
        r"head\s*of\s*(?:oper|r&d|research|quality|production|manufactur)",
        r"manager,?\s*(?:research|r&d|quality|technical|information)",
    ],
}

PERSONA_LABELS = {
    "champion": "Technical Champion / Daily User Advocate",
    "economic_buyer": "Economic Buyer / Budget Holder",
    "technical_influencer": "Technical Influencer / IT Decision Maker",
    "executive_sponsor": "Executive Sponsor / Business Case Approver",
    "end_user_leader": "End User Leader / Team Productivity Owner",
}

# ─────────────────────────────────────────────────────────────────────
# Persona × Segment templates
# ─────────────────────────────────────────────────────────────────────

PERSONA_TEMPLATES = {
    ("champion", "AEC"): {
        "cares_about": [
            "BIM compliance and mandates",
            "Team productivity and collaboration",
            "Design quality and coordination across disciplines",
            "Clash detection and model accuracy",
        ],
        "pain_points": [
            "Managing multiple disconnected tools across disciplines",
            "Clash detection delays slowing project delivery",
            "Manual coordination between Revit/AutoCAD users",
            "Version control chaos across project teams",
        ],
        "kpis": [
            "RFI reduction", "Clash resolution time",
            "Model quality scores", "Design cycle time",
        ],
        "talk_track": (
            "How are you managing coordination between your {product_count} "
            "Autodesk tools today? Are you doing clash detection manually, "
            "or do you have a cloud-based workflow in place?"
        ),
        "discovery_questions": [
            "How many people on your team use BIM tools daily vs occasionally?",
            "What's your current process for design review and coordination?",
            "Are you working on any projects with BIM mandates from clients?",
            "How do you handle model sharing with external partners?",
        ],
        "value_prop": (
            "Consolidate your {current_products} into AEC Collection + "
            "BIM Collaborate Pro for seamless cloud coordination"
        ),
    },
    ("champion", "D&M"): {
        "cares_about": [
            "Design iteration speed",
            "Manufacturing readiness of designs",
            "Generative design and simulation",
            "CAD-to-CAM workflow efficiency",
        ],
        "pain_points": [
            "Slow design-to-manufacturing handoff",
            "Tool-switching between design, simulation, and CAM",
            "Data silos between engineering and shop floor",
            "Maintaining BOMs across systems",
        ],
        "kpis": [
            "Time to first prototype", "Design iteration cycles",
            "Scrap rate reduction", "Engineering change order volume",
        ],
        "talk_track": (
            "I noticed you're using {current_products}. How's the handoff "
            "from design to manufacturing working? Are your teams sharing "
            "a single source of truth, or moving files between systems?"
        ),
        "discovery_questions": [
            "How many design iterations does a typical product go through?",
            "Are you doing any simulation in-house, or outsourcing it?",
            "What's your current CAM workflow -- are designers preparing models for CNC?",
            "How do you manage your Bill of Materials today?",
        ],
        "value_prop": (
            "Connect your {current_products} with Fusion for integrated "
            "design, simulation, and CAM in a single cloud platform"
        ),
    },
    ("champion", "M&E"): {
        "cares_about": [
            "Render quality and speed",
            "Pipeline efficiency and asset management",
            "Real-time collaboration on scenes",
            "Tool interoperability (Maya/3ds Max/Nuke pipeline)",
        ],
        "pain_points": [
            "Long render times blocking creative iteration",
            "Asset versioning across production stages",
            "Licensing bottlenecks during peak production",
        ],
        "kpis": [
            "Render turnaround time", "Asset reuse rate",
            "Production throughput", "Deadline hit rate",
        ],
        "talk_track": (
            "How are you managing your {product_count}-tool pipeline? "
            "Are artists sharing assets through a centralized system or "
            "passing files manually?"
        ),
        "discovery_questions": [
            "What does your rendering pipeline look like end-to-end?",
            "How do you handle asset versioning across departments?",
            "Are you seeing licensing bottlenecks during crunch periods?",
        ],
        "value_prop": (
            "Streamline your pipeline with M&E Collection for Maya, "
            "3ds Max, and ShotGrid integration"
        ),
    },
    ("economic_buyer", "AEC"): {
        "cares_about": [
            "Project profitability and cost control",
            "Enterprise licensing efficiency",
            "Digital transformation ROI",
            "Competitive differentiation",
        ],
        "pain_points": [
            "Overspending on point solutions that don't integrate",
            "Hard to quantify ROI of BIM investment",
            "Talent retention -- engineers want modern tools",
            "Client demands for digital deliverables increasing",
        ],
        "kpis": [
            "Cost per project", "License utilization rate",
            "Win rate on BIM-mandate projects", "Revenue per employee",
        ],
        "talk_track": (
            "Your team is on {current_products} today. Many firms your "
            "size are consolidating to Collections to save 30-40%% on "
            "per-seat licensing while giving teams access to the full suite."
        ),
        "discovery_questions": [
            "How do you currently manage your software licensing costs?",
            "Are your clients increasingly requiring BIM deliverables?",
            "What's your biggest challenge in scaling your engineering capacity?",
        ],
        "value_prop": (
            "Consolidating {seat_count} individual subscriptions into "
            "AEC Collection could save up to 40%% while unlocking "
            "cloud collaboration tools"
        ),
    },
    ("economic_buyer", "D&M"): {
        "cares_about": [
            "Time-to-market", "Engineering productivity",
            "Total cost of ownership for engineering tools",
            "Competitive pressure from faster rivals",
        ],
        "pain_points": [
            "Fragmented toolchain increasing overhead",
            "Difficulty hiring engineers who know your legacy tools",
            "Manufacturing errors from disconnected design-to-shop processes",
        ],
        "kpis": [
            "Time-to-market", "Engineering cost per product",
            "First-pass yield", "Tool ROI",
        ],
        "talk_track": (
            "I've been working with similar manufacturers in Czech Republic "
            "and the consistent theme is that teams using integrated "
            "design-to-manufacturing platforms ship 2-3x faster."
        ),
        "discovery_questions": [
            "What's your biggest bottleneck in getting products to market?",
            "How are you measuring engineering productivity today?",
            "Are you exploring Industry 4.0 or digital twin initiatives?",
        ],
        "value_prop": (
            "Unify design, simulation, and manufacturing on Fusion "
            "to reduce time-to-market and lower total engineering cost"
        ),
    },
    ("economic_buyer", "M&E"): {
        "cares_about": [
            "Production cost efficiency",
            "Studio competitiveness for international projects",
            "Scalable licensing during production peaks",
        ],
        "pain_points": [
            "Variable production demand making license planning hard",
            "International clients requiring specific tool compatibility",
        ],
        "kpis": [
            "Cost per frame/shot", "License utilization during peaks",
            "Project profitability",
        ],
        "talk_track": (
            "Studios your size often benefit from Flex tokens to handle "
            "production peaks without over-committing on annual seats."
        ),
        "discovery_questions": [
            "How do you handle licensing during production peaks?",
            "Are you competing for international projects that require specific tool capabilities?",
        ],
        "value_prop": (
            "M&E Collection with Flex licensing gives your team "
            "access to the full pipeline while scaling with demand"
        ),
    },
    ("technical_influencer", "AEC"): {
        "cares_about": [
            "Deployment simplicity", "License management",
            "Data security and compliance", "Cloud vs on-prem strategy",
        ],
        "pain_points": [
            "Managing updates across dozens of seats",
            "Shadow IT -- teams installing unauthorized tools",
            "Cloud security concerns for project data",
        ],
        "kpis": [
            "Uptime / availability", "License compliance",
            "Deployment time per update", "Support ticket volume",
        ],
        "talk_track": (
            "How are you managing deployments for your {seat_count} "
            "Autodesk seats? Many IT teams find the named-user model "
            "eliminates license server headaches entirely."
        ),
        "discovery_questions": [
            "What's your current deployment and update strategy for Autodesk tools?",
            "Are you managing any license servers today?",
            "What's your position on cloud-based tools for engineering data?",
        ],
        "value_prop": (
            "Named-user licensing + Autodesk Account simplifies "
            "deployment and eliminates license server management"
        ),
    },
    ("technical_influencer", "D&M"): {
        "cares_about": [
            "System integration", "PDM/PLM data management",
            "ERP connectivity", "Security and IP protection",
        ],
        "pain_points": [
            "Integrating CAD data with ERP/MRP systems",
            "Version control across multiple CAD formats",
            "IP protection for design files",
        ],
        "kpis": [
            "System integration reliability", "Data breach incidents",
            "PDM adoption rate",
        ],
        "talk_track": (
            "How is your engineering data flowing into your ERP today? "
            "Many manufacturers find Vault or Fusion Manage bridges "
            "that gap effectively."
        ),
        "discovery_questions": [
            "What PDM/PLM system are you using to manage design data?",
            "How does engineering data get into your ERP/MRP system?",
            "Are you dealing with multiple CAD formats from suppliers?",
        ],
        "value_prop": (
            "Vault or Fusion Manage connects your engineering data "
            "to ERP while protecting intellectual property"
        ),
    },
}


def classify_contact(title: str, segment: str = "") -> str:
    """Map a job title to a persona type.

    Returns one of: champion, economic_buyer, technical_influencer,
    executive_sponsor, end_user_leader, or 'unknown'.
    """
    if not title:
        return "unknown"

    title_lower = title.lower().strip()

    for persona_type, patterns in PERSONA_TITLE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, title_lower, re.IGNORECASE):
                return persona_type

    return "unknown"


def get_persona_template(persona_type: str, segment: str) -> dict:
    """Get the outreach template for a persona x segment combination.

    Falls back to the most common segment match if exact combo not found.
    """
    key = (persona_type, segment)
    if key in PERSONA_TEMPLATES:
        return PERSONA_TEMPLATES[key]

    for seg in ["AEC", "D&M", "M&E"]:
        fallback_key = (persona_type, seg)
        if fallback_key in PERSONA_TEMPLATES:
            return PERSONA_TEMPLATES[fallback_key]

    return {
        "cares_about": ["Business growth", "Operational efficiency"],
        "pain_points": ["Fragmented toolchain", "Scalability challenges"],
        "kpis": ["Productivity", "Cost efficiency"],
        "talk_track": (
            "I'd love to understand how your team is using "
            "{current_products} today and where you see the biggest "
            "opportunities for improvement."
        ),
        "discovery_questions": [
            "What are your biggest technology challenges this year?",
            "How do you evaluate new tools or platforms?",
            "What does your decision-making process look like for engineering tools?",
        ],
        "value_prop": "Autodesk solutions to streamline your workflows",
    }


def generate_outreach(
    contact: dict,
    account: dict,
    enrichment: dict,
) -> dict:
    """Generate personalized outreach for a specific contact at an account.

    Args:
        contact: Dict with first_name, last_name, title, persona_type.
        account: Dict with company_name, primary_segment, products, etc.
        enrichment: Dict with enrichment data (hiring, revenue, etc.).

    Returns:
        Dict with email_draft, linkedin_message, discovery_questions, brief.
    """
    persona_type = contact.get("persona_type", "unknown")
    segment = account.get("primary_segment", "AEC")
    template = get_persona_template(persona_type, segment)

    context = _build_context(contact, account, enrichment)

    email = _generate_email(contact, account, template, context)
    linkedin = _generate_linkedin(contact, account, template, context)
    brief = _generate_brief(contact, account, template, context)
    questions = _personalize_questions(template, context)

    return {
        "persona_type": persona_type,
        "persona_label": PERSONA_LABELS.get(persona_type, persona_type),
        "email_draft": email,
        "linkedin_message": linkedin,
        "brief": brief,
        "discovery_questions": questions,
        "talk_track": _fill_template(template.get("talk_track", ""), context),
        "pain_points": template.get("pain_points", []),
        "kpis": template.get("kpis", []),
    }


def _build_context(contact: dict, account: dict, enrichment: dict) -> dict:
    """Build template variable context from all available data."""
    products = account.get("products", [])
    if isinstance(products, str):
        products = [p.strip() for p in products.split(",")]

    signals = []
    if enrichment.get("hiring_signal"):
        total = enrichment.get("total_jobs", 0)
        signals.append(f"actively hiring ({total} open positions)")
    if enrichment.get("revenue_growth") and enrichment["revenue_growth"] > 10:
        signals.append(f"strong revenue growth ({enrichment['revenue_growth']:.0f}%)")
    if enrichment.get("autodesk_tools_in_jobs"):
        tools = enrichment.get("autodesk_tools_in_jobs", [])
        if isinstance(tools, list):
            signals.append(f"hiring for {', '.join(tools[:3])} roles")
    if enrichment.get("competitor_tools_in_jobs"):
        tools = enrichment.get("competitor_tools_in_jobs", [])
        if isinstance(tools, list):
            signals.append(f"also using {', '.join(tools[:3])}")

    return {
        "first_name": contact.get("first_name", ""),
        "last_name": contact.get("last_name", ""),
        "title": contact.get("title", ""),
        "company_name": account.get("company_name", "the company"),
        "current_products": ", ".join(products[:5]) if products else "Autodesk tools",
        "product_count": str(len(products)),
        "seat_count": str(account.get("total_seats", "several")),
        "segment": account.get("primary_segment", ""),
        "signals": signals,
        "employee_count": str(enrichment.get("employee_count", "")),
        "renewal_date": account.get("next_renewal", ""),
    }


def _fill_template(template_str: str, context: dict) -> str:
    """Fill {placeholders} in a template string with context values."""
    try:
        return template_str.format(**context)
    except (KeyError, IndexError):
        for key, val in context.items():
            template_str = template_str.replace("{" + key + "}", str(val))
        return template_str


def _generate_email(
    contact: dict, account: dict, template: dict, context: dict
) -> str:
    """Generate a personalized email draft."""
    name = contact.get("first_name") or contact.get("last_name") or ""
    company = account.get("company_name", "")
    signals = context.get("signals", [])

    signal_line = ""
    if signals:
        signal_line = (
            f"\n\nI noticed {company} is {signals[0]}, which is "
            "typically when teams benefit most from re-evaluating "
            "their design technology stack."
        )

    pain = template.get("pain_points", ["workflow inefficiency"])
    vp = _fill_template(template.get("value_prop", ""), context)

    email = f"""Subject: {company} + Autodesk -- quick question about your {context.get('segment', '')} workflow

Hi {name},

I work with {context.get('segment', '')} teams across Czech Republic and noticed your team at {company} is using {context.get('current_products', 'Autodesk tools')}.{signal_line}

One pattern I see with similar companies is {pain[0].lower() if pain else 'workflow fragmentation'} -- and the most successful teams are addressing it by {vp.lower() if vp else 'consolidating their toolchain'}.

Would you be open to a 15-minute call to explore whether this is relevant for your team?

Best regards,
Martin"""

    return email.strip()


def _generate_linkedin(
    contact: dict, account: dict, template: dict, context: dict
) -> str:
    """Generate a short LinkedIn connection message."""
    name = contact.get("first_name") or contact.get("last_name") or ""
    company = account.get("company_name", "")

    msg = (
        f"Hi {name}, I help {context.get('segment', '')} teams in Czech Republic "
        f"get more from their Autodesk tools. I saw that {company} is using "
        f"{context.get('current_products', 'Autodesk')} and thought it'd be "
        "worth connecting -- I often share insights on how similar teams "
        "are improving their workflows. Happy to connect!"
    )
    return msg.strip()


def _generate_brief(
    contact: dict, account: dict, template: dict, context: dict
) -> str:
    """Generate a pre-call briefing for the AE."""
    signals = context.get("signals", [])
    pain = template.get("pain_points", [])
    kpis = template.get("kpis", [])
    persona_label = PERSONA_LABELS.get(
        contact.get("persona_type", ""), "Stakeholder"
    )

    brief = f"""ACCOUNT BRIEF: {account.get('company_name', '')}
CONTACT: {contact.get('first_name', '')} {contact.get('last_name', '')} ({contact.get('title', '')})
PERSONA: {persona_label}
SEGMENT: {context.get('segment', '')}
CURRENT PRODUCTS: {context.get('current_products', '')}
SEATS: {context.get('seat_count', '')}
EMPLOYEES: {context.get('employee_count', 'unknown')}

SIGNALS:
{chr(10).join('  - ' + s for s in signals) if signals else '  - No recent signals detected'}

LIKELY PAIN POINTS:
{chr(10).join('  - ' + p for p in pain[:4])}

KPIs THEY CARE ABOUT:
{chr(10).join('  - ' + k for k in kpis[:4])}

TALK TRACK:
  {_fill_template(template.get('talk_track', ''), context)}

VALUE PROPOSITION:
  {_fill_template(template.get('value_prop', ''), context)}"""

    return brief.strip()


def _personalize_questions(template: dict, context: dict) -> list:
    """Return personalized discovery questions."""
    raw = template.get("discovery_questions", [])
    return [_fill_template(q, context) for q in raw]


def generate_account_outreach(
    account: dict, enrichment: dict
) -> list:
    """Generate outreach for all contacts at an account.

    Returns list of outreach dicts, one per contact.
    """
    contacts = enrichment.get("contacts", [])
    if not contacts:
        return []

    results = []
    for contact in contacts:
        outreach = generate_outreach(contact, account, enrichment)
        outreach["contact"] = contact
        results.append(outreach)

    return results
