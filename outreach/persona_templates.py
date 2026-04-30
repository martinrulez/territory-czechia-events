"""Persona-aware outreach templates for Autodesk CZ territory.

Maps contact titles to persona categories, each with:
- Pain points specific to that role
- Relevant Autodesk products
- Industry-specific hooks
- Signal-triggered conversation starters
- Event-based openers

Used by the Outreach Composer to build highly targeted, data-backed messages.
"""

from __future__ import annotations

PERSONAS = {
    "executive": {
        "label": "Executive / C-Suite",
        "titles": [
            "ceo", "chief executive", "chief operating", "coo",
            "chief technology", "cto", "chief digital", "cdo",
            "chief information", "cio", "chief financial", "cfo",
            "managing director", "general manager", "owner", "founder",
            "jednatel", "výkonný ředitel", "generální ředitel",
            "ředitel společnosti",
        ],
        "pain_points": [
            "Digital transformation ROI and total cost of ownership",
            "Competitive pressure driving need for faster innovation",
            "Operational efficiency gaps between design and delivery",
            "Talent retention through modern tooling and workflows",
            "Risk of vendor lock-in vs. platform consolidation benefits",
            "Growth strategy alignment with technology investments",
        ],
        "products": ["AEC Collection", "PDMC", "Fusion", "Autodesk Platform", "ACC Build"],
        "industry_hooks": {
            "AEC": [
                "Czech AEC leaders report 15-25% productivity gains from platform consolidation",
                "BIM mandates (NDA 40/2025) require strategic technology investments",
                "Firms investing in digital construction see 30% higher win rates on public tenders",
            ],
            "D&M": [
                "Czech manufacturers investing in Fusion see 40% faster time-to-market",
                "Platform consolidation reduces total software TCO by 20-30%",
                "Industry 4.0 readiness is a board-level priority across Czech manufacturing",
            ],
        },
        "signal_hooks": {
            "hiring": "Your company is growing. Fast-scaling teams need a technology foundation that scales with them.",
            "leadership_change": "With new leadership comes an opportunity to evaluate your technology strategy.",
            "public_contracts": "Public contract wins create a perfect moment to invest in scalable design-to-delivery workflows.",
            "competitor_tools": "I noticed your teams use {tools}. We help companies like yours consolidate and reduce TCO.",
        },
        "event_hook": "I see your company will be at {event}. I'd welcome 15 minutes to discuss how Autodesk supports Czech firms' digital strategy.",
    },
    "bim": {
        "label": "BIM / Design Technology",
        "titles": [
            "bim manager", "bim director", "bim coordinator",
            "design technology manager", "design technology director",
            "cad manager", "cad administrator",
        ],
        "pain_points": [
            "Interoperability between design tools and formats (IFC, RVT, DWG)",
            "Collaboration across distributed project teams",
            "BIM mandate compliance (ISO 19650, local regulations)",
            "Model coordination and clash detection efficiency",
            "Standardizing BIM workflows across multiple projects",
            "Data handover from design to construction",
        ],
        "products": ["Revit", "BIM Collaborate Pro", "Navisworks", "ACC Build", "Civil 3D", "AEC Collection"],
        "industry_hooks": {
            "AEC": [
                "BIM mandates are tightening across Europe --- Czech NDA 40/2025 requires BIM for public projects above CZK 50M",
                "Coordination errors cost 5-8% of project budgets on average",
                "Teams using integrated BIM workflows report 30% fewer RFIs",
                "Prefabrication driven by BIM reduces on-site construction time by 20%",
            ],
            "D&M": [
                "Manufacturing firms expanding into construction technology need BIM capabilities",
                "Digital twin integration between factory floor and building design",
            ],
        },
        "signal_hooks": {
            "hiring": "I noticed you're growing your design technology team --- that's usually a sign you're scaling up BIM workflows.",
            "leadership_change": "With new leadership comes new opportunities to revisit your design technology stack.",
            "public_contracts": "Congratulations on the public contract wins. BIM compliance requirements for public projects are a natural fit.",
            "competitor_tools": "I see your team uses {tools} --- many Czech firms are consolidating to Autodesk for better interoperability.",
        },
        "event_hook": "I see your team will be at {event}. We're also attending and I'd love to discuss how BIM workflows are evolving for Czech construction firms.",
    },
    "engineering": {
        "label": "Engineering / R&D Leadership",
        "titles": [
            "engineering manager", "director of engineering", "vp engineering",
            "head of r&d", "director of r&d", "r&d manager",
            "product engineering manager", "design engineering manager",
        ],
        "pain_points": [
            "Slow design iteration cycles limiting time-to-market",
            "Disconnected design-to-manufacturing handoff causing rework",
            "Managing complex multi-component assemblies",
            "Adopting generative design for weight/cost optimization",
            "PLM/PDM integration with CAD environment",
            "Simulation bottlenecks delaying validation",
        ],
        "products": ["Inventor", "Fusion", "Vault", "PDMC", "Moldflow", "Fusion Sim Ext"],
        "industry_hooks": {
            "D&M": [
                "Companies using generative design report 50% fewer prototyping cycles",
                "Integrated CAD-to-CAM workflows eliminate manual translation errors",
                "Cloud collaboration reduces design review time by 40%",
                "Czech automotive suppliers using Inventor + Vault see 25% faster release cycles",
            ],
            "AEC": [
                "Structural engineering firms benefit from Inventor's steel detailing capabilities",
                "Prefab component design in Inventor integrates directly with Revit projects",
            ],
        },
        "signal_hooks": {
            "hiring": "Your team is hiring {count} engineering roles --- that's a lot of new seats that need design tools.",
            "competitor_tools": "I noticed job postings mentioning {tools}. Engineers switching from SolidWorks to Inventor typically gain 30% in assembly performance.",
            "leadership_change": "New engineering leadership often reassesses the design toolchain. Happy to share what's changed in the Autodesk ecosystem.",
        },
        "event_hook": "Your engineering team is attending {event} --- I'd love to show you what generative design and cloud simulation can do for Czech manufacturers.",
    },
    "cto": {
        "label": "CTO / IT Leadership",
        "titles": [
            "cto", "chief technology officer", "cio", "chief information officer",
            "it director", "it manager", "vp of technology",
            "head of digital transformation", "digital transformation director",
            "director of it",
        ],
        "pain_points": [
            "ROI justification for design software investments",
            "Digital transformation roadmap and execution",
            "Legacy system modernization and cloud migration",
            "Data security and compliance in cloud adoption",
            "Vendor consolidation to reduce licensing complexity",
            "Enabling remote/hybrid work for technical teams",
        ],
        "products": ["AEC Collection", "PDMC", "Autodesk Platform Services", "Forma", "Flex"],
        "industry_hooks": {
            "AEC": [
                "Consolidating to the AEC Collection reduces licensing costs by 20-30% vs individual products",
                "Cloud-native workflows enable remote teams without VPN overhead",
                "Autodesk Flex tokens provide usage-based access across the full portfolio",
            ],
            "D&M": [
                "PDMC consolidation simplifies procurement and support for D&M teams",
                "Fusion cloud capabilities eliminate local compute bottlenecks",
                "Centralized Vault deployment reduces IT support tickets by 40%",
            ],
        },
        "signal_hooks": {
            "hiring": "Growing headcount means growing license needs --- Flex tokens give you elastic capacity.",
            "leadership_change": "Technology transitions are a natural moment to evaluate vendor strategy. Let me share what other Czech CTOs are doing.",
        },
        "event_hook": "I see {company} is represented at {event}. Would be great to connect and discuss your technology roadmap.",
    },
    "manufacturing": {
        "label": "Manufacturing / Operations",
        "titles": [
            "manufacturing engineering manager", "vp manufacturing",
            "director of operations", "vp of operations",
            "operations manager", "production manager",
            "plm manager", "pdm manager",
        ],
        "pain_points": [
            "Shop floor to design feedback loops are too slow",
            "Production planning accuracy and waste reduction",
            "Quality control integration with design data",
            "CNC programming efficiency and machine utilization",
            "Supply chain digitization and part traceability",
        ],
        "products": ["Fusion", "Inventor", "Vault", "Fusion Mfg Ext", "PowerMill"],
        "industry_hooks": {
            "D&M": [
                "Integrated CAD/CAM in Fusion reduces programming time by 50%",
                "Digital twins of manufacturing processes cut downtime by 25%",
                "Automated design validation prevents costly production errors",
                "Czech manufacturers using Fusion report 35% faster CNC setup",
            ],
        },
        "signal_hooks": {
            "hiring": "You're hiring production staff --- scaling up usually surfaces design-to-manufacturing pain points we can help with.",
            "competitor_tools": "Your job postings mention {tools}. Many Czech manufacturers are moving to Fusion for integrated CAD/CAM.",
        },
        "event_hook": "MSV Brno / {event} is a great place to see Fusion's manufacturing capabilities in action. Let me know if you'd like a private demo.",
    },
    "construction": {
        "label": "Construction Technology",
        "titles": [
            "construction technology manager", "head of digital construction",
            "construction manager", "project manager",
            "director of construction", "site manager",
        ],
        "pain_points": [
            "Project cost overruns from coordination failures",
            "Schedule delays from RFIs and change orders",
            "Field-to-office data gaps and manual reporting",
            "Subcontractor coordination and accountability",
            "Safety compliance documentation burden",
        ],
        "products": ["ACC Build", "ACC Docs", "BIM Collaborate Pro", "Navisworks", "Civil 3D"],
        "industry_hooks": {
            "AEC": [
                "Construction Cloud users report 75% fewer document-related delays",
                "Automated issue tracking reduces rework by 30%",
                "Real-time field data improves decision speed by 50%",
                "Czech construction firms using ACC report 20% fewer site incidents",
            ],
        },
        "signal_hooks": {
            "public_contracts": "Public project requirements increasingly mandate digital construction workflows. ACC Build makes compliance straightforward.",
            "hiring": "Growing your project team? ACC scales without per-user infrastructure.",
        },
        "event_hook": "I see your team will be at {event}. Czech construction is digitizing fast --- I'd love to share how ACC is helping firms like yours.",
    },
    "media": {
        "label": "Media & Entertainment",
        "titles": [
            "vfx supervisor", "head of production", "technical director",
            "studio manager", "pipeline director", "head of 3d",
            "director of animation", "lead artist",
        ],
        "pain_points": [
            "Render pipeline efficiency and cost management",
            "Asset management across multiple productions",
            "Real-time collaboration on large scenes",
            "Pipeline tool integration and maintenance",
            "Talent shortage and rapid onboarding needs",
        ],
        "products": ["Maya", "3ds Max", "Arnold", "ShotGrid", "Flame"],
        "industry_hooks": {
            "M&E": [
                "Cloud rendering scales capacity without hardware investment",
                "Integrated pipeline tools reduce production overhead by 35%",
                "USD workflows enable cross-studio collaboration",
                "Czech game studios using Maya + ShotGrid cut milestone delivery by 20%",
            ],
        },
        "signal_hooks": {
            "hiring": "You're ramping up your team --- ShotGrid helps onboard new artists quickly with standardized pipelines.",
        },
        "event_hook": "Game Developers Session / {event} is coming up --- let's talk about how Czech studios are scaling with cloud workflows.",
    },
}


def match_persona(title):
    """Match a contact title to a persona key. Returns None if no match."""
    if not title:
        return None
    title_lower = title.lower()
    for key, persona in PERSONAS.items():
        for t in persona["titles"]:
            if t in title_lower:
                return key
    return None


def get_persona(title):
    """Get the full persona dict for a given title. Returns None if no match."""
    key = match_persona(title)
    if key:
        return PERSONAS[key]
    return None


def build_signal_hooks(persona_key, enrichment):
    """Build signal-triggered hooks based on available enrichment data."""
    persona = PERSONAS.get(persona_key)
    if not persona:
        return []

    hooks = []
    signal_templates = persona.get("signal_hooks", {})

    if enrichment.get("hiring_signal") and "hiring" in signal_templates:
        hook = signal_templates["hiring"]
        count = enrichment.get("total_jobs", 0)
        hooks.append(hook.replace("{count}", str(count)))

    if enrichment.get("leadership_change") and "leadership_change" in signal_templates:
        hooks.append(signal_templates["leadership_change"])

    if enrichment.get("has_public_contracts") and "public_contracts" in signal_templates:
        hooks.append(signal_templates["public_contracts"])

    comp_tools = enrichment.get("competitor_tools_in_jobs", [])
    if comp_tools and "competitor_tools" in signal_templates:
        hook = signal_templates["competitor_tools"]
        tools_str = ", ".join(comp_tools[:3])
        hooks.append(hook.replace("{tools}", tools_str))

    return hooks


def build_event_hook(persona_key, event_name, company_name="your team"):
    """Build an event-specific conversation opener."""
    persona = PERSONAS.get(persona_key)
    if not persona or not persona.get("event_hook"):
        return None
    return persona["event_hook"].replace("{event}", event_name).replace("{company}", company_name)


def get_industry_hooks(persona_key, segment):
    """Get industry-specific hooks for a persona + segment combination."""
    persona = PERSONAS.get(persona_key)
    if not persona:
        return []
    hooks = persona.get("industry_hooks", {})
    return hooks.get(segment, hooks.get("AEC", hooks.get("D&M", [])))
