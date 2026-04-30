"""Auto-generate structured intelligence briefs for all accounts.

Tier 1: Synthesizes all enrichment data into a per-account brief
without requiring an external LLM. Purely data-driven.

Each brief contains:
- Executive snapshot
- Outreach readiness assessment
- Persona-upsell fit analysis
- Tech stack evidence
- Growth & timing signals
- Recommended talk tracks / discovery questions
- Contact roster with reach info
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scoring.contact_enricher import UPSELL_PERSONA_MAP

WORKSPACE = Path(__file__).resolve().parent.parent.parent
ENRICHMENT_PATH = str(
    Path(__file__).resolve().parent.parent / "enrichment_data" / "account_enrichment.json"
)
TABLE_DATA_PATH = str(WORKSPACE / "table_data.json")
BRIEFS_PATH = str(WORKSPACE / "account_briefs.json")


SEGMENT_DESCRIPTIONS = {
    "D&M": "Design & Manufacturing",
    "AEC": "Architecture, Engineering & Construction",
    "M&E": "Media & Entertainment",
}

SEGMENT_DESCRIPTIONS_SK = {
    "D&M": "Dizajn a Výroba",
    "AEC": "Architektúra, Inžinierstvo a Stavebníctvo",
    "M&E": "Médiá a Zábava",
}

MATURITY_DESCRIPTIONS = {
    "Entry": "Early-stage Autodesk user with basic tools — strong expansion potential",
    "Expanding": "Growing product footprint — ready for deeper workflow adoption",
    "Established": "Mature deployment — focus on upsell to collections/cloud",
    "Strategic": "Deep investment — target advanced cloud, platform, and Flex plays",
}

MATURITY_DESCRIPTIONS_SK = {
    "Entry": "Počiatočný používateľ Autodesku so základnými nástrojmi — silný potenciál expanzie",
    "Expanding": "Rastúci produktový footprint — pripravený na hlbšiu adopciu workflow",
    "Established": "Vyspelé nasadenie — zamerať sa na upsell do kolekcií/cloud",
    "Strategic": "Hlboká investícia — cieliť pokročilé cloud, platformové a Flex riešenia",
}

DISCOVERY_QUESTIONS = {
    "Fusion Mfg Ext": [
        "How do your design engineers hand off models to manufacturing today?",
        "What percentage of your CNC programs are generated directly from 3D models vs. manually?",
        "How do you manage toolpath libraries and machining strategies across projects?",
        "Have you looked at AI-assisted toolpath generation or automated machining in Fusion 2026?",
    ],
    "Fusion Sim Ext": [
        "At what stage of the design process do you run simulation or FEA today?",
        "How are you validating structural integrity before prototyping?",
        "Have you explored generative design to optimize weight/material usage?",
    ],
    "AEC Collection": [
        "How are your teams coordinating BIM models across disciplines today?",
        "What's your current approach to clash detection before construction starts?",
        "Are you seeing pressure from clients or regulators to deliver in BIM/IFC?",
        "Have you tried the GPU-accelerated views in Revit 2026? Large models are now significantly smoother.",
    ],
    "PDMC": [
        "How do you manage design revisions and BOMs across your engineering team?",
        "What happens when a design change needs to propagate to production?",
        "How are you tracking which version of a design is in production right now?",
        "Are you aware the PDMC saves roughly EUR 1,985/year vs buying Inventor + AutoCAD + Fusion separately?",
    ],
    "BIM Collaborate Pro": [
        "How do external project partners access and review your BIM models?",
        "What's your biggest bottleneck in design review and approval cycles?",
        "How do you track issues and RFIs across project stakeholders?",
    ],
    "Forma": [
        "How early in the design process do you evaluate environmental factors like daylight and wind?",
        "What tools do your architects use for early-stage massing and site analysis?",
        "Have you seen Forma Building Design? It lets architects explore massing options with real-time carbon and daylight analysis before opening Revit.",
    ],
    "AutoCAD (full)": [
        "Are your teams still primarily working in 2D, or are you transitioning to 3D workflows?",
        "How do you manage drawing standards and template libraries across the team?",
        "AutoCAD 2026 opens files up to 11x faster and starts 4x quicker — how much time does your team lose on file load times today?",
    ],
    "Inventor": [
        "How do your engineers currently hand off Inventor models for manufacturing or simulation?",
        "Are you using Vault for revision control, or managing files manually?",
        "Have you explored the Inventor-to-Fusion bridge for cloud-based simulation and generative design?",
    ],
    "Fusion": [
        "How are you using Fusion today — primarily CAD, CAM, or both?",
        "Have you tried Autodesk Assistant in Fusion 2026? It can generate 3D geometry from text prompts.",
        "Are you using the cloud BOM collaboration features for multi-team design reviews?",
    ],
    "M&E Collection": [
        "What does your current content creation pipeline look like?",
        "How are you handling rendering and visualization workloads?",
        "Have you seen Wonder 3D? It generates 3D assets from text prompts for rapid previsualization.",
    ],
}

DISCOVERY_QUESTIONS_SK = {
    "Fusion Mfg Ext": [
        "Ako dnes vaši konštruktéri odovzdávajú modely do výroby?",
        "Koľko percent CNC programov sa generuje priamo z 3D modelov vs. manuálne?",
        "Ako spravujete knižnice nástrojových dráh a obrábacie stratégie naprieč projektmi?",
        "Pozreli ste sa na AI-asistované generovanie nástrojových dráh vo Fusion 2026?",
    ],
    "Fusion Sim Ext": [
        "V akej fáze návrhu dnes robíte simulácie alebo FEA?",
        "Ako validujete štrukturálnu integritu pred prototypovaním?",
        "Skúšali ste generatívny dizajn na optimalizáciu hmotnosti/materiálu?",
    ],
    "AEC Collection": [
        "Ako dnes vaše tímy koordinujú BIM modely naprieč disciplínami?",
        "Aký je váš súčasný prístup k detekcii kolízií pred začiatkom stavby?",
        "Cítite tlak od klientov alebo regulátorov na dodanie v BIM/IFC?",
        "Skúšali ste GPU-akcelerované zobrazenia v Revit 2026? Veľké modely sú teraz výrazne plynulejšie.",
    ],
    "PDMC": [
        "Ako spravujete revízie návrhov a kusovníky naprieč inžinierskym tímom?",
        "Čo sa stane, keď je potrebné propagovať zmenu návrhu do výroby?",
        "Ako sledujete, ktorá verzia návrhu je práve vo výrobe?",
        "Viete, že PDMC ušetrí približne 1 985 EUR/rok oproti nákupu Inventor + AutoCAD + Fusion samostatne?",
    ],
    "BIM Collaborate Pro": [
        "Ako majú externí partneri projektu prístup k vašim BIM modelom a ako ich posudzujú?",
        "Aký je váš najväčší problém v cykloch kontroly a schvaľovania návrhov?",
        "Ako sledujete problémy a RFI naprieč zainteresovanými stranami projektu?",
    ],
    "Forma": [
        "Ako skoro v procese navrhovania hodnotíte environmentálne faktory ako denné svetlo a vietor?",
        "Aké nástroje používajú vaši architekti na ranné hmotové a situačné analýzy?",
        "Videli ste Forma Building Design? Umožňuje architektom skúmať hmotové varianty s analýzou uhlíka a denného svetla v reálnom čase pred otvorením Revitu.",
    ],
    "AutoCAD (full)": [
        "Pracujú vaše tímy stále primárne v 2D, alebo prechádzate na 3D workflow?",
        "Ako spravujete výkresové normy a knižnice šablón naprieč tímom?",
        "AutoCAD 2026 otvára súbory až 11x rýchlejšie a štartuje 4x rýchlejšie — koľko času váš tím stráca čakaním na načítanie súborov?",
    ],
    "Inventor": [
        "Ako vaši inžinieri aktuálne odovzdávajú Inventor modely na výrobu alebo simuláciu?",
        "Používate Vault na kontrolu revízií, alebo spravujete súbory manuálne?",
        "Skúšali ste most Inventor-Fusion pre cloudovú simuláciu a generatívny dizajn?",
    ],
    "Fusion": [
        "Ako dnes používate Fusion — primárne CAD, CAM, alebo oboje?",
        "Skúšali ste Autodesk Assistant vo Fusion 2026? Dokáže generovať 3D geometriu z textových popisov.",
        "Používate funkcie cloudovej kolaborácie kusovníkov pre multi-tímové kontroly návrhov?",
    ],
    "M&E Collection": [
        "Ako vyzerá váš súčasný pipeline tvorby obsahu?",
        "Ako zvládate renderovanie a vizualizačné záťaže?",
        "Videli ste Wonder 3D? Generuje 3D assety z textových popisov pre rýchlu previsualizáciu.",
    ],
}

TALK_TRACKS = {
    "D&M": {
        "hiring": "I noticed you're hiring engineers — when teams grow, design-to-manufacturing handoffs often become the bottleneck. How are you handling that today?",
        "competitor_tools": "I see you're using {tools} alongside Autodesk. Many Czech manufacturers find that connecting design and manufacturing on one platform eliminates the translation errors between systems.",
        "public_contracts": "You've been winning public contracts — congratulations. As project complexity scales, how do you ensure consistent engineering standards across bids?",
        "whitespace": "You're currently using {products}. The natural next step for companies at your stage is {upsell} — it connects {reason}.",
        "renewal": "Your renewal is coming up in {days} days. This is a good time to evaluate whether your current setup matches where your engineering team is headed.",
        "innovation": "Fusion 2026 now has Autodesk Assistant — an AI that generates 3D geometry from text descriptions and automates scripts. Configuration rules let you manage complex product variants with drag-and-drop, no coding needed. The cloud-native BOM means multiple engineers can update simultaneously with live links to design changes.",
    },
    "AEC": {
        "hiring": "With your team growing, BIM coordination across disciplines becomes critical. How are you managing model coordination today?",
        "competitor_tools": "I see your teams work with {tools}. Many AEC firms in Czechia are consolidating onto the Autodesk AEC Collection to reduce interoperability issues.",
        "public_contracts": "You have active public sector contracts. With increasing BIM mandates for public projects, how prepared is your team for full BIM delivery?",
        "whitespace": "You're on {products} now. Companies at your stage typically see the biggest ROI from {upsell} — {reason}.",
        "renewal": "With renewal in {days} days, it's a natural point to consider whether your current setup supports BIM mandates and multi-discipline coordination.",
        "innovation": "Revit 2026 has GPU-accelerated graphics using USD/Hydra — 3D navigation is significantly smoother on large models. Forma Building Design lets your architects explore massing with real-time daylight, carbon, and sun-hours analysis, then export directly to Revit. AutoCAD 2026 opens files up to 11x faster with AI-powered Smart Blocks.",
    },
    "M&E": {
        "whitespace": "You're using {products}. The {upsell} would give your team {reason}.",
        "renewal": "Renewal is in {days} days — a good time to assess your content creation pipeline.",
        "innovation": "Maya and 3ds Max 2026 introduce Wonder 3D — generative AI that converts text or images into 3D assets for previsualization. Smart Bevel handles post-Boolean smoothing non-destructively. Golaem crowd simulation is now included in the M&E Collection. Bifrost adds procedural destruction workflows.",
    },
}

TALK_TRACKS_SK = {
    "D&M": {
        "hiring": "Všimol som si, že naberáte inžinierov — keď tímy rastú, odovzdávanie z návrhu do výroby sa často stáva úzkym miestom. Ako to riešite dnes?",
        "competitor_tools": "Vidím, že používate {tools} popri Autodesku. Mnohí českí výrobcovia zistili, že prepojenie návrhu a výroby na jednej platforme eliminuje chyby pri preklade medzi systémami.",
        "public_contracts": "Vyhráváte verejné zákazky — gratulujeme. Ako s rastúcou komplexnosťou projektov zabezpečujete konzistentné inžinierske štandardy naprieč ponukami?",
        "whitespace": "Aktuálne používate {products}. Prirodzený ďalší krok pre firmy vo vašom štádiu je {upsell} — prepojí {reason}.",
        "renewal": "Obnova vám prichádza za {days} dní. Je to dobrý čas na vyhodnotenie, či vaše súčasné nastavenie zodpovedá smerovaniu vášho inžinierskeho tímu.",
        "innovation": "Fusion 2026 má Autodesk Assistant — AI, ktorý generuje 3D geometriu z textových popisov a automatizuje skripty. Konfiguračné pravidlá umožňujú správu zložitých produktových variantov drag-and-drop, bez kódovania. Cloudový kusovník znamená, že viacero inžinierov môže aktualizovať súčasne.",
    },
    "AEC": {
        "hiring": "S rastúcim tímom sa koordinácia BIM modelov naprieč disciplínami stáva kľúčovou. Ako dnes riadite koordináciu modelov?",
        "competitor_tools": "Vidím, že vaše tímy pracujú s {tools}. Mnohé AEC firmy v Česku konsolidujú na Autodesk AEC Collection na zníženie problémov s interoperabilitou.",
        "public_contracts": "Máte aktívne verejné zákazky. S rastúcimi BIM mandátmi pre verejné projekty, ako je váš tím pripravený na plné BIM dodávky?",
        "whitespace": "Ste na {products}. Firmy vo vašom štádiu typicky vidia najväčšiu návratnosť z {upsell} — {reason}.",
        "renewal": "S obnovou za {days} dní je to prirodzený bod na zváženie, či vaše súčasné nastavenie podporuje BIM mandáty a multi-disciplinárnu koordináciu.",
        "innovation": "Revit 2026 má GPU-akcelerovanú grafiku pomocou USD/Hydra — 3D navigácia je výrazne plynulejšia na veľkých modeloch. Forma Building Design umožňuje architektom skúmať hmotové varianty s analýzou denného svetla, uhlíka a slnečných hodín v reálnom čase, a potom exportovať priamo do Revitu. AutoCAD 2026 otvára súbory až 11x rýchlejšie s AI Smart Blocks.",
    },
    "M&E": {
        "whitespace": "Používate {products}. {upsell} by dal vášmu tímu {reason}.",
        "renewal": "Obnova je za {days} dní — dobrý čas na posúdenie vášho pipeline tvorby obsahu.",
        "innovation": "Maya a 3ds Max 2026 prinášajú Wonder 3D — generatívne AI, ktoré premieňa text alebo obrázky na 3D assety pre previsualizáciu. Smart Bevel zvláda post-Boolean vyhladenie nedeštruktívne. Golaem simulácia davov je teraz súčasťou M&E Collection.",
    },
}


INNOVATION_HIGHLIGHTS = {
    "PDMC": {
        "headline": "PDMC 2026: Unified Design-to-Manufacturing with AI",
        "points": [
            "Autodesk Assistant: AI generates 3D geometry from text prompts and automates scripts in Fusion",
            "Cloud-native real-time BOM — multiple engineers update simultaneously with live links to design changes",
            "Configuration rules: visual drag-and-drop product variant management, no coding needed",
            "PDMC saves ~EUR 1,985/year vs standalone Inventor + AutoCAD + Fusion",
            "Inventor-to-Fusion bridge: send models for cloud-based simulation, generative design, and advanced CAM",
        ],
    },
    "Fusion Mfg Ext": {
        "headline": "Fusion Manufacturing Extension 2026: AI-Powered CAM",
        "points": [
            "5-axis simultaneous machining with intelligent whole-part strategies including deburr and hole recognition",
            "Sheet-based nesting for reduced material waste and faster fabrication",
            "Metals-based additive manufacturing preparation within the same platform",
            "Automated machining: collision avoidance, rotary operations, flow-line finishing",
            "AI-assisted toolpath generation reduces programming time by hours",
        ],
    },
    "Fusion Sim Ext": {
        "headline": "Fusion Simulation Extension: Generative Design & Advanced FEA",
        "points": [
            "Generative design: describe constraints, let AI optimize topology for weight and material savings",
            "Advanced FEA: static stress, modal frequency, thermal, buckling analysis",
            "Injection molding simulation to validate plastic parts before tooling investment",
            "All within the same Fusion platform — no export/import between tools",
        ],
    },
    "Fusion": {
        "headline": "Autodesk Fusion 2026: Cloud-Native CAD/CAM/CAE",
        "points": [
            "Autodesk Assistant: AI writes scripts, generates 3D geometry from text, creates render-quality images",
            "Neural CAD: natural language to editable parametric design geometry",
            "Real-time cloud BOM: multiple users edit simultaneously with live design links",
            "Simplified Home and streamlined startup for faster project access",
            "Electronics browser for hierarchical schematic/PCB navigation and modular design",
        ],
    },
    "AEC Collection": {
        "headline": "AEC Collection 2026: GPU-Accelerated BIM & Cloud Design",
        "points": [
            "Revit 2026: GPU-accelerated graphics (USD/Hydra) — dramatically smoother 3D navigation on large models",
            "ReCap Pro mesh plugin: integrate reality capture data into Revit for retrofit and adaptive reuse projects",
            "Automated view-to-sheet positioning: reduces repetitive documentation tasks",
            "Forma Building Design: schematic design with real-time daylight, carbon, and sun analysis, exports to Revit",
            "AutoCAD 2026: files open up to 11x faster, app starts 4x quicker, AI-powered Smart Blocks",
        ],
    },
    "Forma": {
        "headline": "Autodesk Forma 2026: Unified AECO Industry Cloud",
        "points": [
            "Forma Building Design: cloud-based schematic design with integrated environmental analysis",
            "Direct export to geolocated native Revit models — no manual recreation",
            "ACC is now part of Forma: Build, Takeoff, Design Collaboration, Data Management — one platform",
            "AI-powered Project Data agent for automated project insights",
            "70+ construction updates in March 2026 alone including enhanced dashboards and data connectors",
        ],
    },
    "BIM Collaborate Pro": {
        "headline": "Forma Design Collaboration (formerly BIM Collaborate Pro)",
        "points": [
            "Now part of the Autodesk Forma Industry Cloud platform",
            "Cloud-based clash detection and design coordination across disciplines",
            "Real-time design review with external project partners",
            "Issue and RFI tracking across all project stakeholders",
        ],
    },
    "AutoCAD (full)": {
        "headline": "AutoCAD 2026: Fastest AutoCAD Ever + AI",
        "points": [
            "2D/3D files open up to 11x faster than previous versions",
            "Application starts 4x quicker than AutoCAD 2025",
            "AI-powered Smart Blocks: detect and convert objects into blocks automatically",
            "Autodesk Assistant provides AI-driven insights and automation within AutoCAD",
        ],
    },
    "Inventor": {
        "headline": "Inventor 2026: Strengthened Cloud Bridge to Fusion",
        "points": [
            "Send Inventor components directly to Fusion for generative design, simulation, and manufacturing",
            "Fusion ribbon available directly within Inventor Assembly and Part environments",
            "Desktop Connector for AnyCAD interoperability with Fusion Team",
            "Stronger design-to-production workflow connections reducing manual effort",
        ],
    },
    "M&E Collection": {
        "headline": "M&E Collection 2026: AI-Powered Content Creation",
        "points": [
            "Wonder 3D: generative AI converts text/images into 3D characters and objects for previsualization",
            "Maya MotionMaker: AI-generated animal motion for walks, trots, gallops in seconds",
            "Smart Bevel: non-destructive post-Boolean geometry smoothing in Maya and 3ds Max",
            "Golaem crowd simulation now included in the M&E Collection",
            "Bifrost rigid body dynamics: fully procedural destruction workflows, no scene rebuild needed",
            "New Camera Sequencer in Maya with modern timeline and non-destructive multi-shot editing",
        ],
    },
}


def _tier_label(score):
    if score >= 70:
        return "A"
    if score >= 50:
        return "B"
    if score >= 30:
        return "C"
    return "D"


def _outreach_readiness(row, enrichment=None):
    """Assess how ready we are to reach out to this account."""
    enrichment = enrichment or {}
    score = 0
    factors = []

    if row.get("dm_contacts_count", 0) >= 2:
        score += 25
        factors.append("Multiple decision-maker contacts available")
    elif row.get("dm_contacts_count", 0) >= 1:
        score += 15
        factors.append("At least one decision-maker contact")
    else:
        factors.append("No decision-maker contacts — need to find stakeholders")

    has_email = any(
        c.get("email") or c.get("email_primary")
        for c in row.get("contacts", [])
    )
    if has_email:
        score += 20
        factors.append("Email channel available")
    else:
        factors.append("No email addresses — LinkedIn or phone only")

    if row.get("has_ideal_persona"):
        score += 20
        factors.append("Has ideal persona for top upsell")
    elif row.get("persona_fit_score", 0) > 0:
        score += 10
        factors.append("Has relevant persona, but not ideal for top upsell")
    else:
        factors.append("Missing key personas — prospecting needed")

    cq = row.get("contact_quality", 0)
    if cq >= 70:
        score += 15
    elif cq >= 40:
        score += 8

    days = row.get("days_to_renewal")
    if days is not None and 0 <= days <= 90:
        score += 10
        factors.append(f"Renewal in {days} days — timing is good")
    elif days is not None and 0 <= days <= 180:
        score += 5
        factors.append(f"Renewal in {days} days")

    if row.get("engineering_hiring"):
        score += 10
        factors.append("Actively hiring engineers — growth signal")

    if enrichment.get("upsell_hiring_intent"):
        strength = enrichment.get("intent_strength", "")
        if strength == "strong":
            score += 15
            factors.append("Strong upsell hiring intent — actively hiring roles aligned with top upsell")
        elif strength == "moderate":
            score += 8
            factors.append("Moderate upsell hiring intent — job postings suggest expansion in upsell area")

    if enrichment.get("has_eu_grants"):
        digi = enrichment.get("eu_digi_grants", [])
        if digi:
            score += 12
            factors.append(f"EU grant for digitalization/innovation — earmarked budget available")
        else:
            score += 5
            factors.append("EU grant recipient — may have earmarked investment budget")

    return {"score": min(100, score), "factors": factors}


def _build_talk_tracks(row, lang="en", enrichment=None):
    """Generate applicable talk tracks based on available data."""
    enrichment = enrichment or {}
    seg = row.get("primary_segment", "D&M")
    tt_source = TALK_TRACKS_SK if lang == "sk" else TALK_TRACKS
    tracks_template = tt_source.get(seg, tt_source.get("D&M", {}))
    tracks = []

    if row.get("engineering_hiring") and "hiring" in tracks_template:
        tracks.append({
            "trigger": "Engineering hiring",
            "track": tracks_template["hiring"],
        })

    comp_tools = row.get("comp_tools", []) + row.get("zi_competitor_tech", [])
    if comp_tools and "competitor_tools" in tracks_template:
        tracks.append({
            "trigger": "Competitor tools detected",
            "track": tracks_template["competitor_tools"].format(
                tools=", ".join(comp_tools[:3])
            ),
        })

    if row.get("has_public_contracts") and "public_contracts" in tracks_template:
        tracks.append({
            "trigger": "Public contracts",
            "track": tracks_template["public_contracts"],
        })

    if row.get("top_upsell") and "whitespace" in tracks_template:
        tracks.append({
            "trigger": "Product whitespace",
            "track": tracks_template["whitespace"].format(
                products=row.get("current_products", ""),
                upsell=row.get("top_upsell", ""),
                reason=row.get("top_upsell_reason", ""),
                days=row.get("days_to_renewal", "?"),
            ),
        })

    days = row.get("days_to_renewal")
    if days is not None and 0 <= days <= 120 and "renewal" in tracks_template:
        tracks.append({
            "trigger": "Upcoming renewal",
            "track": tracks_template["renewal"].format(
                days=days,
                products=row.get("current_products", ""),
                upsell=row.get("top_upsell", ""),
                reason=row.get("top_upsell_reason", ""),
            ),
        })

    if "innovation" in tracks_template:
        tracks.append({
            "trigger": "2026 product innovation",
            "track": tracks_template["innovation"],
        })

    intent_summary = row.get("intent_summary", "")
    if not intent_summary:
        from_enrich = row.get("upsell_hiring_intent")
        if from_enrich:
            intent_summary = row.get("intent_summary", "")
    if intent_summary:
        tracks.append({
            "trigger": "Upsell hiring intent",
            "track": intent_summary,
        })

    grant_summary = row.get("eu_grant_summary", "")
    if grant_summary:
        if lang == "sk":
            tracks.append({
                "trigger": "EU dotácia",
                "track": f"Získali ste EU dotáciu ({grant_summary}). Mnohé grantové programy financujú digitalizáciu a modernizáciu nástrojov — je to príležitosť na investíciu do Autodesk platformy.",
            })
        else:
            tracks.append({
                "trigger": "EU grant",
                "track": f"You've received EU funding ({grant_summary}). Many grant programs cover tool modernization and digitalization — this could fund an Autodesk platform investment.",
            })

    inv_detail = row.get("investment_detail", "")
    inv_label = row.get("investment_label", "")
    if inv_label in ("productivity", "capacity"):
        if lang == "sk":
            tracks.append({
                "trigger": "Trend investícií",
                "track": inv_detail,
            })
        else:
            tracks.append({
                "trigger": "Investment trend",
                "track": inv_detail,
            })

    tender_summary = row.get("tender_depth_summary", "")
    if tender_summary:
        if lang == "sk":
            tracks.append({
                "trigger": "Verejné zákazky",
                "track": f"Vaše firma sa aktívne zapája do verejných zákaziek ({tender_summary}). S rastúcou komplexnosťou projektov a požiadavkami na BIM je dôležité mať správne nástroje.",
            })
        else:
            tracks.append({
                "trigger": "Public tender activity",
                "track": f"You're active in public procurement ({tender_summary}). As project complexity grows and BIM mandates take effect, the right tools become critical for compliance and competitiveness.",
            })

    if row.get("facility_expansion") or enrichment.get("facility_expansion"):
        expansion = row.get("expansion_summary", "") or enrichment.get("expansion_summary", "")
        tracks.append({
            "trigger": "Facility expansion",
            "track": f"I see you're expanding ({expansion}). New facilities need new tool infrastructure — this is the perfect time to standardize on a unified platform.",
        })

    if row.get("ma_detected") or enrichment.get("ma_detected"):
        ma = row.get("ma_summary", "") or enrichment.get("ma_summary", "")
        tracks.append({
            "trigger": "M&A / ownership change",
            "track": f"With recent organizational changes ({ma}), tool standardization across entities becomes critical. A unified Autodesk platform can accelerate integration.",
        })

    cert_summary = row.get("certification_summary", "") or enrichment.get("certification_summary", "")
    if cert_summary:
        tracks.append({
            "trigger": "Certification / compliance",
            "track": f"Your compliance requirements ({cert_summary}) drive specific tool needs. Autodesk solutions are built to support these standards natively.",
        })

    if row.get("digital_transformation") or enrichment.get("digital_transformation"):
        tracks.append({
            "trigger": "Digital transformation",
            "track": "Your digital transformation initiative aligns perfectly with Autodesk's platform strategy — cloud-connected workflows, AI-powered automation, and unified data across your design-to-make process.",
        })

    if row.get("esg_signals") or enrichment.get("esg_signals"):
        tracks.append({
            "trigger": "ESG / sustainability",
            "track": "With sustainability reporting becoming mandatory, Autodesk tools like Forma for energy modeling and Fusion for material optimization help embed sustainability directly into the design process.",
        })

    return tracks


def _build_growth_signals(row, enrichment):
    """Extract growth and timing signals."""
    signals = []

    if enrichment.get("hiring_signal"):
        total = enrichment.get("total_jobs", 0)
        eng = enrichment.get("engineering_hiring", False)
        sig = f"Active hiring: {total} open positions"
        if eng:
            sig += " (including engineering roles)"
        signals.append({"type": "hiring", "signal": sig, "strength": "strong" if eng else "moderate"})

    adsk_in_jobs = enrichment.get("autodesk_tools_in_jobs", [])
    if adsk_in_jobs:
        signals.append({
            "type": "tool_adoption",
            "signal": f"Autodesk tools mentioned in job postings: {', '.join(adsk_in_jobs)}",
            "strength": "strong",
        })

    comp_in_jobs = enrichment.get("competitor_tools_in_jobs", [])
    if comp_in_jobs:
        signals.append({
            "type": "competitive",
            "signal": f"Competitor tools in job postings: {', '.join(comp_in_jobs)}",
            "strength": "moderate",
        })

    if enrichment.get("has_public_contracts"):
        count = enrichment.get("smlouvy_contracts_count", 0)
        aec = enrichment.get("smlouvy_aec_contracts", 0)
        val = enrichment.get("smlouvy_value_czk", 0)
        sig = f"Active in public procurement: {count} contracts"
        if val:
            sig += f" (total {val:,.0f} CZK)"
        if aec:
            sig += f", {aec} AEC-related"
        signals.append({"type": "public_sector", "signal": sig, "strength": "moderate"})

    zi_growth = enrichment.get("zi_employee_growth") or {}
    one_y = zi_growth.get("one_year_growth_rate")
    if one_y is not None:
        try:
            rate = float(one_y)
            if abs(rate) > 0.1:
                direction = "growing" if rate > 0 else "shrinking"
                signals.append({
                    "type": "employee_growth",
                    "signal": f"Headcount {direction} {abs(rate):.1f}% YoY (ZoomInfo verified)",
                    "strength": "strong" if rate > 10 else "moderate",
                })
        except (ValueError, TypeError):
            pass

    if enrichment.get("leadership_change"):
        signals.append({
            "type": "leadership",
            "signal": "Recent leadership change detected",
            "strength": "strong",
        })

    zi_funding = enrichment.get("zi_recent_funding_amount")
    if zi_funding and zi_funding > 0:
        signals.append({
            "type": "funding",
            "signal": f"Recent funding: ${zi_funding:,}",
            "strength": "strong",
        })

    days = row.get("days_to_renewal")
    if days is not None and days != "":
        try:
            days = int(days)
        except (ValueError, TypeError):
            days = None
    else:
        days = None
    if days is not None and 0 <= days <= 180:
        signals.append({
            "type": "renewal",
            "signal": f"Autodesk renewal in {days} days ({row.get('nearest_renewal', '')})",
            "strength": "strong" if days <= 90 else "moderate",
        })

    if enrichment.get("upsell_hiring_intent"):
        titles = enrichment.get("intent_matching_titles", [])
        title_str = f" (e.g. {titles[0]})" if titles else ""
        signals.append({
            "type": "upsell_intent",
            "signal": (enrichment.get("intent_summary")
                       or f"Hiring roles aligned with top upsell{title_str}"),
            "strength": "strong" if enrichment.get("intent_strength") == "strong" else "moderate",
        })

    if enrichment.get("has_eu_grants"):
        summary = enrichment.get("eu_grant_summary", "")
        digi = enrichment.get("eu_digi_grants", [])
        if digi:
            signals.append({
                "type": "eu_grant",
                "signal": f"EU grant for digitalization/innovation: {summary}",
                "strength": "strong",
            })
        else:
            signals.append({
                "type": "eu_grant",
                "signal": f"EU grant recipient: {summary}",
                "strength": "moderate",
            })

    rpe = enrichment.get("rev_per_employee")
    inv_label = enrichment.get("investment_label")
    if rpe and inv_label:
        detail = enrichment.get("investment_detail", "")
        strength = "strong" if inv_label in ("productivity", "capacity") else "moderate"
        signals.append({
            "type": "investment_trend",
            "signal": detail or f"Revenue/employee: EUR {rpe:,.0f} ({inv_label})",
            "strength": strength,
        })

    tender_summary = enrichment.get("tender_depth_summary", "")
    if tender_summary:
        signals.append({
            "type": "tender_depth",
            "signal": tender_summary,
            "strength": "strong" if enrichment.get("first_time_tender") else "moderate",
        })

    grant_prog = enrichment.get("grant_program_summary", "")
    if grant_prog:
        signals.append({
            "type": "grant_program",
            "signal": grant_prog,
            "strength": "strong" if enrichment.get("has_digi_budget") else "moderate",
        })

    if enrichment.get("facility_expansion"):
        signals.append({
            "type": "facility_expansion",
            "signal": enrichment.get("expansion_summary", "Facility expansion detected"),
            "strength": "strong",
        })

    if enrichment.get("ma_detected"):
        signals.append({
            "type": "ma_activity",
            "signal": enrichment.get("ma_summary", "M&A activity detected"),
            "strength": "strong" if enrichment.get("ma_type") in ("merger", "acquisition") else "moderate",
        })

    if enrichment.get("certifications_detected"):
        signals.append({
            "type": "certification",
            "signal": enrichment.get("certification_summary", "Certification signal"),
            "strength": "strong" if enrichment.get("bim_mandate_relevant") else "moderate",
        })

    if enrichment.get("digital_transformation"):
        dt_ev = enrichment.get("dt_evidence", [])
        signals.append({
            "type": "digital_transformation",
            "signal": f"Digital transformation signals: {', '.join(dt_ev[:3])}",
            "strength": "moderate",
        })

    if enrichment.get("esg_signals"):
        esg_ev = enrichment.get("esg_evidence", [])
        signals.append({
            "type": "esg",
            "signal": f"ESG/sustainability signals: {', '.join(esg_ev[:3])}",
            "strength": "moderate",
        })

    engagement = enrichment.get("engagement_level", "none")
    if engagement in ("high", "medium"):
        signals.append({
            "type": "event_engagement",
            "signal": enrichment.get("event_summary", "Event engagement detected"),
            "strength": "strong" if engagement == "high" else "moderate",
        })

    return signals


def _build_tech_stack(row, enrichment):
    """Build tech stack evidence."""
    stack = {"autodesk": [], "competitors": [], "total_detected": 0}

    for t in (enrichment.get("zi_autodesk_tech") or []):
        stack["autodesk"].append({"tool": t, "source": "ZoomInfo Technographics"})
    for t in (enrichment.get("autodesk_tools_in_jobs") or []):
        if not any(s["tool"] == t for s in stack["autodesk"]):
            stack["autodesk"].append({"tool": t, "source": "Job postings"})

    for t in (enrichment.get("zi_competitor_tech") or []):
        stack["competitors"].append({"tool": t, "source": "ZoomInfo Technographics"})
    for t in (enrichment.get("competitor_tools_in_jobs") or []):
        if not any(s["tool"] == t for s in stack["competitors"]):
            stack["competitors"].append({"tool": t, "source": "Job postings"})

    stack["total_detected"] = enrichment.get("zi_tech_total", 0)
    return stack


def _build_contacts_roster(row):
    """Build structured contact roster for the brief."""
    roster = []
    for c in row.get("contacts", []):
        entry = {
            "name": c.get("full_name", ""),
            "title": c.get("title", ""),
            "persona": c.get("adsk_persona", "unknown"),
            "relevant": c.get("adsk_relevant", False),
            "source": c.get("source", ""),
            "email": c.get("email", ""),
            "email_estimated": c.get("email_primary", ""),
            "phone": c.get("phone", ""),
            "linkedin": c.get("linkedin_url", ""),
        }
        roster.append(entry)

    roster.sort(key=lambda x: (not x["relevant"], x["persona"] == "unknown", not x["email"]))
    return roster


def generate_brief(row, enrichment, lang="en"):
    """Generate a structured intelligence brief for one account."""
    seg = row.get("primary_segment", "D&M")
    seg_descs = SEGMENT_DESCRIPTIONS_SK if lang == "sk" else SEGMENT_DESCRIPTIONS
    seg_full = seg_descs.get(seg, seg)
    maturity = row.get("maturity_label", "Entry")
    tier = _tier_label(row.get("priority_score", 0))

    outreach = _outreach_readiness(row, enrichment)
    signals = _build_growth_signals(row, enrichment)
    tech = _build_tech_stack(row, enrichment)
    talk_tracks = _build_talk_tracks(row, lang=lang, enrichment=enrichment)
    contacts = _build_contacts_roster(row)

    disc_qs = DISCOVERY_QUESTIONS_SK if lang == "sk" else DISCOVERY_QUESTIONS
    upsell_key = row.get("top_upsell", "")
    discovery = []
    for key, qs in disc_qs.items():
        if key.lower() in upsell_key.lower():
            discovery = qs
            break
    if not discovery:
        discovery = disc_qs.get(
            "AEC Collection" if seg == "AEC" else "PDMC" if seg == "D&M" else "M&E Collection",
            [],
        )

    persona_gap = row.get("missing_personas", [])
    titles_needed = row.get("titles_to_find", [])

    exec_bullets = []

    if lang == "sk":
        exec_bullets.append(
            f"Tier {tier} účet ({row.get('priority_score', 0)}/100) v {seg_full}. "
            f"Zrelosť: {maturity}."
        )
    else:
        exec_bullets.append(
            f"Tier {tier} account ({row.get('priority_score', 0)}/100) in {seg_full}. "
            f"Maturity: {maturity}."
        )

    if row.get("top_upsell"):
        lbl = "Top upsell" if lang == "en" else "Top upsell príležitosť"
        exec_bullets.append(
            f"{lbl}: {row['top_upsell']} — {row.get('top_upsell_reason', '')}."
        )

    pot = row.get("potential_acv_eur", 0)
    cur = row.get("current_acv_eur", 0)
    if pot:
        cur_lbl = "current" if lang == "en" else "aktuálne"
        exec_bullets.append(
            f"Whitespace ACV: EUR {float(pot):,.0f} "
            f"({cur_lbl}: EUR {float(cur):,.0f})."
        )

    strong_signals = [s for s in signals if s["strength"] == "strong"]
    if strong_signals:
        lbl = "Key signals" if lang == "en" else "Kľúčové signály"
        exec_bullets.append(
            f"{lbl}: {'; '.join(s['signal'][:80] for s in strong_signals[:3])}."
        )

    if lang == "sk":
        if outreach["score"] >= 70:
            exec_bullets.append(f"Pripravenosť na oslovenie: VYSOKÁ ({outreach['score']}/100).")
        elif outreach["score"] >= 40:
            exec_bullets.append(f"Pripravenosť na oslovenie: STREDNÁ ({outreach['score']}/100).")
        else:
            exec_bullets.append(f"Pripravenosť na oslovenie: NÍZKA ({outreach['score']}/100).")
    else:
        if outreach["score"] >= 70:
            exec_bullets.append(f"Outreach readiness: HIGH ({outreach['score']}/100).")
        elif outreach["score"] >= 40:
            exec_bullets.append(f"Outreach readiness: MEDIUM ({outreach['score']}/100).")
        else:
            exec_bullets.append(
                f"Outreach readiness: LOW ({outreach['score']}/100) — "
                f"{outreach['factors'][-1] if outreach['factors'] else 'needs more data'}."
            )

    parent = enrichment.get("zi_ultimate_parent_name") or enrichment.get("zi_parent_name")
    if parent:
        parent_emp = enrichment.get("zi_ultimate_parent_employees")
        lbl = "Subsidiary of" if lang == "en" else "Dcérska spoločnosť"
        emp_lbl = "employees globally" if lang == "en" else "zamestnancov globálne"
        parent_info = f"{lbl} {parent}"
        if parent_emp:
            parent_info += f" ({parent_emp:,} {emp_lbl})"
        exec_bullets.append(parent_info + ".")

    if row.get("has_ideal_persona"):
        txt = "Has ideal persona contact for the top upsell product." if lang == "en" else "Má ideálny kontakt persóny pre top upsell produkt."
        exec_bullets.append(txt)
    elif persona_gap:
        if lang == "sk":
            exec_bullets.append(
                f"Chýbajúce kľúčové persóny: {', '.join(persona_gap[:3])}. "
                f"Hľadajte: {', '.join(titles_needed[:3])}."
            )
        else:
            exec_bullets.append(
                f"Missing key personas: {', '.join(persona_gap[:3])}. "
                f"Look for: {', '.join(titles_needed[:3])}."
            )

    innovation = None
    top_upsell = row.get("top_upsell", "")
    if top_upsell in INNOVATION_HIGHLIGHTS:
        innovation = INNOVATION_HIGHLIGHTS[top_upsell]
    else:
        seg_default = {
            "AEC": "AEC Collection",
            "D&M": "PDMC",
            "M&E": "M&E Collection",
        }
        fallback_key = seg_default.get(seg)
        if fallback_key and fallback_key in INNOVATION_HIGHLIGHTS:
            innovation = INNOVATION_HIGHLIGHTS[fallback_key]

    brief = {
        "csn": row["csn"],
        "company_name": row.get("company_name", ""),
        "rank": row.get("rank", 0),
        "tier": tier,
        "segment": seg,
        "segment_full": seg_full,
        "executive_summary": exec_bullets,
        "innovation_highlights": innovation,
        "company_snapshot": {
            "official_name": enrichment.get("official_name", row.get("company_name", "")),
            "ico": enrichment.get("ico", ""),
            "website": row.get("website", ""),
            "city": row.get("city", ""),
            "address": enrichment.get("ares_address", ""),
            "industry": row.get("industry_segment", ""),
            "industry_group": row.get("industry_group", ""),
            "employees_cz": row.get("employee_count"),
            "employees_global": row.get("employee_count_global"),
            "engineering_headcount": (enrichment.get("zi_employee_by_department") or {}).get("engineering"),
            "it_headcount": (enrichment.get("zi_employee_by_department") or {}).get("it"),
            "revenue_eur": row.get("revenue"),
            "revenue_global_eur": row.get("revenue_global"),
            "employee_growth_1y": (enrichment.get("zi_employee_growth") or {}).get("one_year_growth_rate"),
            "parent_company": enrichment.get("zi_ultimate_parent_name") or enrichment.get("zi_parent_name"),
            "parent_employees": enrichment.get("zi_ultimate_parent_employees"),
            "zi_primary_industry": enrichment.get("zi_primary_industry"),
            "zi_company_status": enrichment.get("zi_company_status"),
            "zi_founded_year": enrichment.get("zi_founded_year"),
            "zi_it_budget": (enrichment.get("zi_department_budgets") or {}).get("it"),
            "zi_linkedin": enrichment.get("zi_linkedin_url"),
            "zi_contacts_available": enrichment.get("zi_contacts_available"),
            "maturity": maturity,
            "current_products": row.get("current_products", ""),
            "total_seats": row.get("total_seats", 0),
            "product_count": row.get("product_count", 0),
            "nearest_renewal": row.get("nearest_renewal", ""),
            "reseller": row.get("reseller", ""),
        },
        "outreach_readiness": outreach,
        "growth_signals": signals,
        "tech_stack": tech,
        "upsell_analysis": {
            "top_upsell": row.get("top_upsell", ""),
            "top_upsell_reason": row.get("top_upsell_reason", ""),
            "all_upsells": row.get("all_upsells", ""),
            "current_acv_eur": cur,
            "whitespace_acv_eur": pot,
            "whitespace_score": row.get("whitespace_score", 0),
        },
        "persona_analysis": {
            "contact_quality": row.get("contact_quality", 0),
            "has_ideal_persona": row.get("has_ideal_persona", False),
            "persona_fit_score": row.get("persona_fit_score", 0),
            "missing_personas": persona_gap,
            "titles_to_find": titles_needed,
        },
        "contacts_roster": contacts,
        "discovery_questions": discovery,
        "talk_tracks": talk_tracks,
        "web_signals": [],
    }

    return brief


def generate_all_briefs(lang="en"):
    """Generate briefs for all accounts. lang='sk' for Slovak."""
    with open(TABLE_DATA_PATH) as f:
        table_data = json.load(f)

    with open(ENRICHMENT_PATH) as f:
        enrichment = json.load(f)

    briefs = {}
    for row in table_data:
        csn = row["csn"]
        e = enrichment.get(csn, {})
        brief = generate_brief(row, e, lang=lang)
        briefs[csn] = brief

    out_path = BRIEFS_PATH
    if lang == "sk":
        out_path = BRIEFS_PATH.replace(".json", "_sk.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(briefs, f, ensure_ascii=False, indent=2)

    tiers = {"A": 0, "B": 0, "C": 0, "D": 0}
    readiness = {"high": 0, "medium": 0, "low": 0}
    with_signals = 0
    with_tracks = 0

    for b in briefs.values():
        tiers[b["tier"]] = tiers.get(b["tier"], 0) + 1
        r = b["outreach_readiness"]["score"]
        if r >= 70:
            readiness["high"] += 1
        elif r >= 40:
            readiness["medium"] += 1
        else:
            readiness["low"] += 1
        if b["growth_signals"]:
            with_signals += 1
        if b["talk_tracks"]:
            with_tracks += 1

    print(f"{'=' * 60}")
    print(f"AUTO BRIEF GENERATION COMPLETE ({lang.upper()})")
    print(f"{'=' * 60}")
    print(f"Total briefs: {len(briefs)}")
    print(f"Saved to: {out_path}")
    print(f"\nTier distribution: A={tiers['A']} B={tiers['B']} C={tiers['C']} D={tiers['D']}")
    print(f"Outreach readiness: High={readiness['high']} Medium={readiness['medium']} Low={readiness['low']}")
    print(f"With growth signals: {with_signals}")
    print(f"With talk tracks: {with_tracks}")

    return briefs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", default="en", choices=["en", "sk"],
                        help="Language for briefs: en (English) or sk (Slovak)")
    args = parser.parse_args()
    generate_all_briefs(lang=args.lang)
