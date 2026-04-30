"""Deep research prompt generator for top prioritized accounts.

Takes the top N accounts from the territory scorer + enrichment data,
generates pre-seeded deep research prompts, and optionally exports
completed research into the research dashboard format.

Usage:
    python -m scoring.deep_research_generator                     # top 30
    python -m scoring.deep_research_generator --top 20            # top 20
    python -m scoring.deep_research_generator --format markdown   # save as .md
    python -m scoring.deep_research_generator --dashboard         # export to dashboard
"""

import json
import os
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from scoring.batch_enricher import ENRICHMENT_DIR, load_enrichment, load_prioritized

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "deep_research_prompts"
DASHBOARD_DATA = (
    Path(__file__).resolve().parent.parent.parent
    / "research-dashboard" / "data" / "accounts"
)

RESEARCH_PROMPT_TEMPLATE = """You are a pre-call account intelligence analyst preparing a deep research brief for an Autodesk territory account executive covering Czech Republic. The AE sells the FULL Autodesk portfolio across three motions: D&M (Design & Manufacturing), AEC (Architecture, Engineering & Construction), and M&E (Media & Entertainment).

## Target Company
- **Company:** {company_name}
- **Website:** {website}
- **City:** {city}
- **ICO (Czech business ID):** {ico}
- **Industry:** {industry_group} / {industry_segment}
- **Primary Autodesk segment:** {primary_segment}

## What We Already Know (from CRM / subscription data)
- **Current Autodesk products:** {current_products}
- **Total seats:** {total_seats}
- **Product maturity:** {maturity_label} (level {maturity_level}/5)
- **Nearest renewal:** {nearest_renewal}
- **Reseller:** {reseller}
- **Parent account:** {parent_account}
- **Contact email(s):** {contact_email}

## Scoring Context
- **Priority rank:** #{rank} out of ~4,500 accounts
- **Priority score:** {priority_score}/100
- **Whitespace score:** {whitespace_score}/30 -- {top_upsell_reason}
- **Top upsell opportunity:** {top_upsell}
- **All recommended upsells:** {all_upsells}
- **Estimated current ACV:** EUR {current_acv_eur}
- **Estimated whitespace ACV:** EUR {potential_acv_eur}

## Enrichment Data (pre-collected)
{enrichment_section}

## Contacts Already Identified (CZ-validated)
{contacts_section}

## Tech Stack Evidence (from ZoomInfo + job postings)
{tech_stack_section}

## Public Contracts (smlouvy.gov.cz)
{public_contracts_section}

## Web Signals (auto-scraped from company website)
{web_signals_section}

## Research Instructions

Produce a structured report with these sections. Use ONLY credible, verifiable sources. Clearly separate **verified facts** from **hypotheses**. Write in English.

### 1. Executive Summary
- 5-10 bullet points covering: portfolio classification verdict, strongest entry point, key trigger/timing, buying center overview, top 3 expansion plays, key risk.
- Include our **contact quality assessment**: do we have the right people? What's missing?

### 2. Company Snapshot Table
| Field | Finding |
|---|---|
| Company | |
| Website | |
| HQ | |
| Key Locations | |
| Core Business | |
| Industries Served | |
| Business Model | D&M / AEC / M&E classification with reasoning |
| Scale Indicators | Employees, revenue, capacity, certifications |
| Ownership | |
| Portfolio Classification | D&M-led / AEC-led / M&E-led / Hybrid with justification |
| Why Autodesk Relevant Now | |

### 3. Recent Signals Timeline (last 12 months)
For each signal: date, what happened, why it matters for Autodesk, source.
Look for: M&A, leadership changes, new projects, facility expansion, hiring, funding, digital initiatives, public contracts, awards.
**We already found some web signals (see above) — verify and expand on these.**

### 4. Buying Committee (Top 5-8 Personas)
| Name | Title | Location | Decision Role | Why Relevant | KPIs | Outreach Angle | LinkedIn | Email | Sources | Confidence |
**We already have some contacts (see above). Verify their roles, find additional stakeholders, and identify the IDEAL persona for our top upsell ({top_upsell}).**
{persona_gap_section}
Search for: CTO, Engineering Director, CAD/BIM Manager, IT Director, Head of Design, VP Manufacturing, Design Technology Manager. Include Czech titles (ředitel, vedoucí, jednatel).

### 5. Tech Stack
Split into D&M, AEC, M&E, and Shared buckets.
| Tool/Platform | Category | Evidence | Source | Confidence |
**We already have tech evidence (see above). Verify and expand — look for CAD tools in job postings, project portfolios, partner references, LinkedIn skills, press mentions.**

### 6. Pain Points & Triggers
| Pain Point | Related KPIs | Portfolio Tag | Evidence vs Hypothesis |
Focus on: data silos, multi-site coordination, version control, BIM mandates, manual processes, tool fragmentation, compliance (NIS2/ESG), sustainability.

### 7. Expansion Plays (Top 3-5)
For each play include:
- **What to sell:** specific Autodesk product/solution
- **Portfolio:** D&M / AEC / M&E
- **Why now:** trigger or evidence
- **Value hypothesis:** business outcome, not features
- **Primary sponsor:** name + title from buying committee (use our contacts if applicable)
- **Likely blockers:** and how to disarm
- **Discovery questions:** 2-3 for first meeting
- **Proof points:** reference customers or case studies
- **Confidence:** High / Med / Low

### 8. Talk Tracks (3-5 Openers)
Conversation starters tied to specific signals or pain points. Include portfolio tag. Make them specific to THIS company using the signals and data above.

### 9. Sources
List all sources used with URLs where available.

## Important Guidelines
- Do NOT force a single portfolio. If the company spans D&M and AEC, say so.
- Rank expansion plays by EVIDENCE, not by Autodesk product priority.
- For Czech companies, search in BOTH Czech and English.
- Use ARES (ares.gov.cz), OR (or.justice.cz), LinkedIn, company website, jobs.cz, industry associations.
- The AE already knows the current product list — focus on NEW intelligence they don't have.
- All contacts must be VERIFIED as located in Czechia. Do NOT include parent-company executives from other countries.
- When listing contacts, always include the source and confidence level.
"""


RESEARCH_PROMPT_TEMPLATE_SK = """Si predpredajný analytik účtovej inteligencie pripravujúci hlboký výskumný brief pre obchodného zástupcu Autodesku pokrývajúceho Českú republiku. Obchodník predáva CELÉ portfólio Autodesku naprieč tromi smermi: D&M (Dizajn a Výroba), AEC (Architektúra, Inžinierstvo a Stavebníctvo) a M&E (Médiá a Zábava).

## Cieľová Spoločnosť
- **Spoločnosť:** {company_name}
- **Web:** {website}
- **Mesto:** {city}
- **IČO:** {ico}
- **Odvetvie:** {industry_group} / {industry_segment}
- **Primárny Autodesk segment:** {primary_segment}

## Čo Už Vieme (z CRM / údajov o predplatnom)
- **Aktuálne Autodesk produkty:** {current_products}
- **Celkom licencií:** {total_seats}
- **Zrelosť produktu:** {maturity_label} (úroveň {maturity_level}/5)
- **Najbližšia obnova:** {nearest_renewal}
- **Reseller:** {reseller}
- **Materský účet:** {parent_account}
- **Kontaktný email(y):** {contact_email}

## Kontext Skórovania
- **Poradie priority:** #{rank} z ~4 500 účtov
- **Skóre priority:** {priority_score}/100
- **Whitespace skóre:** {whitespace_score}/30 -- {top_upsell_reason}
- **Top upsell príležitosť:** {top_upsell}
- **Všetky odporúčané upselly:** {all_upsells}
- **Odhadované aktuálne ACV:** EUR {current_acv_eur}
- **Odhadované whitespace ACV:** EUR {potential_acv_eur}

## Obohacovacie Údaje (predzozbierané)
{enrichment_section}

## Už Identifikované Kontakty (CZ-validované)
{contacts_section}

## Dôkazy o Tech Stacku (z ZoomInfo + inzerátov)
{tech_stack_section}

## Verejné Zákazky (smlouvy.gov.cz)
{public_contracts_section}

## Webové Signály (automaticky zozbierané z webu spoločnosti)
{web_signals_section}

## Pokyny pre Výskum

Vytvor štruktúrovaný report s nasledujúcimi sekciami. Použi IBA dôveryhodné, overiteľné zdroje. Jasne oddeľ **overené fakty** od **hypotéz**. Píš v slovenčine.

### 1. Zhrnutie
- 5-10 odrážok pokrývajúcich: verdikt klasifikácie portfólia, najsilnejší vstupný bod, kľúčový spúšťač/načasovanie, prehľad nákupného centra, top 3 expanzné hry, kľúčové riziko.
- Zahrň naše **hodnotenie kvality kontaktov**: máme správnych ľudí? Čo chýba?

### 2. Tabuľka Prehľadu Spoločnosti
| Pole | Zistenie |
|---|---|
| Spoločnosť | |
| Web | |
| Centrála | |
| Kľúčové Lokácie | |
| Hlavné Podnikanie | |
| Obsluhované Odvetvia | |
| Obchodný Model | D&M / AEC / M&E klasifikácia s odôvodnením |
| Ukazovatele Rozsahu | Zamestnanci, tržby, kapacita, certifikácie |
| Vlastníctvo | |
| Klasifikácia Portfólia | D&M-vedená / AEC-vedená / M&E-vedená / Hybridná s odôvodnením |
| Prečo Autodesk Teraz | |

### 3. Časová Os Nedávnych Signálov (posledných 12 mesiacov)
Pre každý signál: dátum, čo sa stalo, prečo je to dôležité pre Autodesk, zdroj.
Hľadaj: M&A, zmeny vedenia, nové projekty, rozšírenie zariadení, nábor, financovanie, digitálne iniciatívy, verejné zákazky, ocenenia.
**Už sme našli niektoré webové signály (viď vyššie) — over a rozšír ich.**

### 4. Nákupný Výbor (Top 5-8 Persón)
| Meno | Funkcia | Lokalita | Rozhodovacia Rola | Prečo Relevantný | KPI | Uhol Oslovenia | LinkedIn | Email | Zdroje | Dôveryhodnosť |
**Už máme niektoré kontakty (viď vyššie). Over ich roly, nájdi ďalších stakeholderov a identifikuj IDEÁLNU persónu pre náš top upsell ({top_upsell}).**
{persona_gap_section}
Hľadaj: CTO, riaditeľ inžinieringu, CAD/BIM Manager, IT riaditeľ, vedúci dizajnu, VP výroby, Design Technology Manager. Vrátane českých titulov (ředitel, vedoucí, jednatel).

### 5. Tech Stack
Rozdeľ do D&M, AEC, M&E a Spoločných skupín.
| Nástroj/Platforma | Kategória | Dôkaz | Zdroj | Dôveryhodnosť |
**Už máme dôkazy o technológiách (viď vyššie). Over a rozšír — hľadaj CAD nástroje v inzerátoch, projektových portfóliách, partnerských referenciách, LinkedIn skills, tlačových zmienach.**

### 6. Bolestivé Body a Spúšťače
| Bolestivý Bod | Súvisiace KPI | Tag Portfólia | Dôkaz vs Hypotéza |
Zameraj sa na: dátové silá, multi-site koordináciu, kontrolu verzií, BIM mandáty, manuálne procesy, fragmentáciu nástrojov, compliance (NIS2/ESG), udržateľnosť.

### 7. Expanzné Hry (Top 3-5)
Pre každú hru zahrň:
- **Čo predať:** konkrétny Autodesk produkt/riešenie
- **Portfólio:** D&M / AEC / M&E
- **Prečo teraz:** spúšťač alebo dôkaz
- **Hypotéza hodnoty:** obchodný výsledok, nie funkcie
- **Primárny sponzor:** meno + funkcia z nákupného výboru (použi naše kontakty ak je to možné)
- **Pravdepodobné blokovače:** a ako ich odzbrojit
- **Objavovacie otázky:** 2-3 pre prvé stretnutie
- **Dôkazné body:** referenčné zákazky alebo prípadové štúdie
- **Dôveryhodnosť:** Vysoká / Stredná / Nízka

### 8. Rečové Stopy (3-5 Otváračov)
Konverzačné začiatky naviazané na konkrétne signály alebo bolestivé body. Zahrň tag portfólia. Urob ich špecifické pre TÚTO spoločnosť pomocou signálov a dát vyššie.

### 9. Zdroje
Zoznam všetkých použitých zdrojov s URL kde je to možné.

## Dôležité Pokyny
- NENÚŤ jediné portfólio. Ak spoločnosť pokrýva D&M aj AEC, povedz to.
- Zoraď expanzné hry podľa DÔKAZOV, nie podľa priority Autodesk produktov.
- Pre české spoločnosti hľadaj v ČESKOM aj ANGLICKOM jazyku.
- Použi ARES (ares.gov.cz), OR (or.justice.cz), LinkedIn, web spoločnosti, jobs.cz, priemyselné asociácie.
- Obchodník už pozná aktuálny zoznam produktov — zameraj sa na NOVÚ inteligenciu, ktorú nemá.
- Všetky kontakty musia byť OVERENÉ ako nachádzajúce sa v Česku. NEZAHRŇUJ manažérov materskej spoločnosti z iných krajín.
- Pri uvádzaní kontaktov vždy zahrň zdroj a úroveň dôveryhodnosti.
"""


def generate_prompts(
    accounts: list,
    enrichment: dict,
    briefs: dict = None,
    output_dir: str = None,
    lang: str = "en",
) -> list:
    """Generate deep research prompts for a list of accounts.

    Returns list of dicts with prompt text and metadata.
    """
    output_dir = output_dir or str(PROMPTS_DIR)
    os.makedirs(output_dir, exist_ok=True)
    briefs = briefs or {}

    template = RESEARCH_PROMPT_TEMPLATE_SK if lang == "sk" else RESEARCH_PROMPT_TEMPLATE

    prompts = []
    for acct in accounts:
        csn = acct["csn"]
        enrich = enrichment.get(csn, {})
        top_upsell = acct.get("top_upsell", "")

        enrichment_section = _format_enrichment(enrich)
        contacts_section = _format_contacts(enrich)
        tech_stack_section = _format_tech_stack(enrich)
        public_contracts_section = _format_public_contracts(enrich)
        web_signals_section = _format_web_signals(briefs, csn)
        persona_gap_section = _format_persona_gap(enrich, top_upsell)

        prompt = template.format(
            company_name=acct.get("company_name", ""),
            website=acct.get("website", ""),
            city=acct.get("city", ""),
            ico=enrich.get("ico", "N/A"),
            industry_group=acct.get("industry_group", ""),
            industry_segment=acct.get("primary_segment", ""),
            primary_segment=acct.get("primary_segment", ""),
            current_products=acct.get("current_products", ""),
            total_seats=acct.get("total_seats", 0),
            maturity_label=acct.get("maturity_label", ""),
            maturity_level=acct.get("maturity_level", ""),
            nearest_renewal=acct.get("nearest_renewal", "N/A"),
            reseller=acct.get("reseller", ""),
            parent_account=acct.get("parent_account", ""),
            contact_email=acct.get("contact_email", ""),
            rank=acct.get("rank", ""),
            priority_score=acct.get("priority_score", ""),
            whitespace_score=acct.get("whitespace_score", ""),
            top_upsell=top_upsell,
            top_upsell_reason=acct.get("top_upsell_reason", ""),
            all_upsells=acct.get("all_upsells", ""),
            current_acv_eur=f"{float(acct.get('current_acv_eur', 0)):,.0f}",
            potential_acv_eur=f"{float(acct.get('potential_acv_eur', 0)):,.0f}",
            enrichment_section=enrichment_section,
            contacts_section=contacts_section,
            tech_stack_section=tech_stack_section,
            public_contracts_section=public_contracts_section,
            web_signals_section=web_signals_section,
            persona_gap_section=persona_gap_section,
        )

        safe_name = re.sub(r"[^\w\s-]", "", acct.get("company_name", "unknown"))
        safe_name = re.sub(r"\s+", "_", safe_name.strip())[:50]
        filename = f"{acct.get('rank', 0):03d}_{safe_name}.md"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(prompt)

        prompts.append({
            "csn": csn,
            "company_name": acct.get("company_name", ""),
            "rank": acct.get("rank", 0),
            "filename": filename,
            "filepath": filepath,
        })

    return prompts


def _format_enrichment(enrich: dict) -> str:
    """Format enrichment data into a readable section for the prompt."""
    lines = []

    if enrich.get("official_name"):
        lines.append(f"- **Official name (ARES):** {enrich['official_name']}")
    if enrich.get("ico"):
        lines.append(f"- **ICO:** {enrich['ico']}")
    if enrich.get("ares_address"):
        lines.append(f"- **Address:** {enrich['ares_address']}")
    if enrich.get("nace_codes"):
        codes = enrich["nace_codes"][:5]
        lines.append(f"- **NACE codes:** {', '.join(str(c) for c in codes)}")
    if enrich.get("ares_primary_segment"):
        lines.append(f"- **ARES segment:** {enrich['ares_primary_segment']}")

    emp = enrich.get("employee_count")
    emp_global = enrich.get("employee_count_global")
    scope = enrich.get("data_scope", "")
    if emp:
        label = "Czech local" if scope == "local" else "estimated" if scope == "estimate" else ""
        lines.append(f"- **Employees (CZ):** {emp}{f' ({label})' if label else ''}")
    if emp_global:
        lines.append(f"- **Employees (Global/Parent):** {emp_global}")

    rev = enrich.get("revenue")
    rev_global = enrich.get("revenue_global")
    if rev:
        lines.append(f"- **Revenue (CZ, EUR):** {rev:,.0f}")
    if rev_global:
        lines.append(f"- **Revenue (Global/Parent, EUR):** {rev_global:,.0f}")
    if enrich.get("revenue_czk"):
        lines.append(f"- **Revenue (kurzy.cz, CZK):** {enrich['revenue_czk']:,.0f}")

    if enrich.get("hiring_signal"):
        jobs = enrich.get("total_jobs", 0)
        eng = enrich.get("engineering_hiring", False)
        lines.append(f"- **Hiring signal:** Active ({jobs} job postings{', incl. engineering' if eng else ''})")
    if enrich.get("leadership_change"):
        lines.append(f"- **Leadership change detected:** Yes")

    cq = enrich.get("contact_quality_score", 0)
    pf = enrich.get("persona_fit", {})
    lines.append(f"- **Contact quality score:** {cq}/100")
    if pf.get("has_ideal_persona"):
        lines.append(f"- **Persona fit:** Has ideal persona for top upsell")
    elif pf.get("missing_personas"):
        lines.append(f"- **Missing personas:** {', '.join(pf['missing_personas'][:3])}")
        if pf.get("titles_to_find"):
            lines.append(f"- **Titles to find:** {', '.join(pf['titles_to_find'][:4])}")

    if enrich.get("zi_domain"):
        lines.append(f"- **Domain (ZoomInfo):** {enrich['zi_domain']}")

    if not lines:
        return "No enrichment data available yet. Use web research to gather this information."

    return "\n".join(lines)


def _format_contacts(enrich: dict) -> str:
    """Format CZ-validated contacts for the prompt."""
    contacts = enrich.get("contacts", [])
    if not contacts:
        return "No contacts identified yet. This is a key gap — the research should find decision makers."

    lines = []
    for c in contacts[:10]:
        name = c.get("full_name", "").strip()
        if not name:
            continue
        title = c.get("title", "—")
        source = c.get("source", "?")
        persona = c.get("adsk_persona", "unknown")
        email = c.get("email") or c.get("email_primary", "")
        email_note = " (estimated)" if not c.get("email") and c.get("email_primary") else ""
        linkedin = c.get("linkedin_url", "")

        line = f"- **{name}** — {title} [{source}] (persona: {persona})"
        if email:
            line += f"\n  - Email: {email}{email_note}"
        if linkedin:
            line += f"\n  - LinkedIn: {linkedin}"
        lines.append(line)

    return "\n".join(lines) if lines else "No named contacts found."


def _format_tech_stack(enrich: dict) -> str:
    """Format tech stack evidence for the prompt."""
    lines = []

    adsk_zi = enrich.get("zi_autodesk_tech") or []
    comp_zi = enrich.get("zi_competitor_tech") or []
    adsk_jobs = enrich.get("autodesk_tools_in_jobs") or []
    comp_jobs = enrich.get("competitor_tools_in_jobs") or []
    total = enrich.get("zi_tech_total", 0)

    if adsk_zi:
        lines.append(f"- **Autodesk products (ZoomInfo):** {', '.join(adsk_zi)}")
    if adsk_jobs:
        lines.append(f"- **Autodesk tools in job postings:** {', '.join(adsk_jobs)}")
    if comp_zi:
        lines.append(f"- **Competitor products (ZoomInfo):** {', '.join(comp_zi)}")
    if comp_jobs:
        lines.append(f"- **Competitor tools in job postings:** {', '.join(comp_jobs)}")
    if total:
        lines.append(f"- **Total technologies detected (ZI):** {total}")

    return "\n".join(lines) if lines else "No tech stack evidence collected. Research needed."


def _format_public_contracts(enrich: dict) -> str:
    """Format public contracts data for the prompt."""
    if not enrich.get("has_public_contracts"):
        return "No public contracts found in smlouvy.gov.cz."

    lines = []
    count = enrich.get("smlouvy_contracts_count", 0)
    aec = enrich.get("smlouvy_aec_contracts", 0)
    val = enrich.get("smlouvy_value_czk", 0)
    lines.append(f"- **Total contracts:** {count}")
    if aec:
        lines.append(f"- **AEC-related contracts:** {aec}")
    if val:
        lines.append(f"- **Total value:** CZK {val:,.0f}")

    return "\n".join(lines)


def _format_web_signals(briefs: dict, csn: str) -> str:
    """Format web signals from the auto-brief."""
    brief = briefs.get(csn, {})
    signals = brief.get("web_signals", [])
    if not signals:
        return "No web signals scraped yet. The research should look for recent news on the company website."

    lines = []
    for s in signals[:8]:
        date = s.get("date", "")
        headline = s.get("headline", "")
        cats = ", ".join(s.get("categories", []))
        url = s.get("source_url", "")
        line = f"- [{cats}] {headline}"
        if date:
            line = f"- **{date}** [{cats}] {headline}"
        if url:
            line += f" (source: {url})"
        lines.append(line)

    return "\n".join(lines)


def _format_persona_gap(enrich: dict, top_upsell: str) -> str:
    """Format persona gap analysis for the prompt."""
    pf = enrich.get("persona_fit", {})
    if pf.get("has_ideal_persona"):
        return "We already have an ideal persona contact for this upsell. Verify their current role and find additional stakeholders for multi-threading."

    missing = pf.get("missing_personas", [])
    titles = pf.get("titles_to_find", [])
    if missing or titles:
        lines = []
        if missing:
            lines.append(f"**Missing persona types:** {', '.join(missing)}")
        if titles:
            lines.append(f"**Priority titles to find:** {', '.join(titles)}")
        lines.append(f"These personas are critical for the {top_upsell} upsell motion.")
        return "\n".join(lines)

    return "Persona analysis not available."


def export_to_dashboard(research_md_path: str) -> str:
    """Import a completed research markdown file into the research dashboard.

    Parses the markdown using the dashboard's import_research parser and
    saves as a JSON account file.

    Returns the path to the created JSON file.
    """
    dashboard_root = Path(__file__).resolve().parent.parent.parent / "research-dashboard"
    sys.path.insert(0, str(dashboard_root))

    from import_research import parse_research

    with open(research_md_path, "r", encoding="utf-8") as f:
        md = f.read()

    account = parse_research(md)
    account["id"] = str(uuid.uuid4())
    account["lastUpdated"] = datetime.now().strftime("%Y-%m-%d")
    account["createdAt"] = datetime.now().strftime("%Y-%m-%d")
    account["rawMarkdown"] = md

    os.makedirs(str(DASHBOARD_DATA), exist_ok=True)
    out_path = str(DASHBOARD_DATA / f"{account['id']}.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(account, f, indent=2, ensure_ascii=False)

    return out_path


def print_prompt_summary(prompts: list):
    """Print summary of generated prompts."""
    print(f"\n{'=' * 60}")
    print("DEEP RESEARCH PROMPTS GENERATED")
    print(f"{'=' * 60}")
    print(f"Total prompts: {len(prompts)}")
    print(f"Output directory: {PROMPTS_DIR}")
    print(f"\n{'Rank':<6} {'Company':<45} {'File'}")
    print("-" * 90)
    for p in prompts:
        print(f"{p['rank']:<6} {p['company_name'][:44]:<45} {p['filename']}")

    print(f"\n--- How to use ---")
    print("1. Open each .md file in the prompts directory")
    print("2. Paste the prompt into a deep research tool (e.g., Perplexity, ChatGPT Deep Research)")
    print("3. Save the output as a .md file in research-dashboard/deep research/")
    print("4. Run: python -m scoring.deep_research_generator --dashboard <path_to_research.md>")
    print("   to import into the research dashboard")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Deep research prompt generator")
    parser.add_argument("--top", type=int, default=30, help="Generate for top N accounts")
    parser.add_argument("--input", default=None, help="Prioritized CSV path")
    parser.add_argument("--enrichment", default=None, help="Enrichment JSON path")
    parser.add_argument("--output", default=None, help="Output directory for prompts")
    parser.add_argument("--lang", default="en", choices=["en", "sk"],
                        help="Language for prompts: en (English) or sk (Slovak)")
    parser.add_argument(
        "--dashboard", default=None, metavar="RESEARCH_MD",
        help="Import a completed research .md file into the dashboard",
    )
    args = parser.parse_args()

    if args.dashboard:
        path = args.dashboard
        if not Path(path).exists():
            print(f"Error: {path} not found")
            sys.exit(1)
        out = export_to_dashboard(path)
        print(f"Imported research to dashboard: {out}")
        return

    project_root = Path(__file__).resolve().parent.parent
    workspace_root = project_root.parent

    input_csv = args.input or str(workspace_root / "prioritized_accounts.csv")
    enrichment_path = args.enrichment or str(ENRICHMENT_DIR / "account_enrichment.json")

    if not Path(input_csv).exists():
        print(f"Error: {input_csv} not found. Run prioritize.py first.")
        sys.exit(1)

    print(f"Loading top {args.top} accounts from {input_csv} ...")
    accounts = load_prioritized(input_csv, top_n=args.top)

    enrichment = load_enrichment(enrichment_path)
    print(f"Enrichment data for {len(enrichment)} accounts")

    briefs_path = project_root.parent / "account_briefs.json"
    briefs = {}
    if briefs_path.exists():
        with open(str(briefs_path)) as f:
            briefs = json.load(f)
        print(f"Loaded {len(briefs)} account briefs")

    output_dir = args.output or str(PROMPTS_DIR)
    print(f"Generating prompts to {output_dir} ...")

    prompts = generate_prompts(accounts, enrichment, briefs=briefs, output_dir=output_dir, lang=args.lang)
    print_prompt_summary(prompts)


if __name__ == "__main__":
    main()
