"""Slovak-language outreach templates for Autodesk CZ/SK territory.

Persona-specific email templates, LinkedIn messages, and signal hooks
all in Slovak. Used by the campaign orchestrator to generate personalized
outreach without needing an LLM call.
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────
# Email templates per play × persona
# ─────────────────────────────────────────────────────────────────────

EMAIL_TEMPLATES = {
    "signal_triggered": {
        "champion": {
            "subject": "{company_name} — {signal_hook_short}",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "{signal_hook}\n\n"
                "Spolupracujem s {segment_label} tímami v Českej a Slovenskej "
                "republike a vidím, že spoločnosti v podobnej situácii ako {company_name} "
                "riešia {pain_point}.\n\n"
                "Mnoho z nich to vyriešilo tým, že {value_prop}.\n\n"
                "Mali by ste 15 minút na krátky hovor o tom, "
                "či to dáva zmysel aj pre váš tím?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
        "economic_buyer": {
            "subject": "{company_name} — {signal_hook_short}",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "{signal_hook}\n\n"
                "Pracujem s firmami vo vašom odvetví v celom regióne a "
                "často vidím, že {pain_point}.\n\n"
                "Firmy vašej veľkosti typicky {value_prop}, "
                "čo im ušetrilo 20–30 % na nákladoch na softvér.\n\n"
                "Stálo by to za 15-minútový hovor?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
        "technical_influencer": {
            "subject": "{company_name} — {signal_hook_short}",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "{signal_hook}\n\n"
                "Ako človek zodpovedný za technologickú infraštruktúru "
                "v {company_name} možno riešite {pain_point}.\n\n"
                "Rád by som sa s vami podelil o to, ako to riešia "
                "podobné firmy — {value_prop}.\n\n"
                "Dalo by sa nájsť 15 minút?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
        "executive_sponsor": {
            "subject": "{company_name} — {signal_hook_short}",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "{signal_hook}\n\n"
                "Spolupracujem s vedením firiem v {segment_label} sektore "
                "v regióne CZ/SK a vidím, že spoločnosti, ktoré {value_prop}, "
                "dosahujú výrazne lepšie výsledky.\n\n"
                "Stálo by za to stručný hovor o tom, "
                "či je to relevantné pre {company_name}?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
        "end_user_leader": {
            "subject": "{company_name} — {signal_hook_short}",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "{signal_hook}\n\n"
                "Ako vedúci tímu v {company_name} pravdepodobne riešite "
                "{pain_point}.\n\n"
                "Rád by som vám ukázal, ako {value_prop} — "
                "videl som to fungovať u podobných tímov v regióne.\n\n"
                "Našli by ste si 15 minút?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
    },
    "upsell": {
        "_default": {
            "subject": "{company_name} — rozšírenie {current_products_short}",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "Vidím, že {company_name} aktuálne využíva {current_products}. "
                "{signal_line}"
                "Pracujem s podobnými firmami v {segment_label} sektore a "
                "často vidím, že pridanie {upsell_product} do existujúceho "
                "workflow výrazne pomáha s {pain_point}.\n\n"
                "Mali by ste 15 minút prediskutovať, "
                "či to dáva zmysel aj pre váš tím?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
    },
    "cold_intro": {
        "_default": {
            "subject": "{company_name} + Autodesk — {segment_label} workflow",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "Spolupracujem s {segment_label} tímami naprieč Českou "
                "a Slovenskou republikou a zaujala ma {company_name}. "
                "{signal_line}"
                "Firmy podobnej veľkosti a zamerania často riešia "
                "{pain_point} — a najúspešnejšie z nich to riešia tak, "
                "že {value_prop}.\n\n"
                "Dalo by sa nájsť 15 minút na krátky hovor?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
    },
    "renewal_followup": {
        "_default": {
            "subject": "{company_name} — obnovenie predplatného Autodesk",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "Blíži sa obnovenie vášho predplatného Autodesk "
                "({current_products}) a rád by som sa s vami stretol "
                "a prediskutoval, ako vám aktuálne nástroje slúžia.\n\n"
                "Od posledného hodnotenia pribudlo niekoľko zaujímavých "
                "noviniek — napríklad {upsell_product}, ktorý by mohol "
                "pomôcť s {pain_point}.\n\n"
                "Našli by ste si 15 minút na strategický prehľad?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
    },
    "event_intro": {
        "_default": {
            "subject": "{event_name} — stretnutie s {company_name}",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "Všimol som si, že {company_name} bude na {event_name}. "
                "Budem tam tiež a rád by som sa s vami stretol.\n\n"
                "{signal_line}"
                "Pracujem s {segment_label} firmami v regióne CZ/SK "
                "a rád by som sa s vami podelil o to, "
                "ako podobné spoločnosti riešia {pain_point}.\n\n"
                "Mali by ste 15 minút na {event_name} alebo pred ním?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
    },
    "re_engagement": {
        "_default": {
            "subject": "{company_name} — novinky od Autodesku",
            "body": (
                "Dobrý deň {first_name},\n\n"
                "Uplynul nejaký čas od našej poslednej komunikácie a "
                "medzitým sa v Autodesku udialo niekoľko zaujímavých vecí, "
                "ktoré sú relevantné pre {segment_label} firmy ako {company_name}.\n\n"
                "{signal_line}"
                "Dávalo by zmysel sa znovu spojiť na 15 minút?\n\n"
                "S pozdravom,\nMartin"
            ),
        },
    },
}


# ─────────────────────────────────────────────────────────────────────
# Signal hooks in Slovak
# ─────────────────────────────────────────────────────────────────────

SIGNAL_HOOKS_SK = {
    "upsell_hiring": {
        "hook": (
            "Všimol som si, že {company_name} hľadá {hiring_roles} — "
            "to zvyčajne znamená, že tím rastie a investuje do tejto oblasti."
        ),
        "short": "rast tímu a nové pozície",
    },
    "hiring_general": {
        "hook": (
            "Vidím, že {company_name} aktívne prijíma nových ľudí "
            "({total_jobs} otvorených pozícií) — to je silný signál rastu."
        ),
        "short": "aktívny nábor nových ľudí",
    },
    "eu_grants": {
        "hook": (
            "Zaregistroval som, že {company_name} získala dotáciu "
            "na {grant_summary} — to je skvelá príležitosť investovať "
            "získané prostriedky do nástrojov, ktoré to podporia."
        ),
        "short": "získaná dotácia na digitalizáciu",
    },
    "competitor_tools": {
        "hook": (
            "Všimol som si, že váš tím pracuje aj s {competitor_tools} — "
            "mnoho firiem v regióne konsoliduje na jednu platformu "
            "pre lepšiu interoperabilitu a nižšie náklady."
        ),
        "short": "konsolidácia nástrojov",
    },
    "public_contracts": {
        "hook": (
            "Gratulujeme k verejným zákazkám — projekty tejto veľkosti "
            "zvyčajne vyžadujú škálovateľné nástroje pre celý tím."
        ),
        "short": "verejné zákazky a škálovanie",
    },
    "facility_expansion": {
        "hook": (
            "Zaregistroval som, že {company_name} expanduje — "
            "nové priestory alebo výrobná linka zvyčajne znamenajú "
            "potrebu rozšíriť aj technologické zázemie."
        ),
        "short": "expanzia a nové priestory",
    },
    "digital_transformation": {
        "hook": (
            "Vidím, že {company_name} investuje do digitálnej transformácie — "
            "to je presne oblasť, kde Autodesk pomáha firmám vo vašom sektore."
        ),
        "short": "digitálna transformácia",
    },
    "ma_activity": {
        "hook": (
            "Zaregistroval som zmeny vo vlastníckej štruktúre {company_name} — "
            "to je zvyčajne moment, kedy firmy prehodnocujú "
            "a štandardizujú svoj softvérový stack."
        ),
        "short": "zmena vlastníctva a štandardizácia",
    },
    "certifications": {
        "hook": (
            "Všimol som si, že {company_name} sa orientuje na certifikácie "
            "({cert_summary}) — to zvyčajne vyžaduje robustnú "
            "infraštruktúru pre správu dát a dokumentáciu."
        ),
        "short": "certifikácie a compliance",
    },
    "esg": {
        "hook": (
            "Vidím, že {company_name} venuje pozornosť ESG a udržateľnosti — "
            "Autodesk ponúka nástroje pre analýzu životného cyklu "
            "a energetickú optimalizáciu priamo v design workflow."
        ),
        "short": "ESG a udržateľnosť",
    },
    "revenue_growth": {
        "hook": (
            "Vidím, že {company_name} výrazne rastie — "
            "rýchlo rastúce firmy potrebujú technológie, "
            "ktoré rastú s nimi."
        ),
        "short": "rast obratu firmy",
    },
}


# ─────────────────────────────────────────────────────────────────────
# Pain points in Slovak by persona × segment
# ─────────────────────────────────────────────────────────────────────

PAIN_POINTS_SK = {
    ("champion", "AEC"): [
        "koordináciu medzi viacerými nástrojmi a disciplínami",
        "detekciu kolízií a presnosť modelov",
        "spoluprácu na projektoch naprieč tímami",
        "splnenie BIM mandátov (NDA 40/2025)",
    ],
    ("champion", "D&M"): [
        "pomalý prechod z návrhu do výroby",
        "prepínanie medzi rôznymi nástrojmi pre dizajn, simuláciu a CAM",
        "dátové silá medzi konštrukciou a výrobou",
        "správu kusovníkov naprieč systémami",
    ],
    ("economic_buyer", "AEC"): [
        "vysoké náklady na neintegrované bodové riešenia",
        "ťažko merateľnú návratnosť investícií do BIM",
        "rastúce požiadavky klientov na digitálne výstupy",
    ],
    ("economic_buyer", "D&M"): [
        "fragmentovanú sadu nástrojov, ktorá zvyšuje overhead",
        "dlhý čas uvedenia produktu na trh",
        "výrobné chyby z odpojeného dizajnového procesu",
    ],
    ("technical_influencer", "AEC"): [
        "správu aktualizácií a nasadení naprieč desiatkami licencií",
        "bezpečnosť projektových dát v cloudovom prostredí",
        "licenčnú compliance a shadow IT",
    ],
    ("technical_influencer", "D&M"): [
        "integráciu CAD dát s ERP/MRP systémami",
        "správu verzií naprieč rôznymi CAD formátmi",
        "ochranu duševného vlastníctva v dizajnových súboroch",
    ],
    ("executive_sponsor", "AEC"): [
        "digitálnu transformáciu a návratnosť investícií",
        "konkurencieschopnosť a rýchlosť inovácií",
        "retenciu talentov cez moderné nástroje",
    ],
    ("executive_sponsor", "D&M"): [
        "rýchlosť inovácií a konkurenčný tlak",
        "efektivitu prevádzky od návrhu po dodanie",
        "celkové náklady na vlastníctvo softvéru",
    ],
    ("end_user_leader", "AEC"): [
        "produktivitu tímu pri každodennej práci s nástrojmi",
        "zdieľanie modelov s externými partnermi",
        "automatizáciu opakujúcich sa úloh v pracovnom postupe",
    ],
    ("end_user_leader", "D&M"): [
        "efektivitu konštrukčných iterácií",
        "prechod z modelu do CNC programovania",
        "prácu s komplexnými zostavami",
    ],
}

VALUE_PROPS_SK = {
    ("champion", "AEC"): "konsolidovali nástroje do AEC Collection + BIM Collaborate Pro pre cloudovú koordináciu",
    ("champion", "D&M"): "prepojili dizajn, simuláciu a CAM v jednej platforme cez Fusion",
    ("economic_buyer", "AEC"): "prešli na AEC Collection a ušetrili 30–40 % na licenciách s prístupom k celému portfóliu",
    ("economic_buyer", "D&M"): "zjednotili dizajn a výrobu na PDMC pre nižšie celkové náklady",
    ("technical_influencer", "AEC"): "prešli na menované licencie a eliminovali správu licenčných serverov",
    ("technical_influencer", "D&M"): "nasadili Vault alebo Fusion Manage pre bezpečnú správu dát s integráciou do ERP",
    ("executive_sponsor", "AEC"): "investovali do platformovej konsolidácie a dosiahli 15–25 % nárast produktivity",
    ("executive_sponsor", "D&M"): "investovali do integrovanej platformy a skrátili čas uvedenia na trh o 40 %",
    ("end_user_leader", "AEC"): "automatizovali koordináciu a znížili počet RFI o 30 %",
    ("end_user_leader", "D&M"): "integrovali CAD/CAM workflow a skrátili čas prípravy výroby o 50 %",
}

SEGMENT_LABELS_SK = {
    "AEC": "stavebníctva a projektovania",
    "D&M": "strojárenstva a výroby",
    "M&E": "médií a zábavy",
    "unknown": "technológií a dizajnu",
    "": "technológií a dizajnu",
}


# ─────────────────────────────────────────────────────────────────────
# LinkedIn message templates in Slovak
# ─────────────────────────────────────────────────────────────────────

LINKEDIN_TEMPLATES_SK = {
    "connection_request": (
        "Dobrý deň {first_name}, spolupracujem s {segment_label} "
        "firmami v CZ/SK regióne na Autodesk riešeniach. "
        "Zaujala ma {company_name} — rád by som sa spojil "
        "a zdieľal relevantné poznatky z odvetvia."
    ),
    "followup_inmail": (
        "Dobrý deň {first_name}, posielal som vám email ohľadne "
        "{signal_hook_short} v {company_name}. Rád by som sa s vami "
        "spojil na 15 minút — mám niekoľko zaujímavých poznatkov "
        "od podobných firiem v regióne. Kedy by sa vám hodilo?"
    ),
}
