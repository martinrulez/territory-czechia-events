"""Entity classifier for event company names.

Determines whether a scraped name represents a real B2B company,
an individual person, an academic institution, or an irrelevant entity
(food vendor, scrape artifact, exhibition name, etc.).

Also provides ICP (Ideal Customer Profile) relevance checking for
Autodesk verticals: AEC, D&M, and M&E.

Used as a guardrail before inserting into the event_companies table.
"""

import json
import re
import unicodedata
from pathlib import Path
from typing import Optional

_ENRICHMENT_PATH = Path(__file__).resolve().parent.parent / "enrichment_data" / "event_company_enrichment.json"
_enrichment_cache: Optional[dict] = None


def _load_enrichment() -> dict:
    global _enrichment_cache
    if _enrichment_cache is None:
        try:
            with open(_ENRICHMENT_PATH, "r") as f:
                _enrichment_cache = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            _enrichment_cache = {}
    return _enrichment_cache


# NACE 2-digit prefixes that map to Autodesk verticals
_ADSK_ICP_NACE = {
    # AEC — Architecture, Engineering & Construction
    "41", "42", "43",   # construction of buildings, civil engineering, specialized construction
    "71",               # architectural & engineering activities

    # D&M — Design & Manufacturing
    "22",               # rubber & plastic products
    "24",               # basic metals
    "25",               # fabricated metal products
    "26",               # computer, electronic, optical products
    "27",               # electrical equipment
    "28",               # machinery & equipment
    "29",               # motor vehicles
    "30",               # other transport equipment
    "33",               # repair & installation of machinery

    # M&E — Media & Entertainment
    "59",               # motion picture, video, television
    "60",               # broadcasting
    "62",               # software & IT services

    # Cross-segment
    "72",               # scientific R&D
}

# NACE codes that frequently cause false positives — they technically match
# but the businesses using them are almost never ADSK prospects
_NACE_FALSE_POSITIVE = {
    # Trade / retail — not end-users of engineering software
    "461", "471", "4690", "469", "46900", "47",
    # Real estate — landlords, not architects
    "68200", "6820", "68310", "68320",
    # Advertising, PR, management consulting
    "731", "73110", "70200", "702", "70220",
    # Office admin / facilities management
    "82100", "821", "82920",
    # Cultural / artistic activities — not B2B engineering
    "9102", "91210", "9499", "9001", "9002", "90310", "90020", "90040", "90390",
    # Design activities (fashion, graphic, product — too broad for ADSK ICP;
    # real architecture/engineering firms register under 711, not 741)
    "741", "74100", "742", "74200",
    # Photography
    "74300",
    # Other professional activities (catch-all)
    "74990", "749",
    # Education
    "855",
    # Rental of personal/household goods
    "772",
    # Translation
    "74300",
    # Unclassified
    "00", "G", "K",
}


# For low-relevance events (design festivals, consumer events), only these
# NACE codes are strong enough evidence of Autodesk ICP fit.  The broad
# codes (43=specialized construction, 22=plastics, 25=metal, 26=electronics,
# 32=other manufacturing, 33=repair) are too noisy at design festivals where
# every artisan brand registers some manufacturing/construction code.
_ADSK_ICP_NACE_STRICT = {
    "41",   # building construction
    "42",   # civil engineering
    "71",   # architectural & engineering activities
    "28",   # machinery & equipment
    "29",   # motor vehicles
    "30",   # other transport equipment
    "59",   # film/video/TV
    "62",   # software & IT
}


def check_icp_relevance(company_name: str, strict: bool = False) -> Optional[dict]:
    """Check if a company's NACE codes indicate Autodesk ICP relevance.

    Args:
        company_name: Name to look up in enrichment data.
        strict: If True, use a narrower set of NACE codes (for low-relevance
                events where broad codes produce too many false positives).

    Returns None if no enrichment data exists (can't determine).
    Returns dict with 'is_icp', 'adsk_vertical', 'reject_reason' otherwise.
    """
    enrichment = _load_enrichment()
    key = company_name.lower().strip()
    entry = enrichment.get(key, {})
    nace_codes = entry.get("nace_codes", [])

    if not nace_codes:
        return None

    real_nace = [str(n) for n in nace_codes if str(n) not in _NACE_FALSE_POSITIVE]
    nace_set = _ADSK_ICP_NACE_STRICT if strict else _ADSK_ICP_NACE

    # For strict mode, require the NACE hit to be among the first 5
    # non-generic codes (primary activity), not a buried secondary code.
    check_nace = real_nace[:5] if strict else real_nace

    for nace in check_nace:
        prefix2 = nace[:2]
        if prefix2 in nace_set:
            vertical = "AEC" if prefix2 in ("41", "42", "43", "71") else \
                       "D&M" if prefix2 in ("22", "24", "25", "26", "27", "28", "29", "30", "33", "72") else \
                       "M&E"
            return {"is_icp": True, "adsk_vertical": vertical, "reject_reason": None}

    mode_label = "strict" if strict else "standard"
    return {
        "is_icp": False,
        "adsk_vertical": None,
        "reject_reason": f"NACE codes not in Autodesk ICP verticals ({mode_label})",
    }


# -----------------------------------------------------------------------
# 1. SCRAPE ARTIFACT DETECTION — highest priority, catches garbage early
# -----------------------------------------------------------------------

def _is_scrape_artifact(name: str) -> str:
    """Detect web-scraping junk: image filenames, CSS class names,
    navigation elements, timestamps, HTML fragments.
    Returns a reject reason string, or empty string if clean."""

    # Image/file extensions
    if re.search(r"\.(png|jpg|jpeg|gif|svg|webp|pdf|bmp|ico)$", name, re.IGNORECASE):
        return "image/file reference"

    # Looks like an internal ID or timestamp: mostly digits/underscores
    stripped = name.replace("-", "").replace("_", "").replace(" ", "")
    if len(stripped) > 3 and sum(c.isdigit() for c in stripped) / len(stripped) > 0.6:
        return "numeric ID or timestamp"

    # CamelCase internal identifiers (e.g. "CF2025header", "PrezentaceCF_CZ_...")
    if re.match(r"^[A-Z][a-z]+[A-Z]", name) and "_" in name:
        return "internal identifier"

    # Image alt-text patterns from scrapers: "MediaPartner_", "logo_", etc.
    if re.search(r"(?:media.?partner|_logo|_header|_banner|_cropped|_kopi[ae]|\bkopie\b|header$)", name, re.IGNORECASE):
        return "image alt-text / media asset"

    # Internal codes: all-caps + digits with no spaces (e.g. "SPS2020", "KZPS_BASIC__NEW_RGB-02")
    no_sep = name.replace("_", "").replace("-", "")
    if re.match(r"^[A-Z0-9]+$", no_sep) and any(c.isdigit() for c in no_sep) and len(name) <= 30:
        return "internal code or reference"

    # Scraped usernames — all-lowercase, short, no spaces (e.g. "janburian1", "mhorak", "cejpek-1")
    if re.match(r"^[a-z][a-z0-9_-]{2,15}$", name) and not name.isalpha():
        return "scraped username"
    if re.match(r"^[a-z]{2,12}$", name) and name not in ("brno", "praha", "ostrava"):
        if not any(c in name for c in "áéíóúůýčďěňřšťžæøå"):
            return "scraped username or slug"

    # "kopia" / "kopie" patterns — duplicated/copied scrape artifacts
    if re.search(r"\bkopi[ae]\b", name, re.IGNORECASE) and len(name.split()) <= 3:
        return "copy/duplicate artifact"

    # "Exhibitors by category ..." — URBIS catalog section headers
    if name.lower().startswith("exhibitors by category"):
        return "catalog section header"

    # Product model patterns from ASIO/BVV: "AS-REWA", "AS-GREEN SLOPE", etc.
    if re.match(r"^[A-Z]{2,4}-[A-Z]", name) and "treatment" not in name.lower():
        if any(kw in name.lower() for kw in ("tank", "fence", "slope", "channel", "rewa", "green", "grey water")):
            return "product model name"

    # Navigation text from Czech websites
    _nav_phrases = {
        "about us", "aktuality", "archiv konferencí", "archiv", "kontakt",
        "historie svazu", "o nás", "partneři", "úvodní strana",
        "novinky", "certifikační orgány", "dodavatelé a certifikáty",
        "kompetence", "kolektivní smlouva", "dotované vzdělávání",
        "future builders", "bytová výstavba", "language picture",
        "přihlásit se", "registrace", "průvodce konferencí",
        "protection of whistleblowers", "brussels representation",
        # SPS Construction & association website nav
        "mapa stránek", "newsletter - archiv", "organizační struktura",
        "pravidla systému", "prezentace", "seznam členů",
        "služby pro členy", "stavebnictví v číslech", "stavíme budoucnost",
        "technické normy", "proč být členem",
        "zásady ochrany osobních údajů", "řídící dokumenty",
        "prevence ve stavebnictví",
        "zastoupení (mezinárodní, česká)",
        "připravované projekty", "ukončené projekty",
        "posílení úrovně sociálního dialogu",
        # BIM association website nav
        "skupina pro bim", "skupina pro esg",
        "skupina pro iniciativu building future",
        "skupina pro legislativu", "skupina pro sociální otázky",
        # Generic association nav
        "projekt ieptt", "projekt ior",
        # Czech nav/action phrases that get scraped as entity names
        "jak se stát členem", "pro média",
        "ke stažení", "fotogalerie", "videogalerie",
        "výroční zprávy", "etický kodex",
    }
    nl = name.lower().strip()
    if nl in _nav_phrases:
        return "website navigation text"
    for phrase in _nav_phrases:
        if nl.startswith(phrase):
            return "website navigation text"

    # Signature-block-style entries (e.g. "podpis_Page_1")
    if "podpis" in nl or "signature" in nl:
        return "signature block artifact"

    return ""


def _is_product_description(name: str) -> str:
    """Detect exhibit/product labels scraped as company names.
    Returns a reject reason string, or empty string if clean."""

    name_lower = name.lower()

    # Technical specs / measurements
    if re.search(r"\d+\s*(?:kw|kwh|mm|cm|ton|hp|mw|bar|rpm)\b", name_lower):
        return "product spec with measurement"

    # Vehicle / equipment descriptions
    vehicle_words = [
        "vehicle", "charger", "terminal", "loader", "utility",
        "electric wheel", "electric utility", "automated driv",
        "e:car", "e:bike", "e:bus",
    ]
    for vw in vehicle_words:
        if vw in name_lower:
            return "product/vehicle description"

    # Long phrases (>50 chars) with many lowercase words — likely descriptions
    words = name.split()
    if len(words) >= 5:
        lowercase_words = sum(1 for w in words if w[0].islower() and w.isalpha())
        if lowercase_words >= 3:
            return "long product/exhibit description"

    # "Something for something" / "Something of something" patterns
    if re.search(r"\bfor the\b|\bfor\s+\w+\s+\w+\b", name_lower) and len(words) >= 5:
        return "descriptive phrase"

    # Patterns like "digital platform" or "Brand ProductName"
    if re.search(r"\b(?:platform|system|solution|dashboard|insights|traffic|luminaire|digitalization)\b", name_lower) and len(words) >= 3:
        return "product/platform name"

    # Brand + Model pattern: "BrandName AlphaNumericModel" (e.g. "Fuso eCanter 7C18")
    if len(words) >= 2 and re.search(r"[A-Za-z]+\d+[A-Za-z]*\d*$", words[-1]):
        return "product model reference"

    # "Smart City Awards" style event names
    if re.search(r"\bawards?\b|\bkonference\b|\bsummit\b|\bcongress\b", name_lower):
        return "event/award name"

    # Infrastructure product lines: "Wastewater treatment plant AS-MONOcomp"
    infra_products = [
        "wastewater", "treatment plant", "rainwater tank",
        "shower trough", "vegetation bag", "grey water",
        "green fence", "green slope", "drain channel",
    ]
    for ip in infra_products:
        if ip in name_lower:
            return "infrastructure product name"

    return ""


# -----------------------------------------------------------------------
# 2. LEGAL-SUFFIX COMPANY DETECTION
# -----------------------------------------------------------------------

_COMPANY_SUFFIXES_ANYWHERE = [
    r"\bs\.?\s?r\.?\s?o\.?\b",
    r"\ba\.\s?s\.?\b",
    r"\bspol\.\s*s\s*r\.?\s*o\.?\b",
    r"\bv\.o\.s\.?\b",
    r"\bk\.s\.?\b",
    r"\bgmbh\b",
    r"\bltd\.?\b",
    r"\binc\.?\b",
    r"\bcorp\.?\b",
    r"\bllc\b",
    r"\bplc\b",
    r"\bsp\.\s*z\s*o\.?\s*o\.?\b",   # Polish sp. z o.o.
    r"\bkft\.?\b",                     # Hungarian Kft.
    r"\bzrt\.?\b",                     # Hungarian Zrt.
    r"\bd\.o\.o\.?\b",                 # Slovenian/Croatian d.o.o.
    r"\bs\.r\.l\.?\b",                 # Italian/Romanian S.r.l.
    r"\bsrl\b",
    r"\bs\.a\.s\.?\b",
    r"\bs\.p\.a\.?\b",
    r"\bgroup\b",
    r"\bholding\b",
    r"\bz\.s\.?\b",
    r"\bz\.ú\.?\b",
    r"\bv\.v\.i\.?\b",                 # Czech public research institution
    r"\bdružstvo\b",
]

# Short 2-letter suffixes that commonly collide with normal words;
# only count them when they appear at the very end of the name.
_COMPANY_SUFFIXES_END_ONLY = [
    r"\bag$",    # German AG
    r"\bse$",    # Societas Europaea
    r"\bsa$",    # Société Anonyme
    r"\bnv$",    # Dutch NV
    r"\bsas$",   # French SAS
    r"\bspa$",   # Italian SpA
]

_COMPANY_SUFFIX_RE = re.compile(
    "|".join(_COMPANY_SUFFIXES_ANYWHERE + _COMPANY_SUFFIXES_END_ONLY),
    re.IGNORECASE,
)

# Weaker company signals — only counted when there's no countervailing
# evidence (e.g. person name).  These words commonly appear in real
# company names but also in exhibition labels.
_COMPANY_WEAK_SIGNALS = [
    r"\bsystems\b", r"\btechnolog(?:y|ies)\b", r"\bsolutions\b",
    r"\bengineering\b", r"\bindustries\b", r"\bmanufacturing\b",
    r"\bconsulting\b", r"\bsoftware\b", r"\binteractive\b",
    r"\bproduction[s]?\b", r"\bgames\b", r"\bpartners\b",
    r"\bautomation\b", r"\belectronics\b", r"\brobotic[s]?\b",
    r"\baerospace\b", r"\bhydrauli[ck]\b", r"\bpneumati[ck]\b",
    r"\bmachines?\b", r"\bmechanik\b", r"\btools?\b",
    r"\bpipes?\b", r"\bvalves?\b", r"\bpumps?\b",
    r"\bplastics?\b", r"\bmetals?\b",
    r"\bnetwork\b", r"\bservices?\b", r"\bresearch\b",
]
_COMPANY_WEAK_RE = re.compile(
    "|".join(_COMPANY_WEAK_SIGNALS), re.IGNORECASE
)


# -----------------------------------------------------------------------
# 3. INSTITUTION DETECTION
# -----------------------------------------------------------------------

_INSTITUTION_KEYWORDS = [
    "univerzita", "university", "universität", "fakulta", "faculty",
    "učení technické", "technické v ",
    "čvut", "všb", "ujep", "umprum", "vut brno", "vutbr",
    "fdu", "favu", "fmu", "všup", "fu tuke", "fmk utb",
    "akademi", "academy",
    "škol", "school",
    "institut", "institute",
    "polytechnic", "hochschule",
    "ústav",
    "priemyselná škola",
]
_INSTITUTION_RE = re.compile(
    "|".join(re.escape(k) for k in _INSTITUTION_KEYWORDS), re.IGNORECASE
)


# -----------------------------------------------------------------------
# 4. IRRELEVANT ENTITY KEYWORDS
# -----------------------------------------------------------------------

_IRRELEVANT_KEYWORDS = [
    # Food / drink
    "food truck", "food stand", "streetfood", "street food",
    "kavárna", "café", "cafe ",
    "pivovar", "brewery",
    "espresso", "coffee",
    "cukrář", "cukrárna", "bakery",
    "pizza", "pizzerie",
    "restaurac", "restaurant",
    "bistro",
    "roastery",
    "churros",
    "lokše",
    "vino ", "wine bar",
    "syráreň",
    ".menu", "menu ",
    "rooftop",
    # Exhibition / festival artifacts
    "showroom",
    "pop-up", "popup",
    "designblok shop", "designblok talent",
    "relax zóna", "kreativní zóna", "pumptrack",
    "talent cards",
    "for kids", "pro děti", "dětem",
    "decoration",
    "streetfood",
    # Jewelry / fashion / cosmetics (not ADSK-relevant B2B)
    "jewelry", "jewellery", ".jewelry",
    "fashion label",
    "cosmetics",
    # Art galleries / museums (not B2B targets)
    "gallery", "galerie",
    "obrazy",
    "vířivek",
    "museum",
    "památník",
    # Publishing / media (not ADSK B2B)
    "nakladatelství",
    "vogue",
    "radiožurnál",
    "shop",
    # Self-referencing event names
    "bimday", "bimnews", "bimas ",
    "cadfórum", "cadforum",
    "lean summit",
    "connected construction days",
    # Brand activations / non-B2B
    "relosy",
    "pumptrack",
    "cirkulární",
    # Retail / consumer brands (not ADSK B2B)
    "kaufland", "hornbach", "bang & olufsen",
    "minotti", "casa moderna",
    # Foundations / philharmonic / cultural (not ADSK B2B)
    "nadační fond", "filharmon",
    # Trade fair self-references
    "bvv trade fairs",
    # Association internal pages
    "tačr esg",
]
_IRRELEVANT_RE = re.compile(
    "|".join(re.escape(k) for k in _IRRELEVANT_KEYWORDS), re.IGNORECASE
)


# -----------------------------------------------------------------------
# 5. GOVERNMENT / PUBLIC SECTOR
# -----------------------------------------------------------------------

_GOV_KEYWORDS = [
    "ministerstvo", "ministry",
    "magistrát", "městský úřad",
    "krajský úřad",
    "správa železnic", "správa komunikací",
    "city of ", "město ",
    "czech chamber", "komora",
    "agentura pro standardizaci",
    "marshal office", "voivodship",
    "letiště", "airport",
]
_GOV_RE = re.compile(
    "|".join(re.escape(k) for k in _GOV_KEYWORDS), re.IGNORECASE
)


# -----------------------------------------------------------------------
# 6. PERSON-NAME DETECTION
# -----------------------------------------------------------------------

_CZ_FIRST_NAMES = {
    # Czech names
    "adam", "adéla", "alena", "aleš", "alexandr", "alice", "alžběta",
    "andrea", "aneta", "anežka", "anna", "antonín",
    "barbora", "bedřich", "blanka", "bohdan", "bohumil", "bohuslav",
    "boris", "božena",
    "dagmar", "dalibor", "dana", "daniel", "daniela", "darina",
    "david", "denisa", "diana", "dita", "dominik", "dominika",
    "elena", "eliška", "ema", "emil", "eva",
    "filip", "františek",
    "gabriela", "hana", "helena", "iva", "ivan", "ivana", "iveta",
    "jakub", "jan", "jana", "jarmila", "jaromír", "jaroslav",
    "jindřich", "jiří", "jiřina", "josef", "josefina",
    "kamil", "kamila", "karel", "karin", "karina", "kateřina",
    "klára", "klaudia", "kristýna", "květa",
    "ladislav", "lenka", "leona", "libuše", "lucie", "ludmila",
    "lukáš", "luděk",
    "magdaléna", "marcela", "marek", "margareta", "marie", "marina",
    "markéta", "marta", "martin", "martina", "matěj", "michaela",
    "michal", "milan", "milena", "miloš", "miroslav", "monika",
    "natálie", "nikola", "nikolas",
    "oldřich", "olga", "ondřej", "otakar",
    "patrik", "pavel", "pavla", "pavlína", "petra", "petr",
    "radek", "radka", "radoslav", "renata", "richard", "robert",
    "romana", "rudolf", "růžena",
    "simona", "soňa", "stanislav", "svatopluk", "šimon", "šárka",
    "štěpán", "štěpánka",
    "tereza", "tomáš", "václav", "valerie", "vendula", "veronika",
    "viktor", "viktorie", "vilém", "vladimír", "vlasta", "vojtěch",
    "zbyněk", "zdeněk", "zdeňka", "zuzana", "žaneta", "žofia",
    # Slovak
    "gréta", "janka", "katka", "ľubomír", "mátyás",
    # International names appearing in CZ event data
    "alexander", "aleksandar", "astrid", "charlie", "christian",
    "christopher", "clara", "claudia", "conrad", "konrad",
    "elisa", "elizabeth", "emma", "erwan",
    "felix", "florian", "frank", "frederik", "friedrich",
    "hans", "heidi", "helen", "hugo",
    "ingrid", "isabel",
    "james", "janja", "jaron", "jason", "jennifer", "john",
    "johan", "jonathan", "julia", "juliana",
    "karl", "katarina", "kenny", "kim", "klaus",
    "laura", "leon", "liam", "lieu", "linda", "lisa", "louise", "lucas",
    "magnus", "marc", "marco", "marcus", "margot", "maria", "marian",
    "mario", "mark", "mascha", "max", "maxim", "mayar", "michael", "mona",
    "nastassia", "nate", "nick", "nina", "nonna", "nora", "nozomi",
    "oliver", "oliwia", "oscar", "otto",
    "patricia", "patrick", "paul", "paula", "peter", "philip", "pierre",
    "rachel", "rebecca", "robin", "rony", "rosa",
    "samuel", "sandra", "sara", "sarah", "sebastian", "silvia",
    "sophie", "stefan", "szymon",
    "taiga", "tatiana", "thomas", "travis",
    "victor", "vincent", "wiktoria",
    "walter", "wolfgang", "youngjin",
    "bartosz", "izaak", "kári",
    # Titles as first words
    "ing", "mgr", "doc", "prof", "bc", "mudr", "judr", "rndr", "phdr",
}


def _strip_diacritics(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


_CZ_FIRST_NAMES_NORM = _CZ_FIRST_NAMES | {
    _strip_diacritics(n) for n in _CZ_FIRST_NAMES
}


def _first_word_is_name(name: str) -> bool:
    """Check if the first word of the string is a known first name."""
    first = name.split()[0] if name.split() else ""
    first_clean = _strip_diacritics(first).lower().rstrip(".,:")
    return first_clean in _CZ_FIRST_NAMES_NORM


def _looks_like_person_name(name: str) -> bool:
    """Heuristic: is this likely a person's name rather than a company?"""
    parts = name.split()
    if len(parts) < 2:
        return False

    if any(c.isdigit() for c in name):
        return False

    first = _strip_diacritics(parts[0]).lower().rstrip(".,:")

    # "Firstname Lastname" — first word is a known name
    if first in _CZ_FIRST_NAMES_NORM:
        return True

    # "Firstname & Firstname" or "Name x Name" collaboration pattern
    if "&" in name or " x " in name.lower():
        sub_parts = re.split(r"\s+&\s+|\s+x\s+", name, flags=re.IGNORECASE)
        names_found = 0
        for sub in sub_parts:
            sub_words = sub.strip().split()
            if sub_words:
                sub_first = _strip_diacritics(sub_words[0]).lower().rstrip(".,:")
                if sub_first in _CZ_FIRST_NAMES_NORM:
                    names_found += 1
        if names_found >= 1:
            return True

    # Two capitalized words where the second is a known name ("Lastname Firstname")
    if len(parts) == 2:
        a, b = parts
        if (a[0].isupper() and b[0].isupper()
                and a.replace("-", "").replace("'", "").isalpha()
                and b.replace("-", "").replace("'", "").isalpha()
                and len(a) >= 2 and len(b) >= 2):
            b_lower = _strip_diacritics(b).lower()
            if b_lower in _CZ_FIRST_NAMES_NORM:
                return True

    return False


def _has_by_person_pattern(name: str) -> bool:
    """Detect 'Something by Firstname Lastname' or 'X by Brand' exhibition naming."""
    match = re.search(r"\bby\s+([A-ZÀ-Ža-zà-ž]\w+)", name)
    if match:
        candidate = _strip_diacritics(match.group(1)).lower()
        if candidate in _CZ_FIRST_NAMES_NORM:
            return True
    # Also catch "by X" where "by" separates what looks like a label from a creator
    if re.search(r"\bby\b", name, re.IGNORECASE) and ":" in name:
        return True
    return False


# -----------------------------------------------------------------------
# MAIN CLASSIFIER
# -----------------------------------------------------------------------

def classify_entity(name: str, event_relevance: int = 5) -> dict:
    """Classify a scraped entity name.

    Args:
        name: The entity name to classify.
        event_relevance: The relevance_score of the event (1-10).
            Used to decide how strict to be with unknowns.

    Returns dict with keys:
        entity_type:   'company' | 'individual' | 'institution' |
                       'irrelevant' | 'government' | 'unknown'
        entity_status: 'pending' | 'rejected'
        reject_reason: str | None
    """
    if not name or not name.strip():
        return _reject("irrelevant", "empty name")

    name = name.strip()
    name_lower = name.lower()

    # --- Scrape artifacts (images, nav text, IDs) ---
    artifact_reason = _is_scrape_artifact(name)
    if artifact_reason:
        return _reject("irrelevant", "scrape artifact: " + artifact_reason)

    # --- Placeholder / generic entries ---
    if name_lower in ("independent", "n/a", "none", "unknown", "tbd", "tba",
                       "brno", "prague", "praha", "ostrava", "karel",
                       "offsite expo", "agro"):
        return _reject("irrelevant", "placeholder or generic entry")

    # --- Institution detection (highest entity priority — universities
    #     often have long names with lowercase words that would
    #     false-positive on the product-description heuristic) ---
    if _INSTITUTION_RE.search(name_lower):
        return _pending("institution")

    # --- Strong legal-suffix company check (before product description —
    #     "FK system - povrchové úpravy, s.r.o." is a real company) ---
    if _COMPANY_SUFFIX_RE.search(name_lower):
        return _pending("company")

    # --- Government / public sector (before product description —
    #     "Ministerstvo průmyslu a obchodu ČR" is government) ---
    if _GOV_RE.search(name_lower):
        return _pending("government")

    # --- Irrelevant entities ---
    match = _IRRELEVANT_RE.search(name_lower)
    if match:
        return _reject("irrelevant", "irrelevant category: " + match.group())

    # --- Product descriptions / exhibit labels (only for entries that
    #     didn't match any strong entity signal above) ---
    product_reason = _is_product_description(name)
    if product_reason:
        return _reject("irrelevant", "product/exhibit label: " + product_reason)

    # --- "Exhibition by Person" pattern (skip if name also has company signals) ---
    if _has_by_person_pattern(name) and not _COMPANY_WEAK_RE.search(name_lower):
        return _reject("individual", "exhibition/art by individual")

    # --- "Person: Exhibition Title" or "Title: subtitle" exhibition naming ---
    if ":" in name and _first_word_is_name(name):
        return _reject("individual", "exhibition label with person name")

    # --- Individual name detection ---
    if _looks_like_person_name(name):
        return _reject("individual", "appears to be a person name")

    # --- Weak company signals (Systems, Engineering, etc.) ---
    if _COMPANY_WEAK_RE.search(name_lower):
        return _pending("company")

    # --- Fallback: unknown, keep pending ---
    return {"entity_type": "unknown", "entity_status": "pending", "reject_reason": None}


def classify_with_icp(name: str, event_relevance: int = 5) -> dict:
    """Full classification: entity type + ICP relevance.

    This is the main entry point for the guardrail pipeline. It first
    classifies the entity type, then checks NACE-based ICP relevance
    for anything that survived the entity filter.

    For low-relevance events (relevance <= 6, e.g. design festivals),
    uses strict NACE filtering because broad codes produce too many
    false positives from artisan brands and consumer products.
    """
    result = classify_entity(name, event_relevance)

    if result["entity_status"] == "rejected":
        return result

    if result["entity_type"] in ("institution", "government"):
        return result

    use_strict = event_relevance <= 6
    icp = check_icp_relevance(name, strict=use_strict)

    if icp is not None and icp["is_icp"]:
        result["entity_type"] = "company"
        return result

    # For high-relevance industry events (MSV, FOR ARCH, etc.), trust
    # the event context over potentially wrong ARES NACE data.  A company
    # with a proper legal suffix exhibiting at an engineering fair is very
    # likely a real prospect, even if ARES returned a wrong ICO.
    if event_relevance >= 8 and result["entity_type"] == "company":
        return result

    if icp is not None and not icp["is_icp"]:
        return _reject("irrelevant", f"not Autodesk ICP: {icp['reject_reason']}")

    if icp is None and result["entity_type"] == "unknown" and event_relevance < 8:
        return _reject("irrelevant", "no industry data and event relevance too low to assume ICP fit")

    return result


def _reject(entity_type: str, reason: str) -> dict:
    return {"entity_type": entity_type, "entity_status": "rejected", "reject_reason": reason}


def _pending(entity_type: str) -> dict:
    return {"entity_type": entity_type, "entity_status": "pending", "reject_reason": None}
