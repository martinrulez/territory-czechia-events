"""Contact enrichment: email pattern generation, LinkedIn URLs, and quality scoring.

Generates probable email addresses from name + domain patterns,
builds LinkedIn search URLs, and scores contact quality per account.
"""

import re
import unicodedata


# ── Czech diacritics removal ─────────────────────────────────────

_CZECH_MAP = str.maketrans(
    "áčďéěíňóřšťúůýžÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ",
    "acdeeinorstuuyzACDEEINORSTUUYZ",
)


def _strip_diacritics(text: str) -> str:
    """Remove Czech diacritics for email-safe names."""
    result = text.translate(_CZECH_MAP)
    result = unicodedata.normalize("NFD", result)
    result = "".join(c for c in result if unicodedata.category(c) != "Mn")
    return result


def _clean_name_for_email(name: str) -> str:
    """Lowercase, strip diacritics, remove titles/suffixes."""
    name = re.sub(
        r"\b(ing|mgr|bc|judr|mudr|phdr|rndr|doc|prof|ph\.?d|csc|mba|msc|dis|acca)\b\.?",
        "",
        name,
        flags=re.IGNORECASE,
    )
    name = name.strip().strip(",").strip()
    return _strip_diacritics(name).lower().strip()


# ── Email pattern generation ─────────────────────────────────────

EMAIL_PATTERNS = [
    "{first}.{last}",        # jan.novak
    "{f}.{last}",            # j.novak
    "{first}{last}",         # jannovak
    "{last}.{first}",        # novak.jan
    "{first}_{last}",        # jan_novak
    "{last}",                # novak
    "{first}",               # jan
    "{f}{last}",             # jnovak
    "{last}{f}",             # novakj
]


def generate_email_candidates(
    first_name: str,
    last_name: str,
    domain: str,
) -> list:
    """Generate probable email addresses from name + domain.

    Returns list of dicts with email and pattern name, ordered by
    likelihood (most common patterns first).
    """
    if not first_name or not last_name or not domain:
        return []

    first = _clean_name_for_email(first_name)
    last = _clean_name_for_email(last_name)

    if not first or not last:
        return []

    domain = domain.strip().lower()
    if domain.startswith("www."):
        domain = domain[4:]
    if domain.startswith("http"):
        domain = domain.split("//")[-1].split("/")[0]

    f = first[0]

    candidates = []
    seen = set()
    for pattern in EMAIL_PATTERNS:
        local = pattern.format(first=first, last=last, f=f)
        local = re.sub(r"[^a-z0-9._-]", "", local)
        email = f"{local}@{domain}"
        if email not in seen:
            seen.add(email)
            candidates.append({
                "email": email,
                "pattern": pattern,
            })

    return candidates


# ── LinkedIn URL generation ──────────────────────────────────────

def generate_linkedin_search_url(
    first_name: str,
    last_name: str,
    company_name: str = "",
    title: str = "",
) -> str:
    """Generate a LinkedIn people search URL for a contact."""
    parts = []
    if first_name:
        parts.append(first_name)
    if last_name:
        parts.append(last_name)
    if company_name:
        parts.append(company_name)

    if not parts:
        return ""

    query = " ".join(parts)
    encoded = query.replace(" ", "%20")
    return (
        f"https://www.linkedin.com/search/results/people/"
        f"?keywords={encoded}&origin=GLOBAL_SEARCH_HEADER"
    )


# ── Persona-to-upsell mapping ───────────────────────────────────

UPSELL_PERSONA_MAP = {
    "Fusion Mfg Ext": {
        "ideal": ["champion", "end_user_leader"],
        "good": ["economic_buyer", "technical_influencer"],
        "titles_to_find": [
            "CAD/CAM Manager", "Manufacturing Engineer",
            "vedoucí výroby", "vedoucí konstrukce",
            "CNC programátor", "technolog",
        ],
    },
    "AEC Collection": {
        "ideal": ["champion", "end_user_leader"],
        "good": ["economic_buyer", "technical_influencer"],
        "titles_to_find": [
            "BIM Manager", "BIM koordinátor",
            "vedoucí projekce", "hlavní projektant",
            "vedoucí ateliéru",
        ],
    },
    "PDMC": {
        "ideal": ["champion", "technical_influencer"],
        "good": ["economic_buyer", "end_user_leader"],
        "titles_to_find": [
            "PLM Manager", "Engineering Manager",
            "vedoucí konstrukce", "hlavní konstruktér",
            "PDM správce",
        ],
    },
    "BIM Collaborate Pro": {
        "ideal": ["champion"],
        "good": ["technical_influencer", "end_user_leader"],
        "titles_to_find": [
            "BIM Coordinator", "Project Manager",
            "BIM manažer", "vedoucí projektů",
        ],
    },
    "Forma": {
        "ideal": ["end_user_leader", "champion"],
        "good": ["economic_buyer"],
        "titles_to_find": [
            "Lead Architect", "Studio Director",
            "hlavní architekt", "vedoucí ateliéru",
        ],
    },
    "M&E Collection": {
        "ideal": ["champion", "end_user_leader"],
        "good": ["economic_buyer"],
        "titles_to_find": [
            "VFX Supervisor", "Pipeline TD",
            "Creative Director", "vedoucí postprodukce",
        ],
    },
    "AutoCAD (full)": {
        "ideal": ["champion", "end_user_leader"],
        "good": ["economic_buyer", "technical_influencer"],
        "titles_to_find": [
            "CAD Manager", "vedoucí konstrukce",
            "hlavní projektant",
        ],
    },
}


def score_persona_upsell_fit(contacts: list, top_upsell: str) -> dict:
    """Score how well the available contacts match the ideal personas for an upsell.

    Returns dict with fit_score (0-100), has_ideal, has_good, missing_personas.
    """
    mapping = None
    for key, val in UPSELL_PERSONA_MAP.items():
        if key.lower() in top_upsell.lower():
            mapping = val
            break

    if not mapping:
        mapping = UPSELL_PERSONA_MAP.get("AEC Collection")

    personas_present = set()
    for c in contacts:
        p = c.get("persona_type", "unknown")
        if p != "unknown":
            personas_present.add(p)

    ideal = set(mapping["ideal"])
    good = set(mapping["good"])

    has_ideal = bool(personas_present & ideal)
    has_good = bool(personas_present & good)
    ideal_found = personas_present & ideal
    good_found = personas_present & good

    missing = (ideal | good) - personas_present
    missing.discard("unknown")

    score = 0
    if has_ideal:
        score += 60
        score += min(20, len(ideal_found) * 15)
    if has_good:
        score += 20
        score += min(10, len(good_found) * 8)
    if len(personas_present) >= 3:
        score += 10

    return {
        "fit_score": min(100, score),
        "has_ideal_persona": has_ideal,
        "has_good_persona": has_good,
        "ideal_found": list(ideal_found),
        "good_found": list(good_found),
        "missing_personas": list(missing),
        "titles_to_find": mapping.get("titles_to_find", []),
    }


# ── Contact quality score ────────────────────────────────────────

def score_contact_quality(contacts: list, top_upsell: str = "") -> dict:
    """Score overall contact quality for an account.

    Returns dict with quality_score (0-100) and breakdown.
    """
    if not contacts:
        return {"quality_score": 0, "breakdown": {}}

    has_email = sum(1 for c in contacts if c.get("email"))
    has_phone = sum(1 for c in contacts if c.get("phone"))
    has_name = sum(1 for c in contacts if c.get("full_name", "").strip())
    has_title = sum(1 for c in contacts if c.get("title"))
    total = len(contacts)

    named_pct = has_name / total if total else 0
    email_pct = has_email / total if total else 0

    persona_fit = score_persona_upsell_fit(contacts, top_upsell)

    # Scoring components
    email_score = min(20, email_pct * 25)         # 0-20: reachability
    persona_score = persona_fit["fit_score"] * 0.4  # 0-40: right people
    multi_thread = min(15, total * 3)              # 0-15: multi-threaded
    phone_score = min(10, has_phone * 5)           # 0-10: phone available
    freshness = 15 if any(                         # 0-15: data freshness
        c.get("source") == "zoominfo" for c in contacts
    ) else 5

    quality = round(
        email_score + persona_score + multi_thread + phone_score + freshness,
        1,
    )

    return {
        "quality_score": min(100, quality),
        "breakdown": {
            "email_reachability": round(email_score, 1),
            "persona_fit": round(persona_score, 1),
            "multi_threading": round(multi_thread, 1),
            "phone_available": round(phone_score, 1),
            "data_freshness": round(freshness, 1),
        },
        "persona_fit": persona_fit,
        "contacts_with_email": has_email,
        "contacts_with_phone": has_phone,
        "contacts_total": total,
    }
