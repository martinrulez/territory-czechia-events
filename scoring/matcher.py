"""Fuzzy company matching with Czech legal suffix normalization."""

import re
import unicodedata

from thefuzz import fuzz

from db.database import get_accounts, get_event_companies, update_event_company_match

LEGAL_SUFFIXES = re.compile(
    r",?\s*\b("
    r"s\.?\s*r\.?\s*o\.?|"
    r"a\.?\s*s\.?|"
    r"spol\.\s*s\s*r\.?\s*o\.?|"
    r"v\.?\s*o\.?\s*s\.?|"
    r"k\.?\s*s\.?|"
    r"s\.?\s*e\.?|"
    r"GmbH|AG|Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?|"
    r"z\.?\s*s\.?|z\.?\s*ú\.?|"
    r"p\.?\s*o\.?"
    r")\s*\.?\s*$",
    re.IGNORECASE,
)

NOISE_WORDS = re.compile(
    r"\b(czech|republic|česká|republika|group|international|technologies|solutions|systems)\b",
    re.IGNORECASE,
)


def normalize_company_name(name: str) -> str:
    """Normalize a company name for fuzzy matching."""
    if not name:
        return ""
    s = name.strip()
    s = LEGAL_SUFFIXES.sub("", s)
    s = NOISE_WORDS.sub("", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s


def match_companies(threshold: int = 90, event_id: int = None) -> dict:
    """Match event companies against existing accounts.

    Args:
        threshold: Minimum fuzzy match score (0-100) to consider a match.
        event_id: If given, only match companies from this event.

    Returns:
        Summary dict with match counts.
    """
    accounts = get_accounts()
    if not accounts:
        return {"success": False, "error": "No accounts in database. Import your client list first."}

    account_map = {}
    for acct in accounts:
        normalized = normalize_company_name(acct["company_name"])
        if normalized:
            account_map[normalized] = acct

    event_companies = get_event_companies(event_id=event_id)
    matched = 0
    unmatched = 0

    for ec in event_companies:
        ec_normalized = normalize_company_name(ec["company_name"])
        if not ec_normalized:
            unmatched += 1
            continue

        best_score = 0
        best_account = None

        for acct_norm, acct in account_map.items():
            score = fuzz.token_sort_ratio(ec_normalized, acct_norm)
            if score > best_score:
                best_score = score
                best_account = acct

        if best_score >= threshold and best_account:
            update_event_company_match(ec["id"], best_account["id"], best_score / 100.0)
            matched += 1
        else:
            unmatched += 1

    return {
        "success": True,
        "matched": matched,
        "unmatched": unmatched,
        "total": len(event_companies),
        "threshold": threshold,
    }
