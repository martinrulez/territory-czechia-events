"""Email pattern guesser with SMTP verification.

Given a person's name and company domain, generates candidate email
addresses using common Czech corporate patterns, then verifies them
via SMTP RCPT TO probing.

Usage:
    from scraper.email_guesser import guess_email, batch_guess_emails

    result = guess_email("Jan", "Novák", "company.cz")
    # {'email': 'jan.novak@company.cz', 'confidence': 'verified', 'pattern': 'first.last'}

    results = batch_guess_emails(contacts_without_email, delay=2.0)
"""

from __future__ import annotations

import dns.resolver
import re
import smtplib
import socket
import time
import unicodedata
from dataclasses import dataclass
from typing import Optional

SMTP_TIMEOUT = 10
SENDER_ADDRESS = "verify@check.example.com"
FAKE_LOCALPART = "xzq8fake7test42noreply"

# Ordered by frequency in Czech corporate environments
PATTERN_NAMES = [
    "first.last",       # jan.novak@
    "initial.last",     # j.novak@
    "initiallast",      # jnovak@
    "first_last",       # jan_novak@
    "last",             # novak@
    "first",            # jan@
    "last.first",       # novak.jan@
    "lastinitial",      # novakj@
    "first.initial",    # jan.n@
]


def _strip_diacritics(text: str) -> str:
    """Remove Czech/Slovak diacritics: ř→r, č→c, š→s, ž→z, etc."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_name(name: str) -> str:
    """Normalize a name for email: lowercase, strip diacritics, remove non-alpha."""
    name = _strip_diacritics(name.strip().lower())
    name = re.sub(r"[^a-z]", "", name)
    return name


def generate_candidates(first_name: str, last_name: str, domain: str) -> list[dict]:
    """Generate candidate email addresses from name + domain."""
    first = _normalize_name(first_name)
    last = _normalize_name(last_name)

    if not first or not last or not domain:
        return []

    fi = first[0]  # first initial
    li = last[0]   # last initial

    patterns = {
        "first.last":    f"{first}.{last}@{domain}",
        "initial.last":  f"{fi}.{last}@{domain}",
        "initiallast":   f"{fi}{last}@{domain}",
        "first_last":    f"{first}_{last}@{domain}",
        "last":          f"{last}@{domain}",
        "first":         f"{first}@{domain}",
        "last.first":    f"{last}.{first}@{domain}",
        "lastinitial":   f"{last}{fi}@{domain}",
        "first.initial": f"{first}.{li}@{domain}",
    }

    return [
        {"email": email, "pattern": name}
        for name in PATTERN_NAMES
        if (email := patterns.get(name))
    ]


def detect_pattern_from_known(known_emails: list[str], domain: str) -> str | None:
    """Detect the email pattern used by a company from known addresses.

    Returns the pattern name (e.g., 'first.last') or None if can't determine.
    This is a heuristic — it needs at least one email where we can
    reverse-engineer the pattern from the local part structure.
    """
    domain_emails = [
        e.lower() for e in known_emails
        if e and "@" in e and e.split("@")[1].lower() == domain.lower()
    ]

    if not domain_emails:
        return None

    dot_count = sum(1 for e in domain_emails if "." in e.split("@")[0])
    underscore_count = sum(1 for e in domain_emails if "_" in e.split("@")[0])

    if dot_count > 0:
        sample_local = [e.split("@")[0] for e in domain_emails if "." in e.split("@")[0]][0]
        parts = sample_local.split(".")
        if len(parts) == 2:
            if len(parts[0]) == 1:
                return "initial.last"
            if len(parts[1]) == 1:
                return "first.initial"
            return "first.last"
    elif underscore_count > 0:
        return "first_last"
    elif domain_emails:
        sample_local = domain_emails[0].split("@")[0]
        if len(sample_local) <= 3:
            return None
        return "initiallast"

    return None


# ─────────────────────────────────────────────────────────────────────
# MX + SMTP verification
# ─────────────────────────────────────────────────────────────────────

@dataclass
class MXInfo:
    domain: str
    mx_host: str
    is_catch_all: bool | None
    mx_provider: str


def get_mx(domain: str) -> MXInfo | None:
    """Lookup MX record for a domain."""
    try:
        answers = dns.resolver.resolve(domain, "MX")
        mx_records = sorted(answers, key=lambda r: r.preference)
        mx_host = str(mx_records[0].exchange).rstrip(".")
        mx_lower = mx_host.lower()

        if "google" in mx_lower or "gmail" in mx_lower:
            provider = "google"
        elif "outlook" in mx_lower or "microsoft" in mx_lower or "protection.outlook" in mx_lower:
            provider = "microsoft"
        elif "protonmail" in mx_lower:
            provider = "protonmail"
        else:
            provider = "other"

        return MXInfo(domain=domain, mx_host=mx_host, is_catch_all=None, mx_provider=provider)
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, Exception):
        return None


def _smtp_check(mx_host: str, email: str) -> str:
    """Probe SMTP server to check if an email address exists.

    Returns: 'valid', 'invalid', 'catch_all', 'error', 'greylisted'
    """
    try:
        with smtplib.SMTP(mx_host, 25, timeout=SMTP_TIMEOUT) as smtp:
            smtp.ehlo("check.example.com")

            try:
                smtp.starttls()
                smtp.ehlo("check.example.com")
            except (smtplib.SMTPNotSupportedError, smtplib.SMTPException):
                pass

            smtp.mail(SENDER_ADDRESS)
            code, message = smtp.rcpt(email)

            if code == 250:
                return "valid"
            elif code == 550 or code == 551 or code == 553:
                return "invalid"
            elif code == 450 or code == 451 or code == 452:
                return "greylisted"
            else:
                return "error"

    except (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected,
            socket.timeout, socket.gaierror, ConnectionRefusedError, OSError):
        return "error"


def check_catch_all(mx_info: MXInfo) -> bool:
    """Test if domain is catch-all by probing a fake address."""
    fake_email = f"{FAKE_LOCALPART}@{mx_info.domain}"
    result = _smtp_check(mx_info.mx_host, fake_email)
    is_catch_all = result == "valid"
    mx_info.is_catch_all = is_catch_all
    return is_catch_all


def verify_email(email: str, mx_info: MXInfo) -> str:
    """Verify a single email. Returns 'verified', 'invalid', or 'error'."""
    result = _smtp_check(mx_info.mx_host, email)
    if result == "valid":
        return "verified"
    elif result == "invalid":
        return "invalid"
    elif result == "greylisted":
        return "error"
    return "error"


# ─────────────────────────────────────────────────────────────────────
# Main guesser logic
# ─────────────────────────────────────────────────────────────────────

def _can_reach_smtp() -> bool:
    """Quick check: can we connect to port 25 at all? (Corporate firewalls often block it.)"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(4)
        s.connect(("gmail-smtp-in.l.google.com", 25))
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


_SMTP_AVAILABLE = None


def smtp_available() -> bool:
    """Cached check for SMTP availability."""
    global _SMTP_AVAILABLE
    if _SMTP_AVAILABLE is None:
        _SMTP_AVAILABLE = _can_reach_smtp()
    return _SMTP_AVAILABLE


def _pick_best(candidates, detected_pattern, confidence_label):
    """Helper: pick the best candidate given a detected pattern."""
    if detected_pattern:
        match = next((c for c in candidates if c["pattern"] == detected_pattern), candidates[0])
        return match["email"], confidence_label, match["pattern"]
    return candidates[0]["email"], "guessed", candidates[0]["pattern"]


def guess_email(
    first_name: str,
    last_name: str,
    domain: str,
    known_emails: list[str] = None,
    delay: float = 1.5,
) -> dict:
    """Guess and verify an email address for a person at a company.

    Verification strategy:
    1. If SMTP port 25 is reachable: full SMTP RCPT TO verification
    2. If SMTP blocked (corporate firewall): rely on pattern detection
       from existing known emails at the company, with MX validation
       to confirm the domain accepts mail.

    Returns dict with:
      - email: the best guess
      - confidence: 'verified' | 'pattern_match' | 'mx_validated' | 'guessed'
      - pattern: which pattern was used
      - all_candidates: list of all generated candidates
      - mx_provider: google/microsoft/other/None
      - is_catch_all: bool or None
    """
    result = {
        "email": None,
        "confidence": None,
        "pattern": None,
        "all_candidates": [],
        "mx_provider": None,
        "is_catch_all": None,
        "error": None,
    }

    candidates = generate_candidates(first_name, last_name, domain)
    result["all_candidates"] = [c["email"] for c in candidates]

    if not candidates:
        result["error"] = "Could not generate candidates (missing name or domain)"
        return result

    detected_pattern = None
    if known_emails:
        detected_pattern = detect_pattern_from_known(known_emails, domain)

    if detected_pattern:
        candidates.sort(
            key=lambda c: (0 if c["pattern"] == detected_pattern else 1)
        )

    mx_info = get_mx(domain)
    if not mx_info:
        email, conf, pat = _pick_best(candidates, detected_pattern, "pattern_match")
        result["email"] = email
        result["confidence"] = conf
        result["pattern"] = pat
        result["error"] = "No MX record — domain may not accept email"
        return result

    result["mx_provider"] = mx_info.mx_provider

    if not smtp_available():
        email, conf, pat = _pick_best(
            candidates,
            detected_pattern,
            "mx_validated" if detected_pattern else "mx_validated",
        )
        result["email"] = email
        result["confidence"] = "pattern_match" if detected_pattern else "mx_validated"
        result["pattern"] = pat
        if not detected_pattern:
            result["error"] = "SMTP blocked — used default pattern with MX validation"
        return result

    is_catch_all = check_catch_all(mx_info)
    result["is_catch_all"] = is_catch_all

    if is_catch_all:
        email, conf, pat = _pick_best(candidates, detected_pattern, "pattern_match")
        result["email"] = email
        result["confidence"] = conf
        result["pattern"] = pat
        return result

    for candidate in candidates:
        time.sleep(delay)
        status = verify_email(candidate["email"], mx_info)

        if status == "verified":
            result["email"] = candidate["email"]
            result["confidence"] = "verified"
            result["pattern"] = candidate["pattern"]
            return result
        elif status == "error":
            email, conf, pat = _pick_best(candidates, detected_pattern, "pattern_match")
            result["email"] = email
            result["confidence"] = conf
            result["pattern"] = pat
            result["error"] = "SMTP error — fell back to pattern"
            return result

    email, conf, pat = _pick_best(candidates, detected_pattern, "pattern_match")
    result["email"] = email
    result["confidence"] = conf
    result["pattern"] = pat
    result["error"] = "All candidates rejected by SMTP — using best guess"
    return result


def batch_guess_emails(
    contacts: list[dict],
    delay: float = 2.0,
    progress_callback=None,
) -> list[dict]:
    """Guess emails for a batch of contacts.

    Each contact dict needs: first_name, last_name, domain
    Optional: known_emails (list of existing emails at the domain)

    Returns list of result dicts (same as guess_email output + input fields).
    """
    results = []
    mx_cache = {}

    for i, contact in enumerate(contacts):
        first = contact.get("first_name", "")
        last = contact.get("last_name", "")
        domain = contact.get("domain", "")
        known = contact.get("known_emails", [])

        if progress_callback:
            progress_callback(i + 1, len(contacts), f"{first} {last} @ {domain}")

        if not first or not last or not domain:
            results.append({
                **contact,
                "guessed_email": None,
                "confidence": None,
                "pattern": None,
                "error": "Missing name or domain",
            })
            continue

        result = guess_email(first, last, domain, known_emails=known, delay=delay)

        results.append({
            **contact,
            "guessed_email": result["email"],
            "confidence": result["confidence"],
            "pattern": result["pattern"],
            "mx_provider": result.get("mx_provider"),
            "is_catch_all": result.get("is_catch_all"),
            "error": result.get("error"),
        })

        if i < len(contacts) - 1:
            time.sleep(delay)

    return results
