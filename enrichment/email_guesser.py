"""Email pattern guesser and validator.

Given a person's first/last name and a company domain, generates candidate
email addresses using common corporate patterns, then validates them via
DNS MX lookup and optional SMTP RCPT TO probing.

Usage:
    from enrichment.email_guesser import guess_email, guess_emails_batch

    result = guess_email("Lukáš", "Mašek", "vikingmasek.com")
    # => {"email": "lukas.masek@vikingmasek.com", "pattern": "first.last",
    #     "confidence": "high", "mx_valid": True, "smtp_valid": True}
"""

import dns.resolver
import re
import smtplib
import socket
import unicodedata
from functools import lru_cache
from typing import Optional


def _strip_diacritics(s: str) -> str:
    """Remove diacritics: á→a, š→s, č→c, ř→r, ž→z, etc."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_name_part(name: str) -> str:
    """Lowercase, strip diacritics, remove non-alpha chars."""
    n = _strip_diacritics(name.lower().strip())
    n = re.sub(r"[^a-z]", "", n)
    return n


def _generate_patterns(first: str, last: str) -> list:
    """Generate candidate email local parts ordered by likelihood."""
    f = _normalize_name_part(first)
    l = _normalize_name_part(last)
    if not f or not l:
        return []

    return [
        ("first.last", f"{f}.{l}"),
        ("flast", f"{f[0]}{l}"),
        ("first_last", f"{f}_{l}"),
        ("firstlast", f"{f}{l}"),
        ("first", f),
        ("last.first", f"{l}.{f}"),
        ("f.last", f"{f[0]}.{l}"),
        ("last", l),
        ("lastf", f"{l}{f[0]}"),
        ("last_first", f"{l}_{f}"),
    ]


@lru_cache(maxsize=512)
def _get_mx_hosts(domain: str) -> list:
    """Return MX hosts for a domain, sorted by priority."""
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=8)
        hosts = sorted(answers, key=lambda r: r.preference)
        return [str(r.exchange).rstrip(".") for r in hosts]
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN,
            dns.resolver.NoNameservers, dns.exception.Timeout, Exception):
        return []


def _check_mx(domain: str) -> bool:
    """Check if domain has MX records (can receive email)."""
    return len(_get_mx_hosts(domain)) > 0


def _smtp_verify(email: str, domain: str, timeout: int = 10) -> Optional[bool]:
    """Attempt SMTP RCPT TO verification. Returns True/False/None (inconclusive)."""
    mx_hosts = _get_mx_hosts(domain)
    if not mx_hosts:
        return None

    for mx_host in mx_hosts[:2]:
        try:
            with smtplib.SMTP(timeout=timeout) as smtp:
                smtp.connect(mx_host, 25)
                smtp.helo("salesagent.local")
                smtp.mail("verify@salesagent.local")
                code, _ = smtp.rcpt(email)
                if code == 250:
                    return True
                elif code == 550:
                    return False
                return None
        except (smtplib.SMTPException, socket.error, OSError, TimeoutError):
            continue

    return None


def _detect_pattern_from_known(known_emails: list, domain: str) -> Optional[str]:
    """Detect the email pattern used at a domain from known emails."""
    domain_lower = domain.lower()
    patterns_seen = {}

    for email in known_emails:
        if not email or "@" not in email:
            continue
        local, em_domain = email.lower().rsplit("@", 1)
        if em_domain != domain_lower:
            continue

        if "." in local:
            parts = local.split(".")
            if len(parts) == 2:
                if len(parts[0]) == 1:
                    pattern = "f.last"
                else:
                    pattern = "first.last"
                patterns_seen[pattern] = patterns_seen.get(pattern, 0) + 1
        elif "_" in local:
            pattern = "first_last"
            patterns_seen[pattern] = patterns_seen.get(pattern, 0) + 1
        elif len(local) > 1 and local[1:].isalpha():
            pattern = "flast"
            patterns_seen[pattern] = patterns_seen.get(pattern, 0) + 1

    if patterns_seen:
        return max(patterns_seen, key=patterns_seen.get)
    return None


def guess_email(
    first_name: str,
    last_name: str,
    domain: str,
    known_emails: list = None,
    verify_smtp: bool = False,
) -> dict:
    """Guess the most likely email for a person at a domain.

    Args:
        first_name: Person's first name (diacritics OK).
        last_name: Person's last name (diacritics OK).
        domain: Company email domain (e.g. "vikingmasek.com").
        known_emails: Optional list of known emails at this domain to detect pattern.
        verify_smtp: If True, attempt SMTP RCPT TO verification (slower, may be blocked).

    Returns:
        dict with keys: email, pattern, confidence, mx_valid, smtp_valid, all_candidates
    """
    if not first_name or not last_name or not domain:
        return {"email": "", "pattern": "", "confidence": "none",
                "mx_valid": False, "smtp_valid": None, "all_candidates": []}

    domain = domain.lower().strip()
    mx_valid = _check_mx(domain)

    if not mx_valid:
        return {"email": "", "pattern": "", "confidence": "none",
                "mx_valid": False, "smtp_valid": None, "all_candidates": []}

    patterns = _generate_patterns(first_name, last_name)
    all_candidates = [f"{local}@{domain}" for _, local in patterns]

    detected_pattern = _detect_pattern_from_known(known_emails or [], domain)
    if detected_pattern:
        for pname, local in patterns:
            if pname == detected_pattern:
                best_email = f"{local}@{domain}"
                smtp_result = None
                if verify_smtp:
                    smtp_result = _smtp_verify(best_email, domain)

                return {
                    "email": best_email,
                    "pattern": detected_pattern,
                    "confidence": "high",
                    "mx_valid": True,
                    "smtp_valid": smtp_result,
                    "all_candidates": all_candidates,
                    "pattern_source": "detected_from_known_emails",
                }

    best_email = all_candidates[0] if all_candidates else ""
    best_pattern = patterns[0][0] if patterns else ""

    smtp_result = None
    if verify_smtp and best_email:
        smtp_result = _smtp_verify(best_email, domain)
        if smtp_result is False:
            for pname, local in patterns[1:]:
                candidate = f"{local}@{domain}"
                check = _smtp_verify(candidate, domain)
                if check is True:
                    best_email = candidate
                    best_pattern = pname
                    smtp_result = True
                    break

    confidence = "medium"
    if detected_pattern:
        confidence = "high"
    elif smtp_result is True:
        confidence = "high"
    elif smtp_result is False:
        confidence = "low"

    return {
        "email": best_email,
        "pattern": best_pattern,
        "confidence": confidence,
        "mx_valid": True,
        "smtp_valid": smtp_result,
        "all_candidates": all_candidates,
    }


def guess_emails_batch(contacts: list, domain: str, known_emails: list = None, verify_smtp: bool = False) -> list:
    """Guess emails for a batch of contacts at the same domain.

    Args:
        contacts: List of dicts with "first_name"/"last_name" (or "name") keys.
        domain: Company domain.
        known_emails: Known emails at this domain to detect pattern.
        verify_smtp: Whether to attempt SMTP verification.

    Returns:
        List of dicts with original contact data plus "guessed_email", "email_confidence", "email_pattern".
    """
    if not domain:
        return contacts

    all_known = list(known_emails or [])
    for c in contacts:
        email = c.get("email", "")
        if email and "@" in email:
            all_known.append(email)

    results = []
    for c in contacts:
        if c.get("email"):
            c["guessed_email"] = ""
            c["email_confidence"] = "known"
            c["email_pattern"] = ""
            results.append(c)
            continue

        fn = c.get("first_name", "")
        ln = c.get("last_name", "")
        if not fn and not ln and c.get("name"):
            parts = c["name"].split(None, 1)
            fn = parts[0] if parts else ""
            ln = parts[1] if len(parts) > 1 else ""

        guess = guess_email(fn, ln, domain, all_known, verify_smtp)
        c["guessed_email"] = guess["email"]
        c["email_confidence"] = guess["confidence"]
        c["email_pattern"] = guess["pattern"]
        results.append(c)

    return results
