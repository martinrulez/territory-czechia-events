"""Territory-wide account prioritization scoring engine.

Reads the raw Autodesk subscription CSV, aggregates accounts by CSN,
computes whitespace, seat expansion, renewal timing, and product maturity
scores. Outputs a ranked CSV of prioritized accounts.
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from scoring.competitors import (
    AEC_PRODUCTS,
    DM_PRODUCTS,
    ME_PRODUCTS,
    PRODUCT_NORMALIZE,
    recommend_upsell,
)

TODAY = datetime.now()

INDUSTRY_TO_SEGMENT = {
    "AEC": "AEC",
    "MFG": "D&M",
    "M&E": "M&E",
    "EDU": "AEC",
    "OTH": "unknown",
}

ENTRY_PRODUCTS = {
    "AutoCAD LT", "Revit LT", "Flex", "AutoCAD Web", "AutoCAD Mobile", "APS",
}
CLOUD_PRODUCTS = {
    "Forma", "BIM Collaborate", "BIM Collaborate Pro", "ACC Docs", "ACC Build",
}
EXTENSION_PRODUCTS = {
    "Fusion Mfg Ext", "Fusion Sim Ext", "Fusion Manage Ext",
    "Fusion Design Ext", "Fusion Manage", "Moldflow", "Workshop XR", "CFD",
}
COLLECTION_PRODUCTS = {"AEC Collection", "PDMC", "M&E Collection"}

PRODUCT_ACV_EUR = {
    "AutoCAD LT": 500,
    "Revit LT": 500,
    "AutoCAD": 2200,
    "AutoCAD Web": 300,
    "AutoCAD Mobile": 150,
    "Revit": 3200,
    "Inventor": 2800,
    "Fusion": 600,
    "Civil 3D": 3200,
    "3ds Max": 2200,
    "Maya": 2200,
    "Navisworks": 2800,
    "Advance Steel": 2800,
    "AEC Collection": 4200,
    "PDMC": 4200,
    "M&E Collection": 4200,
    "Vault": 2800,
    "Forma": 2000,
    "BIM Collaborate": 800,
    "BIM Collaborate Pro": 1500,
    "ACC Docs": 500,
    "ACC Build": 1500,
    "Flex": 350,
    "Fusion Mfg Ext": 2000,
    "Fusion Sim Ext": 2000,
    "Fusion Manage Ext": 1500,
    "Moldflow": 15000,
    "ReCap Pro": 500,
    "Fabrication CAMduct": 2800,
    "Alias": 5000,
    "PowerInspect": 3000,
    "EAGLE": 500,
    "MotionBuilder": 2200,
    "Arnold": 500,
    "Flame": 10000,
    "CFD": 8000,
}
_DEFAULT_ACV = 1000

SEGMENT_TARGETS = {
    "AEC": {
        "collection": "AEC Collection",
        "cloud": ["Forma", "BIM Collaborate Pro", "ACC Build", "ACC Docs"],
        "high_value": ["AEC Collection", "Forma", "BIM Collaborate Pro"],
    },
    "D&M": {
        "collection": "PDMC",
        "cloud": ["Fusion Manage Ext"],
        "high_value": ["PDMC", "Fusion Mfg Ext", "Fusion Sim Ext", "Moldflow"],
    },
    "M&E": {
        "collection": "M&E Collection",
        "cloud": [],
        "high_value": ["M&E Collection"],
    },
}

MATURITY_LABELS = {
    0: "Unknown",
    1: "Entry",
    2: "Foundation",
    3: "Multi-Product",
    4: "Collection",
    5: "Platform",
}


# ── Helpers ──────────────────────────────────────────────────


def normalize_single_product(raw_name: str):
    """Normalize a single product name; returns None for junk values."""
    if not raw_name:
        return None
    clean = raw_name.strip()
    if not clean or clean.lower() in ("unknown", "nan", "none", ""):
        return None
    if clean.startswith(("www.", "http", "eshop.", "wistron.")) or "@" in clean:
        return None
    canonical = PRODUCT_NORMALIZE.get(clean.lower())
    if canonical:
        return canonical
    if any(c.isalpha() for c in clean):
        return clean
    return None


def parse_date(date_str):
    if not date_str or str(date_str).lower() in ("unknown", "nan", "none", ""):
        return None
    try:
        return datetime.strptime(str(date_str).strip(), "%m/%d/%Y")
    except ValueError:
        return None


def _safe_int(val):
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return 0


UNLIMITED_SENTINEL = 9999
UNLIMITED_PRODUCT_CODES = {"BLDUNLT", "BIMCOLL"}
TOKEN_PRODUCTS = {"Flex"}

# Flex tokens: ~2000 tokens ≈ 1 seat equivalent
FLEX_TOKENS_PER_SEAT = 2000


def _normalize_seat_count(seats, product_raw, product_normalized, product_code=""):
    """Normalize inflated seat counts caused by sentinel values and token-based products.

    - 9999 = "unlimited" sentinel -> treated as 1 subscription
    - Flex tokens: converted to equivalent seats (2000 tokens ~ 1 seat)
    - BIM Collaborate unlimited: treated as 1 subscription
    """
    if seats >= UNLIMITED_SENTINEL:
        return 1

    code = product_code.strip().upper()
    if code in UNLIMITED_PRODUCT_CODES:
        return 1

    if product_normalized in TOKEN_PRODUCTS:
        return max(1, seats // FLEX_TOKENS_PER_SEAT)

    return seats


# ── Account aggregation ─────────────────────────────────────


class AccountAggregate:
    """Aggregated account from multiple subscription lines sharing a CSN."""

    __slots__ = (
        "csn", "company_name", "city", "website", "postal_code",
        "products_raw", "products_normalized", "total_seats",
        "seats_by_product", "industry_group", "industry_segment",
        "industry_sub_segment", "agreement_end_dates", "agreement_terms",
        "resellers", "purchaser_emails", "parent_account", "parent_csn",
        "parent_country", "statuses",
    )

    def __init__(self, csn: str):
        self.csn = csn
        self.company_name = ""
        self.city = ""
        self.website = ""
        self.postal_code = ""
        self.products_raw: set = set()
        self.products_normalized: set = set()
        self.total_seats = 0
        self.seats_by_product: dict = defaultdict(int)
        self.industry_group = ""
        self.industry_segment = ""
        self.industry_sub_segment = ""
        self.agreement_end_dates: list = []
        self.agreement_terms: list = []
        self.resellers: set = set()
        self.purchaser_emails: set = set()
        self.parent_account = ""
        self.parent_csn = ""
        self.parent_country = ""
        self.statuses: set = set()

    def add_line(self, row: dict):
        if not self.company_name:
            self.company_name = (row.get("SITE ACCOUNT") or "").strip()
        if not self.city:
            self.city = (row.get("SITE CITY") or "").strip()

        website = (row.get("SITE WEBSITE") or "").strip()
        if website and website.lower() != "unknown" and not self.website:
            self.website = website
        if not self.postal_code:
            self.postal_code = (row.get("SITE POSTAL CODE") or "").strip()

        product_raw = (row.get("PRODUCT LINE") or "").strip()
        if product_raw:
            self.products_raw.add(product_raw)
            normalized = normalize_single_product(product_raw)
            if normalized:
                self.products_normalized.add(normalized)
                raw_seats = _safe_int(row.get("# OF UNITS", 0))
                product_code = (row.get("PRODUCT LINE CODE") or "").strip()
                seats = _normalize_seat_count(raw_seats, product_raw, normalized, product_code)
                if seats > 0:
                    self.seats_by_product[normalized] += seats
                    self.total_seats += seats

        ig = (row.get("INDUSTRY GROUP") or "").strip()
        if ig and ig.upper() != "UNKNOWN" and "@" not in ig and not ig.startswith("www."):
            if not self.industry_group:
                self.industry_group = ig
        iseg = (row.get("INDUSTRY SEGMENT") or "").strip()
        if iseg and iseg.upper() != "UNKNOWN":
            if not self.industry_segment:
                self.industry_segment = iseg
        isub = (row.get("INDUSTRY SUB SEGMENT") or "").strip()
        if isub and isub.upper() != "UNKNOWN":
            if not self.industry_sub_segment:
                self.industry_sub_segment = isub

        end_date = parse_date(row.get("AGREEMENT END DATE"))
        if end_date:
            self.agreement_end_dates.append(end_date)
        term = row.get("AGREE TERM", "")
        try:
            t = int(str(term).strip())
            if 0 < t <= 12:
                self.agreement_terms.append(t)
        except (ValueError, TypeError):
            pass

        reseller = (row.get("RESELLER") or "").strip()
        if reseller and reseller.lower() != "unknown":
            self.resellers.add(reseller)

        email = (row.get("PURCHASER EMAIL") or "").strip()
        if email and email.lower() != "unknown" and "@" in email:
            self.purchaser_emails.add(email)

        parent = (row.get("PARENT ACCOUNT NAME") or "").strip()
        if parent and not self.parent_account:
            self.parent_account = parent
        pcsn = (row.get("PARENT ACCOUNT CSN") or "").strip()
        if pcsn and not self.parent_csn:
            self.parent_csn = pcsn
        pcountry = (row.get("PARENT COUNTRY") or "").strip()
        if pcountry:
            self.parent_country = pcountry

        status = (row.get("ASSET SUBS STATUS") or "").strip()
        if status:
            self.statuses.add(status)

    @property
    def primary_segment(self) -> str:
        return INDUSTRY_TO_SEGMENT.get(self.industry_group.upper(), "unknown")

    @property
    def nearest_renewal(self):
        future = [d for d in self.agreement_end_dates if d >= TODAY]
        return min(future) if future else None

    @property
    def days_to_renewal(self):
        nr = self.nearest_renewal
        return (nr - TODAY).days if nr else None

    @property
    def avg_term(self) -> float:
        return (sum(self.agreement_terms) / len(self.agreement_terms)
                if self.agreement_terms else 0)

    @property
    def maturity_level(self) -> int:
        prods = self.products_normalized
        if not prods:
            return 0
        has_collection = bool(prods & COLLECTION_PRODUCTS)
        has_cloud = bool(prods & CLOUD_PRODUCTS)
        has_extensions = bool(prods & EXTENSION_PRODUCTS)
        core = prods - ENTRY_PRODUCTS - CLOUD_PRODUCTS - EXTENSION_PRODUCTS - COLLECTION_PRODUCTS
        if has_collection and (has_cloud or has_extensions):
            return 5
        if has_collection:
            return 4
        if len(core) >= 2:
            return 3
        if core:
            return 2
        if prods.issubset(ENTRY_PRODUCTS):
            return 1
        return 1

    @property
    def maturity_label(self) -> str:
        return MATURITY_LABELS.get(self.maturity_level, "Unknown")


# ── Company type classification ──────────────────────────────

SOLE_PROPRIETOR_FORMS = {"101", "102", "103", "104", "105"}


def _is_sole_proprietor(enrich: dict) -> bool:
    """Check if account is a sole proprietor (OSVČ) based on ARES legal form."""
    lf = str(enrich.get("legal_form", ""))
    return lf in SOLE_PROPRIETOR_FORMS


def _estimate_company_size(enrich: dict) -> str:
    """Estimate company size from available signals when employee count is missing.

    Returns: 'micro', 'small', 'medium', 'large', 'unknown'
    """
    emp = enrich.get("employee_count")
    if emp and emp > 0:
        if emp >= 250:
            return "large"
        if emp >= 50:
            return "medium"
        if emp >= 10:
            return "small"
        return "micro"

    smlouvy_val = enrich.get("smlouvy_value_czk", 0) or 0
    revenue = enrich.get("revenue", 0) or 0

    if revenue > 10_000_000 or smlouvy_val > 50_000_000:
        return "large"
    if revenue > 2_000_000 or smlouvy_val > 10_000_000:
        return "medium"
    if revenue > 500_000 or smlouvy_val > 2_000_000:
        return "small"

    lf = str(enrich.get("legal_form", ""))
    if lf == "121":
        return "medium"
    if lf in SOLE_PROPRIETOR_FORMS:
        return "micro"

    total_jobs = 0
    try:
        total_jobs = int(enrich.get("total_jobs", 0))
    except (ValueError, TypeError):
        pass
    if total_jobs >= 10:
        return "medium"
    if total_jobs >= 5:
        return "small"

    return "unknown"


# ── Scoring functions ────────────────────────────────────────


def score_whitespace(acct: AccountAggregate, enrich: dict = None):
    """Whitespace score (0‑30) and upsell recommendations."""
    enrich = enrich or {}
    prods = acct.products_normalized
    segment = acct.primary_segment

    if not prods:
        return 0.0, []

    products_str = ", ".join(sorted(prods))
    upsells = recommend_upsell(products_str, industry_segment=segment)

    maturity_mult = {1: 1.0, 2: 0.8, 3: 0.6, 4: 0.35, 5: 0.15}
    base = maturity_mult.get(acct.maturity_level, 0.5)

    if segment in SEGMENT_TARGETS:
        targets = SEGMENT_TARGETS[segment]
        prods_lower = {p.lower() for p in prods}
        if targets["collection"].lower() not in prods_lower and acct.maturity_level <= 3:
            base = min(base + 0.15, 1.0)
        if targets["collection"].lower() in prods_lower:
            if not any(c.lower() in prods_lower for c in targets.get("cloud", [])):
                base = min(base + 0.1, 1.0)

    if _is_sole_proprietor(enrich) and acct.total_seats <= 2:
        seat_mult = 0.15
    else:
        seat_mult = min(1.0, 0.15 + (acct.total_seats / 15) * 0.85)

    return round(30 * base * seat_mult, 1), upsells


def score_capacity(acct: AccountAggregate, employee_count=None, enrich=None) -> float:
    """Seat expansion score (0‑25).

    Uses employee count for penetration analysis when available.
    When ZoomInfo department breakdown is available, uses engineering+IT
    headcount for more accurate technical penetration measurement.
    Falls back to company size estimation from smlouvy/revenue/legal form.
    """
    enrich = enrich or {}
    if acct.total_seats <= 0:
        return 0.0

    if employee_count and employee_count > 0:
        dept = enrich.get("zi_employee_by_department") or {}
        eng = dept.get("engineering") or 0
        it = dept.get("it") or 0
        technical_headcount = eng + it

        if technical_headcount > 5:
            penetration = acct.total_seats / technical_headcount
        else:
            penetration = acct.total_seats / employee_count

        if penetration < 0.02:
            ratio = 1.0
        elif penetration < 0.05:
            ratio = 0.8
        elif penetration < 0.10:
            ratio = 0.6
        elif penetration < 0.20:
            ratio = 0.4
        elif penetration < 0.50:
            ratio = 0.2
        else:
            ratio = 0.05
        size_bonus = min(1.0, employee_count / 500)
        return round(25 * ratio * (0.3 + 0.7 * size_bonus), 1)

    size = _estimate_company_size(enrich)

    size_scores = {
        "large": {"base": 20.0, "seat_penalty": 0.3},
        "medium": {"base": 15.0, "seat_penalty": 0.5},
        "small": {"base": 8.0, "seat_penalty": 0.7},
        "micro": {"base": 2.5, "seat_penalty": 1.0},
        "unknown": {"base": 6.0, "seat_penalty": 0.6},
    }
    cfg = size_scores.get(size, size_scores["unknown"])
    base = cfg["base"]

    if acct.total_seats > 10:
        base *= (1.0 - cfg["seat_penalty"] * min(0.5, acct.total_seats / 100))

    return round(min(25.0, base), 1)


def score_growth(enrich: dict) -> float:
    """Growth / momentum score (0‑20).

    Recalibrated to reward differentiating signals:
    - ZoomInfo employee growth (1Y/2Y) as primary signal when available
    - Engineering hiring and tool mentions weighted heavily
    - Basic hiring signal is near-universal so weighted minimally
    - Job volume as a gradient instead of binary
    """
    s = 0.0

    zi_growth = enrich.get("zi_employee_growth") or {}
    one_y = zi_growth.get("one_year_growth_rate")
    if one_y is not None:
        try:
            rate = float(one_y)
            if rate > 20:
                s += 4.0
            elif rate > 10:
                s += 3.0
            elif rate > 5:
                s += 2.0
            elif rate > 0:
                s += 1.0
        except (ValueError, TypeError):
            pass

    rg = enrich.get("revenue_growth")
    if rg is not None:
        try:
            s += min(5.0, max(0, float(rg)) * 1.0)
        except (ValueError, TypeError):
            pass

    total_jobs = 0
    try:
        total_jobs = int(enrich.get("total_jobs", 0))
    except (ValueError, TypeError):
        pass

    if total_jobs >= 20:
        s += 3.0
    elif total_jobs >= 10:
        s += 2.0
    elif total_jobs >= 5:
        s += 1.0
    elif total_jobs >= 1:
        s += 0.3

    if enrich.get("engineering_hiring"):
        s += 2.5

    if enrich.get("leadership_change"):
        s += 2.5

    autodesk_in_jobs = enrich.get("autodesk_tools_in_jobs")
    if autodesk_in_jobs:
        if isinstance(autodesk_in_jobs, list):
            s += min(3.0, len(autodesk_in_jobs) * 1.5)
        else:
            s += 2.0

    competitor_in_jobs = enrich.get("competitor_tools_in_jobs")
    if competitor_in_jobs:
        if isinstance(competitor_in_jobs, list):
            s += min(2.0, len(competitor_in_jobs) * 0.5)
        else:
            s += 1.5

    zi_adsk_tech = enrich.get("zi_autodesk_tech", [])
    zi_comp_tech = enrich.get("zi_competitor_tech", [])
    if zi_adsk_tech:
        s += min(3.0, len(zi_adsk_tech) * 1.0)
    if zi_comp_tech:
        s += min(2.0, len(zi_comp_tech) * 0.5)

    if enrich.get("has_public_contracts"):
        contracts = enrich.get("smlouvy_contracts_count", 0) or enrich.get("public_contracts_count", 0)
        smlouvy_val = enrich.get("smlouvy_value_czk", 0) or 0
        try:
            if smlouvy_val > 10_000_000:
                s += 2.5
            elif int(contracts) >= 3:
                s += 1.5
            elif int(contracts) >= 1:
                s += 0.5
        except (ValueError, TypeError):
            s += 0.5

        aec_contracts = enrich.get("smlouvy_aec_contracts", 0) or enrich.get("aec_contracts_count", 0)
        try:
            if int(aec_contracts) >= 2:
                s += 1.0
        except (ValueError, TypeError):
            pass

    if enrich.get("upsell_hiring_intent"):
        strength = enrich.get("intent_strength", "")
        if strength == "strong":
            s += 4.0
        elif strength == "moderate":
            s += 2.5
        elif strength == "weak":
            s += 1.0

    if enrich.get("has_eu_grants"):
        digi_grants = enrich.get("eu_digi_grants", [])
        recent = enrich.get("eu_recent_grants", [])
        if enrich.get("has_digi_budget"):
            s += 4.0
        elif digi_grants:
            s += 3.0
        elif recent:
            s += 2.0
        else:
            s += 1.0

    if enrich.get("facility_expansion"):
        s += 3.0

    if enrich.get("ma_detected"):
        ma_type = enrich.get("ma_type", "")
        if ma_type in ("merger", "acquisition"):
            s += 3.5
        elif ma_type == "ownership_change":
            s += 2.0
        else:
            s += 1.5

    if enrich.get("certifications_detected"):
        if enrich.get("bim_mandate_relevant"):
            s += 3.0
        else:
            s += 1.5

    if enrich.get("digital_transformation"):
        s += 2.0

    if enrich.get("esg_signals"):
        s += 1.5

    engagement = enrich.get("engagement_level", "none")
    if engagement == "high":
        s += 2.5
    elif engagement == "medium":
        s += 1.5
    elif engagement == "low":
        s += 0.5

    return min(20.0, round(s, 1))


def score_timing(acct: AccountAggregate) -> float:
    """Renewal timing score (0‑15)."""
    days = acct.days_to_renewal
    if days is None:
        return 3.0
    if days < 0:
        return 1.5
    if days <= 30:
        return 15.0
    if days <= 60:
        return 13.5
    if days <= 90:
        return 12.0
    if days <= 120:
        return 10.5
    if days <= 180:
        return 8.3
    if days <= 365:
        return 5.3
    return 2.3


def score_relationship(acct: AccountAggregate) -> float:
    """Relationship / engagement score (0‑10)."""
    s = 0.0
    avg_t = acct.avg_term
    if avg_t >= 5:
        s += 3.0
    elif avg_t >= 3:
        s += 2.5
    elif avg_t >= 2:
        s += 2.0
    else:
        s += 1.0

    n = len(acct.products_normalized)
    if n >= 5:
        s += 3.0
    elif n >= 3:
        s += 2.5
    elif n >= 2:
        s += 2.0
    else:
        s += 1.0

    top_resellers = {"arkance systems cz", "adeon cz", "graitec", "td synnex"}
    matched = False
    for r in acct.resellers:
        if any(tr in r.lower() for tr in top_resellers):
            s += 2.0
            matched = True
            break
    if not matched and acct.resellers:
        s += 1.0

    return round(min(s, 10.0), 1)


def estimate_current_acv(acct: AccountAggregate) -> float:
    total = 0.0
    for product, seats in acct.seats_by_product.items():
        total += PRODUCT_ACV_EUR.get(product, _DEFAULT_ACV) * seats
    return round(total, 0)


def estimate_potential_acv(acct: AccountAggregate, upsells: list) -> float:
    delta = 0.0
    for u in upsells[:3]:
        target_acv = PRODUCT_ACV_EUR.get(u.get("product", ""), _DEFAULT_ACV)
        relevant_seats = max(1, acct.total_seats // 2)
        delta += target_acv * relevant_seats
    return round(delta, 0)


# ── Pipeline ─────────────────────────────────────────────────


def load_and_aggregate(csv_path: str) -> dict:
    """Load CSV and aggregate subscription lines by CSN."""
    accounts: dict[str, AccountAggregate] = {}
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    for _, row in df.iterrows():
        csn = str(row.get("SITE CSN", "")).strip()
        if not csn or csn.lower() in ("", "site csn"):
            continue
        if csn not in accounts:
            accounts[csn] = AccountAggregate(csn)
        accounts[csn].add_line(row.to_dict())

    return accounts


def score_outreach_readiness(enrich: dict, acct: AccountAggregate) -> float:
    """Outreach readiness score (0‑100). How actionable is this account?

    Factors: contacts available, email channel, domain for research,
    persona fit, contact quality.
    """
    s = 0.0

    contacts = enrich.get("contacts", [])
    dm_contacts = [c for c in contacts if c.get("adsk_relevant") or c.get("persona_type") not in ("unknown", "")]
    if len(dm_contacts) >= 3:
        s += 25
    elif len(dm_contacts) >= 1:
        s += 15
    elif contacts:
        s += 5

    has_verified_email = any(c.get("email") for c in contacts)
    has_estimated_email = any(c.get("email_primary") for c in contacts)
    if has_verified_email:
        s += 20
    elif has_estimated_email:
        s += 10

    pf = enrich.get("persona_fit", {})
    if pf.get("has_ideal_persona"):
        s += 20
    elif pf.get("has_good_persona"):
        s += 10

    if acct.website:
        s += 10
    if enrich.get("zi_domain"):
        s += 5

    cq = enrich.get("contact_quality_score", 0) or 0
    s += min(15, cq * 0.15)

    if acct.purchaser_emails:
        s += 5

    return min(100, round(s, 0))


def score_all(accounts: dict, enrichment: dict = None) -> list:
    """Score every account and return results sorted by priority."""
    enrichment = enrichment or {}
    results = []

    for csn, acct in accounts.items():
        if not acct.products_normalized:
            continue

        enrich = enrichment.get(csn, {})
        emp_count = enrich.get("employee_count")

        ws_score, upsells = score_whitespace(acct, enrich=enrich)
        cap_score = score_capacity(acct, employee_count=emp_count, enrich=enrich)
        grw_score = score_growth(enrich)
        tim_score = score_timing(acct)
        rel_score = score_relationship(acct)
        raw_total = ws_score + cap_score + grw_score + tim_score + rel_score

        if enrich.get("zi_is_defunct"):
            company_mult = 0.0
        elif _is_sole_proprietor(enrich) and acct.total_seats <= 2:
            company_mult = 0.4
        elif _is_sole_proprietor(enrich):
            company_mult = 0.6
        else:
            company_mult = 1.0

        total = round(raw_total * company_mult, 1)
        outreach = score_outreach_readiness(enrich, acct)

        current_acv = estimate_current_acv(acct)
        potential_acv = estimate_potential_acv(acct, upsells)

        top_upsell = upsells[0]["product"] if upsells else ""
        top_reason = upsells[0]["reason"] if upsells else ""
        all_ups = "; ".join(u["product"] for u in upsells[:3])

        size_est = _estimate_company_size(enrich)

        results.append({
            "rank": 0,
            "priority_score": total,
            "outreach_score": outreach,
            "combined_score": round(total * 0.7 + outreach * 0.3, 1),
            "csn": csn,
            "company_name": acct.company_name,
            "city": acct.city,
            "website": acct.website,
            "industry_group": acct.industry_group,
            "industry_segment": acct.industry_segment,
            "primary_segment": acct.primary_segment,
            "current_products": " | ".join(sorted(acct.products_normalized)),
            "product_count": len(acct.products_normalized),
            "total_seats": acct.total_seats,
            "maturity_level": acct.maturity_level,
            "maturity_label": acct.maturity_label,
            "whitespace_score": ws_score,
            "capacity_score": cap_score,
            "growth_score": grw_score,
            "timing_score": tim_score,
            "relationship_score": rel_score,
            "company_type_mult": company_mult,
            "company_size_est": size_est,
            "nearest_renewal": (acct.nearest_renewal.strftime("%Y-%m-%d")
                                if acct.nearest_renewal else ""),
            "days_to_renewal": (acct.days_to_renewal
                                if acct.days_to_renewal is not None else ""),
            "avg_term_years": round(acct.avg_term, 1),
            "current_acv_eur": current_acv,
            "potential_acv_eur": potential_acv,
            "top_upsell": top_upsell,
            "top_upsell_reason": top_reason,
            "all_upsells": all_ups,
            "reseller": " | ".join(sorted(acct.resellers)) if acct.resellers else "",
            "parent_account": acct.parent_account,
            "parent_csn": acct.parent_csn,
            "contact_email": (" | ".join(sorted(acct.purchaser_emails)[:3])
                              if acct.purchaser_emails else ""),
            "employee_count": emp_count or "",
            "revenue": enrich.get("revenue", ""),
            "employee_growth_1y": (enrich.get("zi_employee_growth") or {}).get("one_year_growth_rate", ""),
            "engineering_headcount": (enrich.get("zi_employee_by_department") or {}).get("engineering", ""),
            "it_headcount": (enrich.get("zi_employee_by_department") or {}).get("it", ""),
            "zi_primary_industry": enrich.get("zi_primary_industry", ""),
            "zi_parent_name": enrich.get("zi_ultimate_parent_name") or enrich.get("zi_parent_name", ""),
            "zi_parent_employees": enrich.get("zi_ultimate_parent_employees", ""),
            "zi_company_status": enrich.get("zi_company_status", ""),
            "zi_is_defunct": enrich.get("zi_is_defunct", ""),
            "zi_founded_year": enrich.get("zi_founded_year", ""),
            "zi_contacts_available": enrich.get("zi_contacts_available", ""),
            "zi_it_budget": (enrich.get("zi_department_budgets") or {}).get("it", ""),
            "zi_linkedin_url": enrich.get("zi_linkedin_url", ""),
            "enrichment_confidence": enrich.get("enrichment_confidence", 0),
            "enrichment_sources": ", ".join(enrich.get("enrichment_sources", [])),
            "contacts_count": enrich.get("contacts_count", 0),
            "autodesk_tools_in_jobs": (
                ", ".join(enrich["autodesk_tools_in_jobs"])
                if isinstance(enrich.get("autodesk_tools_in_jobs"), list)
                else ""
            ),
            "competitor_tools_in_jobs": (
                ", ".join(enrich["competitor_tools_in_jobs"])
                if isinstance(enrich.get("competitor_tools_in_jobs"), list)
                else ""
            ),
            "total_jobs": enrich.get("total_jobs", 0),
            "validation_flags": (
                "; ".join(enrich["validation_flags"])
                if isinstance(enrich.get("validation_flags"), list)
                else ""
            ),
            "upsell_hiring_intent": enrich.get("upsell_hiring_intent", False),
            "intent_strength": enrich.get("intent_strength", ""),
            "intent_summary": enrich.get("intent_summary", ""),
            "rev_per_employee": enrich.get("rev_per_employee", ""),
            "investment_label": enrich.get("investment_label", ""),
            "has_eu_grants": enrich.get("has_eu_grants", False),
            "eu_grants_count": enrich.get("eu_grants_count", 0),
            "eu_grant_summary": enrich.get("eu_grant_summary", ""),
            "tender_depth_summary": enrich.get("tender_depth_summary", ""),
            "grant_program_summary": enrich.get("grant_program_summary", ""),
            "facility_expansion": enrich.get("facility_expansion", False),
            "expansion_summary": enrich.get("expansion_summary", ""),
            "ma_detected": enrich.get("ma_detected", False),
            "ma_summary": enrich.get("ma_summary", ""),
            "bim_mandate_relevant": enrich.get("bim_mandate_relevant", False),
            "certification_summary": enrich.get("certification_summary", ""),
            "digital_transformation": enrich.get("digital_transformation", False),
            "esg_signals": enrich.get("esg_signals", False),
            "engagement_level": enrich.get("engagement_level", "none"),
            "event_summary": enrich.get("event_summary", ""),
        })

    results.sort(key=lambda x: x["priority_score"], reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1
    return results


def write_results(results: list, output_path: str):
    if not results:
        print("No results to write.")
        return
    fieldnames = list(results[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"Wrote {len(results)} accounts to {output_path}")


def print_summary(results: list):
    total = len(results)
    if not total:
        print("No accounts scored.")
        return

    print(f"\n{'=' * 70}")
    print("TERRITORY PRIORITIZATION SUMMARY (v2 — recalibrated)")
    print(f"{'=' * 70}")
    print(f"Total accounts scored: {total}")

    tiers = {"A (70+)": 0, "B (50-69)": 0, "C (30-49)": 0, "D (<30)": 0}
    for r in results:
        s = r["priority_score"]
        if s >= 70:
            tiers["A (70+)"] += 1
        elif s >= 50:
            tiers["B (50-69)"] += 1
        elif s >= 30:
            tiers["C (30-49)"] += 1
        else:
            tiers["D (<30)"] += 1

    print("\nPriority Tiers:")
    for tier, count in tiers.items():
        pct = count / total * 100 if total else 0
        print(f"  Tier {tier}: {count} accounts ({pct:.1f}%)")

    sole_prop = sum(1 for r in results if r.get("company_type_mult", 1.0) < 1.0)
    print(f"\n  Sole proprietors penalized: {sole_prop}")

    size_dist: dict = defaultdict(int)
    for r in results:
        size_dist[r.get("company_size_est", "unknown")] += 1
    print("\nCompany Size Estimates:")
    for sz in ("large", "medium", "small", "micro", "unknown"):
        count = size_dist.get(sz, 0)
        pct = count / total * 100 if total else 0
        print(f"  {sz:>8}: {count:>5} ({pct:.1f}%)")

    mat_dist: dict = defaultdict(int)
    for r in results:
        mat_dist[r["maturity_label"]] += 1
    print("\nMaturity Distribution:")
    for label in ("Entry", "Foundation", "Multi-Product", "Collection", "Platform"):
        count = mat_dist.get(label, 0)
        pct = count / total * 100 if total else 0
        print(f"  {label}: {count} ({pct:.1f}%)")

    seg_dist: dict = defaultdict(int)
    for r in results:
        seg_dist[r["primary_segment"]] += 1
    print("\nSegment Distribution:")
    for seg in ("AEC", "D&M", "M&E", "unknown"):
        count = seg_dist.get(seg, 0)
        pct = count / total * 100 if total else 0
        print(f"  {seg}: {count} ({pct:.1f}%)")

    outreach_vals = [r.get("outreach_score", 0) for r in results]
    avg_outreach = sum(outreach_vals) / total if total else 0
    high_outreach = sum(1 for v in outreach_vals if v >= 50)
    print(f"\nOutreach Readiness:")
    print(f"  Average score: {avg_outreach:.0f}/100")
    print(f"  High readiness (50+): {high_outreach} accounts")

    print(f"\n{'=' * 70}")
    print("TOP 20 ACCOUNTS BY PRIORITY SCORE")
    print(f"{'=' * 70}")
    header = (f"{'Rank':<5} {'Score':<7} {'Out':<5} {'Size':<8} {'Company':<35} "
              f"{'Seats':<7} {'Maturity':<14} {'Top Upsell':<22}")
    print(header)
    print("-" * 110)
    for r in results[:20]:
        print(f"{r['rank']:<5} {r['priority_score']:<7} "
              f"{r.get('outreach_score', 0):<5.0f} "
              f"{r.get('company_size_est', '?')[:7]:<8} "
              f"{r['company_name'][:34]:<35} {r['total_seats']:<7} "
              f"{r['maturity_label']:<14} {r['top_upsell'][:21]:<22}")

    total_cur = sum(r["current_acv_eur"] for r in results)
    total_pot = sum(r["potential_acv_eur"] for r in results)
    t50_cur = sum(r["current_acv_eur"] for r in results[:50])
    t50_pot = sum(r["potential_acv_eur"] for r in results[:50])

    print("\nACV Analysis:")
    print(f"  Total current ACV (all accounts):      EUR {total_cur:,.0f}")
    print(f"  Total whitespace potential (all):       EUR {total_pot:,.0f}")
    print(f"  Top 50 current ACV:                    EUR {t50_cur:,.0f}")
    print(f"  Top 50 whitespace potential:            EUR {t50_pot:,.0f}")


# ── CLI ──────────────────────────────────────────────────────


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Territory-wide account prioritization scorer",
    )
    parser.add_argument("csv_path", help="Path to the Autodesk subscription CSV")
    parser.add_argument(
        "-o", "--output", default=None,
        help="Output CSV path (default: prioritized_accounts.csv next to input)",
    )
    parser.add_argument("--top", type=int, default=None, help="Only output top N accounts")
    parser.add_argument(
        "--enrichment", default=None,
        help="Path to enrichment JSON file (CSN -> enrichment data)",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress summary output")
    args = parser.parse_args()

    csv_path = args.csv_path
    output_path = args.output or str(
        Path(csv_path).parent / "prioritized_accounts.csv"
    )

    print(f"Loading accounts from {csv_path} ...")
    accounts = load_and_aggregate(csv_path)
    print(f"Aggregated {len(accounts)} unique accounts (by CSN)")

    enrichment = {}
    if args.enrichment:
        with open(args.enrichment, "r", encoding="utf-8") as f:
            enrichment = json.load(f)
        print(f"Loaded enrichment data for {len(enrichment)} accounts")

    print("Scoring accounts ...")
    results = score_all(accounts, enrichment=enrichment)

    if args.top:
        results = results[:args.top]

    write_results(results, output_path)

    if not args.quiet:
        print_summary(results)

    return results


if __name__ == "__main__":
    main()
