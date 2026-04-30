"""Lead scoring engine with weighted criteria and lead classification."""

from db.database import (
    get_event_companies,
    get_account,
    get_company_event_count,
    get_cached_enrichment,
    get_event,
    update_event_company_score,
)
from scoring.competitors import (
    calculate_whitespace,
    find_competitor_products_in_tech_stack,
    detect_industry_segment,
    ALL_COMPETITOR_COMPANIES,
)

SCORING_WEIGHTS = {
    "existing_client": 15,
    "whitespace_gap": 20,
    "company_size": 10,
    "industry_fit": 15,
    "event_count": 10,
    "competitor_usage": 15,
    "contact_seniority": 10,
    "event_relevance": 5,
}

SENIOR_TITLES = {
    "ceo", "cto", "cfo", "coo", "cio", "vp", "vice president",
    "director", "head of", "chief", "president", "owner", "founder",
    "managing director", "general manager", "partner", "reditel", "jednatel",
}


def _score_seniority(title: str) -> float:
    """Score 0.0-1.0 based on contact title seniority."""
    if not title:
        return 0.0
    title_lower = title.lower()
    for senior in SENIOR_TITLES:
        if senior in title_lower:
            return 1.0
    if any(w in title_lower for w in ("manager", "lead", "senior", "vedouci")):
        return 0.6
    return 0.3


def _score_company_size(employee_count: str) -> float:
    """Score 0.0-1.0 based on company size."""
    if not employee_count:
        return 0.3
    try:
        count = int(str(employee_count).replace(",", "").replace("+", "").strip())
    except ValueError:
        return 0.3
    if count >= 1000:
        return 1.0
    if count >= 500:
        return 0.8
    if count >= 100:
        return 0.6
    if count >= 50:
        return 0.4
    return 0.2


def _score_industry_fit(account_industry: str, event_focus: str, ares_segment: str = None) -> float:
    """Score 0.0-1.0 based on industry alignment with Autodesk segments.

    If ARES NACE-derived segment is available, use it as the most reliable source.
    """
    segment = ares_segment or detect_industry_segment(account_industry)
    if segment == "unknown":
        return 0.3
    if event_focus and segment.lower() == event_focus.lower():
        return 1.0
    return 0.5


def score_leads(event_id: int = None) -> dict:
    """Score all event companies and classify them as leads.

    Args:
        event_id: If given, only score companies from this event.

    Returns:
        Summary dict.
    """
    event_companies = get_event_companies(event_id=event_id)
    scored = 0

    for ec in event_companies:
        score = 0
        lead_class = "new_market"

        is_existing = ec["matched_account_id"] is not None
        account = get_account(ec["matched_account_id"]) if is_existing else None

        event = get_event(ec["event_id"]) if ec["event_id"] else None
        event_focus = event["industry_focus"] if event else None
        event_relevance = (event["relevance_score"] or 5) if event else 5

        if is_existing and account:
            score += SCORING_WEIGHTS["existing_client"]

            whitespace = calculate_whitespace(
                account["autodesk_products"],
                account["industry"],
            )
            gap = whitespace["gap_ratio"]
            score += int(SCORING_WEIGHTS["whitespace_gap"] * gap)

            if account["autodesk_products"]:
                if gap > 0.8:
                    lead_class = "whitespace"
                else:
                    lead_class = "upsell"
            else:
                lead_class = "whitespace"

            size_score = _score_company_size(account["employee_count"])
            score += int(SCORING_WEIGHTS["company_size"] * size_score)

            fit_score = _score_industry_fit(account["industry"], event_focus)
            score += int(SCORING_WEIGHTS["industry_fit"] * fit_score)
        else:
            domain_or_name = (ec["company_domain"] or ec["company_name"]).lower()
            enrichment = get_cached_enrichment("company", domain_or_name)

            ares_key = f"ares:{ec['company_name'].lower().strip()}"
            ares_data = get_cached_enrichment("ares", ares_key)
            ares_segment = None
            if ares_data and ares_data.get("success"):
                ares_segment = ares_data.get("primary_segment")

            if enrichment:
                size_score = _score_company_size(str(enrichment.get("employee_count", "")))
                score += int(SCORING_WEIGHTS["company_size"] * size_score)

                industry = enrichment.get("industry", "")
                fit_score = _score_industry_fit(industry, event_focus, ares_segment=ares_segment)
                score += int(SCORING_WEIGHTS["industry_fit"] * fit_score)

                tech_stack = enrichment.get("tech_stack", [])
                if isinstance(tech_stack, list):
                    comp_matches = find_competitor_products_in_tech_stack(tech_stack)
                    if comp_matches:
                        score += SCORING_WEIGHTS["competitor_usage"]
                        lead_class = "displacement"
            elif ares_segment and ares_segment != "unknown":
                fit_score = _score_industry_fit(None, event_focus, ares_segment=ares_segment)
                score += int(SCORING_WEIGHTS["industry_fit"] * fit_score)
            else:
                score += int(SCORING_WEIGHTS["industry_fit"] * 0.3)

            company_lower = (ec["company_name"] or "").lower()
            for comp in ALL_COMPETITOR_COMPANIES:
                if comp in company_lower or company_lower in comp:
                    lead_class = "displacement"
                    score += SCORING_WEIGHTS["competitor_usage"]
                    break

        role = ec["role"] or ""
        if role == "past_attendee":
            score += 3

        event_count = get_company_event_count(ec["company_name"])
        if event_count >= 3:
            score += SCORING_WEIGHTS["event_count"]
        elif event_count >= 2:
            score += int(SCORING_WEIGHTS["event_count"] * 0.6)
        elif event_count >= 1:
            score += int(SCORING_WEIGHTS["event_count"] * 0.3)

        seniority = _score_seniority(ec["person_title"])
        score += int(SCORING_WEIGHTS["contact_seniority"] * seniority)

        relevance_normalized = min(event_relevance, 10) / 10.0
        score += int(SCORING_WEIGHTS["event_relevance"] * relevance_normalized)

        score = min(score, 100)
        update_event_company_score(ec["id"], score, lead_class)
        scored += 1

    return {"success": True, "scored": scored, "total": len(event_companies)}
