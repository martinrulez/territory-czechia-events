"""Opportunity scoring engine for event companies.

Computes an opportunity score (0-100) for each company attending events,
combining company size, revenue, hiring signals, NACE/industry fit,
event relevance, and estimated deal size.

Scores are stored back into the event_company_enrichment JSON and
can be used by the dashboard for prioritization.

Usage:
    python -m scoring.opportunity_scorer                     # score all
    python -m scoring.opportunity_scorer --export results.csv
"""

import json
import sys
from pathlib import Path

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from db.database import init_db, get_connection
from scoring.event_enricher import (
    load_event_companies,
    load_event_enrichment,
    save_event_enrichment,
)

NACE_SEGMENT_MAP = {
    "41": "AEC", "42": "AEC", "43": "AEC", "71": "AEC", "68": "AEC",
    "24": "D&M", "25": "D&M", "26": "D&M", "27": "D&M",
    "28": "D&M", "29": "D&M", "30": "D&M", "22": "D&M", "33": "D&M",
    "58": "M&E", "59": "M&E", "60": "M&E",
    "62": "M&E", "63": "M&E", "90": "M&E",
}

SEGMENT_ACV_EUR = {
    "AEC": {"small": 4200, "mid": 15000, "large": 60000},
    "D&M": {"small": 4200, "mid": 20000, "large": 80000},
    "M&E": {"small": 4200, "mid": 12000, "large": 50000},
    "unknown": {"small": 2200, "mid": 8000, "large": 30000},
}


def _company_size_tier(employee_count):
    """Return size tier based on employee count."""
    if not employee_count or not isinstance(employee_count, (int, float)):
        return "small"
    if employee_count >= 500:
        return "large"
    if employee_count >= 50:
        return "mid"
    return "small"


def _nace_fit_score(nace_codes):
    """Score 0.0-1.0 based on how well NACE codes align with Autodesk segments."""
    if not nace_codes:
        return 0.2
    matches = 0
    for code in nace_codes:
        prefix = str(code).strip()[:2]
        if prefix in NACE_SEGMENT_MAP:
            matches += 1
    if matches == 0:
        return 0.1
    ratio = matches / len(nace_codes)
    return min(1.0, 0.3 + ratio * 0.7)


def _nace_primary_segment(nace_codes):
    """Determine primary Autodesk segment from NACE codes."""
    if not nace_codes:
        return "unknown"
    segment_counts = {}
    for code in nace_codes:
        prefix = str(code).strip()[:2]
        seg = NACE_SEGMENT_MAP.get(prefix)
        if seg:
            segment_counts[seg] = segment_counts.get(seg, 0) + 1
    if not segment_counts:
        return "unknown"
    return max(segment_counts, key=segment_counts.get)


def _size_score(employee_count):
    """Score 0.0-1.0 based on company size (bigger = higher opportunity)."""
    if not employee_count or not isinstance(employee_count, (int, float)):
        return 0.3
    if employee_count >= 1000:
        return 1.0
    if employee_count >= 500:
        return 0.85
    if employee_count >= 200:
        return 0.7
    if employee_count >= 100:
        return 0.55
    if employee_count >= 50:
        return 0.4
    if employee_count >= 20:
        return 0.3
    return 0.15


def _revenue_score(revenue):
    """Score 0.0-1.0 based on revenue."""
    if not revenue or not isinstance(revenue, (int, float)):
        return 0.2
    if revenue >= 100_000_000:
        return 1.0
    if revenue >= 50_000_000:
        return 0.85
    if revenue >= 10_000_000:
        return 0.7
    if revenue >= 5_000_000:
        return 0.55
    if revenue >= 1_000_000:
        return 0.4
    return 0.2


def _event_relevance_score(event_count, segments_str):
    """Score based on number of events attending and segment match."""
    base = min(1.0, event_count * 0.25)
    if segments_str:
        known = [s.strip() for s in segments_str.split(",") if s.strip() and s.strip() != "None"]
        if len(known) > 1:
            base = min(1.0, base + 0.1)
    return base


def _signal_score(enrichment_entry):
    """Score 0.0-1.0 based on growth/intent signals."""
    score = 0.0
    if enrichment_entry.get("hiring_signal"):
        score += 0.3
        if enrichment_entry.get("engineering_hiring"):
            score += 0.15
        if enrichment_entry.get("autodesk_tools_in_jobs"):
            score += 0.15
        if enrichment_entry.get("competitor_tools_in_jobs"):
            score += 0.1
    if enrichment_entry.get("leadership_change"):
        score += 0.15
    if enrichment_entry.get("has_public_contracts"):
        score += 0.1
    if enrichment_entry.get("revenue_growth") and enrichment_entry["revenue_growth"] > 5:
        score += 0.1
    return min(1.0, score)


def estimate_deal_size(enrichment_entry, segment=None):
    """Estimate potential deal size in EUR based on company profile."""
    seg = segment or enrichment_entry.get("ares_primary_segment", "unknown")
    tier = _company_size_tier(enrichment_entry.get("employee_count"))
    acv_map = SEGMENT_ACV_EUR.get(seg, SEGMENT_ACV_EUR["unknown"])
    base_acv = acv_map[tier]

    multiplier = 1.0
    emp = enrichment_entry.get("employee_count")
    if emp and isinstance(emp, (int, float)):
        potential_seats = max(1, emp * 0.05)
        if potential_seats > 5:
            multiplier = min(potential_seats / 5, 10.0)

    return int(base_acv * multiplier)


WEIGHTS = {
    "size": 0.15,
    "revenue": 0.10,
    "nace_fit": 0.20,
    "event_relevance": 0.15,
    "signals": 0.20,
    "is_client": 0.20,
}


def score_company(company_row, enrichment_entry, account_enrichment=None):
    """Compute opportunity score (0-100) for a single event company."""
    e = enrichment_entry or {}

    size = _size_score(e.get("employee_count"))
    revenue = _revenue_score(e.get("revenue"))
    nace = _nace_fit_score(e.get("nace_codes"))
    events = _event_relevance_score(
        company_row.get("event_count", 1),
        company_row.get("segments", ""),
    )
    signals = _signal_score(e)

    client_bonus = 0.0
    acct_data = None
    if company_row.get("matched_account_id") and account_enrichment:
        client_bonus = 0.7
        acct_data = account_enrichment
    elif company_row.get("matched_account_id"):
        client_bonus = 0.5

    raw = (
        WEIGHTS["size"] * size
        + WEIGHTS["revenue"] * revenue
        + WEIGHTS["nace_fit"] * nace
        + WEIGHTS["event_relevance"] * events
        + WEIGHTS["signals"] * signals
        + WEIGHTS["is_client"] * client_bonus
    )

    score = int(min(100, raw * 100))

    segment = _nace_primary_segment(e.get("nace_codes"))
    deal_size = estimate_deal_size(e, segment)

    timing = []
    if e.get("hiring_signal"):
        total = e.get("total_jobs", 0)
        timing.append(f"Hiring ({total} jobs)")
    if e.get("engineering_hiring"):
        timing.append("Engineering roles open")
    if e.get("leadership_change"):
        timing.append("Leadership change")
    if e.get("has_public_contracts"):
        timing.append("Public contracts")
    if e.get("competitor_tools_in_jobs"):
        tools = e["competitor_tools_in_jobs"]
        timing.append(f"Uses competitor: {', '.join(tools[:3])}")
    if e.get("revenue_growth") and e["revenue_growth"] > 10:
        timing.append(f"Revenue growth {e['revenue_growth']:+.0f}%")

    return {
        "opportunity_score": score,
        "estimated_deal_eur": deal_size,
        "primary_segment": segment,
        "size_tier": _company_size_tier(e.get("employee_count")),
        "timing_signals": timing,
        "fit_score": round(nace, 2),
        "component_scores": {
            "size": round(size, 2),
            "revenue": round(revenue, 2),
            "nace_fit": round(nace, 2),
            "event_relevance": round(events, 2),
            "signals": round(signals, 2),
            "client_bonus": round(client_bonus, 2),
        },
    }


def score_all_event_companies():
    """Score all event companies and store results in enrichment JSON."""
    from scoring.batch_enricher import load_enrichment, ENRICHMENT_DIR

    init_db()

    companies = load_event_companies()
    event_enrichment = load_event_enrichment()
    account_enrichment = load_enrichment(
        str(ENRICHMENT_DIR / "account_enrichment.json")
    )

    print(f"Scoring {len(companies)} event companies...")

    scored = 0
    for co in companies:
        key = co["company_key"]
        e = event_enrichment.get(key, {})

        acct_enrich = None
        if co.get("matched_account_id"):
            with get_connection() as conn:
                acct_row = conn.execute(
                    "SELECT company_name FROM accounts WHERE id=?",
                    (co["matched_account_id"],),
                ).fetchone()
                if acct_row:
                    acct_name = dict(acct_row)["company_name"]
                    for csn, data in account_enrichment.items():
                        stored = data.get("official_name", "").lower()
                        if stored and (
                            acct_name.lower() in stored or stored in acct_name.lower()
                        ):
                            acct_enrich = data
                            break

        result = score_company(co, e, acct_enrich)

        if key not in event_enrichment:
            event_enrichment[key] = {"company_name": co["company_name"]}
        event_enrichment[key].update(result)
        scored += 1

    save_event_enrichment(event_enrichment)

    scores = [
        event_enrichment[co["company_key"]].get("opportunity_score", 0)
        for co in companies
        if co["company_key"] in event_enrichment
    ]
    if scores:
        scores.sort(reverse=True)
        print(f"\nScored {scored} companies")
        print(f"  Score range: {min(scores)} - {max(scores)}")
        print(f"  Median: {scores[len(scores) // 2]}")
        print(f"  Top 20 scoring >= {scores[min(19, len(scores) - 1)]}")

    return event_enrichment


def export_csv(enrichment, output_path):
    """Export scored companies to CSV."""
    import csv

    companies = load_event_companies()
    rows = []
    for co in companies:
        key = co["company_key"]
        e = enrichment.get(key, {})
        rows.append({
            "company_name": co["company_name"],
            "events": co.get("events", ""),
            "event_count": co.get("event_count", 0),
            "lead_class": co.get("lead_class", ""),
            "opportunity_score": e.get("opportunity_score", 0),
            "estimated_deal_eur": e.get("estimated_deal_eur", 0),
            "primary_segment": e.get("primary_segment", ""),
            "size_tier": e.get("size_tier", ""),
            "employee_count": e.get("employee_count", ""),
            "revenue": e.get("revenue", ""),
            "ico": e.get("ico", ""),
            "timing_signals": "; ".join(e.get("timing_signals", [])),
            "fit_score": e.get("fit_score", ""),
        })

    rows.sort(key=lambda r: r["opportunity_score"], reverse=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported {len(rows)} companies to {output_path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Opportunity scoring for event companies")
    parser.add_argument("--export", default=None, help="Export to CSV path")
    args = parser.parse_args()

    enrichment = score_all_event_companies()

    if args.export:
        export_csv(enrichment, args.export)


if __name__ == "__main__":
    main()
