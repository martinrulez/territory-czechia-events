"""Batch enrichment for prioritized accounts.

Reads the prioritized_accounts.csv from Phase 1, enriches top N accounts
via ZoomInfo (employees, revenue) and ARES (ICO, NACE codes), saves results
as JSON, and optionally re-scores with enrichment data.

Usage:
    python -m scoring.batch_enricher                         # enrich top 500
    python -m scoring.batch_enricher --top 100               # top 100 only
    python -m scoring.batch_enricher --ares-only              # skip ZoomInfo
    python -m scoring.batch_enricher --rescore                # re-run scorer
"""

import json
import sys
import time
from pathlib import Path

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from db.database import init_db
from enrichment.zoominfo_client import enrich_company as zi_enrich
from scraper.ares_client import lookup_company as ares_lookup

ENRICHMENT_DIR = Path(__file__).resolve().parent.parent / "enrichment_data"


def load_prioritized(csv_path: str, top_n: int = 500) -> list:
    """Load the top N accounts from the prioritized CSV."""
    import pandas as pd

    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    df = df.head(top_n)
    accounts = []
    for _, row in df.iterrows():
        outreach_raw = row.get("outreach_score", "0")
        combined_raw = row.get("combined_score", "0")
        ctype_raw = row.get("company_type_mult", "1.0")
        accounts.append({
            "rank": int(float(row.get("rank", 0))),
            "priority_score": float(row.get("priority_score", 0)),
            "outreach_score": float(outreach_raw) if outreach_raw else 0,
            "combined_score": float(combined_raw) if combined_raw else 0,
            "company_type_mult": float(ctype_raw) if ctype_raw else 1.0,
            "company_size_est": row.get("company_size_est", "unknown"),
            "csn": row.get("csn", ""),
            "company_name": row.get("company_name", ""),
            "city": row.get("city", ""),
            "website": row.get("website", ""),
            "industry_group": row.get("industry_group", ""),
            "industry_segment": row.get("industry_segment", ""),
            "primary_segment": row.get("primary_segment", ""),
            "current_products": row.get("current_products", ""),
            "product_count": int(float(row.get("product_count", 0))),
            "total_seats": int(float(row.get("total_seats", 0))),
            "maturity_level": int(float(row.get("maturity_level", 0))),
            "maturity_label": row.get("maturity_label", ""),
            "whitespace_score": float(row.get("whitespace_score", 0)),
            "capacity_score": float(row.get("capacity_score", 0)),
            "growth_score": float(row.get("growth_score", 0)),
            "timing_score": float(row.get("timing_score", 0)),
            "relationship_score": float(row.get("relationship_score", 0)),
            "nearest_renewal": row.get("nearest_renewal", ""),
            "current_acv_eur": row.get("current_acv_eur", "0"),
            "potential_acv_eur": row.get("potential_acv_eur", "0"),
            "top_upsell": row.get("top_upsell", ""),
            "top_upsell_reason": row.get("top_upsell_reason", ""),
            "all_upsells": row.get("all_upsells", ""),
            "reseller": row.get("reseller", ""),
            "parent_account": row.get("parent_account", ""),
            "contact_email": row.get("contact_email", ""),
        })
    return accounts


def enrich_with_ares(accounts: list, existing: dict) -> dict:
    """Run ARES lookup for each account, merging into existing enrichment."""
    print(f"\n--- ARES Enrichment ({len(accounts)} accounts) ---")
    success = 0
    cached = 0
    failed = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        if not name or len(name) < 2:
            failed += 1
            continue

        if csn not in existing:
            existing[csn] = {}

        ares_key = f"ares_{csn}"
        if existing[csn].get("ares_done"):
            cached += 1
            _log_progress(i + 1, len(accounts), name, "cached")
            continue

        result = ares_lookup(name)
        if result.get("success"):
            existing[csn]["ico"] = result.get("ico", "")
            existing[csn]["official_name"] = result.get("official_name", "")
            existing[csn]["legal_form"] = result.get("legal_form", "")
            existing[csn]["nace_codes"] = result.get("nace_codes", [])
            existing[csn]["ares_segments"] = result.get("autodesk_segments", [])
            existing[csn]["ares_primary_segment"] = result.get("primary_segment", "")
            existing[csn]["ares_address"] = result.get("address", "")
            existing[csn]["ares_done"] = True
            success += 1
        else:
            existing[csn]["ares_done"] = True
            existing[csn]["ares_error"] = result.get("error", "")
            failed += 1

        _log_progress(i + 1, len(accounts), name, "OK" if result.get("success") else "miss")

    print(f"\nARES: {success} found, {cached} cached, {failed} failed")
    return existing


def enrich_with_zoominfo(accounts: list, existing: dict) -> dict:
    """Run ZoomInfo company enrichment, merging into existing enrichment."""
    print(f"\n--- ZoomInfo Enrichment ({len(accounts)} accounts) ---")
    success = 0
    cached = 0
    failed = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        domain = acct.get("website", "")

        if csn not in existing:
            existing[csn] = {}

        if existing[csn].get("zi_done"):
            cached += 1
            _log_progress(i + 1, len(accounts), name, "cached")
            continue

        result = zi_enrich(company_name=name, domain=domain if domain else None)
        if result.get("success"):
            existing[csn]["employee_count"] = result.get("employee_count")
            existing[csn]["revenue"] = result.get("revenue")
            existing[csn]["zi_company_name"] = result.get("company_name", "")
            existing[csn]["zi_domain"] = result.get("domain", "")
            existing[csn]["zi_city"] = result.get("city", "")
            existing[csn]["zi_country"] = result.get("country", "")
            existing[csn]["zi_done"] = True
            success += 1
        else:
            existing[csn]["zi_done"] = True
            existing[csn]["zi_error"] = result.get("error", "")
            failed += 1

        _log_progress(i + 1, len(accounts), name, "OK" if result.get("success") else "miss")
        time.sleep(0.1)

    print(f"\nZoomInfo: {success} enriched, {cached} cached, {failed} failed")
    return existing


def _log_progress(current: int, total: int, name: str, status: str):
    pct = current / total * 100 if total else 0
    print(f"  [{current}/{total} {pct:5.1f}%] {name[:50]:<50} {status}")


def save_enrichment(enrichment: dict, output_path: str):
    """Save enrichment data as JSON."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enrichment, f, ensure_ascii=False, indent=2)
    print(f"Saved enrichment data for {len(enrichment)} accounts to {output_path}")


def load_enrichment(path: str) -> dict:
    """Load previously saved enrichment data."""
    p = Path(path)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def print_enrichment_summary(enrichment: dict):
    """Print a summary of enrichment coverage."""
    total = len(enrichment)
    with_emp = sum(1 for v in enrichment.values() if v.get("employee_count"))
    with_rev = sum(1 for v in enrichment.values() if v.get("revenue"))
    with_ico = sum(1 for v in enrichment.values() if v.get("ico"))
    with_nace = sum(1 for v in enrichment.values()
                    if v.get("nace_codes") and len(v["nace_codes"]) > 0)

    print(f"\n{'=' * 50}")
    print("ENRICHMENT COVERAGE SUMMARY")
    print(f"{'=' * 50}")
    print(f"Total accounts:        {total}")
    print(f"With employee count:   {with_emp} ({with_emp / total * 100:.1f}%)")
    print(f"With revenue:          {with_rev} ({with_rev / total * 100:.1f}%)")
    print(f"With ICO (ARES):       {with_ico} ({with_ico / total * 100:.1f}%)")
    print(f"With NACE codes:       {with_nace} ({with_nace / total * 100:.1f}%)")

    if with_emp > 0:
        emp_values = [v["employee_count"] for v in enrichment.values()
                      if v.get("employee_count") and isinstance(v["employee_count"], (int, float))]
        if emp_values:
            print(f"\nEmployee count range: {min(emp_values)} - {max(emp_values)}")
            print(f"Median employees: {sorted(emp_values)[len(emp_values) // 2]}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Batch enrichment for prioritized accounts")
    parser.add_argument(
        "--input", default=None,
        help="Path to prioritized_accounts.csv (auto-detected if not given)",
    )
    parser.add_argument("--top", type=int, default=500, help="Enrich top N accounts")
    parser.add_argument("--ares-only", action="store_true", help="Only run ARES (skip ZoomInfo)")
    parser.add_argument("--zi-only", action="store_true", help="Only run ZoomInfo (skip ARES)")
    parser.add_argument(
        "--rescore", action="store_true",
        help="Re-run the territory scorer with enrichment data",
    )
    parser.add_argument(
        "--output", default=None,
        help="Output JSON path for enrichment data",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    workspace_root = project_root.parent

    input_csv = args.input or str(workspace_root / "prioritized_accounts.csv")
    if not Path(input_csv).exists():
        print(f"Error: {input_csv} not found. Run prioritize.py first.")
        sys.exit(1)

    enrichment_path = args.output or str(ENRICHMENT_DIR / "account_enrichment.json")

    init_db()

    print(f"Loading top {args.top} accounts from {input_csv} ...")
    accounts = load_prioritized(input_csv, top_n=args.top)
    print(f"Loaded {len(accounts)} accounts for enrichment")

    enrichment = load_enrichment(enrichment_path)
    print(f"Existing enrichment data: {len(enrichment)} accounts")

    if not args.zi_only:
        enrichment = enrich_with_ares(accounts, enrichment)
        save_enrichment(enrichment, enrichment_path)

    if not args.ares_only:
        enrichment = enrich_with_zoominfo(accounts, enrichment)
        save_enrichment(enrichment, enrichment_path)

    print_enrichment_summary(enrichment)

    if args.rescore:
        print("\n--- Re-scoring with enrichment data ---")
        from scoring.territory_scorer import (
            load_and_aggregate,
            score_all,
            write_results,
            print_summary,
        )

        csv_source = str(workspace_root / "Martin Valovic FY27 1 copy.csv")
        accts = load_and_aggregate(csv_source)
        results = score_all(accts, enrichment=enrichment)
        output_csv = str(workspace_root / "prioritized_accounts_enriched.csv")
        write_results(results, output_csv)
        print_summary(results)


if __name__ == "__main__":
    main()
