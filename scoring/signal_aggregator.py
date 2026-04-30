"""Growth signal aggregator for prioritized accounts.

Orchestrates the Czech-specific signal scrapers (OR, ISVZ, jobs.cz, kurzy.cz)
and merges their outputs into the enrichment JSON used by the territory scorer.

Usage:
    python -m scoring.signal_aggregator                    # top 200, all signals
    python -m scoring.signal_aggregator --top 50           # top 50
    python -m scoring.signal_aggregator --signals or,jobs  # only OR + jobs
    python -m scoring.signal_aggregator --rescore          # re-run scorer after
"""

import json
import sys
import time
from pathlib import Path

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from db.database import init_db
from scoring.batch_enricher import (
    ENRICHMENT_DIR,
    load_enrichment,
    load_prioritized,
    save_enrichment,
)

SIGNAL_SOURCES = ("or", "isvz", "jobs", "kurzy", "smlouvy", "kurzy_pw", "intent", "advanced")


def collect_signals(
    accounts: list,
    enrichment: dict,
    signals: tuple = SIGNAL_SOURCES,
) -> dict:
    """Run selected signal scrapers and merge results into enrichment.

    Each signal source adds specific fields to the enrichment dict,
    plus a boolean flag that the territory scorer uses for growth scoring.
    """
    if "or" in signals:
        enrichment = _collect_or_signals(accounts, enrichment)

    if "kurzy" in signals:
        enrichment = _collect_kurzy_signals(accounts, enrichment)

    if "kurzy_pw" in signals:
        enrichment = _collect_kurzy_pw_signals(accounts, enrichment)

    if "isvz" in signals:
        enrichment = _collect_isvz_signals(accounts, enrichment)

    if "jobs" in signals:
        enrichment = _collect_jobs_signals(accounts, enrichment)

    if "smlouvy" in signals:
        enrichment = _collect_smlouvy_signals(accounts, enrichment)

    if "intent" in signals:
        from scoring.intent_signals import enrich_intent_signals
        enrichment = enrich_intent_signals(enrichment, accounts)

    if "advanced" in signals:
        from scoring.advanced_signals import enrich_advanced_signals
        enrichment = enrich_advanced_signals(enrichment, accounts)

    return enrichment


def _collect_or_signals(accounts: list, enrichment: dict) -> dict:
    """Check leadership changes via OR (justice.cz)."""
    from scraper.or_client import check_leadership_changes

    print(f"\n--- OR / Leadership Change Signals ({len(accounts)} accounts) ---")
    found = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        enrich = enrichment.get(csn, {})
        ico = enrich.get("ico")

        if not ico:
            _log(i + 1, len(accounts), name, "no ICO")
            continue

        if enrich.get("or_done"):
            _log(i + 1, len(accounts), name, "cached")
            continue

        result = check_leadership_changes(ico, months_back=12)
        enrich["or_done"] = True

        if result.get("change_detected"):
            enrich["leadership_change"] = True
            enrich["leadership_changes_count"] = result.get("changes_count", 0)
            enrich["statutory_body"] = result.get("statutory_body", [])
            found += 1
            _log(i + 1, len(accounts), name, f"CHANGE ({result.get('changes_count', 0)})")
        else:
            enrich["leadership_change"] = False
            enrich["statutory_body"] = result.get("statutory_body", [])
            _log(i + 1, len(accounts), name, "no change")

        enrichment[csn] = enrich

    print(f"\nOR: {found} companies with recent leadership changes")
    return enrichment


def _collect_kurzy_signals(accounts: list, enrichment: dict) -> dict:
    """Get financial data from kurzy.cz."""
    from scraper.kurzy_client import get_financial_signal

    print(f"\n--- Kurzy.cz / Financial Signals ({len(accounts)} accounts) ---")
    found = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        enrich = enrichment.get(csn, {})
        ico = enrich.get("ico")

        if not ico:
            _log(i + 1, len(accounts), name, "no ICO")
            continue

        if enrich.get("kurzy_done"):
            _log(i + 1, len(accounts), name, "cached")
            continue

        result = get_financial_signal(ico, name)
        enrich["kurzy_done"] = True

        if result.get("has_financials"):
            if result.get("employees") and not enrich.get("employee_count"):
                enrich["employee_count"] = result["employees"]
            if result.get("revenue_czk"):
                enrich["revenue_czk"] = result["revenue_czk"]
            if result.get("profit_czk"):
                enrich["profit_czk"] = result["profit_czk"]
            if result.get("revenue_growth_pct") is not None:
                enrich["revenue_growth"] = result["revenue_growth_pct"]
            found += 1
            growth_str = (f"{result['revenue_growth_pct']:+.1f}%"
                          if result.get("revenue_growth_pct") is not None else "n/a")
            _log(i + 1, len(accounts), name, f"OK growth={growth_str}")
        else:
            _log(i + 1, len(accounts), name, "no data")

        enrichment[csn] = enrich

    print(f"\nKurzy: {found} companies with financial data")
    return enrichment


def _collect_isvz_signals(accounts: list, enrichment: dict) -> dict:
    """Check public procurement via ISVZ."""
    from scraper.isvz_client import get_procurement_signal

    print(f"\n--- ISVZ / Public Procurement Signals ({len(accounts)} accounts) ---")
    found = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        enrich = enrichment.get(csn, {})

        if enrich.get("isvz_done"):
            _log(i + 1, len(accounts), name, "cached")
            continue

        ico = enrich.get("ico")
        result = get_procurement_signal(name, ico)
        enrich["isvz_done"] = True

        if result.get("has_signal"):
            enrich["has_public_contracts"] = True
            enrich["public_contracts_count"] = result.get("contracts", 0)
            enrich["public_contracts_value_czk"] = result.get("value_czk", 0)
            enrich["aec_contracts_count"] = result.get("aec_contracts", 0)
            found += 1
            _log(i + 1, len(accounts), name,
                 f"contracts={result.get('contracts', 0)}")
        else:
            enrich["has_public_contracts"] = False
            _log(i + 1, len(accounts), name, "none")

        enrichment[csn] = enrich

    print(f"\nISVZ: {found} companies with public contracts")
    return enrichment


def _collect_jobs_signals(accounts: list, enrichment: dict) -> dict:
    """Check hiring activity via jobs.cz."""
    from scraper.jobs_scraper import get_hiring_signal

    print(f"\n--- Jobs.cz / Hiring Signals ({len(accounts)} accounts) ---")
    found = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        enrich = enrichment.get(csn, {})

        if enrich.get("jobs_done"):
            _log(i + 1, len(accounts), name, "cached")
            continue

        result = get_hiring_signal(name)
        enrich["jobs_done"] = True

        if result.get("hiring_signal"):
            enrich["hiring_signal"] = True
            enrich["total_jobs"] = result.get("total_jobs", 0)
            enrich["engineering_hiring"] = result.get("engineering_hiring", False)
            enrich["autodesk_tools_in_jobs"] = result.get("autodesk_tools", [])
            enrich["competitor_tools_in_jobs"] = result.get("competitor_tools", [])
            enrich["relevant_roles"] = result.get("relevant_roles", [])
            found += 1
            tools = result.get("autodesk_tools", []) + result.get("competitor_tools", [])
            tools_str = ", ".join(tools[:3]) if tools else "generic"
            _log(i + 1, len(accounts), name,
                 f"jobs={result.get('total_jobs', 0)} tools=[{tools_str}]")
        else:
            enrich["hiring_signal"] = False
            _log(i + 1, len(accounts), name, "not hiring")

        enrichment[csn] = enrich

    print(f"\nJobs: {found} companies actively hiring")
    return enrichment


def _collect_kurzy_pw_signals(accounts: list, enrichment: dict) -> dict:
    """Get employee data from Kurzy.cz via Playwright headless browser."""
    from scraper.kurzy_playwright import get_financial_signal_pw, close_browser

    print(f"\n--- Kurzy PW / Headless Browser Signals ({len(accounts)} accounts) ---")
    found = 0
    tried = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        enrich = enrichment.get(csn, {})
        ico = enrich.get("ico")

        if not ico:
            _log(i + 1, len(accounts), name, "no ICO")
            continue

        if enrich.get("kurzy_pw_done"):
            _log(i + 1, len(accounts), name, "cached")
            continue

        if enrich.get("employee_count"):
            enrich["kurzy_pw_done"] = True
            _log(i + 1, len(accounts), name, "already has employees")
            enrichment[csn] = enrich
            continue

        tried += 1
        result = get_financial_signal_pw(ico, name)
        enrich["kurzy_pw_done"] = True

        if result.get("has_financials"):
            if result.get("employees") and not enrich.get("employee_count"):
                enrich["employee_count"] = result["employees"]
                enrich["employee_source"] = "kurzy_pw"
            if result.get("revenue_czk") and not enrich.get("revenue_czk"):
                enrich["revenue_czk"] = result["revenue_czk"]
            if result.get("profit_czk") and not enrich.get("profit_czk"):
                enrich["profit_czk"] = result["profit_czk"]
            if result.get("revenue_growth_pct") is not None and enrich.get("revenue_growth") is None:
                enrich["revenue_growth"] = result["revenue_growth_pct"]
            found += 1
            _log(i + 1, len(accounts), name, f"emp={result.get('employees', '-')}")
        else:
            _log(i + 1, len(accounts), name, "no data")

        enrichment[csn] = enrich

    close_browser()
    print(f"\nKurzy PW: {found}/{tried} companies with employee data")
    return enrichment


def _collect_smlouvy_signals(accounts: list, enrichment: dict) -> dict:
    """Check public contract registry (smlouvy.gov.cz)."""
    from scraper.smlouvy_client import get_procurement_signal

    print(f"\n--- Smlouvy.gov.cz / Public Contract Signals ({len(accounts)} accounts) ---")
    found = 0

    for i, acct in enumerate(accounts):
        csn = acct["csn"]
        name = acct["company_name"]
        enrich = enrichment.get(csn, {})
        ico = enrich.get("ico")

        if enrich.get("smlouvy_done"):
            _log(i + 1, len(accounts), name, "cached")
            continue

        result = get_procurement_signal(name, ico or "")
        enrich["smlouvy_done"] = True

        if result.get("has_signal"):
            enrich["has_public_contracts"] = True
            enrich["smlouvy_contracts_count"] = result.get("contracts", 0)
            enrich["smlouvy_value_czk"] = result.get("value_czk", 0)
            enrich["smlouvy_aec_contracts"] = result.get("aec_contracts", 0)
            enrich["smlouvy_aec_value_czk"] = result.get("aec_value_czk", 0)
            enrich["smlouvy_dm_contracts"] = result.get("dm_contracts", 0)
            found += 1
            _log(i + 1, len(accounts), name,
                 f"contracts={result.get('contracts', 0)} AEC={result.get('aec_contracts', 0)}")
        else:
            enrich["has_public_contracts"] = False
            _log(i + 1, len(accounts), name, "none")

        enrichment[csn] = enrich
        time.sleep(0.3)

    print(f"\nSmlouvy: {found} companies with public contracts")
    return enrichment


def _log(current: int, total: int, name: str, status: str):
    pct = current / total * 100 if total else 0
    print(f"  [{current}/{total} {pct:5.1f}%] {name[:50]:<50} {status}")


def print_signal_summary(enrichment: dict):
    """Print a summary of signal coverage."""
    total = len(enrichment)
    if not total:
        return

    with_lc = sum(1 for v in enrichment.values() if v.get("leadership_change"))
    with_fin = sum(1 for v in enrichment.values() if v.get("revenue_czk"))
    with_growth = sum(1 for v in enrichment.values()
                      if v.get("revenue_growth") is not None)
    with_hiring = sum(1 for v in enrichment.values() if v.get("hiring_signal"))
    with_procurement = sum(1 for v in enrichment.values() if v.get("has_public_contracts"))
    with_smlouvy = sum(1 for v in enrichment.values() if v.get("smlouvy_contracts_count"))
    with_kurzy_pw = sum(1 for v in enrichment.values() if v.get("employee_source") == "kurzy_pw")
    with_comp_tools = sum(1 for v in enrichment.values()
                          if v.get("competitor_tools_in_jobs"))

    print(f"\n{'=' * 50}")
    print("GROWTH SIGNAL COVERAGE")
    print(f"{'=' * 50}")
    print(f"Total enriched accounts:   {total}")
    print(f"Leadership changes:        {with_lc}")
    print(f"Financial data:            {with_fin}")
    print(f"Revenue growth data:       {with_growth}")
    print(f"Actively hiring:           {with_hiring}")
    print(f"Public contracts (ISVZ):   {with_procurement}")
    print(f"Public contracts (smlouvy):{with_smlouvy}")
    print(f"Kurzy PW employees:        {with_kurzy_pw}")
    print(f"Competitor tools detected: {with_comp_tools}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Growth signal aggregator")
    parser.add_argument("--top", type=int, default=200, help="Process top N accounts")
    parser.add_argument(
        "--signals", default="or,kurzy,kurzy_pw,isvz,jobs,smlouvy",
        help="Comma-separated signal sources (or,kurzy,kurzy_pw,isvz,jobs,smlouvy)",
    )
    parser.add_argument("--rescore", action="store_true",
                        help="Re-run territory scorer after signal collection")
    parser.add_argument("--input", default=None, help="Prioritized CSV path")
    parser.add_argument("--enrichment", default=None, help="Enrichment JSON path")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    workspace_root = project_root.parent

    input_csv = args.input or str(workspace_root / "prioritized_accounts.csv")
    enrichment_path = args.enrichment or str(ENRICHMENT_DIR / "account_enrichment.json")

    if not Path(input_csv).exists():
        print(f"Error: {input_csv} not found. Run prioritize.py first.")
        sys.exit(1)

    init_db()

    print(f"Loading top {args.top} accounts from {input_csv} ...")
    accounts = load_prioritized(input_csv, top_n=args.top)
    print(f"Loaded {len(accounts)} accounts")

    enrichment = load_enrichment(enrichment_path)
    print(f"Existing enrichment: {len(enrichment)} accounts")

    selected = tuple(s.strip() for s in args.signals.split(",") if s.strip())
    print(f"Running signals: {', '.join(selected)}")

    enrichment = collect_signals(accounts, enrichment, signals=selected)
    save_enrichment(enrichment, enrichment_path)
    print_signal_summary(enrichment)

    if args.rescore:
        print("\n--- Re-scoring with signal data ---")
        from scoring.territory_scorer import (
            load_and_aggregate,
            print_summary,
            score_all,
            write_results,
        )

        csv_source = str(workspace_root / "Martin Valovic FY27 1 copy.csv")
        accts = load_and_aggregate(csv_source)
        results = score_all(accts, enrichment=enrichment)
        output_csv = str(workspace_root / "prioritized_accounts_enriched.csv")
        write_results(results, output_csv)
        print_summary(results)


if __name__ == "__main__":
    main()
