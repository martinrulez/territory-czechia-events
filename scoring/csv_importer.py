"""Import client list from CSV/Excel into the accounts table.

Handles Autodesk subscription exports where each row is a product/subscription
and companies appear multiple times. Aggregates products per company.
"""

import io

import pandas as pd

from db.database import upsert_account

COLUMN_MAP = {
    "company name": "company_name",
    "company": "company_name",
    "account name": "company_name",
    "account": "company_name",
    "site account": "company_name",
    "name": "company_name",
    "firma": "company_name",
    "nazev": "company_name",
    "název": "company_name",
    "domain": "domain",
    "website": "domain",
    "web": "domain",
    "site website": "domain",
    "industry": "industry",
    "odvetvi": "industry",
    "odvětví": "industry",
    "segment": "industry",
    "industry group": "industry_group",
    "industry segment": "industry_segment",
    "industry sub segment": "industry_sub_segment",
    "employees": "employee_count",
    "employee count": "employee_count",
    "# employees": "employee_count",
    "pocet zamestnancu": "employee_count",
    "products": "autodesk_products",
    "autodesk products": "autodesk_products",
    "current products": "autodesk_products",
    "product line": "product_line",
    "produkty": "autodesk_products",
    "status": "account_status",
    "account status": "account_status",
    "asset subs status": "account_status",
    "stav": "account_status",
    "notes": "notes",
    "poznamky": "notes",
    "poznámky": "notes",
    "site city": "city",
    "site country": "country",
    "site csn": "csn",
    "# of units": "units",
    "segm": "segm",
    "purchaser email": "purchaser_email",
    "agreement end date": "agreement_end_date",
    "reseller": "reseller",
    "parent account name": "parent_account",
}


def _map_columns(df: pd.DataFrame) -> dict:
    """Map DataFrame columns to canonical field names."""
    mapping = {}
    for col in df.columns:
        col_clean = col.strip().lstrip("\ufeff")
        col_lower = col_clean.lower()
        if col_lower in COLUMN_MAP:
            mapping[col] = COLUMN_MAP[col_lower]
    return mapping


def import_client_csv(file_content, filename: str = "clients.csv") -> dict:
    """Import a client list from CSV or Excel bytes/string.

    Aggregates multiple rows per company (subscription-per-row format).
    Combines all product lines into a single comma-separated field.

    Returns:
        Summary dict with import counts.
    """
    try:
        if filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(file_content) if isinstance(file_content, bytes) else file_content)
        else:
            if isinstance(file_content, bytes):
                file_content = file_content.decode("utf-8-sig")
            df = pd.read_csv(io.StringIO(file_content) if isinstance(file_content, str) else file_content)
    except Exception as e:
        return {"success": False, "error": f"Failed to read file: {e}"}

    if df.empty:
        return {"success": False, "error": "File is empty"}

    col_mapping = _map_columns(df)
    if "company_name" not in col_mapping.values():
        return {
            "success": False,
            "error": f"Could not find a company name column. Found columns: {list(df.columns)}",
            "columns_found": list(df.columns),
        }

    df = df.rename(columns={k: v for k, v in col_mapping.items()})

    companies = {}
    for _, row in df.iterrows():
        company_name = _clean(row.get("company_name"))
        if not company_name:
            continue

        key = company_name.lower().strip()

        if key not in companies:
            companies[key] = {
                "company_name": company_name,
                "domain": None,
                "industry": None,
                "products": set(),
                "segm": set(),
                "city": None,
                "units": 0,
                "status": None,
                "email": None,
                "reseller": None,
                "parent": None,
                "industry_segment": None,
            }

        rec = companies[key]

        domain = _clean(row.get("domain"))
        if domain and domain.lower() != "unknown" and not rec["domain"]:
            rec["domain"] = domain

        industry = _clean(row.get("industry_group")) or _clean(row.get("industry"))
        if industry and industry != "UNKNOWN" and not rec["industry"]:
            rec["industry"] = industry

        ind_seg = _clean(row.get("industry_segment"))
        if ind_seg and ind_seg != "UNKNOWN":
            rec["industry_segment"] = ind_seg

        product = _clean(row.get("product_line")) or _clean(row.get("autodesk_products"))
        if product:
            rec["products"].add(product)

        segm = _clean(row.get("segm"))
        if segm:
            rec["segm"].add(segm)

        units = row.get("units")
        try:
            rec["units"] += int(units)
        except (ValueError, TypeError):
            pass

        status = _clean(row.get("account_status"))
        if status and status != "UNKNOWN":
            rec["status"] = status

        city = _clean(row.get("city"))
        if city and not rec["city"]:
            rec["city"] = city

        email = _clean(row.get("purchaser_email"))
        if email and not rec["email"]:
            rec["email"] = email

        reseller = _clean(row.get("reseller"))
        if reseller and not rec["reseller"]:
            rec["reseller"] = reseller

        parent = _clean(row.get("parent_account"))
        if parent and not rec["parent"]:
            rec["parent"] = parent

    imported = 0
    skipped = 0
    for rec in companies.values():
        company_name = rec["company_name"]
        if not company_name:
            skipped += 1
            continue

        products_str = ", ".join(sorted(rec["products"])) if rec["products"] else None
        segm_str = ", ".join(sorted(rec["segm"])) if rec["segm"] else None

        notes_parts = []
        if rec["city"]:
            notes_parts.append(f"City: {rec['city']}")
        if rec["units"] > 0:
            notes_parts.append(f"Total units: {rec['units']}")
        if rec["reseller"]:
            notes_parts.append(f"Reseller: {rec['reseller']}")
        if rec["parent"] and rec["parent"].lower() != company_name.lower():
            notes_parts.append(f"Parent: {rec['parent']}")
        if rec["industry_segment"]:
            notes_parts.append(f"Segment: {rec['industry_segment']}")
        if rec["email"]:
            notes_parts.append(f"Email: {rec['email']}")

        industry = rec["industry"]
        if segm_str and industry:
            industry = f"{industry} ({segm_str})"
        elif segm_str:
            industry = segm_str

        upsert_account(
            company_name=company_name,
            domain=rec["domain"],
            industry=industry,
            autodesk_products=products_str,
            account_status=rec["status"] or "client",
            notes="; ".join(notes_parts) if notes_parts else None,
        )
        imported += 1

    return {
        "success": True,
        "imported": imported,
        "skipped": skipped,
        "total": len(df),
        "unique_companies": len(companies),
        "columns_mapped": {k: v for k, v in col_mapping.items()},
    }


def _clean(val) -> str:
    """Clean a cell value, returning None for empty/nan."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("nan", "none", "", "unknown"):
        return None
    return s
