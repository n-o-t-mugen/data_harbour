import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import sqlite3
import glob


# =======================
# CONFIG
# =======================

load_dotenv()

DISBURSED_FILE = Path(os.getenv("DATA_FILE_PATH"))
COLLECTION_FILE = Path(os.getenv("COLLECTION_FILE_PATH"))

TARGET_COLUMN = os.getenv("TARGET_COLUMN")  # Pancard
CASE_SENSITIVE = os.getenv("CASE_SENSITIVE", "true").lower() == "true"

# Normalize DISPLAY_COLUMNS too (spaces → "")
DISPLAY_COLUMNS = [
    c.strip().replace(" ", "")
    for c in os.getenv("DISPLAY_COLUMNS", "").split(",")
    if c.strip()
]

# Updated constants to match normalized headers (spaces → "")
LEAD_ID_COL = "LeadID"
REPAYMENT_DATE_COL = "RepayDate"
COLLECTION_DATE_COL = "CollectedDate"
LOAN_NO_COL = "LoanNo"

REQUIRED_DISBURSED_COLS = {
    TARGET_COLUMN,
    LOAN_NO_COL,
    LEAD_ID_COL,
    REPAYMENT_DATE_COL,
    *DISPLAY_COLUMNS,
}

REQUIRED_COLLECTION_COLS = {
    LOAN_NO_COL,
    LEAD_ID_COL,
    COLLECTION_DATE_COL,
}

INT_LEAD_ID = "__lead_id"
INT_REPAY_DATE = "__repay_date"
INT_COLLECTION_DATE = "__collection_date"

GRACE_DAYS = 3


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names: strip whitespace, remove spaces."""
    df.columns = df.columns.str.strip().str.replace(" ", "")
    return df


def read_csv_flexible(path: Path) -> pd.DataFrame:
    """
    Read CSV that may have metadata rows before actual headers.
    Automatically skips rows until valid headers are found.
    Uses only first 20 rows for validation.
    """
    # Try different approaches to find headers
    for skiprows in [0, 1, 2, 3, 5, 10]:
        try:
            df = pd.read_csv(
                path,
                dtype=str,
                low_memory=False,
                skiprows=skiprows,
                nrows=20,  # Just for validation
            )
            df = normalize_headers(df)
            # Check if we have required identifier columns
            if all(col in df.columns for col in [LEAD_ID_COL, LOAN_NO_COL]):
                print(f"✅ Found valid headers after skipping {skiprows} rows")
                return df
        except Exception:
            continue

    # Fallback: search for header row
    all_rows = pd.read_csv(path, header=None, dtype=str).fillna("")
    for i, row in all_rows.iterrows():
        if LEAD_ID_COL.replace(" ", "") in row.values or "Lead ID" in row.values:
            df = pd.read_csv(path, skiprows=i, dtype=str, low_memory=False, nrows=20)
            df = normalize_headers(df)
            if all(col in df.columns for col in [LEAD_ID_COL, LOAN_NO_COL]):
                print(f"✅ Found valid headers at row {i}")
                return df

    raise ValueError(f"Could not find valid headers in {path.name}")


def validate_columns(df: pd.DataFrame, required: set, name: str):
    """Validate required columns exist after normalization."""
    missing = required - set(df.columns)
    if missing:
        print(f"Available columns: {list(df.columns)}")
        print(f"Required columns: {list(required)}")
        raise KeyError(f"{name} missing columns: {sorted(missing)}")


def read_full_csv_flexible(path: Path) -> pd.DataFrame:
    """Read complete CSV with flexible header detection."""
    for skiprows in [0, 1, 2, 3, 5, 10]:
        try:
            df = pd.read_csv(path, dtype=str, low_memory=False, skiprows=skiprows)
            df = normalize_headers(df)
            if LEAD_ID_COL in df.columns and LOAN_NO_COL in df.columns:
                print(f"✅ Reading full CSV with headers at row {skiprows}")
                return df
        except Exception:
            continue

    # Fallback: search for header row
    all_rows = pd.read_csv(path, header=None, dtype=str).fillna("")
    for i, row in all_rows.iterrows():
        if LEAD_ID_COL.replace(" ", "") in row.values or "Lead ID" in row.values:
            df = pd.read_csv(path, skiprows=i, dtype=str, low_memory=False)
            df = normalize_headers(df)
            if LEAD_ID_COL in df.columns and LOAN_NO_COL in df.columns:
                print(f"✅ Full CSV parsed from row {i}")
                return df

    raise ValueError(f"Could not parse full CSV: {path.name}")


def list_product_dbs(base_dir: Path | None = None) -> list[Path]:
    """
    Return all .db files in the given dir (default: CWD).
    """
    if base_dir is None:
        base_dir = Path.cwd()

    return [Path(p) for p in glob.glob(str(base_dir / "*.db"))]


def process_uploaded_files(disbursed_path: Path, collection_path: Path) -> tuple[str, sqlite3.Connection]:
    """
    1. Validate columns exist using flexible CSV reader (sample only).
    2. Extract product from FIRST VALID Loan No cell.
    3. Check both files have SAME product name.
    4. Create/overwrite {PRODUCT}.db in CWD.
    5. Load FULL CSVs into 'disbursed' and 'collection' tables.
    6. Return (product_name, connection).
    """
    # Quick validation - don't load full data yet
    disbursed_df = read_csv_flexible(disbursed_path)
    collection_df = read_csv_flexible(collection_path)

    validate_columns(disbursed_df, REQUIRED_DISBURSED_COLS, "Disbursed")
    validate_columns(collection_df, REQUIRED_COLLECTION_COLS, "Collection")

    def get_first_product(df: pd.DataFrame, col: str) -> str:
        for val in df[col].dropna():
            clean_val = str(val).strip()
            if clean_val and len(clean_val) >= 3:
                return clean_val[:3].upper()
        raise ValueError("No valid Loan No found")

    disbursed_product = get_first_product(disbursed_df, LOAN_NO_COL)
    collection_product = get_first_product(collection_df, LOAN_NO_COL)

    if disbursed_product != collection_product:
        raise ValueError(f"Product mismatch - Disbursed: {disbursed_product}, Collection: {collection_product}")

    product_name = disbursed_product
    db_path = Path.cwd() / f"{product_name}.db"

    if db_path.exists():
        print(f"⚠️  Using existing DB: {db_path}")
    else:
        print(f"✅ Creating DB: {db_path}")

    conn = sqlite3.connect(db_path)

    # Load full CSVs into DB tables (dump everything)
    full_disbursed = read_full_csv_flexible(disbursed_path)
    full_collection = read_full_csv_flexible(collection_path)

    # Persist complete data for this product
    full_disbursed.to_sql("disbursed", conn, if_exists="replace", index=False)
    full_collection.to_sql("collection", conn, if_exists="replace", index=False)
    print(
        f"✅ Loaded disbursed ({len(full_disbursed)}) "
        f"and collection ({len(full_collection)}) into DB for product {product_name}"
    )

    return product_name, conn


def evaluate_payment_status_for_conn(pan_value: str, conn: sqlite3.Connection) -> dict:
    """
    Core evaluator for a single product DB connection.
    """
    if not pan_value.strip():
        raise ValueError("PAN cannot be empty")

    value = pan_value.strip()
    pan_col = TARGET_COLUMN.replace(" ", "")

    # Filter disbursed records by PAN in SQL
    if CASE_SENSITIVE:
        disbursed_pan_df = pd.read_sql_query(
            f"SELECT * FROM disbursed WHERE {pan_col} = ?",
            conn,
            params=[value],
        )
    else:
        disbursed_pan_df = pd.read_sql_query(
            f"SELECT * FROM disbursed WHERE LOWER({pan_col}) = LOWER(?)",
            conn,
            params=[value],
        )

    if disbursed_pan_df.empty:
        result_df = pd.DataFrame()
    else:
        # Read all collection data for this product
        collection_df = pd.read_sql_query("SELECT * FROM collection", conn)

        disbursed_pan_df = normalize_headers(disbursed_pan_df)
        collection_df = normalize_headers(collection_df)

        disbursed_pan_df = disbursed_pan_df.rename(
            columns={LEAD_ID_COL: INT_LEAD_ID, REPAYMENT_DATE_COL: INT_REPAY_DATE}
        )
        collection_df = collection_df.rename(
            columns={LEAD_ID_COL: INT_LEAD_ID, COLLECTION_DATE_COL: INT_COLLECTION_DATE}
        )

        disbursed_pan_df[INT_REPAY_DATE] = pd.to_datetime(
            disbursed_pan_df[INT_REPAY_DATE], errors="coerce"
        )
        collection_df[INT_COLLECTION_DATE] = pd.to_datetime(
            collection_df[INT_COLLECTION_DATE], errors="coerce"
        )

        collection_agg = collection_df.groupby(INT_LEAD_ID, as_index=False)[
            INT_COLLECTION_DATE
        ].max()
        merged = pd.merge(disbursed_pan_df, collection_agg, on=INT_LEAD_ID, how="left")

        rows = []
        for _, row in merged.iterrows():
            repay_date = row[INT_REPAY_DATE]
            collect_date = row[INT_COLLECTION_DATE]

            if pd.isna(collect_date):
                status = "NOT_COLLECTED"
            elif collect_date < repay_date:
                status = "EARLY"
            elif collect_date == repay_date:
                status = "ON_TIME"
            else:
                delay_days = (collect_date - repay_date).days
                status = "COOLING_PERIOD" if delay_days <= GRACE_DAYS else "LATE"

            record = {
                "pan": pan_value,
                # "query_timestamp": pd.Timestamp.now().isoformat(),
                "LeadID": row[INT_LEAD_ID],
                "RepayDate": repay_date.date() if pd.notna(repay_date) else None,
                "CollectionDate": collect_date.date() if pd.notna(collect_date) else None,
                "PaymentStatus": status,
                **{col: row.get(col) for col in DISPLAY_COLUMNS},
            }
            rows.append(record)

        result_df = pd.DataFrame(rows)

    # Save ONLY query results to DB 'queries' table
    if not result_df.empty:
        result_df.to_sql("queries", conn, if_exists="append", index=False)
        print(f"✅ Saved {len(result_df)} query results to DB")
    else:
        print("ℹ️  No results to save")

    return {"pan": pan_value, "total_records": len(result_df), "table": result_df}


def evaluate_payment_status(pan_value: str, conn: sqlite3.Connection) -> dict:
    """
    Backwards-compatible wrapper for single-DB evaluation.
    """
    return evaluate_payment_status_for_conn(pan_value, conn)


def evaluate_payment_across_all_products(pan_value: str) -> dict:
    """
    Check the given PAN in every product DB (*.db) in the current directory.
    Returns:
      {
        "pan": <PAN>,
        "total_records": <int>,
        "table": <DataFrame with Product column>
      }
    """
    if not pan_value.strip():
        raise ValueError("PAN cannot be empty")

    db_paths = list_product_dbs()
    all_rows = []

    for db_path in db_paths:
        product_name = db_path.stem  # "NBL" from "NBL.db"
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            result = evaluate_payment_status_for_conn(pan_value, conn)
            df = result["table"]
            if not df.empty:
                df = df.copy()
                df["Product"] = product_name
                all_rows.append(df)
        except Exception as e:
            print(f"⚠️  Skipping DB {db_path} due to error: {e}")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
    else:
        combined = pd.DataFrame()

    return {
        "pan": pan_value,
        "total_records": len(combined),
        "table": combined,
    }


# =======================
# MAIN WORKFLOW
# =======================

# if __name__ == "__main__":
#     conn = None
#     try:
#         # Load current CSVs into their product DB
#         product, conn = process_uploaded_files(DISBURSED_FILE, COLLECTION_FILE)
#         conn.close()
#         conn = None

#         # Example: search this PAN across ALL product DBs in CWD
#         pan = "BBUPM2364P"
#         result = evaluate_payment_across_all_products(pan)
#         print(f"\nPAN: {result['pan']} | Records across all products: {result['total_records']}")
#         print(result["table"])
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         import traceback

#         traceback.print_exc()
#     finally:
#         if conn:
#             conn.close()
#             print("✅ DB connection closed")
