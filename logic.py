import os
import glob
import sqlite3
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# =======================
# CONFIG
# =======================

load_dotenv()

TARGET_COLUMN = os.getenv("TARGET_COLUMN")  # Pancard
CASE_SENSITIVE = os.getenv("CASE_SENSITIVE", "true").lower() == "true"

DISPLAY_COLUMNS = [
    c.strip().replace(" ", "")
    for c in os.getenv("DISPLAY_COLUMNS", "").split(",")
    if c.strip()
]

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

# =======================
# HELPERS
# =======================


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.replace(" ", "")
    return df


def validate_columns(df: pd.DataFrame, required: set, name: str):
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"{name} missing columns: {sorted(missing)}")


def read_csv_flexible(path: Path, sample_only: bool = False) -> pd.DataFrame:
    encodings_to_try = ["utf-8", "utf-8-sig", "latin1", "cp1252"]

    for encoding in encodings_to_try:
        for skiprows in [0, 1, 2, 3, 5, 10]:
            try:
                df = pd.read_csv(
                    path,
                    dtype=str,
                    low_memory=False,
                    skiprows=skiprows,
                    encoding=encoding,
                    nrows=20 if sample_only else None,
                )
                df = normalize_headers(df)

                if LEAD_ID_COL in df.columns and LOAN_NO_COL in df.columns:
                    print(
                        f"✅ Parsed {path.name} using encoding={encoding}, skiprows={skiprows}"
                    )
                    return df

            except UnicodeDecodeError:
                continue
            except Exception:
                continue

    raise ValueError(
        f"Could not parse CSV {path.name} — unsupported encoding or corrupt file"
    )


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table_name,),
    )
    return cur.fetchone() is not None


def upsert_dataframe(
    conn: sqlite3.Connection,
    table_name: str,
    new_df: pd.DataFrame,
):
    """
    Insert only NEW rows (exact full-row match) into table.
    """
    new_df = normalize_headers(new_df)

    if table_exists(conn, table_name):
        existing_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        existing_df = normalize_headers(existing_df)

        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates()

        combined.to_sql(table_name, conn, if_exists="replace", index=False)
        inserted = len(combined) - len(existing_df)
    else:
        new_df = new_df.drop_duplicates()
        new_df.to_sql(table_name, conn, if_exists="replace", index=False)
        inserted = len(new_df)

    print(f"✅ {table_name}: inserted {inserted} new rows")


# =======================
# MAIN INGESTION
# =======================


def process_uploaded_files(
    disbursed_path: Path,
    collection_path: Path,
) -> tuple[str, sqlite3.Connection]:

    disbursed_sample = read_csv_flexible(disbursed_path, sample_only=True)
    collection_sample = read_csv_flexible(collection_path, sample_only=True)

    validate_columns(disbursed_sample, REQUIRED_DISBURSED_COLS, "Disbursed")
    validate_columns(collection_sample, REQUIRED_COLLECTION_COLS, "Collection")

    def get_product(df: pd.DataFrame) -> str:
        for val in df[LOAN_NO_COL].dropna():
            val = str(val).strip()
            if len(val) >= 3:
                return val[:3].upper()
        raise ValueError("Could not infer product")

    product_name = get_product(disbursed_sample)
    if get_product(collection_sample) != product_name:
        raise ValueError("Product mismatch between files")

    db_path = Path.cwd() / f"{product_name}.db"
    conn = sqlite3.connect(db_path)

    # Read FULL CSVs
    full_disbursed = read_csv_flexible(disbursed_path)
    full_collection = read_csv_flexible(collection_path)

    # UPSERT behavior
    upsert_dataframe(conn, "disbursed", full_disbursed)
    upsert_dataframe(conn, "collection", full_collection)

    return product_name, conn


# =======================
# DB DISCOVERY
# =======================


def list_product_dbs(base_dir: Path | None = None) -> list[Path]:
    if base_dir is None:
        base_dir = Path.cwd()
    return [Path(p) for p in glob.glob(str(base_dir / "*.db"))]


# =======================
# PAYMENT EVALUATION
# =======================


def evaluate_payment_status_for_conn(
    pan_value: str, conn: sqlite3.Connection
) -> dict:
    if not pan_value.strip():
        raise ValueError("PAN cannot be empty")

    pan_col = TARGET_COLUMN.replace(" ", "")
    value = pan_value.strip()

    if CASE_SENSITIVE:
        disbursed_df = pd.read_sql_query(
            f"SELECT * FROM disbursed WHERE {pan_col} = ?",
            conn,
            params=[value],
        )
    else:
        disbursed_df = pd.read_sql_query(
            f"SELECT * FROM disbursed WHERE LOWER({pan_col}) = LOWER(?)",
            conn,
            params=[value],
        )

    if disbursed_df.empty:
        return {"pan": pan_value, "total_records": 0, "table": pd.DataFrame()}

    collection_df = pd.read_sql_query("SELECT * FROM collection", conn)

    disbursed_df = normalize_headers(disbursed_df)
    collection_df = normalize_headers(collection_df)

    disbursed_df = disbursed_df.rename(
        columns={
            LEAD_ID_COL: INT_LEAD_ID,
            REPAYMENT_DATE_COL: INT_REPAY_DATE,
        }
    )
    collection_df = collection_df.rename(
        columns={
            LEAD_ID_COL: INT_LEAD_ID,
            COLLECTION_DATE_COL: INT_COLLECTION_DATE,
        }
    )

    disbursed_df[INT_REPAY_DATE] = pd.to_datetime(
        disbursed_df[INT_REPAY_DATE], errors="coerce"
    )
    collection_df[INT_COLLECTION_DATE] = pd.to_datetime(
        collection_df[INT_COLLECTION_DATE], errors="coerce"
    )

    collection_agg = (
        collection_df.groupby(INT_LEAD_ID, as_index=False)[
            INT_COLLECTION_DATE
        ].max()
    )

    merged = pd.merge(
        disbursed_df, collection_agg, on=INT_LEAD_ID, how="left"
    )

    rows = []
    for _, row in merged.iterrows():
        repay = row[INT_REPAY_DATE]
        collect = row[INT_COLLECTION_DATE]

        if pd.isna(collect):
            status = "NOT_COLLECTED"
        elif collect < repay:
            status = "EARLY"
        elif collect == repay:
            status = "ON_TIME"
        else:
            status = (
                "COOLING_PERIOD"
                if (collect - repay).days <= GRACE_DAYS
                else "LATE"
            )

        rows.append(
            {
                "pan": pan_value,
                "LeadID": row[INT_LEAD_ID],
                "RepayDate": repay.date() if pd.notna(repay) else None,
                "CollectionDate": collect.date()
                if pd.notna(collect)
                else None,
                "PaymentStatus": status,
                **{col: row.get(col) for col in DISPLAY_COLUMNS},
            }
        )

    result_df = pd.DataFrame(rows)
    result_df.to_sql("queries", conn, if_exists="append", index=False)

    return {
        "pan": pan_value,
        "total_records": len(result_df),
        "table": result_df,
    }


def evaluate_payment_across_all_products(pan_value: str) -> dict:
    all_rows = []

    for db in list_product_dbs():
        conn = sqlite3.connect(db)
        try:
            result = evaluate_payment_status_for_conn(pan_value, conn)
            df = result["table"]
            if not df.empty:
                df["Product"] = db.stem
                all_rows.append(df)
        finally:
            conn.close()

    combined = (
        pd.concat(all_rows, ignore_index=True)
        if all_rows
        else pd.DataFrame()
    )

    return {
        "pan": pan_value,
        "total_records": len(combined),
        "table": combined,
    }
