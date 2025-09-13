# etl.py
import os
import re
import sqlite3
from datetime import datetime
import pandas as pd

# config 
DATA_DIR = "/Users/thomassimmons/c/new/data"
FILES = {
    "products": os.path.join(DATA_DIR, "/Users/thomassimmons/c/new/data/products.csv"),
    "orders": os.path.join(DATA_DIR, "/Users/thomassimmons/c/new/data/products.csv"),
    "order_items": os.path.join(DATA_DIR, "/Users/thomassimmons/c/new/data/order_items.csv"),
    "customers": os.path.join(DATA_DIR, "/Users/thomassimmons/c/new/data/customers.csv"),
}
DB_PATH = os.path.join(DATA_DIR, "retail.db")
IF_EXISTS = "replace"  # 'fail' | 'replace' | 'append'

# ---------- HELPERS ----------
def snake(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"__+", "_", s)
    return s.strip("_").lower()

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [snake(c) for c in df.columns]
    return df

def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]):
            df[c] = df[c].astype("string").str.strip()
    return df

def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def coerce_dates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)
    return df

def dedupe(df: pd.DataFrame, key_cols: list[str] | None = None) -> pd.DataFrame:
    df = df.copy()
    if key_cols and all(k in df.columns for k in key_cols):
        return df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
    return df.drop_duplicates(keep="first").reset_index(drop=True)

def read_csv_flex(path: str) -> pd.DataFrame:
    # Try sensible defaults. If you know your separators/encodings, set them explicitly.
    return pd.read_csv(path, low_memory=False)

def write_df(con, name: str, df: pd.DataFrame):
    df.to_sql(name, con, if_exists=IF_EXISTS, index=False)

def add_indexes(con, table: str, indexes: list[tuple[str, str]]):
    # indexes: [(index_name, column_expression)]
    cur = con.cursor()
    for idx_name, col_expr in indexes:
        cur.execute(f'DROP INDEX IF EXISTS "{idx_name}"')
        cur.execute(f'CREATE INDEX "{idx_name}" ON "{table}" ({col_expr})')
    con.commit()

# ---------- EXTRACT ----------
def extract() -> dict[str, pd.DataFrame]:
    dfs = {}
    for name, path in FILES.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing file: {path}")
        df = read_csv_flex(path)
        df = clean_columns(df)
        df = trim_strings(df)
        dfs[name] = df
    return dfs

# ---------- TRANSFORM ----------
def transform(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    # Guess common numeric/date fields and coerce if present
    numeric_candidates = {
        "products": ["price", "cost", "msrp"],
        "orders": ["total_amount", "subtotal", "tax", "shipping_cost", "discount"],
        "order_items": ["quantity", "unit_price", "line_total", "discount"],
        "customers": [],
    }
    date_candidates = {
        "orders": ["order_date", "ship_date", "delivery_date", "created_at", "updated_at"],
        "customers": ["created_at", "updated_at", "signup_date"],
        "products": ["created_at", "updated_at", "release_date"],
        "order_items": ["created_at", "updated_at"],
    }

    # Apply coercions + dedupe
    for name, df in dfs.items():
        df = coerce_numeric(df, numeric_candidates.get(name, []))
        df = coerce_dates(df, date_candidates.get(name, []))
        # Deduplicate on common keys when obvious
        if name == "customers":
            key = [c for c in ["customer_id", "id"] if c in df.columns][:1]
            df = dedupe(df, key_cols=key or None)
        elif name == "products":
            key = [c for c in ["product_id", "id"] if c in df.columns][:1]
            df = dedupe(df, key_cols=key or None)
        elif name == "orders":
            key = [c for c in ["order_id", "id"] if c in df.columns][:1]
            df = dedupe(df, key_cols=key or None)
        elif name == "order_items":
            # Often order_items has a surrogate key or is unique on (order_id, product_id, maybe line_number)
            key = [k for k in ["order_item_id", "id"] if k in df.columns][:1]
            sub = [k for k in ["order_id", "product_id", "line_number"] if k in df.columns]
            df = dedupe(df, key_cols=key or (sub if len(sub) >= 2 else None))
        dfs[name] = df

    # Optional: derive clean totals if possible
    if "order_items" in dfs:
        oi = dfs["order_items"].copy()
        if "line_total" not in oi.columns:
            if {"quantity", "unit_price"}.issubset(oi.columns):
                oi["line_total"] = (oi["quantity"].astype("float") * oi["unit_price"].astype("float"))
        dfs["order_items"] = oi

    # Optional: build a star-schema fact table if columns exist
    can_build_fact = all(k in dfs for k in ["customers", "orders", "order_items", "products"])
    fact = None
    if can_build_fact:
        cust = dfs["customers"].copy()
        ords = dfs["orders"].copy()
        items = dfs["order_items"].copy()
        prods = dfs["products"].copy()

        # Key discovery
        cid = "customer_id" if "customer_id" in ords.columns else ("customer_id" if "customer_id" in cust.columns else None)
        oid = "order_id" if "order_id" in items.columns else ("order_id" if "order_id" in ords.columns else None)
        pid = "product_id" if "product_id" in items.columns else ("product_id" if "product_id" in prods.columns else None)
        order_date_col = "order_date" if "order_date" in ords.columns else None

        if all([cid, oid, pid]) and cid in ords.columns and oid in ords.columns and pid in items.columns:
            # Join: items -> orders
            fact = items.merge(ords, on=oid, how="left", suffixes=("", "_order"))
            # Join: + customers
            if cid in fact.columns and cid in cust.columns:
                fact = fact.merge(cust, on=cid, how="left", suffixes=("", "_customer"))
            # Join: + products
            if pid in fact.columns and pid in prods.columns:
                fact = fact.merge(prods, on=pid, how="left", suffixes=("", "_product"))

            # Keep handy analytics fields if present
            keep_dates = [c for c in [order_date_col] if c and c in fact.columns]
            keep_nums = [c for c in ["quantity", "unit_price", "line_total", "total_amount"] if c in fact.columns]
            id_cols = [c for c in [oid, cid, pid, "order_item_id"] if c in fact.columns]
            # plus a reasonable set of descriptors if they exist
            maybe_desc = [c for c in ["status", "state", "city", "category", "sub_category", "brand"] if c in fact.columns]
            base_cols = list(dict.fromkeys(id_cols + keep_dates + keep_nums + maybe_desc))
            fact = fact.loc[:, base_cols].copy()

            # Derive totals if still missing
            if "line_total" not in fact.columns and {"quantity", "unit_price"}.issubset(fact.columns):
                fact["line_total"] = fact["quantity"].astype(float) * fact["unit_price"].astype(float)

            # Add simple date parts
            if order_date_col and pd.api.types.is_datetime64_any_dtype(fact[order_date_col]):
                fact["order_year"] = fact[order_date_col].dt.year
                fact["order_month"] = fact[order_date_col].dt.month
                fact["order_day"] = fact[order_date_col].dt.day

            dfs["fact_order_items"] = fact

    return dfs

# ---------- LOAD ----------
def load(dfs: dict[str, pd.DataFrame], db_path: str = DB_PATH):
    if os.path.exists(db_path):
        os.remove(db_path) if IF_EXISTS == "replace" else None
    con = sqlite3.connect(db_path)

    # Write base tables
    for name, df in dfs.items():
        write_df(con, name, df)

    # Add useful indexes if columns exist
    if "orders" in dfs:
        cols = dfs["orders"].columns
        idx = []
        if "order_id" in cols: idx.append(("idx_orders_order_id", "order_id"))
        if "customer_id" in cols: idx.append(("idx_orders_customer_id", "customer_id"))
        if "order_date" in cols: idx.append(("idx_orders_order_date", "order_date"))
        add_indexes(con, "orders", idx)

    if "order_items" in dfs:
        cols = dfs["order_items"].columns
        idx = []
        if "order_id" in cols: idx.append(("idx_order_items_order_id", "order_id"))
        if "product_id" in cols: idx.append(("idx_order_items_product_id", "product_id"))
        add_indexes(con, "order_items", idx)

    if "fact_order_items" in dfs:
        cols = dfs["fact_order_items"].columns
        idx = []
        if "order_id" in cols: idx.append(("idx_fact_order_id", "order_id"))
        if "customer_id" in cols: idx.append(("idx_fact_customer_id", "customer_id"))
        if "product_id" in cols: idx.append(("idx_fact_product_id", "product_id"))
        if "order_year" in cols: idx.append(("idx_fact_year", "order_year"))
        if "order_month" in cols: idx.append(("idx_fact_month", "order_month"))
        add_indexes(con, "fact_order_items", idx)

    con.close()
    return db_path

# ---------- RUN ----------
if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting ETL…")
    dfs = extract()
    dfs = transform(dfs)
    db_file = load(dfs, DB_PATH)
    print(f"ETL complete. SQLite database at: {db_file}")

    # Quick sanity prints
    for t in ["customers", "products", "orders", "order_items", "fact_order_items"]:
        if t in dfs:
            print(t, "-> rows:", len(dfs[t]), "cols:", len(dfs[t].columns))
