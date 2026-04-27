"""
ingestion.py
------------
PIPELINE STEP 1: Data Ingestion

PURPOSE:
    This script is the entry point of the batch processing pipeline.
    It reads the raw CSV files produced by generate_data.py and loads
    them into the PostgreSQL raw schema without any transformation.

    The raw schema is the immutable layer of the pipeline. Data is stored
    exactly as received. No cleaning, no enrichment, no type changes beyond
    what is necessary for database storage.

DATABASE TABLES WRITTEN:
    raw.clients       — one row per user, exactly as received
    raw.foodlog_week  — one row per food log entry, exactly as received

"""

import pandas as pd
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text

# ── Path configuration ────────────────────────────────────────────────────────
SOURCE_DIR  = "data/source"
CLIENTS_CSV = os.path.join(SOURCE_DIR, "clients.csv")
FOODLOG_CSV = os.path.join(SOURCE_DIR, "foodlog_week.csv")

# ── Database connection from environment variables ────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "nutrition")
DB_USER     = os.getenv("DB_USER",     "pipeline")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline123")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Expected schema definitions ───────────────────────────────────────────────
# These define what columns we expect from each source file.
# If any column is missing, ingestion fails immediately. This is a lightweight governance check at the pipeline entry point.

CLIENTS_SCHEMA = {
    "user_id":        "string",
    "age":            "Int64",    # nullable integer — can hold NaN
    "gender":         "string",
    "weight_kg":      "Float64",  # nullable float — can hold NaN
    "height_cm":      "Float64",  # nullable — ~2% missing values expected
    "goal":           "string",
    "activity_level": "string",   # nullable — ~2% missing values expected
    "join_date":      "string",
}

FOODLOG_SCHEMA = {
    "log_id":    "string",
    "user_id":   "string",
    "timestamp": "string",
    "meal_type": "string",
    "food_item": "string",
    "calories":  "Float64",
    "protein_g": "Float64",
    "carbs_g":   "Float64",
    "fat_g":     "Float64",
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def log(message: str):
    
   # Timestamped logger.
   
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def get_engine():
    """
    Creates a SQLAlchemy database engine and tests the connection.
    Exits immediately if the database is not reachable.
    """
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log("  Database connection established.")
        return engine
    except Exception as e:
        log(f"  ERROR: Cannot connect to database: {e}")
        log(f"  Connection: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        sys.exit(1)


def create_raw_schema(engine):
 
    #Creates the raw schema in PostgreSQL if it does not already exist.
  
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()
    log("  Raw schema ready.")


def validate_schema(df: pd.DataFrame, expected_schema: dict,
                    file_name: str):
    """
    Checks that all expected columns are present in the incoming file.
    Exits the pipeline if any column is missing.

    This enforces a basic data contract at the pipeline entry point:
    if the source file does not match the expected structure, we refuse
    to ingest it rather than passing malformed data downstream.
    """
    missing = [col for col in expected_schema if col not in df.columns]
    if missing:
        log(f"  SCHEMA ERROR in {file_name}: missing columns: {missing}")
        log("  Ingestion aborted. The source file does not match the expected schema.")
        sys.exit(1)
    log(f"  Schema validation passed — all expected columns present.")


def ingest_to_postgresql(csv_path: str, table_name: str,
                          schema: dict, engine, label: str):
    """
    Core ingestion function. Reads a CSV file and writes it to
    the PostgreSQL raw schema without any transformation.

    Steps:
      1. Check the source file exists
      2. Read the CSV into a Pandas DataFrame
      3. Validate the schema (column presence check)
      4. Cast columns to expected types
      5. Report missing values
      6. Write to PostgreSQL raw schema

    The table is replaced on each weekly run.
    This means each Monday's pipeline run overwrites the previous week's
    raw data with the new upload — matching the weekly batch cycle.
    """
    log(f"Starting ingestion: {label}")
    log(f"  Source : {csv_path}")
    log(f"  Target : PostgreSQL raw.{table_name}")

    # ── Step 1: Check source file exists ─────────────────────────────────────
    if not os.path.exists(csv_path):
        log(f"  ERROR: Source file not found: {csv_path}")
        log("  Have you run generate_data.py first?")
        sys.exit(1)

    # ── Step 2: Read CSV ──────────────────────────────────────────────────────
    df = pd.read_csv(csv_path)
    log(f"  Loaded {len(df):,} rows from CSV.")

    # ── Step 3: Validate schema ───────────────────────────────────────────────
    validate_schema(df, schema, csv_path)

    # ── Step 4: Cast types ────────────────────────────────────────────────────
    # We use nullable Pandas types (Int64, Float64) to preserve NaN values.
    # Standard Python int/float would silently convert NaN to 0,
    # destroying the information that a value is genuinely missing.
    for col, dtype in schema.items():
        try:
            if dtype in ("Int64", "Float64"):
                df[col] = pd.array(df[col], dtype=dtype)
            elif dtype == "string":
                df[col] = df[col].astype("string")
        except Exception as e:
            log(f"  WARNING: Could not cast column '{col}' to {dtype}: {e}")

    log("  Type casting complete.")

    # ── Step 5: Report missing values ─────────────────────────────────────────
    # We report but do not fix missing values here.
    # Fixing them is the responsibility of the processing service.
    # The raw layer must reflect the data exactly as received.
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if len(nulls) > 0:
        log(f"  Missing values detected (will be handled in processing):")
        for col, count in nulls.items():
            log(f"    {col}: {count:,} missing ({count/len(df)*100:.1f}%)")
    else:
        log("  No missing values detected.")

    # ── Step 6: Write to PostgreSQL ───────────────────────────────────────────
    # if_exists="replace" drops and recreates the table on each run.
    # This ensures the raw layer always reflects the current week's upload.
    df.to_sql(
        name=table_name,
        schema="raw",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=10000   # writes in batches — efficient for large tables
    )

    log(f"  Written to PostgreSQL: raw.{table_name} ({len(df):,} rows)")
    log(f"  Ingestion complete: {label}\n")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("INGESTION SERVICE — Batch Pipeline Step 1")
    log("=" * 60)
    log("")

    # ── Connect to database ───────────────────────────────────────────────────
    log("Connecting to PostgreSQL...")
    engine = get_engine()
    create_raw_schema(engine)
    log("")

    # ── Ingest clients ────────────────────────────────────────────────────────
    # clients.csv is the static user database.
    # It changes only when users register or update their profiles.
    clients = ingest_to_postgresql(
        csv_path   = CLIENTS_CSV,
        table_name = "clients",
        schema     = CLIENTS_SCHEMA,
        engine     = engine,
        label      = "clients"
    )

    # ── Ingest food log ───────────────────────────────────────────────────────
    # foodlog_week.csv represents one week of food logging activity.
    # A new file arrives every Monday and triggers the pipeline.
    foodlog = ingest_to_postgresql(
        csv_path   = FOODLOG_CSV,
        table_name = "foodlog_week",
        schema     = FOODLOG_SCHEMA,
        engine     = engine,
        label      = "foodlog_week"
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    log("=" * 60)
    log("INGESTION COMPLETE — Summary")
    log("=" * 60)
    log(f"  raw.clients      : {len(clients):>10,} rows")
    log(f"  raw.foodlog_week : {len(foodlog):>10,} rows")
    log("")
    log("  Raw layer is now populated in PostgreSQL.")
    log("  Data is immutable — raw schema is never modified after ingestion.")
    log("  Next step: run processing.py")
    log("=" * 60)


if __name__ == "__main__":
    main()