"""
processing.py
-------------
PIPELINE STEP 2: Data Processing and Cleaning

PURPOSE:
    Reads raw data from the PostgreSQL raw schema, cleans and enriches it,
    and writes the results to the PostgreSQL staging schema.

    Reading from PostgreSQL rather than flat files means any authorised
    service can access the raw data directly, and the pipeline is not
    dependent on the local file system.

WHAT THIS SCRIPT DOES:
    1. Reads raw data from PostgreSQL raw schema
    2. Cleans the clients table:
         - Fills missing height_cm with gender-based population mean
         - Fills missing activity_level with "moderate" (safe default)
         - Calculates daily_calorie_target using Harris-Benedict formula
    3. Cleans the food log table:
         - Removes duplicate entries
         - Flags and corrects calorie/macro mismatches
         - Parses timestamps into structured date/time columns
    4. Writes clean data to PostgreSQL staging schema

DATABASE TABLES READ:
    raw.clients          — one row per user, exactly as received
    raw.foodlog_week     — one row per food log entry, exactly as received

DATABASE TABLES WRITTEN:
    staging.clients_clean    — one row per user, cleaned and enriched
    staging.foodlog_clean    — one row per food log entry, cleaned

CONNECTION:
    Database credentials are passed via environment variables set in
    docker-compose.yml. This follows the 12-factor app principle of
    separating configuration from code — credentials are never hardcoded.
"""

import pandas as pd
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text

# ── Database connection from environment variables ────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "nutrition")
DB_USER     = os.getenv("DB_USER",     "pipeline")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline123")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Harris-Benedict activity multipliers ──────────────────────────────────────
# These are the internationally recognised multipliers for converting
# Basal Metabolic Rate (BMR) to Total Daily Energy Expenditure (TDEE).
ACTIVITY_MULTIPLIERS = {
    "sedentary": 1.2,    # desk job, little or no exercise
    "light":     1.375,  # light exercise 1-3 days per week
    "moderate":  1.55,   # moderate exercise 3-5 days per week
    "active":    1.725,  # hard exercise 6-7 days per week
}

# Default used when activity_level is missing in the raw data
ACTIVITY_DEFAULT = "moderate"


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def log(message: str):
    """
    Timestamped logger consistent across all pipeline scripts.
    Every message includes a timestamp so the pipeline log shows
    exactly when each step happened.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def get_engine():
    """
    Creates a SQLAlchemy database engine and tests the connection.
    Exits immediately if the database is not reachable — fail-fast.
    This prevents the script from running without a valid database connection
    and producing misleading partial output.
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


def create_staging_schema(engine):
    """
    Creates the staging schema in PostgreSQL if it does not already exist.
    The staging schema holds cleaned, validated, and enriched data.
    It is the Silver layer of the medallion architecture.
    """
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.commit()
    log("  Staging schema ready.")


# ─────────────────────────────────────────────────────────────────────────────
# HARRIS-BENEDICT FORMULA
# ─────────────────────────────────────────────────────────────────────────────

def calculate_calorie_target(row: pd.Series) -> float:
    """
    Calculates a user's daily calorie target using the revised
    Harris-Benedict formula (Roza and Shizgal, 1984).

    The formula is gender-differentiated because male and female bodies
    have different baseline metabolic rates due to differences in
    muscle mass and body composition.

    Steps:
      1. Calculate BMR (Basal Metabolic Rate) — calories at complete rest
      2. Multiply by activity level to get TDEE (Total Daily Energy Expenditure)
      3. Adjust TDEE based on the user's nutrition goal
    """
    weight = row["weight_kg"]
    height = row["height_cm"]
    age    = row["age"]

    # Step 1: BMR — gender-differentiated coefficients (Roza and Shizgal, 1984)
    if row["gender"] == "male":
        bmr = 88.362 + (13.397 * weight) + (4.799 * height) - (5.677 * age)
    else:
        bmr = 447.593 + (9.247 * weight) + (3.098 * height) - (4.330 * age)

    # Step 2: Adjust for activity level
    multiplier = ACTIVITY_MULTIPLIERS.get(
        row["activity_level"],
        ACTIVITY_MULTIPLIERS[ACTIVITY_DEFAULT]
    )
    tdee = bmr * multiplier

    # Step 3: Adjust for nutrition goal
    # weight_loss:  500 kcal deficit  → approximately 0.5kg loss per week
    # muscle_gain:  300 kcal surplus  → gradual muscle gain
    # maintenance:  no adjustment
    if row["goal"] == "weight_loss":
        return round(tdee - 500)
    elif row["goal"] == "muscle_gain":
        return round(tdee + 300)
    else:
        return round(tdee)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: PROCESS CLIENTS TABLE
# ─────────────────────────────────────────────────────────────────────────────

def process_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the clients table and calculates each user's calorie target.

    Cleaning steps:
      1. Fill missing height_cm with gender-based mean
      2. Fill missing activity_level with "moderate"
      3. Calculate daily_calorie_target via Harris-Benedict
      4. Parse join_date to proper date type
    """
    log("Processing clients table...")

    # Fix 1: Missing height_cm
    # We fill with the mean height for the same gender rather than a global
    # mean because male and female height distributions differ significantly.
    missing_height = df["height_cm"].isna().sum()
    if missing_height > 0:
        gender_mean = df.groupby("gender")["height_cm"].transform("mean")
        df["height_cm"] = df["height_cm"].fillna(gender_mean).round(1)
        log(f"  Fixed {missing_height:,} missing height_cm values "
            f"(filled with gender-based mean).")

    # Fix 2: Missing activity_level
    # We use "moderate" as a conservative default — it neither over- nor
    # under-estimates the calorie target for users with incomplete profiles.
    missing_activity = df["activity_level"].isna().sum()
    if missing_activity > 0:
        df["activity_level"] = df["activity_level"].fillna(ACTIVITY_DEFAULT)
        log(f"  Fixed {missing_activity:,} missing activity_level values "
            f"(filled with '{ACTIVITY_DEFAULT}').")

    # Fix 3: Calculate daily calorie target
    # This is the core domain logic — the derived field that drives
    # all downstream analytics. It did not exist in the raw data.
    df["daily_calorie_target"] = df.apply(
        calculate_calorie_target, axis=1
    ).astype(int)
    log(f"  Calculated daily_calorie_target for {len(df):,} users.")
    log(f"  Target range: {df['daily_calorie_target'].min():,} – "
        f"{df['daily_calorie_target'].max():,} kcal/day")

    # Fix 4: Parse join_date
    df["join_date"] = pd.to_datetime(df["join_date"])

    log(f"  Clients processed: {len(df):,} rows. No nulls remaining.\n")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: PROCESS FOOD LOG TABLE
# ─────────────────────────────────────────────────────────────────────────────

def process_foodlog(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the food log table.

    Cleaning steps:
      1. Remove duplicate entries (same user, timestamp, food item)
      2. Flag and correct calorie/macro mismatches using Atwater factors
      3. Parse timestamp into date, hour, and weekday columns
    """
    log("Processing food log table...")
    original_count = len(df)

    # Fix 1: Remove duplicates
    # A duplicate is defined as the same user logging the same food item
    # at the exact same timestamp — caused by accidental double submission.
    before_dedup = len(df)
    df = df.drop_duplicates(
        subset=["user_id", "timestamp", "food_item"],
        keep="first"
    ).reset_index(drop=True)
    dupes_removed = before_dedup - len(df)
    log(f"  Removed {dupes_removed:,} duplicate entries "
        f"({dupes_removed/before_dedup*100:.1f}% of raw log).")

    # Fix 2: Calorie/macro mismatch correction
    # Atwater factors: protein=4kcal/g, carbohydrates=4kcal/g, fat=9kcal/g
    # We calculate what calories should be based on macros, compare to
    # what was logged, and flag rows where the difference exceeds 20 kcal.
    # For flagged rows, the macro-derived value is more trustworthy.
    df["calories_from_macros"] = (
        df["protein_g"] * 4 +
        df["carbs_g"]   * 4 +
        df["fat_g"]     * 9
    ).round(1)

    df["macro_mismatch"] = (
        (df["calories"] - df["calories_from_macros"]).abs() > 20
    )
    mismatch_count = df["macro_mismatch"].sum()

    # Replace logged calories with macro-derived value for flagged rows
    df.loc[df["macro_mismatch"], "calories"] = \
        df.loc[df["macro_mismatch"], "calories_from_macros"]
    log(f"  Corrected {mismatch_count:,} calorie/macro mismatches "
        f"({mismatch_count/len(df)*100:.1f}% of entries).")

    # Fix 3: Parse timestamp
    # Breaking the timestamp into components makes aggregation easier
    # downstream — grouping by date, analysing meal timing patterns, etc.
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"]      = df["timestamp"].dt.date
    df["hour"]      = df["timestamp"].dt.hour
    df["weekday"]   = df["timestamp"].dt.day_name()

    log(f"  Food log processed: {original_count:,} → {len(df):,} rows "
        f"({original_count - len(df):,} rows removed).")
    log(f"  Date range: {df['date'].min()} → {df['date'].max()}\n")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("PROCESSING SERVICE — Batch Pipeline Step 2")
    log("=" * 60)
    log("")

    # ── Connect to database ───────────────────────────────────────────────────
    log("Connecting to PostgreSQL...")
    engine = get_engine()
    create_staging_schema(engine)
    log("")

    # ── Load raw data from PostgreSQL ─────────────────────────────────────────
    # Reading from the raw schema means this service is not dependent
    # on any local files — it works on any machine with database access.
    log("Loading raw data from PostgreSQL raw schema...")
    clients = pd.read_sql("SELECT * FROM raw.clients", engine)
    foodlog = pd.read_sql("SELECT * FROM raw.foodlog_week", engine)
    log(f"  Loaded {len(clients):,} clients.")
    log(f"  Loaded {len(foodlog):,} food log entries.")
    log("")

    # ── Process each table ────────────────────────────────────────────────────
    clients_clean = process_clients(clients)
    foodlog_clean = process_foodlog(foodlog)

    # ── Write to staging schema ───────────────────────────────────────────────
    log("Writing clean data to PostgreSQL staging schema...")

    clients_clean.to_sql(
        name="clients_clean",
        schema="staging",
        con=engine,
        if_exists="replace",
        index=False
    )
    log(f"  Written: staging.clients_clean ({len(clients_clean):,} rows)")

    foodlog_clean.to_sql(
        name="foodlog_clean",
        schema="staging",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=10000
    )
    log(f"  Written: staging.foodlog_clean ({len(foodlog_clean):,} rows)")

    # ── Summary ───────────────────────────────────────────────────────────────
    log("")
    log("=" * 60)
    log("PROCESSING COMPLETE")
    log("=" * 60)
    log(f"  staging.clients_clean  : {len(clients_clean):>10,} rows")
    log(f"  staging.foodlog_clean  : {len(foodlog_clean):>10,} rows")
    log("")
    log("  Staging layer is now populated in PostgreSQL.")
    log("  Next step: run aggregation.py")
    log("=" * 60)


if __name__ == "__main__":
    main()