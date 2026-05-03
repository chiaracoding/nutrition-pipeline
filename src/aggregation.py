"""
aggregation.py
--------------
PIPELINE STEP 3: Data Aggregation

PURPOSE:
    Reads clean data from the PostgreSQL staging schema, runs SQL
    aggregations using DuckDB, and writes the results to the PostgreSQL
    analytics schema. Produces two outputs: a weekly nutrition summary
    per user and a company-level insights report grouped by goal.

WHAT THIS SCRIPT DOES:
    1. Reads clean data from PostgreSQL staging schema
    2. Uses DuckDB to run SQL aggregations in memory
    3. Calculates weekly nutrition summary per user including:
         - Total and average daily calories
         - Macro totals and percentage splits
         - Calorie delta vs personal target
         - Goal status (on_track / surplus / deficit)
         - Feedback message per user
         - Data quality flag
    4. Calculates company-level insights by goal group
    5. Writes all results to PostgreSQL analytics schema

DATABASE TABLES WRITTEN:
    analytics.weekly_nutrition    — one row per user, weekly summary
    analytics.company_insights    — one row per goal group
"""

import duckdb
import pandas as pd
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text

# ── Database connection ───────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "nutrition")
DB_USER     = os.getenv("DB_USER",     "pipeline")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline123")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def get_engine():
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log("  Database connection established.")
        return engine
    except Exception as e:
        log(f"  ERROR: Cannot connect to database: {e}")
        sys.exit(1)


def create_analytics_schema(engine):
    """
    Creates the analytics schema in PostgreSQL if it does not exist.
    The analytics schema holds aggregated, query-ready data that
    the FastAPI serving layer reads from directly.
    """
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        conn.commit()
    log("  Analytics schema ready.")


def build_feedback(row) -> str:
    """Generates a plain-language weekly feedback message per user."""
    delta     = abs(int(row["weekly_calorie_delta"]))
    daily_avg = int(row["avg_daily_calories"])
    target    = int(row["daily_calorie_target"])
    goal      = row["goal"].replace("_", " ")

    if row["goal_status"] == "on_track":
        return (f"Great week! You averaged {daily_avg} kcal/day against "
                f"your {target} kcal target. You are on track with your "
                f"{goal} goal.")
    elif row["goal_status"] == "surplus":
        return (f"You consumed {delta} kcal more than your weekly target "
                f"({daily_avg} kcal/day vs {target} kcal target). "
                f"Consider reducing portion sizes to support your {goal} goal.")
    else:
        return (f"You consumed {delta} kcal less than your weekly target "
                f"({daily_avg} kcal/day vs {target} kcal target). "
                f"Try to increase your intake to fuel your {goal} goal.")


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATION: Weekly nutrition per user
# ─────────────────────────────────────────────────────────────────────────────

def aggregate_weekly(clients: pd.DataFrame,
                     foodlog: pd.DataFrame) -> pd.DataFrame:
    """
    Uses DuckDB to aggregate the food log into weekly summaries per user.
    DuckDB reads the pandas DataFrames directly in memory — no file I/O.
    Results are returned as a pandas DataFrame for writing to PostgreSQL.
    """
    log("  Running weekly aggregation with DuckDB...")

    con = duckdb.connect()
    con.register("clients", clients)
    con.register("foodlog", foodlog)

    query = """
        WITH daily_totals AS (
            SELECT
                user_id,
                date,
                SUM(calories)  AS daily_calories,
                SUM(protein_g) AS daily_protein,
                SUM(carbs_g)   AS daily_carbs,
                SUM(fat_g)     AS daily_fat
            FROM foodlog
            GROUP BY user_id, date
        ),
        weekly_totals AS (
            SELECT
                user_id,
                ROUND(SUM(daily_calories), 1)  AS weekly_calories,
                ROUND(AVG(daily_calories), 1)  AS avg_daily_calories,
                ROUND(SUM(daily_protein), 1)   AS weekly_protein_g,
                ROUND(SUM(daily_carbs), 1)     AS weekly_carbs_g,
                ROUND(SUM(daily_fat), 1)       AS weekly_fat_g,
                COUNT(DISTINCT date)           AS days_logged
            FROM daily_totals
            GROUP BY user_id
        ),
        quality AS (
            SELECT
                user_id,
                ROUND(SUM(CASE WHEN macro_mismatch THEN 1 ELSE 0 END)
                      * 100.0 / COUNT(*), 1) AS mismatch_pct
            FROM foodlog
            GROUP BY user_id
        )
        SELECT
            c.user_id,
            c.gender,
            c.age,
            c.goal,
            c.activity_level,
            c.daily_calorie_target,
            w.weekly_calories,
            w.avg_daily_calories,
            w.weekly_protein_g,
            w.weekly_carbs_g,
            w.weekly_fat_g,
            w.days_logged,
            (c.daily_calorie_target * 7)                        AS weekly_calorie_target,
            ROUND(w.weekly_calories
                  - (c.daily_calorie_target * 7), 1)            AS weekly_calorie_delta,
            CASE
                WHEN ABS(w.weekly_calories - (c.daily_calorie_target * 7))
                     <= (c.daily_calorie_target * 7 * 0.10)
                THEN 'on_track'
                WHEN w.weekly_calories > (c.daily_calorie_target * 7)
                THEN 'surplus'
                ELSE 'deficit'
            END                                                  AS goal_status,
            ROUND(w.weekly_protein_g * 4 * 100.0
                  / NULLIF(w.weekly_protein_g*4
                           + w.weekly_carbs_g*4
                           + w.weekly_fat_g*9, 0), 1)           AS macro_protein_pct,
            ROUND(w.weekly_carbs_g * 4 * 100.0
                  / NULLIF(w.weekly_protein_g*4
                           + w.weekly_carbs_g*4
                           + w.weekly_fat_g*9, 0), 1)           AS macro_carbs_pct,
            ROUND(w.weekly_fat_g * 9 * 100.0
                  / NULLIF(w.weekly_protein_g*4
                           + w.weekly_carbs_g*4
                           + w.weekly_fat_g*9, 0), 1)           AS macro_fat_pct,
            q.mismatch_pct                                       AS data_quality_mismatch_pct
        FROM clients c
        LEFT JOIN weekly_totals w ON c.user_id = w.user_id
        LEFT JOIN quality       q ON c.user_id = q.user_id
        ORDER BY c.user_id
    """

    result = con.execute(query).df()

    # Add feedback message per user
    result["feedback_message"] = result.apply(build_feedback, axis=1)
    # Add batch timestamp
    result["week_processed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    con.close()
    log(f"  Weekly aggregation complete: {len(result):,} user summaries.")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATION: Company-level insights
# ─────────────────────────────────────────────────────────────────────────────

def aggregate_insights(weekly: pd.DataFrame) -> pd.DataFrame:
    """Produces company-level summary statistics grouped by goal."""
    log("  Running company insights aggregation...")

    con = duckdb.connect()
    con.register("weekly", weekly)

    query = """
        SELECT
            goal,
            COUNT(*)                                                AS total_users,
            ROUND(AVG(avg_daily_calories), 0)                      AS avg_daily_kcal,
            ROUND(AVG(daily_calorie_target), 0)                    AS avg_daily_target,
            ROUND(AVG(weekly_calorie_delta), 0)                    AS avg_weekly_delta,
            SUM(CASE WHEN goal_status = 'on_track' THEN 1 ELSE 0 END) AS users_on_track,
            SUM(CASE WHEN goal_status = 'surplus'  THEN 1 ELSE 0 END) AS users_in_surplus,
            SUM(CASE WHEN goal_status = 'deficit'  THEN 1 ELSE 0 END) AS users_in_deficit,
            ROUND(SUM(CASE WHEN goal_status = 'on_track' THEN 1 ELSE 0 END)
                  * 100.0 / NULLIF(COUNT(*), 0), 1)                AS compliance_rate_pct,
            ROUND(AVG(days_logged), 1)                             AS avg_days_logged,
            CASE
                WHEN ROUND(SUM(CASE WHEN goal_status = 'on_track'
                     THEN 1 ELSE 0 END) * 100.0
                     / NULLIF(COUNT(*), 0), 1) >= 40 THEN 'GREEN'
                WHEN ROUND(SUM(CASE WHEN goal_status = 'on_track'
                     THEN 1 ELSE 0 END) * 100.0
                     / NULLIF(COUNT(*), 0), 1) >= 20 THEN 'AMBER'
                ELSE 'RED'
            END                                                    AS compliance_rag
        FROM weekly
        GROUP BY goal
        ORDER BY goal
    """

    result = con.execute(query).df()
    result["week_processed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con.close()

    log(f"  Company insights complete: {len(result)} goal groups.")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("AGGREGATION SERVICE — Batch Pipeline Step 3")
    log("=" * 60)
    log("")

    log("Connecting to PostgreSQL...")
    engine = get_engine()
    create_analytics_schema(engine)
    log("")

    log("Loading staging data from PostgreSQL...")
    clients = pd.read_sql("SELECT * FROM staging.clients_clean", engine)
    foodlog = pd.read_sql("SELECT * FROM staging.foodlog_clean", engine)
    log(f"  Loaded {len(clients):,} clients and {len(foodlog):,} food log entries.")
    log("")

    log("Running aggregations...")
    weekly   = aggregate_weekly(clients, foodlog)
    insights = aggregate_insights(weekly)
    log("")

    log("Writing results to PostgreSQL analytics schema...")
    weekly.to_sql(
        name="weekly_nutrition",
        schema="analytics",
        con=engine,
        if_exists="replace",
        index=False
    )
    log(f"  Written: analytics.weekly_nutrition ({len(weekly):,} rows)")

    insights.to_sql(
        name="company_insights",
        schema="analytics",
        con=engine,
        if_exists="replace",
        index=False
    )
    log(f"  Written: analytics.company_insights ({len(insights)} rows)")

    log("")
    log("=" * 60)
    log("AGGREGATION COMPLETE")
    log("=" * 60)
    log(f"  analytics.weekly_nutrition  : {len(weekly):>10,} rows")
    log(f"  analytics.company_insights  : {len(insights):>10,} rows")
    log("")
    log("  Next step: API is now serving results at http://localhost:8000")
    log("=" * 60)


if __name__ == "__main__":
    main()