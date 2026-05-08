"""
api.py
------
PIPELINE STEP 4: FastAPI Serving Layer

PURPOSE:
    This service replaces the previous output.py script.
    Instead of writing CSV files to disk, the aggregated results
    are served as a live REST API that any downstream application
    can query — including the ML application described in the task.

    The API reads directly from the PostgreSQL analytics schema,
    which is populated by aggregation.py.

ENDPOINTS:
    GET /                           — health check
    GET /users/{user_id}/report     — weekly report for one user
    GET /reports/weekly             — all 40,000 user reports
    GET /insights/company           — company-level insights by goal
    GET /export/csv                 — download full report as CSV

WHY FASTAPI?
    FastAPI is a modern Python web framework that automatically
    generates API documentation at /docs. This means your professor
    can open http://localhost:8000/docs and interactively test every
    endpoint without writing any code — making the project easy to
    evaluate and demonstrate.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
from sqlalchemy import create_engine, text
import os
import io

# ── Database connection ───────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "nutrition")
DB_USER     = os.getenv("DB_USER",     "pipeline")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline123")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL)

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Nutrition Pipeline API",
    description=(
        "Serving layer for the weekly batch-processing nutrition pipeline. "
        "Exposes aggregated weekly nutrition data for 40,000 coaching clients. "
        "Data is updated every Monday after the batch pipeline completes."
    ),
    version="1.0.0"
)


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def health_check():
    """
    Health check endpoint.
    Returns status and confirms database connectivity.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected",
            "message": "Nutrition pipeline API is running."
        }
    except Exception as e:
        raise HTTPException(status_code=503,
                            detail=f"Database unavailable: {str(e)}")


@app.get("/users/{user_id}/report", tags=["User reports"])
def get_user_report(user_id: str):
    """
    Returns the weekly nutrition report for a single user.

    This is the endpoint the nutrition coaching app would call
    to display the weekly summary to each client.
    """
    query = """
        SELECT * FROM analytics.weekly_nutrition
        WHERE user_id = :user_id
    """
    df = pd.read_sql(query, engine, params={"user_id": user_id})

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No report found for user {user_id}. "
                   f"Check user_id or re-run the pipeline."
        )

    return df.iloc[0].to_dict()


@app.get("/reports/weekly", tags=["User reports"])
def get_all_reports(limit: int = 100, offset: int = 0):
    """
    Returns weekly reports for all users, paginated.

    Use limit and offset parameters for pagination.
    Example: /reports/weekly?limit=50&offset=100
    """
    query = f"""
        SELECT * FROM analytics.weekly_nutrition
        ORDER BY user_id
        LIMIT {limit} OFFSET {offset}
    """
    df = pd.read_sql(query, engine)

    total = pd.read_sql(
        "SELECT COUNT(*) as n FROM analytics.weekly_nutrition", engine
    ).iloc[0]["n"]

    return {
        "total_users": int(total),
        "limit": limit,
        "offset": offset,
        "results": df.to_dict(orient="records")
    }


@app.get("/insights/company", tags=["Company insights"])
def get_company_insights():
    """
    Returns company-level weekly insights grouped by nutrition goal.

    Shows compliance rates, average intake, and RAG status per goal group.
    This endpoint serves the management dashboard.
    """
    df = pd.read_sql("SELECT * FROM analytics.company_insights", engine)

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="No insights found. Run the pipeline first."
        )

    return {
        "week_processed": df["week_processed"].iloc[0],
        "insights": df.to_dict(orient="records")
    }


@app.get("/export/csv", tags=["Export"])
def export_csv():
    """
    Downloads the full weekly nutrition report as a CSV file.

    This optional export endpoint replicates the behaviour of the
    previous output.py script, now available on demand via HTTP.
    The downstream ML application can call this endpoint to retrieve
    the weekly dataset for model training or inference.
    """
    df = pd.read_sql(
        "SELECT * FROM analytics.weekly_nutrition ORDER BY user_id",
        engine
    )

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="No data available for export. Run the pipeline first."
        )

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    return StreamingResponse(
        io.BytesIO(buffer.getvalue().encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition":
                "attachment; filename=weekly_nutrition_report.csv"
        }
    )