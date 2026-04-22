# Nutrition Pipeline

Batch processing pipeline for a nutrition coaching application.
Built with Python, PostgreSQL, DuckDB, and Docker.

## Setup

```bash
docker-compose up --build
```

## Architecture

- **Ingestion** — CSV to Parquet, schema validation
- **Processing** — data cleaning, Harris-Benedict formula
- **Aggregation** — DuckDB SQL, weekly summaries per user
- **API** — FastAPI serving layer on http://localhost:8000