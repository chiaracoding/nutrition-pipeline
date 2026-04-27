# Nutrition Pipeline

Batch processing pipeline for a nutrition coaching application.
Built with Python, PostgreSQL, DuckDB, and Docker.

## Setup

Make sure Docker Desktop is running, then:

```bash
docker-compose up --build
```

The full pipeline runs automatically. The API is available at
http://localhost:8000 once all services complete.

## Architecture

The pipeline follows the medallion architecture pattern.
All data layers are stored in PostgreSQL that follows the medaillon architecture.

- **Ingestion** — reads weekly CSV upload, writes to `raw` schema
- **Processing** — cleans data, applies Harris-Benedict formula, writes to `staging` schema
- **Aggregation** — DuckDB SQL aggregations, writes weekly summaries to `analytics` schema
- **API** — FastAPI serving layer, reads from `analytics` schema