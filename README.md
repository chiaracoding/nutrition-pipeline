# Nutrition Pipeline

Batch processing pipeline for a nutrition coaching application serving 40,000 clients.
Built with Python, PostgreSQL, DuckDB, and Docker.

## Overview

Each Monday, the nutrition coaching app uploads a weekly food log CSV containing
approximately 1 million timestamped entries. The pipeline processes this data
automatically and serves the results via a REST API.

## Setup

Make sure Docker Desktop is running, then:

```bash
docker-compose up --build
```

The full pipeline runs automatically in the correct order. Once complete, the API
is available at http://localhost:8000

Interactive API documentation is available at http://localhost:8000/docs

## Architecture

The pipeline follows the medallion architecture pattern, with all data layers
stored in PostgreSQL.

```
CSV upload → Ingestion → raw schema → Processing → staging schema
          → Aggregation → analytics schema → FastAPI → consumers
```

- **Ingestion** — validates incoming schema, reads weekly CSV upload, writes to `raw` schema
- **Processing** — cleans data, fills missing values, applies Harris-Benedict formula, writes to `staging` schema
- **Aggregation** — DuckDB SQL aggregations, produces weekly summaries per user, writes to `analytics` schema
- **API** — FastAPI serving layer, reads from `analytics` schema, exposes results via HTTP endpoints

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Health check |
| `GET /users/{user_id}/report` | Weekly report for one user |
| `GET /reports/weekly` | All user reports (paginated) |
| `GET /insights/company` | Company-level insights by goal group |
| `GET /export/csv` | Download full report as CSV |

## Project Structure

```
nutrition-pipeline/
├── src/
│   ├── generate_data.py    — synthetic data generation
│   ├── ingestion.py        — CSV to PostgreSQL raw schema
│   ├── processing.py       — data cleaning and enrichment
│   ├── aggregation.py      — DuckDB SQL aggregations
│   └── api.py              — FastAPI serving layer
├── docker/
│   ├── Dockerfile.pipeline — shared image for pipeline services
│   └── Dockerfile.api      — image for FastAPI service
├── docker-compose.yml      — orchestrates all services
├── requirements.txt        — Python dependencies
└── data/
    └── source/             — generated CSV files (not committed)
```

## Reproducibility

The pipeline is fully reproducible. Clone the repository, ensure Docker Desktop
is running, and execute `docker-compose up --build`. All data is generated
synthetically via `generate_data.py` using a fixed random seed (42), meaning
results are identical on every machine and every run.

## AI Assistance

This project was developed with the support of Claude (Anthropic) as an AI
coding assistant. AI assistance was used for code structure, implementation
guidance, and debugging. All architectural decisions, written submissions,
and domain logic reflect the author's own understanding and judgement.