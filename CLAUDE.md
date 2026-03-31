# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kimball-style Star Schema ELT pipeline PoC. Extracts from PostgreSQL (OLTP), loads into BigQuery Emulator, and transforms with dbt into a dimensional model. Everything runs in Docker Compose.

## Commands

```bash
make up              # Build and start all services (postgres → bigquery-emulator → el → dbt)
make down            # Stop and remove all containers + volumes
make restart         # Clean restart (down → up)

make dbt-run         # Run dbt models only
make dbt-test        # Run dbt tests only
make dbt-build       # Run dbt build (run + test)
make el-rerun        # Re-run EL pipeline

make psql            # Connect to PostgreSQL (ecsite DB)
make bq Q="SELECT …" # Query BigQuery Emulator (table format)
make ui              # Start optional Web UI at http://localhost:3001
make ui-down         # Stop Web UI
```

## Architecture

### Pipeline Flow

PostgreSQL (5 OLTP tables) → **EL** (Python, full extract + truncate-load) → BigQuery Emulator `raw` dataset → **dbt** (`raw` → `dwh`) → Star Schema

### dbt Model Layers (`dbt/models/`)

- **staging/** (`stg_*`): 1:1 with raw tables. Type casting and column renaming only.
- **intermediate/** (`int_*`): JOINs and business logic (region mapping, category denormalization, amount calculations).
- **marts/** (`dim_*`, `fact_*`): Final Star Schema. Surrogate keys via `ROW_NUMBER()`.

All models are materialized as `table` (not views or incremental).

### BigQuery Emulator Compatibility

dbt-bigquery does not natively support BigQuery Emulator. The project uses `dbt/bq_emulator_patch.py` — a monkey patch applied at startup via `dbt/run_dbt.py` — to override client creation, query execution, and DDL operations. This is the most fragile part of the codebase.

### Web UI (`ui/`)

Optional React + TypeScript + Vite app (behind `ui` Docker Compose profile). Built as multi-stage Docker image with nginx serving static files and proxying BigQuery API requests.

## Key Constraints

- BigQuery Emulator runs on `linux/amd64` platform (forced in docker-compose.yml).
- The EL script (`el/main.py`) defines BigQuery schemas explicitly in `TABLE_SCHEMAS` — these must stay in sync with `postgres/init/01_ddl.sql`.
- BigQuery project ID is `poc-project` throughout; dataset names are `raw` (landing) and `dwh` (transformed).
- Ports: PostgreSQL=15432, BigQuery REST=9050, BigQuery gRPC=9060, Web UI=3001.
