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

PostgreSQL (8 OLTP tables) → **EL** (Python, full extract + truncate-load) → BigQuery Emulator `raw` dataset → **dbt** (`raw` → `dwh`) → Star Schema (3 fact tables + 6 dimensions)

### dbt Model Layers (`dbt/models/`)

- **staging/** (`stg_*`): 1:1 with raw tables (8 models). Type casting and column renaming only.
- **intermediate/** (`int_*`): JOINs, business logic, and analysis models (8 models). Includes region mapping, RFM scoring, LTV calculation, ABC analysis, session aggregation, inventory metrics.
- **marts/** (`dim_*`, `fact_*`): Final Star Schema (9 models). 3 fact tables (fact_sales, fact_page_views, fact_inventory_daily) + 6 dimensions (dim_date, dim_time, dim_customer, dim_product, dim_order_status, dim_coupon).

All models are materialized as `table` (not views or incremental).

### Seed Data Generation

`scripts/generate_seed.py` generates realistic seed data with configurable volume (constants at top of file). Uses fixed random seed for reproducibility. Features customer segments, Pareto product popularity, seasonal patterns, and funnel-consistent page views. Run `python scripts/generate_seed.py` to regenerate `postgres/init/02_seed.sql`.

### BigQuery Emulator Compatibility

dbt-bigquery does not natively support BigQuery Emulator. The project uses `dbt/bq_emulator_patch.py` — a monkey patch applied at startup via `dbt/run_dbt.py` — to override client creation, query execution, and DDL operations. This is the most fragile part of the codebase.

**Emulator limitations affecting SQL patterns:**
- NTILE(), PERCENT_RANK() may not work → use threshold-based CASE WHEN
- SAFE_DIVIDE() may not work → use NULLIF() for zero-division protection
- Large window functions (ROWS BETWEEN) on 10K+ rows can timeout → simplify or pre-aggregate
- Query timeout is 600s (`bq_emulator_patch.py` line 290)

### Web UI (`ui/`)

Optional React + TypeScript + Vite app (behind `ui` Docker Compose profile). Built as multi-stage Docker image with nginx serving static files and proxying BigQuery API requests.

## Key Constraints

- BigQuery Emulator runs on `linux/amd64` platform (forced in docker-compose.yml).
- The EL script (`el/main.py`) defines BigQuery schemas explicitly in `TABLE_SCHEMAS` — these must stay in sync with `postgres/init/01_ddl.sql`.
- The seed data generator (`scripts/generate_seed.py`) output must match the DDL column definitions.
- BigQuery project ID is `poc-project` throughout; dataset names are `raw` (landing) and `dwh` (transformed).
- Ports: PostgreSQL=15432, BigQuery REST=9050, BigQuery gRPC=9060, Web UI=3001.
- Data volume is limited by BigQuery Emulator performance. Current default (1K customers, 5K orders) works reliably. Larger volumes may cause timeouts.
