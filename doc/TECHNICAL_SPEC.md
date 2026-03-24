# Technical Specification
## Seventh Art Analytics

**Version:** 1.0
**Date:** 2026-03-24
**Status:** Draft

---

## 1. Overview

Seventh Art Analytics is a self-hosted data platform that ingests the IMDB public dataset daily, processes it through a medallion architecture, and exposes the data through a natural language interface powered by a local LLM. The entire platform runs locally via Docker Compose.

---

## 2. Architecture

### 2.1 High-Level Data Flow

```
IMDB Public Datasets (TSV.GZ)
        ↓
    [ Extract ]  — download & store raw files to MinIO S3
        ↓
    [ Load ]     — Spark jobs write raw data to Iceberg (Bronze)
        ↓
    [ Transform ] — dbt models clean and model data (Silver → Gold)
        ↓
    [ Promote ]  — stage schemas swapped to prod atomically
        ↓
    [ Serve ]    — Text-to-SQL AI agent queries the Gold layer
```

### 2.2 Medallion Layers

| Layer | Iceberg Namespace | Owner | Purpose |
|-------|------------------|-------|---------|
| Bronze | `demo.stage_raw` | Spark | Raw IMDB data, schema-on-read, rebuilt daily |
| Silver | `demo.stage_canonical` / `demo.prod_canonical` | dbt | Cleaned, typed, normalised entities |
| Gold | `demo.stage_analytics` / `demo.prod_analytics` | dbt | Star schema optimised for BI and LLM queries |

### 2.3 Write-Audit-Publish Pattern

Transformations write to `stage_*` namespaces first. After data quality checks pass, a dbt macro promotes tables atomically to `prod_*` via `CREATE OR REPLACE TABLE AS SELECT *`. This ensures the production layer is never in a partial state.

---

## 3. Infrastructure

### 3.1 Services

| Service | Image | Port | Role |
|---------|-------|------|------|
| spark-master | custom-spark-iceberg | 8080 | Spark cluster master |
| spark-worker-1 | custom-spark-iceberg | — | 2 cores, 2GB memory |
| spark-worker-2 | custom-spark-iceberg | — | 2 cores, 2GB memory |
| spark-thrift-server | custom-spark-iceberg | 10000, 4040 | JDBC endpoint for dbt and text-to-sql |
| rest | tabulario/iceberg-rest:1.6.0 | 8181 | Iceberg REST catalog |
| minio | minio/minio | 9000, 9001 | S3-compatible object storage |
| postgres | postgres:14 | 5432 | Airflow metadata + Iceberg catalog metadata |
| airflow-webserver | custom-airflow | 8088 | Airflow UI and REST API |
| airflow-scheduler | custom-airflow | — | DAG scheduling |
| text-to-sql | custom image | 8001 | FastAPI NL-to-SQL agent |

### 3.2 Networking

All services communicate over a custom Docker bridge network: `iceberg_net`. MinIO is accessible internally via the alias `warehouse.minio`.

### 3.3 Persistent Storage

| Volume | Service | Contents |
|--------|---------|----------|
| `minio_data` | MinIO | Raw files and Iceberg data/metadata files |
| `postgres_db_volume` | PostgreSQL | Airflow and catalog metadata |

### 3.4 Tech Stack Versions

| Component | Version |
|-----------|---------|
| Apache Airflow | 2.9.3 |
| Apache Spark | 3.5.5 |
| Apache Iceberg | 1.8.1 |
| dbt-core | 1.11.2 |
| dbt-spark | 1.10.0rc1 |
| PostgreSQL | 14 |
| Hadoop (S3A) | 3.3.4 |
| Java | OpenJDK 17 |
| Iceberg REST catalog | tabulario/iceberg-rest 1.6.0 |

---

## 4. Orchestration

### 4.1 DAG: `daily_prod_etl_medallion`

Scheduled manually (no cron). Triggered daily to process a new IMDB snapshot.

### 4.2 Pipeline Stages

```
1. EXTRACT (parallel)
   └── 7 Python tasks — download IMDB TSV.GZ files → MinIO S3

2. LOAD (sequential)
   ├── create_tables — DROP & CREATE raw Iceberg tables
   └── 7 Spark jobs — read S3 files → write to demo.stage_raw

3. DBT SETUP
   ├── dbt deps
   └── dbt seed (regions, languages, title variant patterns)

4. CANONICAL LAYER
   ├── dbt run --target stage_canonical
   └── dbt test (data quality checks)

5. ANALYTICS LAYER
   ├── dbt run --target stage_analytics
   └── dbt test (data quality checks)

6. PROMOTION
   ├── promote stage_canonical → prod_canonical
   └── promote stage_analytics → prod_analytics
```

### 4.3 Retry Strategy

- On failure: wait 30 minutes, then re-trigger the entire DAG
- Maximum 3 retries, tracked via Airflow Variable `DAILY_SNAPSHOT_RETRY_COUNT`
- Feature flag `ENABLE_EXTRACT_STAGE` allows skipping extraction (reuse last snapshot)

### 4.4 Snapshot Metadata

Every run passes three values through the pipeline and stores them in every table:

| Field | Description |
|-------|-------------|
| `snapshot_date` | UTC date of the IMDB snapshot (YYYY-MM-DD) |
| `ingested_at_timestamp` | UTC timestamp of ingestion |
| `snapshot_try` | Retry attempt number (1 on first run) |

---

## 5. Storage

### 5.1 MinIO Buckets

| Bucket | Contents | Path Pattern |
|--------|----------|--------------|
| `data` | Raw IMDB TSV.GZ files | `imdb/year={YYYY}/month={MM}/day={DD}/{filename}` |
| `warehouse` | Iceberg metadata and Parquet data files | Managed by Iceberg catalog |

### 5.2 Iceberg Catalog

- **Type:** REST catalog (tabulario/iceberg-rest)
- **Metadata store:** PostgreSQL database `catalog_metadata`
- **File format:** Parquet
- **Catalog name:** `demo`

### 5.3 Iceberg Namespaces

| Namespace | Tables | Lifecycle |
|-----------|--------|-----------|
| `demo.stage_raw` | 7 | Dropped and recreated each run |
| `demo.stage_canonical` | 13 | Overwritten each run |
| `demo.stage_analytics` | 6 | Overwritten each run |
| `demo.prod_canonical` | 13 | Replaced atomically after quality checks |
| `demo.prod_analytics` | 6 | Replaced atomically after quality checks |

---

## 6. Data Model

### 6.1 Bronze Layer — `demo.stage_raw`

Direct load from IMDB TSV files. All columns are strings; nulls represented as `\N` from source.

| Table | Source File | Key Columns |
|-------|-------------|-------------|
| `name_basics` | name.basics.tsv.gz | nconst, primaryName, birthYear, deathYear |
| `title_basics` | title.basics.tsv.gz | tconst, titleType, primaryTitle, startYear, genres |
| `title_akas` | title.akas.tsv.gz | titleId, title, region, language, isOriginalTitle |
| `title_crew` | title.crew.tsv.gz | tconst, directors, writers |
| `title_episode` | title.episode.tsv.gz | tconst, parentTconst, seasonNumber, episodeNumber |
| `title_principals` | title.principals.tsv.gz | tconst, nconst, category, characters |
| `title_ratings` | title.ratings.tsv.gz | tconst, averageRating, numVotes |

All tables include: `snapshot_date`, `ingested_at_timestamp`, `snapshot_try`.

### 6.2 Silver Layer — `demo.prod_canonical`

Normalised, typed entities. Surrogate keys generated via `dbt_utils.generate_surrogate_key`.

**Dimension entities:**

| Table | PK | Description |
|-------|-----|-------------|
| `title` | title_id | Film/series master record |
| `person` | person_id | Cast and crew master record |
| `title_type` | title_type_id | movie, tvSeries, tvEpisode, etc. |
| `role` | role_id | actor, actress, director, writer, etc. |
| `genre` | genre_id | Action, Drama, Comedy, etc. |

**Relationship entities:**

| Table | Keys | Description |
|-------|------|-------------|
| `title_person_role` | title_id, person_id, role_id | Who did what in which title |
| `title_genre` | title_id, genre_id | Which genres belong to which title |
| `title_episode` | series_title_id, episode_title_id | Episode-to-series relationships |
| `title_localized` | title_id, region_id, language_id | Localised title names by region |

**Seeded lookup tables:** `regions`, `languages`

### 6.3 Gold Layer — `demo.prod_analytics`

Star schema. Denormalised for query performance and LLM SQL generation.

**Dimensions:**

`dim_title`:
```
title_id, primary_title, original_title, title_type_name,
release_year, duration_minutes, is_adult,
average_rating, number_of_votes,
decade,          -- derived: '1990s', '2000s', etc.
rating_bucket    -- derived: '0-5', '5-7', '7-8', '8-9', '9+'
```

`dim_person`:
```
person_id, name, birth_year, death_year
```

**Facts:**

`fact_title_genre_flat` — one row per title × genre:
```
title_id, genre_name, title_type_name,
release_year, is_adult, average_rating, number_of_votes
```

`fact_title_cast_crew` — one row per title × person × role:
```
title_id, person_id, role_name,
release_year, average_rating, number_of_votes
```

`fact_title_release` — one row per localised release:
```
title_id, localized_title, region_name, language_name, is_original_title
```

`fact_episode` — fully denormalised episodes:
```
series_title_id, episode_title_id,
season_number, episode_number,
episode_primary_title, episode_release_year,
episode_average_rating, episode_number_of_votes,
series_primary_title, series_average_rating
```

**Join guide:**
- Filter by genre → `fact_title_genre_flat`
- Filter by person/role → `fact_title_cast_crew` + `dim_person`
- Filter by region/language → `fact_title_release` + `dim_title`
- Episode queries → `fact_episode` (no joins needed)
- Title attributes (decade, rating_bucket) always come from `dim_title`

---

## 7. Transformation (dbt)

### 7.1 Project Configuration

- **Project name:** `movie_data_platform`
- **Connection:** Spark Thrift Server at port 10000 via `dbt-spark` (thrift method)
- **Two targets:** `stage_canonical` and `stage_analytics`
- **Materialisation:** `view` for raw sources, `table` (Iceberg) for canonical and analytics

### 7.2 Model Structure

```
models/stage/
├── raw/
│   └── sources.yml          — defines stage_raw as dbt source
├── canonical/               — 11 SQL models → Silver layer
└── analytics/
    ├── dimensions/          — dim_title, dim_person
    └── facts/               — fact_title_genre_flat, fact_title_cast_crew,
                               fact_title_release, fact_episode
```

### 7.3 Seeds

| Seed | Purpose |
|------|---------|
| `regions.csv` | ISO region codes and names |
| `languages.csv` | ISO language codes and names |
| `title_variant_pattern_map.csv` | Title normalisation patterns |

### 7.4 Stage-to-Production Promotion

Two macros handle atomic promotion:
- `promote_canonical_to_prod.sql` — promotes 13 canonical tables
- `promote_analytics_to_prod.sql` — promotes 6 analytics tables

Mechanism: `CREATE OR REPLACE TABLE demo.prod_X.{table} AS SELECT * FROM demo.stage_X.{table}`

---

## 8. AI Text-to-SQL Service

### 8.1 Overview

A FastAPI service that accepts a natural language question and returns SQL + query results. Built on LangGraph with an agentic retry loop for self-correction.

### 8.2 API

**Endpoint:** `POST /query`

Request:
```json
{
  "question": "Which directors have directed the most films since 2000?",
  "model": "qwen2.5-coder",
  "version": 6
}
```

Response:
```json
{
  "sql": "SELECT ...",
  "result": { "columns": [...], "rows": [[...]] },
  "error": null,
  "metrics": {
    "model": "qwen2.5-coder:7b",
    "prompt_tokens": 1024,
    "generated_tokens": 45,
    "tokens_per_second": 15.2,
    "total_time_sec": 3.2
  }
}
```

### 8.3 Agent Workflow (LangGraph)

```
START
  ↓
load_prompt_configuration  — load YAML config for model/version
  ↓
generate_sql               — call local LLM via Ollama
  ↓
sanitize_sql               — strip markdown, add LIMIT if missing
  ↓
validate_sql               — enforce SELECT-only, no DDL/DML
  ↓
execute_sql                — run on Spark Thrift Server via JDBC
  ↓
  ├─ error AND retry < 3? → repair_sql (send error context back to LLM)
  │                              ↓ back to sanitize_sql
  └─ success OR max retries → END
```

### 8.4 SQL Safety Rules

1. Query must start with `SELECT` or `WITH`
2. Forbidden keywords: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, `MERGE`, `GRANT`, `REVOKE`
3. No semicolons mid-query
4. `LIMIT` clause required — auto-appended (`LIMIT 10`) if missing

### 8.5 LLM Integration

- **Runtime:** Ollama (local inference, no external API calls)
- **Connection:** `http://host.docker.internal:11434`
- **Default model:** `qwen2.5-coder:7b`
- **Generation parameters:** temperature 0.05, top_p 0.8, context window 16384

### 8.6 Database Connection

The service connects to Spark Thrift Server via JDBC (HiveDriver) targeting `demo.stage_analytics` as the default catalog/schema.

---

## 9. Evaluation Framework

### 9.1 Purpose

Evaluate and compare multiple local LLM models on NL-to-SQL accuracy using a fixed benchmark question set. Results are published as a comparison table for reproducibility.

### 9.2 Benchmark Questions

- **Location:** `ai/eval/questions_analytics.yml`
- **Count:** 45 questions
- **Difficulty levels:** simple, intermediate, complex
- **Coverage:** genre aggregation, cast/crew queries, episode navigation, localised releases, cross-fact joins

### 9.3 Evaluation Process

1. Load question set
2. For each question: `POST /query` with specified model and version
3. Record: question, generated SQL, expected SQL, error (if any), performance metrics
4. Save results to `ai/eval/{model}_v{version}_results.txt`

### 9.4 Prompt Version Control

Each model/version combination has a YAML config at:
`ai/eval/version_control/{model}/eval_config_v{N}.yml`

Config contains: model name, context window, temperature, top_p, system prompt, user prompt template, schema block (table/column definitions), semantic layer (NL aliases, enum values, table selection guide).

### 9.5 Benchmark Output

Results table comparing models on:
- Overall accuracy (% correct)
- Accuracy by difficulty (simple / intermediate / complex)
- Average tokens per second
- Average total response time

---

## 10. Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8088 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| Spark Driver UI | http://localhost:4040 | — |
| MinIO Console | http://localhost:9001 | admin / password |
| Iceberg REST API | http://localhost:8181 | — |
| Text-to-SQL API | http://localhost:8001 | — |
| Spark Thrift JDBC | jdbc:hive2://localhost:10000/demo.stage_analytics | dbt / (empty) |

---

## 11. Running the Platform

```bash
# Start all services
export PROJECTS_DIR=/Users/alvarovilaplana/projects
docker compose up -d

# Trigger the daily pipeline
airflow dags trigger daily_prod_etl_medallion

# Run an evaluation
python ai/eval/eval_script.py
```
