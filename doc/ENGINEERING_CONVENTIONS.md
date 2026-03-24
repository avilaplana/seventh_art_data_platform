# Engineering Conventions
## Seventh Art Analytics

**Version:** 1.0
**Date:** 2026-03-24
**Status:** Draft

---

## 1. Naming Conventions

### 1.1 General Rule
All names use **lowercase snake_case** across the entire codebase — SQL, Python, dbt, Airflow, and file names. No exceptions except environment variables (see §1.7).

### 1.2 Iceberg Namespaces and Tables

| Pattern | Example |
|---------|---------|
| `demo.{stage\|prod}_{layer}` | `demo.stage_raw`, `demo.prod_analytics` |
| Table names: lowercase snake_case | `title_person_role`, `fact_title_cast_crew` |

Layers: `raw` → `canonical` → `analytics`
Lifecycle: `stage_*` during processing, promoted to `prod_*` after quality checks pass.

### 1.3 dbt Model Names

| Layer | Prefix | Example |
|-------|--------|---------|
| Canonical (Silver) | none | `title`, `person`, `genre`, `title_type` |
| Analytics dimensions | `dim_` | `dim_title`, `dim_person` |
| Analytics facts | `fact_` | `fact_title_cast_crew`, `fact_episode` |

File names match model names exactly: `dim_title.sql`, `fact_episode.sql`.

### 1.4 Column Names

**Bronze layer (`stage_raw`):** Source column names are kept as-is from IMDB (camelCase):
```
nconst, primaryName, tconst, averageRating, numVotes
```

**Silver and Gold layers:** All columns converted to lowercase snake_case:
```
person_id, primary_title, average_rating, number_of_votes, release_year
```

**Primary keys:** `{entity}_id` suffix — `title_id`, `person_id`, `genre_id`, `role_id`

**Foreign keys:** Same naming as the referenced PK — `title_id`, `person_id`

**Snapshot metadata columns** — present on every table in every layer:
```
snapshot_date          DATE
ingested_at_timestamp  TIMESTAMP
snapshot_try           INT
```

### 1.5 Python Files and Functions

| Type | Pattern | Example |
|------|---------|---------|
| Extract scripts | `extract_{entity}_to_s3.py` | `extract_name_basics_to_s3.py` |
| Load scripts | `load_to_iceberg_{entity}.py` | `load_to_iceberg_title_ratings.py` |
| Utility modules | `{purpose}_utils.py` | `s3_utils.py`, `spark_utils.py` |
| DAG files | `{scope}_dag.py` | `etl_dag.py` |
| Functions | lowercase snake_case | `main()`, `build_spark_submit()` |

### 1.6 Airflow DAG and Task IDs

| Type | Pattern | Example |
|------|---------|---------|
| DAG ID | `{frequency}_{scope}_{pipeline}` | `daily_prod_etl_medallion` |
| Extract tasks | `extract_{entity}_to_s3` | `extract_title_crew_to_s3` |
| Load tasks | `load_SPARK_stage_raw_{job}` | `load_SPARK_stage_raw_create_tables` |
| dbt tasks | `transform_DBT_{target}` | `transform_DBT_stage_canonical_layer` |
| Quality checks | `transform_DBT_data_quality_check_{target}` | — |
| Promotion tasks | `promote_DBT_stage_{layer}_to_prod` | — |
| Control flow | descriptive verb phrase | `wait_30_minutes_before_dag_retry` |

### 1.7 Environment Variables
UPPERCASE with underscores:
```
PROJECTS_DIR, S3_ENDPOINT, AWS_ACCESS_KEY_ID
AIRFLOW__CORE__EXECUTOR  (double underscore for section nesting)
ENABLE_EXTRACT_STAGE, DAILY_SNAPSHOT_RETRY_COUNT  (feature flags)
```

### 1.8 S3 Path Structure
```
s3a://data/imdb/year={YYYY}/month={MM}/day={DD}/{filename}.tsv.gz
```
Month and day are zero-padded. Constructed dynamically from `datetime.utcnow()`.

---

## 2. SQL Style

### 2.1 Keywords
All SQL keywords in **UPPERCASE**:
```sql
SELECT, FROM, WHERE, JOIN, LEFT JOIN, ON, CASE, WHEN, THEN, ELSE, END,
WITH, AS, GROUP BY, ORDER BY, LIMIT, CAST, COALESCE, IS NULL, IS NOT NULL
```

### 2.2 Indentation
4 spaces. One column or condition per line.

```sql
SELECT
    title_id,
    primary_title,
    CAST(startYear AS INT) AS release_year
FROM {{ source('stage_raw', 'title_basics') }}
WHERE titleType IS NOT NULL
```

### 2.3 CTEs
Used to build up transformations in named steps. CTE names are lowercase snake_case and describe the transformation stage.

```sql
WITH raw_genres AS (
    SELECT
        tconst,
        genres
    FROM {{ source('stage_raw', 'title_basics') }}
),
distinct_genres AS (
    SELECT DISTINCT
        genre_name
    FROM raw_genres
    LATERAL VIEW explode(split(genres, ',')) AS genre_name
)
SELECT * FROM distinct_genres
```

### 2.4 JOINs
Explicit JOIN type. `ON` condition on same line as JOIN for single conditions.

```sql
FROM {{ ref('title') }} t
JOIN {{ ref('title_type') }} tt ON t.title_type_id = tt.title_type_id
LEFT JOIN {{ ref('title_person_role') }} tpr ON t.title_id = tpr.title_id
```

### 2.5 NULL Handling

IMDB uses `\N` as a null sentinel. Always convert at the canonical layer:
```sql
CASE
    WHEN birthYear = '\\N' THEN NULL
    ELSE CAST(birthYear AS INT)
END AS birth_year
```

Use `COALESCE` for default values:
```sql
CAST(COALESCE(averageRating, 0) AS DECIMAL(18,2)) AS average_rating
```

### 2.6 Derived Columns
Computed columns use CASE statements. Logic is explicit, not hidden in functions.

```sql
-- Decade bucketing
CONCAT(CAST(FLOOR(release_year / 10) * 10 AS STRING), 's') AS decade,

-- Rating bucketing
CASE
    WHEN average_rating IS NULL THEN 'no_rating'
    WHEN average_rating < 5    THEN '0-5'
    WHEN average_rating < 7    THEN '5-7'
    WHEN average_rating < 8    THEN '7-8'
    WHEN average_rating < 9    THEN '8-9'
    ELSE '9+'
END AS rating_bucket
```

### 2.7 Column Data Types

| Type | Usage |
|------|-------|
| `STRING` | IDs, names, codes, labels |
| `INT` | Years, counts, episode/season numbers |
| `DECIMAL(18,2)` | Ratings and other decimal metrics |
| `BOOLEAN` | Flags (e.g., `is_adult`, `is_original_title`) |
| `DATE` | `snapshot_date` |
| `TIMESTAMP` | `ingested_at_timestamp` |

---

## 3. dbt Conventions

### 3.1 Referencing Data

- Raw layer (Bronze): always via `{{ source('stage_raw', 'table_name') }}`
- Canonical models: always via `{{ ref('model_name') }}`
- Never hardcode schema or table names in SQL

### 3.2 Surrogate Keys
Generated with `dbt_utils.generate_surrogate_key()`. Pass the minimal set of columns that uniquely identifies the entity:

```sql
{{ dbt_utils.generate_surrogate_key(['genre_name']) }}       AS genre_id,
{{ dbt_utils.generate_surrogate_key(['role']) }}             AS role_id,
{{ dbt_utils.generate_surrogate_key(['title_id', 'nconst']) }} AS cast_id
```

### 3.3 Model Configuration
Materialisation is set at project level in `dbt_project.yml` — individual models do not repeat it.

```yaml
models:
  movie_data_platform:
    stage:
      raw:
        +materialized: view
      canonical:
        +materialized: table
        +file_format: iceberg
        +catalog: demo
      analytics:
        +materialized: table
        +file_format: iceberg
        +catalog: demo
```

### 3.4 Schema and Tests (YAML)
Every canonical and analytics model has a YAML file with:
- `contract.enforced: true`
- Column-level `data_type` declarations
- `not_null` and `unique` tests on PKs
- `relationships` tests on FKs
- `dbt_utils.unique_combination_of_columns` for composite keys

```yaml
models:
  - name: genre
    config:
      contract:
        enforced: true
    columns:
      - name: genre_id
        data_type: string
        tests:
          - not_null
          - unique
      - name: genre_name
        data_type: string
        tests:
          - not_null
```

### 3.5 Seeds
Reference data (regions, languages) is managed as dbt seeds, not hardcoded in SQL. Seed files have a corresponding YAML with column types.

### 3.6 Macros
Promotion macros loop through a list of table names and generate `CREATE OR REPLACE TABLE AS SELECT *` statements. Use `{{ log(..., info=True) }}` to surface progress in dbt logs.

---

## 4. Python Conventions

### 4.1 Spark Job Structure
Every Spark load script follows the same template:

```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from s3_utils import object_path

def main(snapshot_date: str, ingested_at_timestamp: str, snapshot_try: int):
    spark = SparkSession.builder \
        .appName("load-{entity}-Iceberg-MinIO") \
        .getOrCreate()

    df = spark.read.option(...).csv(f"s3a://...")

    df \
        .withColumn("snapshot_date", lit(snapshot_date)) \
        .withColumn("ingested_at_timestamp", lit(ingested_at_timestamp)) \
        .withColumn("snapshot_try", lit(snapshot_try)) \
        .writeTo("demo.stage_raw.{entity}").createOrReplace()

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot_date", required=True)
    parser.add_argument("--ingested_at_timestamp", required=True)
    parser.add_argument("--snapshot_try", required=True, type=int)
    args = parser.parse_args()
    main(args.snapshot_date, args.ingested_at_timestamp, args.snapshot_try)
```

### 4.2 Argument Naming
CLI arguments use `--snake_case`. All arguments are `required=True`. Type hints are on the function signature, not on argparse definitions.

### 4.3 S3 Path Construction
Always use the `object_path()` utility from `s3_utils.py`. Never construct paths inline.

### 4.4 DataFrame Operations
Method chaining using backslash continuation:
```python
df \
    .withColumn("col", lit(value)) \
    .writeTo("demo.stage_raw.table").createOrReplace()
```

### 4.5 Import Order
1. Standard library (`argparse`, `datetime`, `os`)
2. Third-party (`boto3`, `requests`, `pyspark`)
3. Local modules (`s3_utils`, `spark_utils`)

---

## 5. Project Structure

```
seventh_art_analytics/
├── ai/
│   ├── eval/                        # Benchmark questions, eval scripts, results
│   │   └── version_control/         # Prompt configs per model and version
│   └── text-to-sql/                 # FastAPI NL-to-SQL service
│       └── app/
│           └── compiler/            # LangGraph agent nodes, graph, state
├── airflow/
│   ├── dags/                        # DAG definitions and utilities
│   └── docker/                      # Airflow Dockerfile + requirements.txt
├── db/
│   └── init.sql                     # PostgreSQL initialisation
├── doc/                             # All project documentation
├── extract/
│   └── src/                         # One script per IMDB entity
├── load/
│   ├── docker/                      # Spark Dockerfile + spark-defaults.conf + core-site.xml
│   └── src/                         # One load script per IMDB entity + s3_utils.py
├── transform/
│   └── data_platform/               # dbt project root
│       ├── macros/                  # Promotion macros
│       ├── models/stage/
│       │   ├── raw/                 # sources.yml only
│       │   ├── canonical/           # Silver layer models
│       │   └── analytics/
│       │       ├── dimensions/      # dim_* models
│       │       └── facts/           # fact_* models
│       ├── seeds/                   # regions.csv, languages.csv
│       ├── dbt_project.yml
│       └── profiles.yml
└── docker-compose.yml
```

**Rules:**
- Docker configuration lives in `{component}/docker/`, not at project root
- Each layer has its own `src/` directory
- Documentation lives in `doc/`
- One script per entity — no monolithic files

---

## 6. Airflow Conventions

### 6.1 Feature Flags
Stored as Airflow Variables, UPPERCASE names:
```python
ENABLE_EXTRACT_STAGE     # "true" / "false"
DAILY_SNAPSHOT_RETRY_COUNT  # integer as string
```

Evaluated with: `Variable.get("FLAG_NAME").lower() == "true"`

### 6.2 Task Dependencies
Use `TriggerRule.NONE_FAILED` (not `ALL_SUCCESS`) on tasks that should proceed even when upstream tasks are skipped by a feature flag.

### 6.3 dbt Tasks
Run via `DockerOperator` targeting the `dbt-spark` image. Mount the `transform/` directory, pass the target name, and set `network_mode` to `iceberg_net`.

### 6.4 Spark Tasks
Run via `BashOperator` using `spark_utils.build_spark_submit()`. Always pass `snapshot_date`, `ingested_at_timestamp`, and `snapshot_try`.

---

## 7. Docker Conventions

### 7.1 YAML Anchors
Shared environment blocks are defined as YAML anchors at the top of `docker-compose.yml` and merged into services:
```yaml
x-spark-env: &spark-env
  AWS_ACCESS_KEY_ID: admin
  ...

services:
  spark-master:
    environment:
      <<: *spark-env
```

### 7.2 Service Ordering
Services defined in dependency order: infrastructure (postgres, minio) → compute (spark) → orchestration (airflow) → application (text-to-sql).

### 7.3 Host Mounts
Use `${PROJECTS_DIR}` for all bind mounts. Never hardcode absolute paths in `docker-compose.yml`.
Always run with:
```bash
export PROJECTS_DIR=/Users/alvarovilaplana/projects && docker compose up -d
```
