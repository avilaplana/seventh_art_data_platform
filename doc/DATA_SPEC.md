# Data Specification
## Seventh Art Analytics

**Version:** 1.0
**Date:** 2026-03-24
**Status:** Draft

---

## 1. Overview

This document defines the data contracts for all three medallion layers: Bronze (raw), Silver (canonical), and Gold (analytics). It covers table schemas, field semantics, transformation rules, data quality guarantees, and known limitations.

---

## 2. Source Data

### 2.1 IMDB Public Dataset

**URL:** `https://datasets.imdbws.com/`
**Format:** TSV (tab-separated), gzip-compressed
**Refresh:** New snapshot published daily
**Licence:** Free for personal, non-commercial use only
**Null sentinel:** `\N` (literal backslash-N) used in place of NULL for all missing values

**Files ingested:**

| File | Description |
|------|-------------|
| `name.basics.tsv.gz` | People (cast, crew, directors) |
| `title.basics.tsv.gz` | Titles (movies, series, episodes) |
| `title.akas.tsv.gz` | Localised titles by region/language |
| `title.crew.tsv.gz` | Director and writer assignments |
| `title.episode.tsv.gz` | Episode-to-series relationships |
| `title.principals.tsv.gz` | Cast and crew per title with ordering |
| `title.ratings.tsv.gz` | Average rating and vote count per title |

---

## 3. Bronze Layer — `demo.stage_raw`

### 3.1 Characteristics

- All columns are `STRING` type — no casting at this layer
- Null sentinel `\N` is preserved as-is, not converted to NULL
- Three metadata columns added at ingest time to every table
- Tables are **dropped and recreated** on every daily run

**Metadata columns (all tables):**

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_date` | STRING | UTC date of the IMDB snapshot (YYYY-MM-DD) |
| `ingested_at_timestamp` | STRING | UTC timestamp of ingestion (YYYY-MM-DD HH:MM:SS) |
| `snapshot_try` | STRING | Retry attempt number (1 on first run) |

---

### 3.2 Table: `name_basics`

| Column | Nullable | Description | Notes |
|--------|----------|-------------|-------|
| `nconst` | NO | Unique person identifier (e.g. `nm0000001`) | IMDB-assigned, stable |
| `primaryName` | NO | Full name as displayed on IMDB | May contain non-ASCII characters |
| `birthYear` | YES | Year of birth | `\N` if unknown |
| `deathYear` | YES | Year of death | `\N` if still living or unknown |
| `primaryProfession` | YES | Comma-separated list of professions | e.g. `actor,director` — `\N` if unknown |
| `knownForTitles` | YES | Comma-separated list of tconst values | `\N` if not populated; not all persons have values |

---

### 3.3 Table: `title_basics`

| Column | Nullable | Description | Notes |
|--------|----------|-------------|-------|
| `tconst` | NO | Unique title identifier (e.g. `tt0000001`) | IMDB-assigned, stable |
| `titleType` | NO | Type of title | Values: `movie`, `tvSeries`, `tvEpisode`, `short`, `tvShort`, `tvMiniSeries`, `tvMovie`, `video`, `videoGame`, `tvSpecial` |
| `primaryTitle` | NO | Main display title used on IMDB | |
| `originalTitle` | NO | Title in the original language of production | |
| `isAdult` | NO | Adult content flag | `0` or `1` as string |
| `startYear` | YES | Release year (or series start year) | `\N` if unknown |
| `endYear` | YES | Series end year | `\N` for non-series or ongoing |
| `runtimeMinutes` | YES | Runtime in minutes | `\N` if unknown; not reliable for series |
| `genres` | YES | Comma-separated list of genres | `\N` if not classified; up to 3 genres per title |

---

### 3.4 Table: `title_akas`

| Column | Nullable | Description | Notes |
|--------|----------|-------------|-------|
| `titleId` | NO | FK to `tconst` in `title_basics` | |
| `ordering` | NO | Order of this localised title for the same titleId | Integer as string |
| `title` | NO | Localised title text | |
| `region` | YES | ISO region code (e.g. `US`, `GB`, `NO`) | `\N` if not specified |
| `language` | YES | ISO language code (e.g. `en`, `es`, `fr`) | `\N` if not specified |
| `types` | YES | Type of this localisation | e.g. `original`, `imdbDisplay`, `festival` — `\N` if unknown |
| `attributes` | YES | Additional context (e.g. `working title`) | `\N` if not specified |
| `isOriginalTitle` | YES | Whether this is the original title | `0` or `1` as string; `\N` if unknown |

---

### 3.5 Table: `title_crew`

| Column | Nullable | Description | Notes |
|--------|----------|-------------|-------|
| `tconst` | NO | FK to `title_basics` | |
| `directors` | YES | Comma-separated list of nconst values | `\N` if no director |
| `writers` | YES | Comma-separated list of nconst values | `\N` if no writer |

> **Note:** This table is loaded to Bronze but not exposed directly in the canonical layer. Director and writer relationships are derived via `title_principals`.

---

### 3.6 Table: `title_episode`

| Column | Nullable | Description | Notes |
|--------|----------|-------------|-------|
| `tconst` | NO | Episode title identifier | |
| `parentTconst` | NO | Series title identifier | |
| `seasonNumber` | YES | Season number | `\N` if unknown or not applicable |
| `episodeNumber` | YES | Episode number within the season | `\N` if unknown |

---

### 3.7 Table: `title_principals`

| Column | Nullable | Description | Notes |
|--------|----------|-------------|-------|
| `tconst` | NO | FK to `title_basics` | |
| `ordering` | NO | Rank of this person in the title's principal cast/crew | Lower = more prominent |
| `nconst` | NO | FK to `name_basics` | |
| `category` | NO | Role category | Values: `actor`, `actress`, `director`, `writer`, `producer`, `composer`, `cinematographer`, `editor`, `production_designer`, `self`, `archive_footage`, `archive_sound` |
| `job` | YES | Specific job description | `\N` if not applicable |
| `characters` | YES | JSON array of character names | e.g. `["Tony Stark"]` — `\N` if not applicable |

---

### 3.8 Table: `title_ratings`

| Column | Nullable | Description | Notes |
|--------|----------|-------------|-------|
| `tconst` | NO | FK to `title_basics` | |
| `averageRating` | NO | Weighted average of user ratings | Scale: 1.0–10.0 |
| `numVotes` | NO | Number of votes that contributed to the rating | Titles with very few votes are excluded by IMDB from this file |

> **Note:** Not all titles in `title_basics` have a corresponding entry in `title_ratings`. Titles with insufficient votes are excluded by IMDB.

---

## 4. Silver Layer — `demo.prod_canonical`

### 4.1 Transformation Rules (Bronze → Silver)

| Rule | Description |
|------|-------------|
| `\N` → NULL | All `\N` sentinel values converted to proper NULL via CASE statements |
| Type casting | Numeric columns cast to INT or DECIMAL; date/timestamp columns explicitly cast |
| `isAdult` | `'1'` → `TRUE`, everything else → `FALSE` |
| `averageRating` | `COALESCE(..., 0)` — defaults to `0.00` when no rating exists |
| `numVotes` | `COALESCE(..., 0)` — defaults to `0` when no rating exists |
| Surrogate keys | Generated via `dbt_utils.generate_surrogate_key()` for lookup entities |
| Genre explosion | Comma-separated `genres` string exploded into one row per genre |
| Character cleaning | JSON brackets and quotes stripped from `characters` field |
| Person filter | `title_person_role` only includes persons that exist in `name_basics` |
| Snapshot metadata | Forwarded as typed columns (DATE, TIMESTAMP, INT) |

---

### 4.2 Table: `title`

Core title entity. One row per IMDB title.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | PK — IMDB tconst (e.g. `tt0000001`) |
| `title_type_id` | STRING | NO | FK → `title_type.title_type_id` |
| `primary_title` | STRING | NO | Main display title |
| `original_title` | STRING | NO | Title in original production language |
| `is_adult` | BOOLEAN | NO | True if adult content |
| `release_year` | INT | YES | First release year — NULL if unknown |
| `duration_minutes` | INT | YES | Runtime in minutes — NULL if unknown or series |
| `average_rating` | DECIMAL(18,2) | NO | Average IMDB rating — 0.00 if unrated |
| `number_of_votes` | INT | NO | Vote count — 0 if unrated |
| `snapshot_date` | DATE | NO | |
| `ingested_at_timestamp` | TIMESTAMP | NO | |
| `snapshot_try` | INT | NO | |

**Tests:** `title_id` not_null + unique; `title_type_id` not_null + FK relationship

---

### 4.3 Table: `person`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `person_id` | STRING | NO | PK — IMDB nconst (e.g. `nm0000001`) |
| `name` | STRING | NO | Full name |
| `birth_year` | INT | YES | Year of birth — NULL if unknown |
| `death_year` | INT | YES | Year of death — NULL if living or unknown |
| `snapshot_date` | DATE | NO | |
| `ingested_at_timestamp` | TIMESTAMP | NO | |
| `snapshot_try` | INT | NO | |

---

### 4.4 Table: `title_type`

Lookup table for title types. Derived by extracting distinct `titleType` values.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_type_id` | STRING | NO | PK — surrogate key on `title_type_name` |
| `title_type_name` | STRING | NO | e.g. `movie`, `tvSeries`, `tvEpisode`, `short` |

---

### 4.5 Table: `genre`

Lookup table for genres. Derived by exploding the comma-separated genres field.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `genre_id` | STRING | NO | PK — surrogate key on `genre_name` |
| `genre_name` | STRING | NO | e.g. `Action`, `Drama`, `Comedy` |

**Known values:** Action, Adult, Adventure, Animation, Biography, Comedy, Crime, Documentary, Drama, Family, Fantasy, Film-Noir, Game-Show, History, Horror, Music, Musical, Mystery, News, Reality-TV, Romance, Sci-Fi, Short, Sport, Talk-Show, Thriller, War, Western

---

### 4.6 Table: `role`

Lookup table for cast/crew role categories.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `role_id` | STRING | NO | PK — surrogate key on `role_name` |
| `role_name` | STRING | NO | e.g. `actor`, `actress`, `director`, `writer`, `producer` |

---

### 4.7 Table: `title_genre` (also named `genre_title`)

Bridge table. One row per title × genre combination.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | FK → `title.title_id` |
| `genre_id` | STRING | YES | FK → `genre.genre_id` — NULL if genre could not be resolved |

> Titles with `\N` genres are excluded entirely (filtered in WHERE clause).

---

### 4.8 Table: `title_person_role`

Relationships between titles, people, and roles.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | FK → `title.title_id` |
| `person_id` | STRING | NO | FK → `person.person_id` |
| `role_id` | STRING | YES | FK → `role.role_id` |
| `job` | STRING | YES | Specific job description — NULL if not applicable |
| `number_of_roles` | INT | NO | Number of distinct roles this person has in the title |
| `characters` | MAP | YES | Map of ordering → character name |

> Only persons that exist in `name_basics` are included (INNER JOIN filter).

---

### 4.9 Table: `title_episode`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `series_title_id` | STRING | NO | FK → `title.title_id` (the series) |
| `episode_title_id` | STRING | NO | FK → `title.title_id` (the episode) |
| `season_number` | INT | YES | Season number — NULL if unknown |
| `episode_number` | INT | YES | Episode number within season — NULL if unknown |

---

### 4.10 Table: `title_localized`

Localised title names per region and language.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | FK → `title.title_id` |
| `title` | STRING | NO | Localised title text |
| `ordering` | INT | NO | Ordering within the title's localisations |
| `region_id` | STRING | YES | FK → `regions.region_id` — NULL if region not in seed |
| `language_id` | STRING | YES | FK → `languages.language_id` — NULL if language not in seed |
| `title_context_id` | STRING | YES | FK → `title_context.title_context_id` — NULL if no context |
| `is_original_title` | BOOLEAN | NO | True if this is the original release title |

---

### 4.11 Table: `regions` (seed)

| Column | Type | Description |
|--------|------|-------------|
| `region_id` | STRING | PK — surrogate key |
| `region_code` | STRING | ISO region code (e.g. `US`, `GB`, `NO`) |
| `region_name` | STRING | Human-readable region name (e.g. `United States`, `Norway`) |

---

### 4.12 Table: `languages` (seed)

| Column | Type | Description |
|--------|------|-------------|
| `language_id` | STRING | PK — surrogate key |
| `language_code` | STRING | ISO language code (e.g. `en`, `es`, `fr`) |
| `language_name` | STRING | Human-readable language name (e.g. `English`, `Spanish`) |

---

## 5. Gold Layer — `demo.prod_analytics`

### 5.1 Transformation Rules (Silver → Gold)

| Rule | Description |
|------|-------------|
| Denormalisation | Fact tables carry selected dimension attributes to reduce required joins for common queries |
| `decade` derived | `CONCAT(FLOOR(release_year / 10) * 10, 's')` — e.g. `1990s`, `2020s` — NULL if `release_year` is NULL |
| `rating_bucket` derived | Bucketed from `average_rating` — see §5.2 for values |
| Snapshot metadata | Not forwarded to Gold layer — analytics tables are point-in-time stable |
| `fact_title_release` join | Requires `dim_title` for ratings/type; regions and languages resolved from canonical |

---

### 5.2 Table: `dim_title`

One row per IMDB title. Primary dimension for all analytical queries.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | PK |
| `primary_title` | STRING | NO | Main display title |
| `original_title` | STRING | YES | Title in original production language |
| `title_type_name` | STRING | NO | e.g. `movie`, `tvSeries`, `tvEpisode` |
| `release_year` | INT | YES | First release year |
| `duration_minutes` | INT | YES | Runtime in minutes |
| `is_adult` | BOOLEAN | NO | Adult content flag |
| `average_rating` | DECIMAL | YES | Average IMDB rating (1.0–10.0); NULL if unrated |
| `number_of_votes` | INT | YES | Vote count; NULL if unrated |
| `decade` | STRING | YES | Derived from `release_year`: `1990s`, `2000s`, etc. |
| `rating_bucket` | STRING | NO | Derived from `average_rating`: `no_rating`, `0-5`, `5-7`, `7-8`, `8-9`, `9+` |

**Tests:** `title_id` not_null + unique; `primary_title` not_null; `title_type_name` not_null

---

### 5.3 Table: `dim_person`

One row per person. Simple pass-through from canonical `person`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `person_id` | STRING | NO | PK |
| `name` | STRING | NO | Full name |
| `birth_year` | INT | YES | Year of birth |
| `death_year` | INT | YES | Year of death — NULL if living or unknown |

---

### 5.4 Table: `fact_title_genre_flat`

One row per title × genre. Designed for genre-based filtering without array operations.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | FK → `dim_title` |
| `genre_name` | STRING | NO | Genre name |
| `title_type_name` | STRING | NO | Denormalised from `dim_title` |
| `release_year` | INT | YES | Denormalised from `dim_title` |
| `is_adult` | BOOLEAN | YES | Denormalised from `dim_title` |
| `average_rating` | DECIMAL | YES | Denormalised from `dim_title` |
| `number_of_votes` | INT | YES | Denormalised from `dim_title` |

**Composite key test:** `(title_id, genre_name, title_type_name)` must be unique
**Join to get:** `primary_title`, `decade`, `rating_bucket`, `duration_minutes` → JOIN `dim_title`

---

### 5.5 Table: `fact_title_cast_crew`

One row per title × person × role.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | FK → `dim_title` |
| `person_id` | STRING | NO | FK → `dim_person` |
| `role_name` | STRING | NO | e.g. `actor`, `actress`, `director`, `writer` |
| `release_year` | INT | YES | Denormalised from canonical `title` |
| `average_rating` | DECIMAL | YES | Denormalised from canonical `title` |
| `number_of_votes` | INT | YES | Denormalised from canonical `title` |

**Join to get:** `primary_title`, `title_type_name` → JOIN `dim_title`; `name` → JOIN `dim_person`

---

### 5.6 Table: `fact_title_release`

One row per localised release (title × region × language).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title_id` | STRING | NO | FK → `dim_title` |
| `localized_title` | STRING | NO | Title text in the target region/language |
| `is_original_title` | BOOLEAN | NO | True if this is the original release title |
| `region_name` | STRING | NO | Human-readable region (e.g. `Norway`) |
| `language_name` | STRING | NO | Human-readable language (e.g. `Norwegian`) |

**Join to get:** ratings, type, decade → JOIN `dim_title`

---

### 5.7 Table: `fact_episode`

Fully denormalised. One row per episode. No joins needed for basic episode queries.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `series_title_id` | STRING | NO | FK → `dim_title` (series) |
| `episode_title_id` | STRING | NO | FK → `dim_title` (episode) |
| `season_number` | INT | YES | Season number |
| `episode_number` | INT | YES | Episode number within season |
| `episode_primary_title` | STRING | NO | Episode title — denormalised |
| `episode_release_year` | INT | YES | Episode release year — denormalised |
| `episode_average_rating` | DECIMAL | YES | Episode rating — denormalised |
| `episode_number_of_votes` | INT | YES | Episode vote count — denormalised |
| `series_primary_title` | STRING | NO | Series title — denormalised |
| `series_average_rating` | DECIMAL | YES | Series rating — denormalised |

---

## 6. Data Quality Rules

### 6.1 Approach

Data quality is enforced declaratively via dbt contract enforcement and column-level tests. All canonical and analytics models have `contract.enforced: true`, which means dbt will fail if the actual schema does not match the declared column types.

### 6.2 Test Inventory

| Table | Column | Test |
|-------|--------|------|
| `title` | `title_id` | not_null, unique |
| `title` | `title_type_id` | not_null, relationships → `title_type` |
| `title` | `primary_title` | not_null |
| `title` | `original_title` | not_null |
| `title` | `is_adult` | not_null |
| `title` | `average_rating` | not_null |
| `title` | `number_of_votes` | not_null |
| `person` | `person_id` | not_null, unique |
| `genre` | `genre_id` | not_null, unique |
| `dim_title` | `title_id` | not_null, unique |
| `dim_title` | `primary_title` | not_null |
| `dim_title` | `title_type_name` | not_null |
| `dim_person` | `person_id` | not_null, unique |
| `fact_title_genre_flat` | `title_id` | not_null, relationships → `dim_title` |
| `fact_title_genre_flat` | `genre_name` | not_null |
| `fact_title_genre_flat` | `title_type_name` | not_null |
| `fact_title_genre_flat` | (composite) | unique: `(title_id, genre_name, title_type_name)` |

---

## 7. Known Data Limitations

| Limitation | Affected Tables | Detail |
|------------|----------------|--------|
| Unrated titles | `title`, `dim_title`, all facts | Titles not in `title_ratings` get `average_rating = 0.00` and `number_of_votes = 0` at canonical layer. At analytics layer these appear as NULL. |
| Unknown release year | `title`, `dim_title` | `release_year` is NULL for many titles. `decade` is also NULL in those cases. |
| Unknown runtime | `title`, `dim_title` | `duration_minutes` is NULL for most TV series, episodes, and older titles. |
| No birth/death place | `person`, `dim_person` | IMDB public data does not include nationality or country of birth. Birthplace queries cannot be answered with V1 data. |
| No awards data | All layers | Oscars, BAFTAs and other awards are not part of the IMDB public dataset. |
| No box office data | All layers | Revenue figures are not part of the IMDB public dataset. |
| Region/language coverage | `title_localized` | Only regions and languages present in the seeds (`regions.csv`, `languages.csv`) are resolved. Others remain with NULL region_id or language_id. |
| `title_crew` not in canonical | — | Director/writer data from `title_crew` is available in Bronze but not exposed in canonical — those relationships are covered by `title_principals`. |
| Persons without `name_basics` entry | `title_person_role` | Persons in `title_principals` who have no matching record in `name_basics` are silently excluded (INNER JOIN filter). |
| Genres limited to 3 per title | `title_genre`, `fact_title_genre_flat` | IMDB caps genres at 3 per title. This is a source limitation, not a pipeline issue. |
