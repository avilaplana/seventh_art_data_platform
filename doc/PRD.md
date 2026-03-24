# Product Requirements Document
## Seventh Art Analytics

**Version:** 1.0
**Date:** 2026-03-24
**Status:** Draft

---

## 1. Problem Statement

Cinema and TV enthusiasts who want to answer specific questions about films, series, and people in the industry are forced to navigate IMDB's web application — a tool designed for browsing, not for answering analytical questions. The experience is slow, the data is fragmented across multiple pages, and there is no way to ask compound or comparative questions.

**The problem:** There is no personal, conversational tool that lets a cinema lover ask a natural language question about the film industry and get back structured data and visualisations as an answer.

---

## 2. Vision

A personal analytics platform for the film industry where the user types a question in plain language — from a browser or mobile — and receives data, tables, and charts as the answer. Think of it as a private analyst who knows everything about cinema and responds instantly.

---

## 3. Personas

### Primary Persona — The Cinema Lover (sole user, v1)
- Watches films and series regularly across multiple genres and countries
- Has specific, factual questions that IMDB does not answer easily
- Is not a data analyst — expects to type in natural language, not SQL
- Accesses the tool from a browser on desktop or mobile
- Values accuracy and speed of answer over aesthetics

### Secondary Persona — The Data Engineering Learner (the builder)
- Uses this project as a practice environment for data engineering concepts
- Wants to exercise: medallion architecture, dbt, Spark, Iceberg, Airflow, AI/LLM integration
- Measures success both by product quality and by the engineering concepts practised

---

## 4. Goals & Success Metrics

| Goal | Metric |
|------|--------|
| User can ask a natural language question and get a data answer | 80%+ of benchmark questions answered correctly by the best evaluated model |
| Local LLM models are evaluated and compared | Benchmark table published with accuracy scores per model across a fixed question set |
| Data is fresh and consistent | IMDB snapshot ingested and processed daily |
| Answer is visual, not just a raw table | Charts/graphs rendered for aggregation questions |
| Platform is extensible | New data sources can be added without re-architecting the pipeline |

---

## 5. Scope

### V1 — In Scope
- Ingest and process the **IMDB public dataset** (daily snapshot)
- Answer questions about:
  - Titles: films, TV series, episodes — metadata, ratings, genres, country of origin
  - People: cast, crew, directors, writers — names, roles, biographical data
  - Relationships: who acted in what, who directed what, what genre a film belongs to
- Natural language interface (prompt in browser)
- Data presented as tables and basic charts
- **Evaluation of multiple local LLM models** (via Ollama) for NL-to-SQL accuracy, with results published as a benchmark table
- Single user

### V1 — Out of Scope (flagged for future iterations)
- **Awards data** (e.g. Oscars, BAFTAs) — requires a dedicated source; not in IMDB public data
- **Box office / revenue data** — requires The Numbers, Box Office Mojo, or similar; not in IMDB public data
- **Streaming availability** (Netflix, HBO, etc.)
- **Review sentiment** (Rotten Tomatoes, Metacritic)
- **Multi-user support / authentication**
- **Real-time data** (IMDB public data is a daily snapshot)

### Future Iterations
| Capability | Likely Source |
|------------|--------------|
| Awards (Oscars, BAFTAs, etc.) | Academy Awards database, Wikidata |
| Box office revenue | The Numbers (nashNumbers API), Box Office Mojo |
| Streaming availability | JustWatch API, TMDB |
| Critic reviews | OMDB API, Rotten Tomatoes |

---

## 6. User Stories

### Core — Answered with IMDB data (V1)
| ID | Story |
|----|-------|
| US-01 | As a user, I want to ask "Who were the main actresses in the Norwegian film Sentimental Value?" and get a list of names with their roles |
| US-02 | As a user, I want to ask "How many Spanish films were the most voted in 1986?" and get a ranked list with vote counts |
| US-03 | As a user, I want to ask "Where is Steven Spielberg from?" and get his biographical data |
| US-04 | As a user, I want to ask questions about genres, decades, ratings, and get comparative charts as answers |
| US-05 | As a user, I want to access the tool from my mobile browser |
| US-06 | As a builder, I want to evaluate multiple local LLM models (via Ollama) on a fixed benchmark question set and produce a comparison table showing accuracy per model |

### Blocked — Require future data sources
| ID | Story | Blocker |
|----|-------|---------|
| US-07 | As a user, I want to ask "How many Oscars did Al Pacino win?" | Needs awards data source |
| US-08 | As a user, I want to ask "How much revenue did The Secret Agent generate?" | Needs box office data source |

---

## 7. Functional Requirements

### Data Layer
- FR-01: Ingest IMDB public dataset daily (titles, names, ratings, crew, principals, episodes)
- FR-02: Transform raw data through a medallion architecture (Bronze → Silver/Canonical → Gold/Analytics)
- FR-03: The analytics layer must expose a clean, query-optimised schema suitable for SQL generation
- FR-04: Data must be consistent and fully refreshed daily

### AI / Query Layer
- FR-05: Accept a natural language question as input
- FR-06: Translate the question into SQL against the analytics schema
- FR-07: Execute the SQL and return structured results
- FR-08: Return an error message when a question cannot be answered with available data (e.g. awards, revenue)
- FR-09: Run a fixed benchmark question set against multiple local LLM models (via Ollama) and record accuracy results per model
- FR-10: Produce a benchmark results table comparing models on: accuracy, and model name/version

### Presentation Layer
- FR-10: Display results as a table and/or chart depending on question type
- FR-11: Interface must be usable on mobile browser
- FR-12: Response time under 10 seconds for typical questions

---

## 8. Non-Functional Requirements

| Category | Requirement |
|----------|-------------|
| Freshness | Data updated daily, aligned with IMDB snapshot release |
| Accuracy | SQL generated must return correct results for 80%+ of benchmark questions |
| Availability | Best-effort for a personal project; no formal SLA |
| Extensibility | Pipeline designed so new data sources can be added as new medallion branches |
| Portability | Runs locally via Docker Compose |

---

## 9. Constraints

- **Data licence:** IMDB public data is free for personal, non-commercial use only
- **Infrastructure:** Runs locally on a single machine (Docker Compose); no cloud deployment in v1
- **LLM:** Natural language to SQL is powered by a local LLM running via Ollama — no external API calls in v1
- **Budget:** Zero — all tools must be open source or have a free tier
- **Team:** Single engineer (the builder)

---

## 10. Open Questions

| # | Question | Owner |
|---|----------|-------|
| OQ-01 | Which awards data source is most complete and accessible for future v2? | Builder to investigate |
| OQ-02 | Which box office data source has a usable free tier? | Builder to investigate |
| OQ-03 | Should the frontend be a custom web app or a wrapper around an existing BI tool? | Builder to decide |
| OQ-04 | Which local LLM models should be included in the benchmark evaluation? (e.g. Llama 3, Mistral, DeepSeek, Phi) | Builder to decide |
