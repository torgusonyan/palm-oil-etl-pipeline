# Palm Oil Supply Chain: ETL Pipeline with Airflow Orchestration

An automated **ETL (Extract, Transform, Load)** pipeline for Indonesian palm oil
supply-chain data. Raw data is extracted and cleaned with **PySpark**, validated
with **Great Expectations**, orchestrated and scheduled with **Apache Airflow**,
and loaded into a **MongoDB Atlas** (NoSQL) database. The whole stack runs in a
single **Docker** container (JupyterLab + Airflow + Spark).

**Author:** Ryan Farid Pratama <br>
**Tech Stack:** Python · PySpark · Pandas · Great Expectations · Apache Airflow · MongoDB · Docker

> **Scope:** This project covers the data engineering pipeline (extraction,
> transformation, validation, orchestration, and loading). The EDA in the
> notebook explains *why* the cleaning decisions are made; downstream analytical
> querying is out of scope.

---

## Pipeline Architecture

```
Trase (CSV)          PySpark              Great Expectations        Apache Airflow            MongoDB Atlas
(palm oil supply   (clean + reshape)     (validate quality)        (schedule + run)          (NoSQL store)
 chain data)
     │                    │                      │                       │                         │
     ▼                    ▼                      ▼                       ▼                         ▼
  Extract  ─────────▶  Transform  ─────────▶  Validate  ───────────▶  Orchestrate  ──────────▶   Load
```

**A. Pre-Automation (`pra_automation.ipynb`)**
1. Environment setup (Spark session).
2. Extract the dataset with PySpark, sampled to the **Kalimantan** region.
3. Exploratory Data Analysis (EDA) to define the cleaning logic.
4. Transform: whitelist columns, drop ambiguous `UNKNOWN` rows, cast numeric types.
5. Validate the cleaned data with Great Expectations.

**B. Automation (Airflow DAG)**
- `extract.py`, `transform.py`, `load.py` are orchestrated by `automation_DAG.py`
  as `extract >> transform >> load`, scheduled to run on Airflow.

---

## Key Decisions & Insights

- **Regional sampling (Kalimantan):** the full national dataset is too large to
  work with and commit to GitHub, so the pipeline samples Kalimantan, reducing the
  file by ~87%.
- **No missing values:** the per-column NULL check returns zero across the board,
  so no imputation is needed — cleaning focuses on relevance, not gap-filling.
- **Dropping ambiguous rows:** 18,042 rows where *both* the exporter and importer
  group are `UNKNOWN` are removed, because they cannot be attributed to a specific
  company and would add noise to supply-chain analysis (588,476 → 570,434 rows).
- **Storage-aware modeling:** only data-dictionary columns are kept and the final
  set is ~94% smaller, which keeps the loaded collection within the MongoDB Atlas
  free-tier limit.

---

## Data Validation (Great Expectations)

Seven expectations are defined and all pass against the cleaned data:

| # | Expectation | Column | Rule |
| :--- | :--- | :--- | :--- |
| 1 | Values unique | `transaction_route_id` | No duplicate transaction IDs |
| 2 | Values between | `volume` | Between 0 and 5,000,000 |
| 3 | Values in set | `product_type` | `CRUDE PALM OIL` or `REFINED PALM OIL` |
| 4 | Type in list | `total_emission_exposure` | float / float64 / integer |
| 5 | Column exists | `volume` | Column present |
| 6 | Most common in set | `province_of_production` | `KALIMANTAN TENGAH` |
| 7 | Value length between | `province_of_production_trase_id` | Length 4–10 |

Results are rendered as browsable Data Docs via a Great Expectations Checkpoint.

---

## Repository Structure

```
.
├── pra_automation.ipynb          # Pre-automation notebook (EDA → transform → validate)
├── workspace/
│   └── dags/
│       ├── automation_DAG.py     # Airflow DAG: extract >> transform >> load
│       ├── extract.py            # Read raw CSV into a Spark DataFrame
│       ├── transform.py          # Clean & reshape the data
│       └── load.py               # Stream the result into MongoDB Atlas
├── gx/                           # Great Expectations project (suite, checkpoint, validations)
├── docker-compose.yml            # JupyterLab + Airflow + Spark + Airflow metadata DB
├── Dockerfile                    # Python 3.13 + Java + Airflow image
├── entrypoint.sh                 # Container startup (Jupyter and/or Airflow)
├── spark-defaults.conf           # Spark configuration
├── requirements.txt              # Python dependencies
├── atlas-credentials.env.example # Template for MongoDB credentials (copy to .env)
├── postgresql-42.6.0.jar         # PostgreSQL JDBC driver for Spark
└── DAG_graph.png                 # Rendered Airflow DAG
```

---

## Getting Started

### Prerequisites
- Docker and Docker Compose
- A MongoDB Atlas cluster (free tier works)

### Configure credentials
Copy the template and fill in your real MongoDB connection string. The real
`.env` is git-ignored and must never be committed.

```bash
cp atlas-credentials.env.example .env
```
```
MONGODB_USERNAME=your_username
MONGODB_PASSWORD=your_password
MONGODB_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
```

### Run the stack
```bash
docker compose up --build
```
- JupyterLab → http://localhost:8888
- Airflow → http://localhost:8080
- Spark UI → http://localhost:4040

### Run the pipeline
1. **Explore / validate:** open `pra_automation.ipynb` in JupyterLab and run it
   top to bottom (EDA, transformation, and Great Expectations validation).
2. **Automate:** in the Airflow UI, enable and trigger the `automation_DAG`. It
   runs `extract >> transform >> load` and writes the cleaned data to MongoDB.

---

## Dataset

Indonesia Palm Oil — Supply Chain Data, from
[Trase](https://trase.earth/open-data/datasets/supply-chains-indonesia-palm-oil).

> Chandra, A., Benedict, J., Kamaluddin, A., Putri, S. M., Suavet, C., Gollnow, F.,
> Biddle, H., Mueller, C., Ribeiro, V., & Heilmayr, R. (2026). *Indonesia palm oil
> supply chain: Data and Methods (2018–2024)* (Version 1.2.5) [Dataset]. Trase.
> https://doi.org/10.48650/0717-AJ12

---

## Notes

- **Credentials** are read only from a git-ignored `.env` file; the public repo
  contains only `atlas-credentials.env.example` with placeholders.
- **Raw and intermediate data** (`*.csv`, `*.parquet`, `data/`) are git-ignored and
  regenerated by running the pipeline.
- **Memory-safe loading:** `load.py` streams partitions with `foreachPartition`
  rather than `toPandas()`, which avoids Java `OutOfMemoryError` in Docker.
