# Clinical-DataOps-ETL-Pipeline

**Multi-source clinical data ingestion, validation, quarantine, and orchestration**

## Executive summary

Clinical-DataOps-ETL-Pipeline is a containerized ETL demonstration that brings
together three heterogeneous sample sources: outpatient lab measurements in
CSV, a legacy patient registry in SQLite, and wearable readings in JSON. A
Prefect flow extracts the records, validates them against declarative Pandera
data contracts with type coercion, and routes accepted and rejected rows to
separate PostgreSQL tables.

The pipeline demonstrates several useful data-engineering patterns: explicit
source boundaries, schema-based quality checks, row-level quarantine for
expected validation failures, observable orchestration, and repeatable local
deployment with Docker Compose. It is an educational portfolio project using
public and simulated sample data—not a medical system, a source of clinical
advice, or a production-certified healthcare platform.

## Key features

- **Heterogeneous ingestion:** Reads CSV, SQLite, and JSON with their respective
  Python data tools, while resolving source paths relative to the project.
- **Data contracts and coercion:** Uses Pandera schemas to coerce compatible
  values and enforce required columns, types, nullability, and domain checks.
- **Row-level quarantine:** Captures rows that fail Pandera schema validation,
  including the validation message and source name, rather than dropping them.
- **Prefect orchestration:** Runs extraction, validation, and loading as named
  Prefect tasks inside a named flow. The extraction task retries twice after a
  failure; task and flow states are visible through Prefect.
- **PostgreSQL outputs:** Writes accepted records to three source-specific
  tables and rejected records to a shared quarantine table.
- **Power BI reporting:** Includes a report with pipeline KPIs, quarantine
  breakdowns, source comparisons, and patient health metric visuals.
- **Containerized stack:** Compose starts PostgreSQL and Prefect, waits for
  health checks, generates sample inputs, and runs the ETL in dependency order.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        CSV["Lab results<br/>CSV"]
        SQLite["Patient registry<br/>SQLite"]
        JSON["Wearable vitals<br/>JSON"]
    end

    subgraph Prefect["Prefect: Clinical ETL Pipeline Flow"]
        Extract["Extract Clinical Sources"]
        Validate["Pandera validation<br/>and type coercion"]
        Route{"Row passes<br/>schema checks?"}
        Load["Load Data to PostgreSQL"]
    end

    subgraph PostgreSQL
        Labs["clinical_lab_results"]
        Patients["clinical_patient_registry"]
        Vitals["clinical_sensor_vitals"]
        Quarantine["clinical_quarantined_records"]
    end

    CSV --> Extract
    SQLite --> Extract
    JSON --> Extract
    Extract --> Validate
    Validate --> Route
    Route -->|"Accepted rows"| Load
    Route -->|"Schema failures + reason"| Load
    Load --> Labs
    Load --> Patients
    Load --> Vitals
    Load --> Quarantine
```

The flow is sequential: extraction produces three DataFrames, validation
produces three clean DataFrames plus a combined quarantine DataFrame, and the
load task writes those outputs to PostgreSQL. Pandera validation failures are
handled per row. Other failures—such as missing input files or an unavailable
database—are raised and fail the corresponding task/flow.

## Technical stack

| Area | Technology | Use in this project |
| --- | --- | --- |
| Language and data handling | Python, pandas | ETL implementation and tabular source processing |
| Validation contracts | Pandera | Type coercion and per-source schema/domain validation |
| Orchestration | Prefect | Named tasks and flow, task state tracking, extraction retries |
| Relational storage and SQL access | PostgreSQL, SQLAlchemy, pandas `to_sql` | Persist clean and quarantined records |
| Reporting | Power BI | Dashboard visuals for record processing, data quality, and health metrics |
| Source formats | CSV, SQLite, JSON | Three distinct input systems |
| Containerization | Docker, Docker Compose | Reproducible application, database, and Prefect services |

## Power BI visualization

The project includes an editable Power BI report and a screenshot of its
dashboard:

- [Power BI report](assets/Visual%20representation.pbix)
- [Dashboard screenshot](assets/PowerBI%20visualisation.PNG)

The screenshot shows KPI cards for processed records, quarantined records, and
compliance, alongside a quarantine-error breakdown, a heart-rate visual, a
BMI-versus-blood-pressure scatter plot, and a donut chart comparing anomalies
by source. In this captured report state, the cards show 768 processed records
and 128 quarantined records; the source chart attributes those quarantines to
the patient registry (64), lab results (47), and wearable vitals (17).

These figures describe the screenshot, not guaranteed results for future ETL
runs. Also, the heart-rate visual currently shows a single summed value rather
than a time series, and the displayed 83.33% compliance rate does not directly
follow from the two record totals shown. Verify the measure definitions and
chart fields in Power BI before interpreting those as a trend or a quality
rate.

## Repository layout

```text
Clinical-DataOps-ETL-Pipeline/
├── .dockerignore             # Excludes secrets, virtual environments, and caches from builds
├── .env.example              # Example Docker Compose settings (no real credentials)
├── .gitignore                # Excludes local secrets and generated Python files
├── Dockerfile                # Python ETL image
├── docker-compose.yml        # PostgreSQL, Prefect, seeding, and ETL services
├── etl_tasks.py              # Extraction, validation/quarantine, and PostgreSQL tasks
├── main_flow.py              # Prefect flow entry point
├── README.md
├── requirements.txt
├── schemas.py                # Pandera contracts for each source
├── seed_data.py              # Optional generator for the three sample inputs
├── assets/
│   ├── PowerBI visualisation.PNG  # Dashboard screenshot
│   └── Visual representation.pbix # Editable Power BI report
└── source_data/
    ├── lab_results.csv
    ├── legacy_patients.db
    └── sensor_vitals.json
```

## Run locally

### Prerequisites

- Python 3.12 (the Docker image uses Python 3.12)
- A reachable PostgreSQL server and an existing target database
- Internet access only if you intend to regenerate the sample files

### Install and configure

Clone the repository and change into its directory:

```powershell
git clone <repository-url> Clinical-DataOps-ETL-Pipeline
cd Clinical-DataOps-ETL-Pipeline
```

Create and activate a virtual environment, then install dependencies:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Create a `.env` file in the project root with a SQLAlchemy PostgreSQL URL:

```dotenv
DATABASE_URL=postgresql+psycopg://USER:PASSWORD@localhost:5432/analytics_db
```

Replace the example username, password, host, and database with your local
PostgreSQL settings. If credentials contain URL-special characters, percent
encode those characters in the URL. Keep `.env` private; it is excluded from
the Docker build context and Git.

### Generate data and run

The repository includes sample files in `source_data`, so regeneration is
optional. To download the public Pima dataset and overwrite those three files:

```powershell
python seed_data.py
```

Then run the Prefect flow:

```powershell
python main_flow.py
```

The loader replaces the output tables on each successful write; this is a
repeatable latest-snapshot demo, not an append-only history. The output tables
are described below.

## Run with Docker Compose

Docker Compose is the recommended way to run the full stack without installing
PostgreSQL locally.

1. Install and start Docker Desktop.
2. If you do not already have a `.env`, create it from the example:

   ```powershell
   Copy-Item .env.example .env
   ```

   If `.env` already exists, add or update the Compose settings below manually;
   do not overwrite other entries. Set a strong `POSTGRES_PASSWORD`:

   ```dotenv
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=replace-with-a-strong-password
   POSTGRES_DB=analytics_db
   POSTGRES_PORT=5432
   ```

3. From the project root, build and start the stack:

   ```powershell
   docker compose up --build
   ```

Compose starts the PostgreSQL database and Prefect server, waits for their
health checks, runs the `seed_data` one-shot service, then starts the ETL. The
seed service downloads the public sample dataset and writes all three inputs
into a shared named volume. The ETL connects to the Compose hostname
`postgres_db` (not `localhost`) and sends Prefect events to `prefect`.

- Prefect UI: <http://localhost:4200>
- PostgreSQL from the host: `localhost:5432` by default. Change `POSTGRES_PORT`
  if port 5432 is already occupied.

The ETL container exits after the flow completes; PostgreSQL and Prefect stay
running. Stop the stack with `Ctrl+C`. To stop and remove containers while
preserving database and source-data volumes:

```powershell
docker compose down
```

To also delete those persistent volumes and their contents:

```powershell
docker compose down --volumes
```

The latter permanently deletes the Compose-managed PostgreSQL data, Prefect
state, and generated source files. The first Compose run needs internet access
to download both container images and the sample dataset.

## Data contracts and quality handling

The contracts in `schemas.py` are applied separately to the three inputs:

| Source | Contract highlights |
| --- | --- |
| Lab CSV | Positive patient ID and glucose; non-negative insulin; recorded timestamp |
| Patient SQLite table | Positive patient ID; age from 1 to 120; positive blood pressure; BMI from 10 to 70; binary outcome |
| Vitals JSON | Positive patient ID; heart rate from 30 to 220; oxygen saturation from 70 to 100; recorded timestamp |

Each schema enables Pandera coercion, so values that can be converted to the
declared types are converted before checks run. Rows that fail a Pandera
`SchemaError` are kept out of the clean DataFrame and added to
`clinical_quarantined_records` with:

- the original source columns and values;
- `quarantine_reason`, containing the Pandera validation error; and
- `source_origin`, identifying the CSV, SQLite, or JSON source.

For example, non-positive patient IDs, zero glucose, or out-of-range vital
measurements fail the relevant checks and are quarantined. Compatible values
may be coerced; values that cannot be coerced fail validation. A schema
validation failure for one row does not stop validation of the remaining rows.
Missing source files, database connectivity failures, and other unexpected
errors are not silently quarantined: they are raised and cause the task/flow to
fail.

The current seed script adapts the public Pima Indians Diabetes data for this
pipeline by naming its `Pregnancies` field `patient_id` and adding simulated
vitals and timestamps. This is demonstration data, not a real patient
identifier mapping. Its zero and repeated pregnancy counts also intentionally
produce records that violate this project's positive-ID rule; additional
invalid values are inserted to exercise quarantine behavior.

## PostgreSQL output tables

| Table | Contents |
| --- | --- |
| `clinical_lab_results` | Validated lab CSV rows |
| `clinical_patient_registry` | Validated legacy SQLite rows |
| `clinical_sensor_vitals` | Validated wearable JSON rows |
| `clinical_quarantined_records` | Rejected rows, validation reasons, and source labels |

Each table is written with pandas `to_sql(if_exists="replace")`. Consequently,
each write replaces that table rather than appending rows. The four writes are
performed sequentially; they are **not currently wrapped in a single
all-tables transaction**, so a database failure during loading can leave some
tables updated and others unchanged. This behavior is explicit so the demo is
not mistaken for an atomic production load.

## Operational notes and limitations

- The Compose seed step downloads the upstream public dataset each time its
  one-shot container is run; it overwrites the files in the shared source-data
  volume. For repeatable runs, use a fixed, versioned input dataset instead.
- The extraction task is configured for two retries with a five-second delay.
  Validation and loading do not currently have retry policies.
- The sample data, identifiers, and simulated vitals are for software
  demonstration only. Do not use them for patient care or infer clinical
  outcomes from them.
- This repository demonstrates foundational production patterns; it does not
  claim regulatory compliance, clinical-grade security, atomic multi-table
  loading, or production operational support.
