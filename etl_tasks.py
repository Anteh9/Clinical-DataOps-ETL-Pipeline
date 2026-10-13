import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd
import pandera.pandas as pa
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from prefect import task

from schemas import (
    lab_results_schema,
    legacy_patients_schema,
    sensor_vitals_schema,
)


PROJECT_DIR = Path(__file__).resolve().parent
SOURCE_DATA_DIR = PROJECT_DIR / "source_data"


@task(retries=2, retry_delay_seconds=5, name="Extract Clinical Sources")
def extract_clinical_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print("Extracting data from 3 clinical sources...")

    csv_path = SOURCE_DATA_DIR / "lab_results.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing clinical source file: {csv_path}")
    df_labs = pd.read_csv(csv_path)

    sqlite_path = SOURCE_DATA_DIR / "legacy_patients.db"
    if not sqlite_path.exists():
        raise FileNotFoundError(f"Missing clinical database: {sqlite_path}")
    with closing(sqlite3.connect(sqlite_path)) as connection:
        df_patients = pd.read_sql_query(
            "SELECT * FROM legacy_patients", connection
        )

    json_path = SOURCE_DATA_DIR / "sensor_vitals.json"
    if not json_path.exists():
        raise FileNotFoundError(f"Missing clinical API stream file: {json_path}")
    with json_path.open(encoding="utf-8") as source_file:
        df_vitals = pd.DataFrame(json.load(source_file))

    print("Extraction complete across all 3 clinical sources.")
    return df_labs, df_patients, df_vitals


@task(name="Validate and Quarantine Clinical Data")
def validate_and_quarantine_task(
    df_labs: pd.DataFrame,
    df_patients: pd.DataFrame,
    df_vitals: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print("Running Pandera clinical validation checks with schema coercion...")

    def validate_stream(
        df: pd.DataFrame, schema: pa.DataFrameSchema, source_name: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        clean_records = []
        quarantined_records = []

        for _, row in df.iterrows():
            single_row_df = pd.DataFrame([row])
            try:
                validated_df = schema.validate(single_row_df, lazy=False)
                clean_records.append(validated_df.iloc[0].to_dict())
            except pa.errors.SchemaError as exc:
                error_msg = str(exc).strip()
                if not error_msg or error_msg.lower() == "none":
                    error_msg = "Schema validation check failed."

                row_dict = row.to_dict()
                row_dict["quarantine_reason"] = error_msg
                row_dict["source_origin"] = source_name
                quarantined_records.append(row_dict)

        df_clean = (
            pd.DataFrame(clean_records)
            if clean_records
            else pd.DataFrame(columns=df.columns)
        )
        quarantine_columns = list(df.columns) + [
            "quarantine_reason",
            "source_origin",
        ]
        df_quarantine = (
            pd.DataFrame(quarantined_records, columns=quarantine_columns)
            if quarantined_records
            else pd.DataFrame(columns=quarantine_columns)
        )

        return df_clean, df_quarantine

    clean_labs, q_labs = validate_stream(
        df_labs, lab_results_schema, "lab_results_csv"
    )
    clean_patients, q_patients = validate_stream(
        df_patients, legacy_patients_schema, "legacy_patients_db"
    )
    clean_vitals, q_vitals = validate_stream(
        df_vitals, sensor_vitals_schema, "sensor_vitals_json"
    )

    all_quarantines = pd.concat(
        [q_labs, q_patients, q_vitals], ignore_index=True
    )

    print("Validation Summary:")
    print(f"   - Clean Labs: {len(clean_labs)} | Quarantined: {len(q_labs)}")
    print(
        f"   - Clean Patients: {len(clean_patients)} | "
        f"Quarantined: {len(q_patients)}"
    )
    print(f"   - Clean Vitals: {len(clean_vitals)} | Quarantined: {len(q_vitals)}")

    return clean_labs, clean_patients, clean_vitals, all_quarantines


@task(name="Load Data to PostgreSQL")
def load_to_postgres(
    clean_labs: pd.DataFrame,
    clean_patients: pd.DataFrame,
    clean_vitals: pd.DataFrame,
    all_quarantines: pd.DataFrame,
) -> None:
    print("Loading clean records and audit logs into PostgreSQL...")
    load_dotenv(PROJECT_DIR / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        database_user = os.environ.get("DATABASE_USER")
        database_password = os.environ.get("DATABASE_PASSWORD")
        database_host = os.environ.get("DATABASE_HOST")
        database_name = os.environ.get("DATABASE_NAME")
        missing_settings = [
            name
            for name, value in (
                ("DATABASE_USER", database_user),
                ("DATABASE_PASSWORD", database_password),
                ("DATABASE_HOST", database_host),
                ("DATABASE_NAME", database_name),
            )
            if not value
        ]
        if missing_settings:
            raise RuntimeError(
                "Set DATABASE_URL or provide all database settings: "
                + ", ".join(missing_settings)
                + "."
            )

        database_url = URL.create(
            "postgresql+psycopg",
            username=database_user,
            password=database_password,
            host=database_host,
            port=int(os.environ.get("DATABASE_PORT", "5432")),
            database=database_name,
        )

    engine = create_engine(database_url)
    try:
        clean_labs.to_sql(
            "clinical_lab_results", engine, if_exists="replace", index=False
        )
        clean_patients.to_sql(
            "clinical_patient_registry", engine, if_exists="replace", index=False
        )
        clean_vitals.to_sql(
            "clinical_sensor_vitals", engine, if_exists="replace", index=False
        )
        all_quarantines.to_sql(
            "clinical_quarantined_records", engine, if_exists="replace", index=False
        )

        print("Clinical ETL Pipeline completed successfully via Prefect!")
    except Exception as exc:
        print(f"Database loading failed: {exc}")
        raise
    finally:
        engine.dispose()
