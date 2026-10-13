import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
SOURCE_DATA_DIR = PROJECT_DIR / "source_data"
DATA_URL = (
    "https://raw.githubusercontent.com/jbrownlee/Datasets/master/"
    "pima-indians-diabetes.data.csv"
)


def generate_seed_data() -> None:
    SOURCE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("Downloading public health/clinical sample dataset...")

    columns = [
        "patient_id",
        "glucose",
        "blood_pressure",
        "skin_thickness",
        "insulin",
        "bmi",
        "pedigree_function",
        "age",
        "outcome",
    ]
    try:
        df_raw = pd.read_csv(DATA_URL, names=columns)
    except Exception as exc:
        raise RuntimeError(f"Failed to download clinical dataset: {exc}") from exc

    # Add simulated vitals and timestamps to the public dataset.
    df_raw["heart_rate"] = 75 + (df_raw["age"] % 25)
    df_raw["spo2_level"] = 98 - (df_raw["glucose"] % 10)
    df_raw["recorded_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # Keep invalid examples so the validation task has records to quarantine.
    df_raw.loc[5:10, "heart_rate"] = -99
    df_raw.loc[15:18, "glucose"] = 0

    print(
        "Successfully downloaded and prepared clinical dataset containing "
        f"{len(df_raw)} records."
    )

    # Split into lab export (40%), patient registry (40%), and vitals (20%).
    first_split = int(len(df_raw) * 0.4)
    second_split = int(len(df_raw) * 0.8)

    df_labs = df_raw.iloc[:first_split][
        ["patient_id", "glucose", "insulin", "recorded_at"]
    ]
    df_labs.to_csv(SOURCE_DATA_DIR / "lab_results.csv", index=False)

    df_patients = df_raw.iloc[first_split:second_split][
        ["patient_id", "age", "blood_pressure", "bmi", "outcome"]
    ]
    sqlite_path = SOURCE_DATA_DIR / "legacy_patients.db"
    with closing(sqlite3.connect(sqlite_path)) as connection:
        with connection:
            df_patients.to_sql(
                "legacy_patients", connection, index=False, if_exists="replace"
            )

    df_vitals = df_raw.iloc[second_split:][
        ["patient_id", "heart_rate", "spo2_level", "recorded_at"]
    ]
    with (SOURCE_DATA_DIR / "sensor_vitals.json").open(
        "w", encoding="utf-8"
    ) as output_file:
        json.dump(df_vitals.to_dict(orient="records"), output_file, indent=4)

    print(f"Clinical data partitioned under '{SOURCE_DATA_DIR}':")
    print(f"   - {SOURCE_DATA_DIR / 'lab_results.csv'}")
    print(f"   - {sqlite_path}")
    print(f"   - {SOURCE_DATA_DIR / 'sensor_vitals.json'}")


if __name__ == "__main__":
    generate_seed_data()
