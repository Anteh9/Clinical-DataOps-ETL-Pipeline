from prefect import flow

from etl_tasks import (
    extract_clinical_sources,
    load_to_postgres,
    validate_and_quarantine_task,
)


@flow(name="Clinical ETL Pipeline Flow")
def clinical_etl_flow() -> None:
    df_labs, df_patients, df_vitals = extract_clinical_sources()
    clean_labs, clean_patients, clean_vitals, all_quarantines = (
        validate_and_quarantine_task(df_labs, df_patients, df_vitals)
    )
    load_to_postgres(clean_labs, clean_patients, clean_vitals, all_quarantines)


if __name__ == "__main__":
    clinical_etl_flow()