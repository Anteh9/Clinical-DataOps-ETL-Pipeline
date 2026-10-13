import pandera.pandas as pa

# coerce=True converts compatible input values before validation.
sensor_vitals_schema = pa.DataFrameSchema(
    {
        "patient_id": pa.Column(int, pa.Check.greater_than(0), nullable=False),
        "heart_rate": pa.Column(float, pa.Check.between(30.0, 220.0), nullable=False),
        "spo2_level": pa.Column(float, pa.Check.between(70.0, 100.0), nullable=False),
        "recorded_at": pa.Column(str, nullable=False),
    },
    coerce=True,
)

lab_results_schema = pa.DataFrameSchema(
    {
        "patient_id": pa.Column(int, pa.Check.greater_than(0), nullable=False),
        "glucose": pa.Column(float, pa.Check.greater_than(0.0), nullable=False),
        "insulin": pa.Column(
            float, pa.Check.greater_than_or_equal_to(0.0), nullable=False
        ),
        "recorded_at": pa.Column(str, nullable=False),
    },
    coerce=True,
)

legacy_patients_schema = pa.DataFrameSchema(
    {
        "patient_id": pa.Column(int, pa.Check.greater_than(0), nullable=False),
        "age": pa.Column(int, pa.Check.between(1, 120), nullable=False),
        "blood_pressure": pa.Column(float, pa.Check.greater_than(0.0), nullable=False),
        "bmi": pa.Column(float, pa.Check.between(10.0, 70.0), nullable=False),
        "outcome": pa.Column(int, pa.Check.isin([0, 1]), nullable=False),
    },
    coerce=True,
)