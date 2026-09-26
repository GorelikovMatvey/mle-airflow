"""Clean the churn training dataset produced by prepare_churn_dataset."""

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    inspect,
)


@dag(
    dag_id="clean_churn_dataset",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ETL"],
)
def clean_churn_dataset():
    @task()
    def create_table() -> None:
        engine = PostgresHook(postgres_conn_id="destination_db").get_sqlalchemy_engine()
        metadata = MetaData()
        table = Table(
            "clean_users_churn",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("customer_id", String),
            Column("begin_date", DateTime),
            Column("end_date", DateTime),
            Column("type", String),
            Column("paperless_billing", String),
            Column("payment_method", String),
            Column("monthly_charges", Float),
            Column("total_charges", Float),
            Column("internet_service", String),
            Column("online_security", String),
            Column("online_backup", String),
            Column("device_protection", String),
            Column("tech_support", String),
            Column("streaming_tv", String),
            Column("streaming_movies", String),
            Column("gender", String),
            Column("senior_citizen", Integer),
            Column("partner", String),
            Column("dependents", String),
            Column("multiple_lines", String),
            Column("target", Integer),
            UniqueConstraint("customer_id", name="clean_users_churn_customer_id_key"),
        )
        try:
            if not inspect(engine).has_table(table.name):
                metadata.create_all(engine)
        finally:
            engine.dispose()

    @task()
    def extract() -> pd.DataFrame:
        hook = PostgresHook(postgres_conn_id="destination_db")
        connection = hook.get_conn()
        try:
            data = pd.read_sql("SELECT * FROM users_churn", connection)
        finally:
            connection.close()
        # The database key is different for otherwise identical customers.
        return data.drop(columns="id")

    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        data = data.copy()

        feature_cols = data.columns.drop("customer_id").tolist()
        duplicates = data.duplicated(subset=feature_cols, keep=False)
        data = data.loc[~duplicates].reset_index(drop=True)

        missing = data.isnull().sum()
        cols_with_nans = missing[missing > 0].index.drop("end_date", errors="ignore")
        for col in cols_with_nans:
            if pd.api.types.is_numeric_dtype(data[col]):
                fill_value = data[col].mean()
            else:
                fill_value = data[col].mode().iloc[0]
            data[col] = data[col].fillna(fill_value)

        numeric_cols = data.select_dtypes(include=["float"]).columns
        potential_outliers = pd.DataFrame(index=data.index)
        for col in numeric_cols:
            q1 = data[col].quantile(0.25)
            q3 = data[col].quantile(0.75)
            margin = 1.5 * (q3 - q1)
            lower, upper = q1 - margin, q3 + margin
            potential_outliers[col] = ~data[col].between(lower, upper)
        outliers = potential_outliers.any(axis=1)
        return data.loc[~outliers].reset_index(drop=True)

    @task()
    def load(data: pd.DataFrame) -> None:
        PostgresHook(postgres_conn_id="destination_db").insert_rows(
            table="clean_users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=["customer_id"],
            rows=data.astype(object).where(pd.notna(data), None).itertuples(index=False, name=None),
        )

    table_ready = create_table()
    extracted = extract()
    table_ready >> extracted
    load(transform(extracted))


clean_churn_dataset()
