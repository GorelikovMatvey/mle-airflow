"""Steps of the operator-based churn ETL pipeline."""

import pandas as pd
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


def create_table() -> None:
    engine = PostgresHook(postgres_conn_id="destination_db").get_sqlalchemy_engine()
    metadata = MetaData()
    table = Table(
        "alt_users_churn",
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
        UniqueConstraint("customer_id", name="alt_users_churn_customer_id_key"),
    )
    try:
        if not inspect(engine).has_table(table.name):
            metadata.create_all(engine)
    finally:
        engine.dispose()


def extract(**context) -> None:
    sql = """
        SELECT
            c.customer_id, c.begin_date, c.end_date, c.type,
            c.paperless_billing, c.payment_method,
            c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup,
            i.device_protection, i.tech_support, i.streaming_tv,
            i.streaming_movies, p.gender, p.senior_citizen,
            p.partner, p.dependents, ph.multiple_lines
        FROM contracts AS c
        LEFT JOIN internet AS i USING (customer_id)
        LEFT JOIN personal AS p USING (customer_id)
        LEFT JOIN phone AS ph USING (customer_id)
    """
    connection = PostgresHook(postgres_conn_id="source_db").get_conn()
    try:
        data = pd.read_sql(sql, connection)
    finally:
        connection.close()
    context["ti"].xcom_push(key="extracted_data", value=data)


def transform(**context) -> None:
    data = context["ti"].xcom_pull(task_ids="extract", key="extracted_data")
    data = data.copy()
    data["target"] = (data["end_date"] != "No").astype(int)
    data["end_date"] = data["end_date"].replace("No", None)
    context["ti"].xcom_push(key="transformed_data", value=data)


def load(**context) -> None:
    data = context["ti"].xcom_pull(task_ids="transform", key="transformed_data")
    PostgresHook(postgres_conn_id="destination_db").insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=["customer_id"],
        rows=data.where(pd.notna(data), None).itertuples(index=False, name=None),
    )
