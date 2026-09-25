import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import (
    Column, DateTime, Float, Integer, MetaData, String, Table,
    UniqueConstraint, inspect,
)


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ETL"],
)
def prepare_churn_dataset():
    @task()
    def create_table():
        engine = PostgresHook(postgres_conn_id="destination_db").get_sqlalchemy_engine()
        metadata = MetaData()
        users_churn = Table(
            "users_churn", metadata,
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
            UniqueConstraint("customer_id", name="unique_customer_id"),
        )
        if not inspect(engine).has_table(users_churn.name):
            metadata.create_all(engine)
        engine.dispose()

    @task()
    def extract() -> pd.DataFrame:
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
        hook = PostgresHook(postgres_conn_id="source_db")
        conn = hook.get_conn()
        try:
            return pd.read_sql(sql, conn)
        finally:
            conn.close()

    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        data = data.copy()
        data["target"] = (data["end_date"] != "No").astype(int)
        data["end_date"] = data["end_date"].replace("No", None)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook(postgres_conn_id="destination_db")
        hook.insert_rows(
            table="users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=["customer_id"],
            rows=data.where(pd.notna(data), None).itertuples(index=False, name=None),
        )

    table_ready = create_table()
    data = extract()
    table_ready >> data
    load(transform(data))


prepare_churn_dataset()
