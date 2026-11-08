import pendulum

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.raw.export import export_data
from etl.raw.transform import transform_data
from etl.raw.load import load_data
from etl.core.price_body_groups import (
    rebuild_price_body_group_bridge,
)


@dag(
    dag_id="price_body_types_raw_etl",
    description="Загрузка price_body_types в RAW",
    schedule=timedelta(minutes=10),
    start_date=pendulum.datetime(
        2026,
        10,
        26,
        tz="Europe/Moscow",
    ),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["price_body_types", "etl", "raw", "core"],
)
def price_body_types_raw_etl():

    @task
    def export_price_body_types() -> str:
        return export_data(
            conn_id="oltp_postgres",
            table_name="price_body_types",
        )

    @task
    def transform_price_body_types(file_path: str) -> str:
        return transform_data(
            file_path=file_path,
            source_id_column="id",
        )

    @task
    def load_price_body_types(file_path: str) -> dict:
        return load_data(
            conn_id="dwh_postgres",
            file_path=file_path,
            table_name="price_body_types",
        )

    @task
    def build_price_body_group_bridge(fact_result: dict) -> dict:
        return rebuild_price_body_group_bridge(
            conn_id="dwh_postgres"
    )

    exported_file = export_price_body_types()

    transformed_file = transform_price_body_types(
        exported_file
    )

    fact_result = load_price_body_types(transformed_file)

    build_price_body_group_bridge(fact_result)

price_body_types_raw_etl()