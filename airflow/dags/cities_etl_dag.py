import pendulum

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.raw.export import export_data
from etl.raw.transform import transform_data
from etl.raw.load import load_data
from etl.core.cities import load_cities_core


@dag(
    dag_id="cities_raw_etl",
    description="Загрузка cities в RAW",
    schedule=timedelta(minutes=10),
    start_date=pendulum.datetime(
        2026,
        10,
        26,
        tz="Europe/Moscow",
    ),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["cities", "etl", "raw", "core"],
)
def cities_raw_etl():

    @task
    def export_cities() -> str:
        return export_data(
            conn_id="oltp_postgres",
            table_name="cities",
        )

    @task
    def transform_cities(file_path: str) -> str:
        return transform_data(
            file_path=file_path,
            source_id_column="id",
        )

    @task
    def load_cities(file_path: str) -> dict:
        return load_data(
            conn_id="dwh_postgres",
            file_path=file_path,
            table_name="cities",
        )

    @task
    def load_cities_to_core(raw_result: dict) -> dict:
        return load_cities_core(conn_id="dwh_postgres")

    exported_file = export_cities()

    transformed_file = transform_cities(
        exported_file
    )

    raw_result = load_cities(transformed_file)
    load_cities_to_core(raw_result)

cities_raw_etl()