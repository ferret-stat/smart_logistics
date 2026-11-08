import pendulum

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.raw.export import export_data
from etl.raw.transform import transform_data
from etl.raw.load import load_data
from etl.core.body_types import load_body_types_core


@dag(
    dag_id="body_types_raw_etl",
    description="Загрузка body_types в RAW",
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
    tags=["smart-logistics", "etl", "raw"],
)
def body_types_raw_etl():

    @task
    def export_body_types() -> str:
        return export_data(
            conn_id="oltp_postgres",
            table_name="body_types",
        )

    @task
    def transform_body_types(file_path: str) -> str:
        return transform_data(
            file_path=file_path,
            source_id_column="id",
        )

    @task
    def load_body_types(file_path: str) -> dict:
        return load_data(
            conn_id="dwh_postgres",
            file_path=file_path,
            table_name="body_types",
        )

    @task
    def load_body_types_to_core(raw_result: dict) -> dict:
        return load_body_types_core(conn_id="dwh_postgres")

    exported_file = export_body_types()

    transformed_file = transform_body_types(
        exported_file
    )
    raw_result = load_body_types(transformed_file)
    load_body_types_to_core(raw_result)


body_types_raw_etl()