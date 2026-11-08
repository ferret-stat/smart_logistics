import pendulum

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.raw.export import export_data
from etl.raw.transform import transform_data
from etl.raw.load import load_data


@dag(
    dag_id="exchange_rates_raw_etl",
    description="Загрузка exchange_rates в RAW",
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
    tags=["smart-logistics", "etl", "raw"],
)
def exchange_rates_raw_etl():

    @task
    def export_exchange_rates() -> str:
        return export_data(
            conn_id="oltp_postgres",
            table_name="exchange_rates",
        )

    @task
    def transform_exchange_rates(file_path: str) -> str:
        return transform_data(
            file_path=file_path,
            source_id_column="id",
        )

    @task
    def load_exchange_rates(file_path: str) -> dict:
        return load_data(
            conn_id="dwh_postgres",
            file_path=file_path,
            table_name="exchange_rates",
        )

    exported_file = export_exchange_rates()

    transformed_file = transform_exchange_rates(
        exported_file
    )

    load_exchange_rates(transformed_file)


exchange_rates_raw_etl()