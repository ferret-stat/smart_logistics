import pendulum

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.raw.export import export_data
from etl.raw.transform import transform_data
from etl.raw.load import load_data


@dag(
    dag_id="prices_raw_etl",
    description="Загрузка prices в RAW",
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
def prices_raw_etl():

    @task
    def get_watermark():
        hook = PostgresHook(postgres_conn_id="dwh_postgres")

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT last_updated_at
                    FROM raw.etl_watermarks
                    WHERE table_name = 'prices'
                """)
                result = cursor.fetchone()

        return result[0].isoformat() if result and result[0] else None

    @task
    def export_prices(updated_after: str | None) -> str:
        return export_data(
            conn_id="oltp_postgres",
            table_name="prices",
            updated_after=updated_after,
        )

    @task
    def transform_prices(file_path: str) -> str:
        return transform_data(
            file_path=file_path,
            source_id_column="id",
        )

    @task
    def load_prices(file_path: str) -> dict:
        return load_data(
            conn_id="dwh_postgres",
            file_path=file_path,
            table_name="prices",
            watermark_column="updated_at",
        )
    watermark = get_watermark()

    exported_file = export_prices(watermark)

    transformed_file = transform_prices(
            exported_file
        )
    
    load_prices(transformed_file)

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
    tags=["smart-logistics", "etl", "raw"],
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

    exported_file = export_cities()

    transformed_file = transform_cities(
        exported_file
    )

    load_cities(transformed_file)


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

    exported_file = export_body_types()

    transformed_file = transform_body_types(
        exported_file
    )

    load_body_types(transformed_file)


prices_raw_etl()
exchange_rates_raw_etl()
cities_raw_etl()
body_types_raw_etl()