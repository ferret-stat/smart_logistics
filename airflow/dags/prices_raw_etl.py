import pendulum

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.raw.export import export_data
from etl.raw.transform import transform_data
from etl.raw.load import load_data
from etl.core.fact_prices import load_prices_core
from etl.mart.route_card import rebuild_mart_route_card


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
    tags=["prices", "etl", "raw", "core", "mart"],
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

    @task
    def load_prices_to_core(raw_result: dict) -> dict:
        return load_prices_core(conn_id="dwh_postgres")

    @task
    def build_mart_route_card(bridge_result: dict) -> dict:

        if bridge_result.get("status") != "SUCCESS":
            raise ValueError(
                "Загрузка bridge_price_body_group завершилась ошибкой"
            )

        return rebuild_mart_route_card(
            conn_id="dwh_postgres"
        )
        
    watermark = get_watermark()

    exported_file = export_prices(watermark)

    transformed_file = transform_prices(
            exported_file
        )
    
    raw_result = load_prices(transformed_file)
    bridge_result = load_prices_to_core(raw_result)
    mart_result = build_mart_route_card(bridge_result)


prices_raw_etl()