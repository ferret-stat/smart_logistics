import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="ml_dataset_prepare",
    description="Подготовка датасета для модели стоимости перевозок",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 11, 8, tz="Europe/Berlin"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "dataset"],
)
def ml_dataset_prepare():

    @task
    def prepare_dataset() -> dict:
        from ml.config import AS_OF_DATE
        from ml.dataset import (
            load_price_dataset,
            split_chronologically,
            save_splits,
        )

        hook = PostgresHook(
            postgres_conn_id="dwh_postgres"
        )

        conn = hook.get_conn()

        try:
            # Получаем исходный датасет.
            df = load_price_dataset(
                conn=conn,
                as_of_date=AS_OF_DATE,
            )
        finally:
            conn.close()

        # Делим по времени.
        splits = split_chronologically(df)

        # Сохраняем на общий Docker volume.
        output_dir = "/opt/airflow/volumes/ml/dataset"

        save_splits(
            splits=splits,
            output_dir=output_dir,
        )

        return {
            "total_rows": len(df),
            "as_of_date": AS_OF_DATE.isoformat(),
            "splits": splits.summary(),
            "output_dir": output_dir,
            "status": "SUCCESS",
        }

    prepare_dataset()

ml_dataset_prepare()
