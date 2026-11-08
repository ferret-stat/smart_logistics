import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="ml_pipeline",
    description="Подготовка данных, обучение ML и сохранение прогнозов",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 11, 8, tz="Europe/Berlin"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["smart-logistics", "ml", "pipeline"],
)
def ml_pipeline():

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
            df = load_price_dataset(
                conn=conn,
                as_of_date=AS_OF_DATE,
            )
        finally:
            conn.close()

        splits = split_chronologically(df)

        output_dir = "/opt/airflow/volumes/ml/dataset"

        save_splits(
            splits=splits,
            output_dir=output_dir,
        )

        return {
            "status": "SUCCESS",
            "total_rows": len(df),
            "splits": splits.summary(),
            "output_dir": output_dir,
        }

    @task
    def train_model(dataset_result: dict) -> dict:
        if dataset_result.get("status") != "SUCCESS":
            raise ValueError(
                "Подготовка датасета завершилась ошибкой."
            )

        from ml.train import main

        metrics = main()

        if not metrics.get("catboost_test"):
            raise ValueError(
                "Не получены метрики CatBoost на Test."
            )

        return {
            "status": "SUCCESS",
            "model_version": metrics["model_version"],
            "test_metrics": metrics["catboost_test"],
            "best_iterations": metrics["best_iterations"],
            "artifacts": metrics["artifacts"],
        }

    @task
    def persist_predictions(training_result: dict) -> dict:
        if training_result.get("status") != "SUCCESS":
            raise ValueError(
                "Обучение модели завершилось ошибкой."
            )

        from ml.persist_predictions import load_predictions_to_mart

        result = load_predictions_to_mart(
            conn_id="dwh_postgres"
        )

        return result

    dataset_result = prepare_dataset()
    training_result = train_model(dataset_result)
    persist_predictions(training_result)


ml_pipeline()
