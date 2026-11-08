import csv
import io
from pathlib import Path

import numpy as np
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook

from ml.config import (
    PREDICTIONS_DIR,
    MODEL_VERSION,
)


REQUIRED_COLUMNS = [
    "price_id",
    "loading_date",
    "dataset_type",
    "predicted_price_rub",
    "actual_price_rub",
    "model_version",
]


def load_predictions_to_mart(
    prediction_path: str | Path | None = None,
    conn_id: str = "dwh_postgres",
) -> dict:
    """Загружает ML-прогнозы в mart.ml_price_predictions."""

    if prediction_path is None:
        prediction_path = (
            Path(PREDICTIONS_DIR)
            / f"{MODEL_VERSION}_test.parquet"
        )

    prediction_path = Path(prediction_path)

    if not prediction_path.exists():
        raise FileNotFoundError(
            f"Файл прогнозов не найден: {prediction_path}"
        )

    df = pd.read_parquet(prediction_path)

    missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"В файле отсутствуют колонки: {sorted(missing_columns)}"
        )

    df = df[REQUIRED_COLUMNS].copy()

    if df.empty:
        raise ValueError("Файл прогнозов пустой.")

    if df["price_id"].isna().any():
        raise ValueError("Обнаружены пустые price_id.")

    if df["price_id"].duplicated().any():
        raise ValueError("В прогнозах есть дубликаты price_id.")

    if not (df["dataset_type"] == "TEST").all():
        raise ValueError(
            "Ожидаются только прогнозы тестовой выборки."
        )

    if not (df["model_version"] == MODEL_VERSION).all():
        raise ValueError(
            "Версия модели в файле не совпадает с MODEL_VERSION."
        )

    df["loading_date"] = pd.to_datetime(
        df["loading_date"],
        errors="raise",
    )

    for column in (
        "predicted_price_rub",
        "actual_price_rub",
    ):
        df[column] = pd.to_numeric(
            df[column],
            errors="raise",
        )

        values = df[column].to_numpy(dtype=np.float64)

        if not np.isfinite(values).all():
            raise ValueError(
                f"В {column} обнаружены NaN или бесконечности."
            )

    if (df["predicted_price_rub"] < 0).any():
        raise ValueError("Найдены отрицательные прогнозы.")

    if (df["actual_price_rub"] <= 0).any():
        raise ValueError(
            "Фактическая цена должна быть больше нуля."
        )

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        with conn:
            with conn.cursor() as cursor:

                # Временная таблица для COPY.
                cursor.execute("""
                    CREATE TEMP TABLE stage_ml_predictions (
                        price_id BIGINT NOT NULL,
                        loading_date DATE NOT NULL,
                        dataset_type TEXT NOT NULL,
                        predicted_price_rub NUMERIC(18, 2) NOT NULL,
                        actual_price_rub NUMERIC(18, 2) NOT NULL,
                        model_version TEXT NOT NULL
                    ) ON COMMIT DROP
                """)

                # Формируем CSV для PostgreSQL COPY.
                buffer = io.StringIO()
                writer = csv.writer(buffer, lineterminator="\n")

                for row in df.itertuples(index=False, name=None):
                    (
                        price_id,
                        loading_date,
                        dataset_type,
                        predicted_price,
                        actual_price,
                        model_version,
                    ) = row

                    writer.writerow([
                        int(price_id),
                        pd.Timestamp(loading_date).date().isoformat(),
                        dataset_type,
                        repr(float(predicted_price)),
                        repr(float(actual_price)),
                        model_version,
                    ])

                buffer.seek(0)

                cursor.copy_expert(
                    """
                    COPY stage_ml_predictions (
                        price_id,
                        loading_date,
                        dataset_type,
                        predicted_price_rub,
                        actual_price_rub,
                        model_version
                    )
                    FROM STDIN
                    WITH (FORMAT CSV)
                    """,
                    buffer,
                )

                # Проверяем соответствие прогнозов актуальным фактам Core.
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM stage_ml_predictions s
                    LEFT JOIN core.fact_prices f
                        ON f.price_id = s.price_id
                    WHERE
                        f.price_id IS NULL
                        OR f.loading_date IS DISTINCT FROM s.loading_date
                        OR f.price_rub IS DISTINCT FROM s.actual_price_rub
                """)

                inconsistent_count = cursor.fetchone()[0]

                if inconsistent_count:
                    raise ValueError(
                        "Обнаружены прогнозы, не соответствующие "
                        f"текущим фактам Core: {inconsistent_count}"
                    )

                # Загружаем прогнозы в Mart.
                # Ошибки abs_error и APE рассчитываем в PostgreSQL.
                cursor.execute("""
                    INSERT INTO mart.ml_price_predictions (
                        price_id,
                        loading_date,
                        dataset_type,
                        predicted_price_rub,
                        actual_price_rub,
                        abs_error,
                        ape,
                        model_version
                    )
                    SELECT
                        price_id,
                        loading_date,
                        dataset_type,
                        predicted_price_rub,
                        actual_price_rub,

                        ABS(
                            actual_price_rub - predicted_price_rub
                        ),

                        ABS(
                            actual_price_rub - predicted_price_rub
                        ) / NULLIF(actual_price_rub, 0) * 100,

                        model_version

                    FROM stage_ml_predictions

                    ON CONFLICT (price_id, model_version)
                    DO UPDATE SET
                        loading_date = EXCLUDED.loading_date,
                        dataset_type = EXCLUDED.dataset_type,
                        predicted_price_rub =
                            EXCLUDED.predicted_price_rub,
                        actual_price_rub =
                            EXCLUDED.actual_price_rub,
                        abs_error = EXCLUDED.abs_error,
                        ape = EXCLUDED.ape,
                        predicted_at = NOW()

                    WHERE ROW(
                        mart.ml_price_predictions.loading_date,
                        mart.ml_price_predictions.dataset_type,
                        mart.ml_price_predictions.predicted_price_rub,
                        mart.ml_price_predictions.actual_price_rub,
                        mart.ml_price_predictions.abs_error,
                        mart.ml_price_predictions.ape
                    ) IS DISTINCT FROM ROW(
                        EXCLUDED.loading_date,
                        EXCLUDED.dataset_type,
                        EXCLUDED.predicted_price_rub,
                        EXCLUDED.actual_price_rub,
                        EXCLUDED.abs_error,
                        EXCLUDED.ape
                    )
                """)

                affected_rows = cursor.rowcount

                cursor.execute("""
                    SELECT
                        COUNT(*) AS predictions_count,
                        ROUND(AVG(abs_error), 2) AS avg_abs_error,
                        ROUND(AVG(ape), 2) AS avg_ape
                    FROM mart.ml_price_predictions
                    WHERE model_version = %s
                      AND dataset_type = 'TEST'
                """, (MODEL_VERSION,))

                predictions_count, avg_abs_error, avg_ape = (
                    cursor.fetchone()
                )

        return {
            "source_rows": len(df),
            "affected_rows": affected_rows,
            "predictions_count": predictions_count,
            "avg_abs_error": float(avg_abs_error),
            "avg_ape": float(avg_ape),
            "model_version": MODEL_VERSION,
            "status": "SUCCESS",
        }

    finally:
        conn.close()


if __name__ == "__main__":
    result = load_predictions_to_mart()
    print(result)
