
from dataclasses import dataclass
from pathlib import Path
from datetime import date

import numpy as np
import pandas as pd

from .config import (
    AS_OF_DATE,
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    FETCH_SIZE,
    TARGET_COLUMN,
    TRAIN_RATIO,
    VALIDATION_RATIO,
)

DATASET_COLUMNS = [
    "price_id",
    "loading_date",
    "loading_city_gc_id",
    "unloading_city_gc_id",
    "distance",
    "total_weight",
    "total_volume",
    "weight_category",
    "volume_category",
    "body_group_1",
    "body_group_2",
    "loading_year",
    "loading_month",
    "loading_weekday",
    "price_type",
    "complex",
    "target_price_rub",
]

PRICE_DATASET_SQL = """
WITH ranked_body_groups AS (
    SELECT
        b.price_id,
        bg.std_id,
        ROW_NUMBER() OVER (
            PARTITION BY b.price_id
            ORDER BY bg.std_id
        ) AS group_number
    FROM core.bridge_price_body_group b
    JOIN core.dim_body_group bg
        ON bg.body_group_id = b.body_group_id
),
body_group_features AS (
    SELECT
        price_id,
        MAX(std_id) FILTER (
            WHERE group_number = 1
        ) AS body_group_1,
        MAX(std_id) FILTER (
            WHERE group_number = 2
        ) AS body_group_2
    FROM ranked_body_groups
    GROUP BY price_id
)
SELECT
    f.price_id,
    f.loading_date,
    COALESCE(origin.gc_id::TEXT, 'UNKNOWN')
        AS loading_city_gc_id,
    COALESCE(destination.gc_id::TEXT, 'UNKNOWN')
        AS unloading_city_gc_id,
    f.distance,
    f.total_weight,
    f.total_volume,
    COALESCE(
        NULLIF(BTRIM(f.weight_category), ''),
        'UNKNOWN'
    ) AS weight_category,
    COALESCE(
        NULLIF(BTRIM(f.volume_category), ''),
        'UNKNOWN'
    ) AS volume_category,
    COALESCE(bg.body_group_1::TEXT, 'UNKNOWN')
        AS body_group_1,
    COALESCE(bg.body_group_2::TEXT, 'NONE')
        AS body_group_2,
    EXTRACT(YEAR FROM f.loading_date)::INTEGER
        AS loading_year,
    EXTRACT(MONTH FROM f.loading_date)::INTEGER::TEXT
        AS loading_month,
    EXTRACT(ISODOW FROM f.loading_date)::INTEGER::TEXT
        AS loading_weekday,
    COALESCE(f.price_type::TEXT, 'UNKNOWN')
        AS price_type,
    CASE
        WHEN COALESCE(f.complex, FALSE) THEN 'TRUE'
        ELSE 'FALSE'
    END AS complex,
    f.price_rub AS target_price_rub
FROM core.fact_prices f
LEFT JOIN core.dim_city origin
    ON origin.city_id = f.loading_city_id
LEFT JOIN core.dim_city destination
    ON destination.city_id = f.unloading_city_id
LEFT JOIN body_group_features bg
    ON bg.price_id = f.price_id
WHERE
    f.is_fact IS TRUE
    AND f.outlier_mark IS FALSE
    AND f.loading_date IS NOT NULL
    AND f.loading_date <= %s
    AND f.price_rub > 0
    AND f.distance > 0
ORDER BY
    f.loading_date,
    f.price_id
"""


@dataclass
class DatasetSplits:
    """Хронологически разделённый ML-датасет."""

    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame

    train_end_date: date
    validation_end_date: date

    def summary(self) -> dict:
        """Возвращает статистику разделения."""

        result = {}

        for name, part in (
            ("train", self.train),
            ("validation", self.validation),
            ("test", self.test),
        ):
            result[name] = {
                "rows": len(part),
                "date_from": part["loading_date"].min().date().isoformat(),
                "date_to": part["loading_date"].max().date().isoformat(),
                "share_pct": round(
                    len(part) / (
                        len(self.train)
                        + len(self.validation)
                        + len(self.test)
                    ) * 100,
                    2,
                ),
            }

        result["train_end_date"] = self.train_end_date.isoformat()
        result["validation_end_date"] = (
            self.validation_end_date.isoformat()
        )

        return result


def load_price_dataset(
    conn,
    as_of_date: date = AS_OF_DATE,
) -> pd.DataFrame:
    """Загружает обучающую выборку из Core"""

    chunks = []

    with conn.cursor(name="ml_price_dataset_cursor") as cursor:
        cursor.itersize = FETCH_SIZE

        cursor.execute(
            PRICE_DATASET_SQL,
            (as_of_date,),
        )

        columns = DATASET_COLUMNS

        while True:
            rows = cursor.fetchmany(FETCH_SIZE)

            if not rows:
                break

            chunks.append(
                pd.DataFrame.from_records(
                    rows,
                    columns=columns,
                )
            )

    if not chunks:
        raise ValueError(
            "Запрос не вернул данных для обучения."
        )

    df = pd.concat(
        chunks,
        ignore_index=True,
    )

    # Контроль уникальности наблюдений.
    duplicates = df["price_id"].duplicated().sum()

    if duplicates:
        raise ValueError(
            f"В датасете обнаружено повторных price_id: {duplicates}"
        )

    # Преобразуем дату.
    df["loading_date"] = pd.to_datetime(
        df["loading_date"],
        errors="raise",
    )

    # Преобразуем числовые признаки.
    for column in [
        "distance",
        "total_weight",
        "total_volume",
        "loading_year",
        TARGET_COLUMN,
    ]:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    # Категориальные признаки должны быть строковыми.
    for column in CATEGORICAL_FEATURES:
        df[column] = (
            df[column]
            .fillna("UNKNOWN")
            .astype(str)
        )

    # Цель должна быть положительной и конечной.
    target = df[TARGET_COLUMN].to_numpy(dtype="float64")

    if not np.isfinite(target).all():
        raise ValueError(
            "В целевой переменной обнаружены NaN или бесконечные значения."
        )

    if (target <= 0).any():
        raise ValueError(
            "В целевой переменной обнаружены значения <= 0."
        )

    # Дополнительный контроль даты среза.
    if (df["loading_date"].dt.date > as_of_date).any():
        raise ValueError(
            "В датасете обнаружены записи позднее as_of_date."
        )

    # Сортировка для воспроизводимости.
    df = df.sort_values(
        ["loading_date", "price_id"]
    ).reset_index(drop=True)

    return df


def split_chronologically(
    df: pd.DataFrame,
    train_ratio: float = TRAIN_RATIO,
    validation_ratio: float = VALIDATION_RATIO,
) -> DatasetSplits:
    """
    Делит данные по времени, не разделяя один loading_date
    между разными выборками.
    """

    if df.empty:
        raise ValueError("Датасет пустой.")

    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio должен быть между 0 и 1.")

    if not 0 < validation_ratio < 1:
        raise ValueError(
            "validation_ratio должен быть между 0 и 1."
        )

    if train_ratio + validation_ratio >= 1:
        raise ValueError(
            "Сумма train_ratio и validation_ratio должна быть меньше 1."
        )

    date_counts = (
        df.groupby("loading_date")
        .size()
        .sort_index()
    )

    if len(date_counts) < 3:
        raise ValueError(
            "Недостаточно уникальных дат для трёх временных выборок."
        )

    cumulative_counts = date_counts.cumsum().to_numpy()

    train_target = int(np.ceil(len(df) * train_ratio))

    validation_target = int(
        np.ceil(len(df) * (train_ratio + validation_ratio))
    )

    train_position = np.searchsorted(
        cumulative_counts,
        train_target,
        side="left",
    )

    validation_position = np.searchsorted(
        cumulative_counts,
        validation_target,
        side="left",
    )

    train_end_date = date_counts.index[train_position].date()

    validation_end_date = date_counts.index[
        validation_position
    ].date()

    if train_end_date >= validation_end_date:
        raise ValueError(
            "Не удалось сформировать непересекающиеся временные интервалы."
        )

    train = df[
        df["loading_date"].dt.date <= train_end_date
    ].copy()

    validation = df[
        (df["loading_date"].dt.date > train_end_date)
        & (
            df["loading_date"].dt.date
            <= validation_end_date
        )
    ].copy()

    test = df[
        df["loading_date"].dt.date > validation_end_date
    ].copy()

    if train.empty or validation.empty or test.empty:
        raise ValueError(
            "Одна из выборок пустая. Проверь временное распределение данных."
        )

    return DatasetSplits(
        train=train,
        validation=validation,
        test=test,
        train_end_date=train_end_date,
        validation_end_date=validation_end_date,
    )


def make_xy(
    part: pd.DataFrame,
) -> tuple[pd.DataFrame, np.ndarray]:
    """
    Подготавливает признаки и логарифмированную целевую переменную.
    """

    X = part[FEATURE_COLUMNS].copy()

    y = np.log1p(
        part[TARGET_COLUMN].to_numpy(dtype="float64")
    )

    return X, y


def save_splits(
    splits: DatasetSplits,
    output_dir: str | Path,
) -> None:
    """Сохраняет три выборки в Parquet."""

    output_path = Path(output_dir)
    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    splits.train.to_parquet(
        output_path / "train.parquet",
        index=False,
    )

    splits.validation.to_parquet(
        output_path / "validation.parquet",
        index=False,
    )

    splits.test.to_parquet(
        output_path / "test.parquet",
        index=False,
    )