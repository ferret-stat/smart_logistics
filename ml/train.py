import json
from pathlib import Path

import numpy as np
import pandas as pd

from catboost import CatBoostRegressor
from sklearn.dummy import DummyRegressor

from ml.config import (
    DATASET_DIR,
    MODEL_DIR,
    METRICS_DIR,
    PREDICTIONS_DIR,
    FEATURE_COLUMNS,
    NUMERIC_FEATURES,
    CATEGORICAL_FEATURES,
    TARGET_COLUMN,
    MODEL_VERSION,
    RANDOM_SEED,
    CATBOOST_ITERATIONS,
    CATBOOST_DEPTH,
    CATBOOST_LEARNING_RATE,
    EARLY_STOPPING_ROUNDS,
    LOG_TARGET,
)

from ml.evaluate import evaluate_regression


def load_splits() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Загружает Train, Validation и Test."""

    train = pd.read_parquet(
        DATASET_DIR / "train.parquet"
    )

    validation = pd.read_parquet(
        DATASET_DIR / "validation.parquet"
    )

    test = pd.read_parquet(
        DATASET_DIR / "test.parquet"
    )

    return train, validation, test


def validate_splits(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
) -> None:
    """Проверяет структуру и хронологию выборок."""

    required_columns = (
        FEATURE_COLUMNS
        + [
            "price_id",
            "loading_date",
            TARGET_COLUMN,
        ]
    )

    for name, part in (
        ("train", train),
        ("validation", validation),
        ("test", test),
    ):
        missing = set(required_columns) - set(part.columns)

        if missing:
            raise ValueError(
                f"{name}: отсутствуют колонки {sorted(missing)}"
            )

        if part.empty:
            raise ValueError(
                f"Выборка {name} пустая."
            )

        if part["price_id"].duplicated().any():
            raise ValueError(
                f"В выборке {name} есть дубликаты price_id."
            )

        if part[TARGET_COLUMN].isna().any():
            raise ValueError(
                f"В {name} есть пропуски целевой переменной."
            )

    # Проверяем отсутствие пересечений между выборками.
    train_ids = set(train["price_id"])
    validation_ids = set(validation["price_id"])
    test_ids = set(test["price_id"])

    if train_ids & validation_ids:
        raise ValueError("Train пересекается с Validation.")

    if train_ids & test_ids:
        raise ValueError("Train пересекается с Test.")

    if validation_ids & test_ids:
        raise ValueError("Validation пересекается с Test.")

    # Проверяем временной порядок.
    train_max = pd.to_datetime(train["loading_date"]).max()
    validation_min = pd.to_datetime(
        validation["loading_date"]
    ).min()
    validation_max = pd.to_datetime(
        validation["loading_date"]
    ).max()
    test_min = pd.to_datetime(test["loading_date"]).min()

    if not train_max < validation_min:
        raise ValueError(
            "Train и Validation нарушают временной порядок."
        )

    if not validation_max < test_min:
        raise ValueError(
            "Validation и Test нарушают временной порядок."
        )


def prepare_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Готовит признаки к передаче в CatBoost."""

    X = df[FEATURE_COLUMNS].copy()

    for column in NUMERIC_FEATURES:
        X[column] = pd.to_numeric(
            X[column],
            errors="coerce",
        )

        X[column] = X[column].replace(
            [np.inf, -np.inf],
            np.nan,
        )

    for column in CATEGORICAL_FEATURES:
        X[column] = (
            X[column]
            .fillna("UNKNOWN")
            .astype(str)
        )

    return X


def transform_target(
    values: pd.Series,
) -> np.ndarray:
    """Преобразует цену в целевую переменную модели."""

    target = values.to_numpy(dtype=np.float64)

    if not np.isfinite(target).all() or (target <= 0).any():
        raise ValueError(
            "Целевая переменная должна содержать "
            "только конечные положительные значения."
        )

    if LOG_TARGET:
        return np.log1p(target)

    return target


def inverse_target(
    predictions: np.ndarray,
) -> np.ndarray:
    """Возвращает прогноз из логарифмической шкалы в рубли."""

    predictions = np.asarray(
        predictions,
        dtype=np.float64,
    )

    if LOG_TARGET:
        predictions = np.expm1(predictions)

    return np.maximum(predictions, 0)


def main() -> dict:
    """Обучает baseline и CatBoost, оценивает Test."""

    for directory in (
        MODEL_DIR,
        METRICS_DIR,
        PREDICTIONS_DIR,
    ):
        Path(directory).mkdir(
            parents=True,
            exist_ok=True,
        )

    train, validation, test = load_splits()

    validate_splits(
        train,
        validation,
        test,
    )

    # Подготовка признаков.
    X_train = prepare_features(train)
    X_validation = prepare_features(validation)
    X_test = prepare_features(test)

    y_train = transform_target(train[TARGET_COLUMN])
    y_validation = transform_target(
        validation[TARGET_COLUMN]
    )
    y_test_rub = test[TARGET_COLUMN].to_numpy(
        dtype=np.float64
    )

    # Baseline
    baseline = DummyRegressor(
        strategy="median"
    )

    baseline.fit(
        X_train,
        y_train,
    )

    baseline_validation = inverse_target(
        baseline.predict(X_validation)
    )

    baseline_validation_metrics = evaluate_regression(
        validation[TARGET_COLUMN].to_numpy(),
        baseline_validation,
    )


    # CatBoost: подбор числа итераций по Validation
    model_selector = CatBoostRegressor(
        iterations=CATBOOST_ITERATIONS,
        depth=CATBOOST_DEPTH,
        learning_rate=CATBOOST_LEARNING_RATE,
        loss_function="RMSE",
        eval_metric="RMSE",
        random_seed=RANDOM_SEED,
        early_stopping_rounds=EARLY_STOPPING_ROUNDS,
        verbose=100,
        thread_count=-1,
        allow_writing_files=False,
    )

    model_selector.fit(
        X_train,
        y_train,
        cat_features=CATEGORICAL_FEATURES,
        eval_set=(X_validation, y_validation),
        use_best_model=True,
    )

    catboost_validation_predictions = inverse_target(
        model_selector.predict(X_validation)
    )

    catboost_validation_metrics = evaluate_regression(
        validation[TARGET_COLUMN].to_numpy(),
        catboost_validation_predictions,
    )

    best_iteration = model_selector.get_best_iteration()

    if best_iteration < 0:
        best_iteration = CATBOOST_ITERATIONS - 1

    best_iterations = max(best_iteration + 1, 1)

    # Финальное обучение Train + Validation
    X_train_full = pd.concat(
        [X_train, X_validation],
        ignore_index=True,
    )

    y_train_full = np.concatenate(
        [y_train, y_validation]
    )

    final_model = CatBoostRegressor(
        iterations=best_iterations,
        depth=CATBOOST_DEPTH,
        learning_rate=CATBOOST_LEARNING_RATE,
        loss_function="RMSE",
        eval_metric="RMSE",
        random_seed=RANDOM_SEED,
        verbose=100,
        thread_count=-1,
        allow_writing_files=False,
    )

    final_model.fit(
        X_train_full,
        y_train_full,
        cat_features=CATEGORICAL_FEATURES,
    )

    # Оценка на Test
    baseline_final = DummyRegressor(
        strategy="median"
    )

    baseline_final.fit(
        X_train_full,
        y_train_full,
    )

    baseline_test_predictions = inverse_target(
        baseline_final.predict(X_test)
    )

    catboost_test_predictions = inverse_target(
        final_model.predict(X_test)
    )

    baseline_test_metrics = evaluate_regression(
        y_test_rub,
        baseline_test_predictions,
    )

    catboost_test_metrics = evaluate_regression(
        y_test_rub,
        catboost_test_predictions,
    )

    # Сохраняем модель
    model_path = Path(MODEL_DIR) / f"{MODEL_VERSION}.cbm"

    final_model.save_model(
        str(model_path)
    )

    # Сохраняем тестовые прогнозы
    absolute_errors = np.abs(
        y_test_rub - catboost_test_predictions
    )

    ape = (
        absolute_errors
        / y_test_rub
        * 100
    )

    predictions = pd.DataFrame({
        "price_id": test["price_id"].to_numpy(),
        "loading_date": test["loading_date"].to_numpy(),
        "dataset_type": "TEST",
        "predicted_price_rub": catboost_test_predictions,
        "actual_price_rub": y_test_rub,
        "abs_error": absolute_errors,
        "ape": ape,
        "model_version": MODEL_VERSION,
    })

    predictions_path = (
        Path(PREDICTIONS_DIR)
        / f"{MODEL_VERSION}_test.parquet"
    )

    predictions.to_parquet(
        predictions_path,
        index=False,
    )

    # Сохраняем метрики и параметры
    metrics = {
        "model_version": MODEL_VERSION,
        "target": TARGET_COLUMN,
        "log_target": LOG_TARGET,

        "dataset": {
            "train_rows": len(train),
            "validation_rows": len(validation),
            "test_rows": len(test),
            "train_from": str(train["loading_date"].min()),
            "train_to": str(train["loading_date"].max()),
            "validation_from": str(
                validation["loading_date"].min()
            ),
            "validation_to": str(
                validation["loading_date"].max()
            ),
            "test_from": str(test["loading_date"].min()),
            "test_to": str(test["loading_date"].max()),
        },

        "baseline_validation": baseline_validation_metrics,
        "baseline_test": baseline_test_metrics,
        "catboost_test": catboost_test_metrics,
        "catboost_validation": catboost_validation_metrics,

        "best_iterations": best_iterations,

        "parameters": {
            "depth": CATBOOST_DEPTH,
            "learning_rate": CATBOOST_LEARNING_RATE,
            "random_seed": RANDOM_SEED,
        },

        "artifacts": {
            "model": str(model_path),
            "test_predictions": str(predictions_path),
        },
    }

    metrics_path = (
        Path(METRICS_DIR)
        / f"{MODEL_VERSION}_metrics.json"
    )

    with metrics_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            metrics,
            file,
            ensure_ascii=False,
            indent=4,
        )

    print("\nBASELINE TEST")
    print(json.dumps(
        baseline_test_metrics,
        ensure_ascii=False,
        indent=4,
    ))

    print("\nCATBOOST TEST")
    print(json.dumps(
        catboost_test_metrics,
        ensure_ascii=False,
        indent=4,
    ))

    print(f"\nBest iterations: {best_iterations}")
    print(f"Model: {model_path}")
    print(f"Predictions: {predictions_path}")
    print(f"Metrics: {metrics_path}")

    return metrics


if __name__ == "__main__":
    main()
