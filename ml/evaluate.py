
import numpy as np

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)


def evaluate_regression(
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> dict:
    """Рассчитывает метрики регрессии в исходной шкале цен."""

    y_true = np.asarray(y_true, dtype=np.float64)
    y_pred = np.asarray(y_pred, dtype=np.float64)

    if len(y_true) != len(y_pred):
        raise ValueError(
            "Размеры y_true и y_pred не совпадают."
        )

    if len(y_true) == 0:
        raise ValueError("Нельзя оценить пустую выборку.")

    if not np.isfinite(y_true).all():
        raise ValueError("В y_true есть NaN или бесконечности.")

    if not np.isfinite(y_pred).all():
        raise ValueError("В y_pred есть NaN или бесконечности.")

    if (y_true <= 0).any():
        raise ValueError("В y_true есть неположительные значения.")

    # Не допускаем отрицательные прогнозы стоимости.
    y_pred = np.maximum(y_pred, 0)

    absolute_errors = np.abs(y_true - y_pred)

    return {
        "mae": float(
            mean_absolute_error(y_true, y_pred)
        ),
        "rmse": float(
            np.sqrt(mean_squared_error(y_true, y_pred))
        ),
        "mape": float(
            np.mean(absolute_errors / y_true) * 100
        ),
        "r2": float(
            r2_score(y_true, y_pred)
        ),
        "wape": float(
            absolute_errors.sum() / y_true.sum() * 100
        ),
    }
