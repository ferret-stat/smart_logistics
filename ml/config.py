from pathlib import Path
from datetime import date


BASE_DIR = Path(__file__).resolve().parent
FETCH_SIZE = 20_000
DATASET_DIR = Path("/opt/airflow/volumes/ml/dataset")

ARTIFACTS_DIR = Path("/opt/airflow/volumes/ml/artifacts")

MODEL_DIR = ARTIFACTS_DIR / "models"
METRICS_DIR = ARTIFACTS_DIR / "metrics"
PREDICTIONS_DIR = ARTIFACTS_DIR / "predictions"

AS_OF_DATE = date(2026, 11, 8)

TRAIN_RATIO = 0.70
VALIDATION_RATIO = 0.15
TEST_RATIO = 0.15

TARGET_COLUMN = "target_price_rub"

NUMERIC_FEATURES = [
    "distance",
    "total_weight",
    "total_volume",
    "loading_year",
]

CATEGORICAL_FEATURES = [
    "loading_city_gc_id",
    "unloading_city_gc_id",
    "weight_category",
    "volume_category",
    "body_group_1",
    "body_group_2",
    "loading_month",
    "loading_weekday",
    "price_type",
    "complex",
]

FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES

MODEL_VERSION = "catboost_baseline_v1"

RANDOM_SEED = 42
CATBOOST_ITERATIONS = 2000
CATBOOST_DEPTH = 8
CATBOOST_LEARNING_RATE = 0.05
EARLY_STOPPING_ROUNDS = 150

LOG_TARGET = True
