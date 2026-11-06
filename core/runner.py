import logging
from pathlib import Path

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "core.yaml"

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """Создание подключения к PostgreSQL."""

    database_url = (
        f"postgresql+psycopg://"
        f"{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

    return create_engine(
        database_url,
        pool_pre_ping=True,
    )


def load_config() -> dict:
    """Загрузка конфигурации SQL-скриптов."""

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config


def execute_script(
    engine: Engine,
    script: dict,
    transaction_per_script: bool = True,
) -> None:
    """Выполнение одного SQL-скрипта."""

    script_path = (BASE_DIR / script["file"]).resolve()

    # Не разрешаем выходить за пределы директории core
    if not script_path.is_relative_to(BASE_DIR.resolve()):
        raise ValueError(f"Недопустимый путь: {script['file']}")

    if not script_path.is_file():
        raise FileNotFoundError(f"SQL-файл не найден: {script_path}")

    logger.info(
        "Выполнение скрипта: %s | %s",
        script["name"],
        script["description"],
    )

    sql = script_path.read_text(encoding="utf-8")

    if transaction_per_script:
        with engine.begin() as connection:
            connection.exec_driver_sql(sql)
    else:
        with engine.connect() as connection:
            connection.exec_driver_sql(sql)
            connection.commit()

    logger.info("Успешно выполнен: %s", script["name"])


def run():
    config = load_config()
    engine = get_engine()

    settings = config.get("settings", {})
    stop_on_error = settings.get("stop_on_error", True)
    transaction_per_script = settings.get(
        "transaction_per_script", True
    )

    scripts = sorted(
        config.get("scripts", []),
        key=lambda item: item["order"],
    )

    for script in scripts:
        if not script.get("enabled", True):
            logger.info("Пропуск: %s", script["name"])
            continue

        try:
            execute_script(
                engine,
                script,
                transaction_per_script,
            )

        except Exception:
            logger.exception(
                "Ошибка выполнения: %s",
                script["name"],
            )

            if stop_on_error:
                raise

    engine.dispose()


if __name__ == "__main__":
    run()
