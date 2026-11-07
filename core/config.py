import os
import yaml

from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import URL

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent

class Settings:
    pg_db = os.getenv('POSTGRES_DB')
    pg_user = os.getenv('POSTGRES_USER')
    pg_pass = os.getenv('POSTGRES_PASS')
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = int(os.getenv('POSTGRES_PORT', '5432'))
    batch_size = int(os.getenv('BATCH_SIZE', '10000'))

    if not all([pg_user, pg_pass, pg_db, batch_size]):
            raise ValueError("Missing PostgreSQL environment variables")
    
    database_url = URL.create(
        drivername="postgresql+psycopg",
        username=pg_user,
        password=pg_pass,
        host=pg_host,
        port=pg_port,
        database=pg_db,
    )

    config_path = BASE_DIR / "core.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(
            f"Missing YAML config: {config_path}"
        )

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

settings = Settings()