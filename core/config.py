import os

from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import URL

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent

class Settings:
    batch_size = int(os.getenv('BATCH_SIZE', '10000'))

    oltp_db = os.getenv('OLTP_DB')
    oltp_user = os.getenv('OLTP_USER')
    oltp_pass = os.getenv('OLTP_PASS')
    oltp_host = os.getenv("OLTP_HOST", "localhost")
    oltp_port = int(os.getenv('OLTP_PORT', '5432'))

    dwh_db = os.getenv('DWH_DB')
    dwh_user = os.getenv('DWH_USER')
    dwh_pass = os.getenv('DWH_PASS')
    dwh_host = os.getenv("DWH_HOST", "localhost")
    dwh_port = int(os.getenv('DWH_PORT', '5434'))
    
    oltp_url = URL.create(
        drivername="postgresql+psycopg",
        username=oltp_user,
        password=oltp_pass,
        host=oltp_host,
        port=oltp_port,
        database=oltp_db,
    )

    dwh_url = URL.create(
            drivername="postgresql+psycopg",
            username=dwh_user,
            password=dwh_pass,
            host=dwh_host,
            port=dwh_port,
            database=dwh_db,
        )

    

settings = Settings()