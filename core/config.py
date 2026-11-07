import os

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent

class Settings:
    batch_size = int(os.getenv('BATCH_SIZE', '10000'))

    oltp_db = os.getenv('OLTP_DB')
    oltp_user = os.getenv('OLTP_USER')
    oltp_pass = os.getenv('OLTP_PASS')

    dwh_db = os.getenv('DWH_DB')
    dwh_user = os.getenv('DWH_USER')
    dwh_pass = os.getenv('DWH_PASS')
    

settings = Settings()