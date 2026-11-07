
import json
import re

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context

from core.config import settings


VOLUME_DIR = Path("/opt/airflow/volumes")
BATCH_SIZE = settings.batch_size


def json_serializer(value):
    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    raise TypeError(f"Unsupported type: {type(value)}")


def get_file_name(table_name: str) -> str:
    context = get_current_context()

    run_id = re.sub(
        r"[^a-zA-Z0-9_-]",
        "_",
        context["run_id"],
    )

    return f"{table_name}_{run_id}.jsonl"


def export_data(
    conn_id: str,
    table_name: str,
) -> str:


    if BATCH_SIZE <= 0:
        raise ValueError(f"Invalid BATCH_SIZE: {BATCH_SIZE}")

    export_dir = VOLUME_DIR / "export"
    export_dir.mkdir(parents=True, exist_ok=True)

    file_path = export_dir / get_file_name(table_name)

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        with conn.cursor() as schema_cursor:
            schema_cursor.execute(
                f'SELECT * FROM public."{table_name}" LIMIT 0'
            )

            columns = [
                desc[0]
                for desc in schema_cursor.description
            ]

        with conn.cursor(name=f"export_{table_name}") as cursor:

            cursor.itersize = BATCH_SIZE

            query = f'SELECT * FROM public."{table_name}" ORDER BY id'

            cursor.execute(query)

            row_count = 0

            with file_path.open("w", encoding="utf-8") as file:

                while True:
                    rows = cursor.fetchmany(BATCH_SIZE)

                    if not rows:
                        break

                    for row in rows:
                        record = dict(zip(columns, row))

                        file.write(
                            json.dumps(
                                record,
                                default=json_serializer,
                                ensure_ascii=False,
                            ) + "\n"
                        )

                        row_count += 1

            print(
                f"Export completed: table={table_name}, "
                f"rows={row_count}, file={file_path}"
            )

    finally:
        conn.close()

    return str(file_path)
