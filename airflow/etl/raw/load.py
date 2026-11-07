import csv
import io
import json
import uuid

from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.config import settings

BATCH_SIZE = settings.batch_size


def load_data(
    conn_id: str,
    file_path: str,
    table_name: str,
    source_id_column: str = "source_id",
) -> dict:


    batch_id = str(uuid.uuid4())

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    total_rows = 0

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw.etl_batches (
                    batch_id,
                    status
                )
                VALUES (%s, 'RUNNING')
                """,
                (batch_id,),
            )

            cursor.execute(
                """
                CREATE TEMP TABLE stage_raw (
                    source_id BIGINT NOT NULL,
                    payload JSONB NOT NULL
                ) ON COMMIT PRESERVE ROWS
                """
            )

        conn.commit()

        with Path(file_path).open(
            "r",
            encoding="utf-8",
        ) as file:

            while True:
                buffer = io.StringIO()
                writer = csv.writer(buffer)

                batch_rows = 0

                for _ in range(BATCH_SIZE):
                    line = file.readline()

                    if not line:
                        break

                    record = json.loads(line)

                    source_id = record[source_id_column]

                    payload = json.dumps(
                        record,
                        ensure_ascii=False,
                    )

                    writer.writerow((source_id, payload))
                    batch_rows += 1

                if batch_rows == 0:
                    break

                buffer.seek(0)

                with conn.cursor() as cursor:

                    cursor.execute(
                        "TRUNCATE TABLE pg_temp.stage_raw"
                    )

                    cursor.copy_expert(
                        """
                        COPY pg_temp.stage_raw (
                            source_id,
                            payload
                        )
                        FROM STDIN
                        WITH (FORMAT CSV)
                        """,
                        buffer,
                    )

                    cursor.execute(
                        f"""
                        INSERT INTO raw.{table_name} (
                            source_id,
                            payload,
                            batch_id
                        )
                        SELECT
                            source_id,
                            payload,
                            %s
                        FROM pg_temp.stage_raw
                        ON CONFLICT (
                            source_id,
                            batch_id
                        )
                        DO UPDATE SET
                            payload = EXCLUDED.payload,
                            extracted_at = NOW()
                        """,
                        (batch_id,),
                    )

                conn.commit()

                total_rows += batch_rows

        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE raw.etl_batches
                SET
                    status = 'SUCCESS',
                    finished_at = NOW()
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

        conn.commit()

        return {
            "batch_id": batch_id,
            "table_name": table_name,
            "rows_loaded": total_rows,
        }

    except Exception as exc:
        conn.rollback()

        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE raw.etl_batches
                SET
                    status = 'FAILED',
                    finished_at = NOW(),
                    error_message = %s
                WHERE batch_id = %s
                """,
                (str(exc)[:4000], batch_id),
            )

        conn.commit()
        raise

    finally:
        conn.close()
