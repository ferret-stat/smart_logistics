from airflow.providers.postgres.hooks.postgres import PostgresHook


LATEST_CITIES_CTE = """
    WITH latest_cities AS (
        SELECT DISTINCT ON (source_id)
            source_id,
            payload
        FROM raw.cities
        ORDER BY
            source_id,
            extracted_at DESC,
            batch_id DESC
    )
"""


def validate_cities(cursor) -> None:
    # Проверяем, что gc_id заполнен и содержит целое число.
    cursor.execute(
        LATEST_CITIES_CTE
        + """
        SELECT COUNT(*)
        FROM latest_cities
        WHERE COALESCE(
            NULLIF(BTRIM(payload->>'gc_id'), ''),
            ''
        ) !~ '^-?[0-9]+$'
        """
    )

    invalid_count = cursor.fetchone()[0]

    if invalid_count:
        raise ValueError(
            f"В raw.cities найдено записей с некорректным gc_id: "
            f"{invalid_count}"
        )
    
    cursor.execute(
        LATEST_CITIES_CTE
        + """
        SELECT
            (payload->>'gc_id')::BIGINT AS gc_id,
            ARRAY_AGG(
                DISTINCT BTRIM(payload->>'name')
                ORDER BY BTRIM(payload->>'name')
            ) AS names
        FROM latest_cities
        GROUP BY (payload->>'gc_id')::BIGINT
        HAVING COUNT(
            DISTINCT LOWER(NULLIF(BTRIM(payload->>'name'), ''))
        ) > 1
        """
    )

    conflicts = cursor.fetchall()

    if conflicts:
        raise ValueError(
            f"Обнаружены разные названия для одинакового gc_id: "
            f"{conflicts}"
        )


def upsert_dim_city(cursor) -> int:
    """Загружает уникальные города в core.dim_city."""

    cursor.execute(
        LATEST_CITIES_CTE
        + """
        , city_rows AS (
            SELECT
                source_id,
                (payload->>'gc_id')::BIGINT AS gc_id,
                NULLIF(BTRIM(payload->>'name'), '') AS city_name,
                NULLIF(BTRIM(payload->>'full_name_local'), '')
                    AS full_name_local,
                NULLIF(BTRIM(payload->>'full_name_gc'), '')
                    AS full_name_gc,
                NULLIF(payload->>'latitude', '')::NUMERIC AS latitude,
                NULLIF(payload->>'longitude', '')::NUMERIC AS longitude
            FROM latest_cities
        ),
        canonical_cities AS (
            SELECT DISTINCT ON (gc_id)
                gc_id,
                city_name,
                full_name_local,
                full_name_gc,
                latitude,
                longitude
            FROM city_rows
            ORDER BY gc_id, source_id
        )
        INSERT INTO core.dim_city (
            gc_id,
            city_name,
            full_name_local,
            full_name_gc,
            latitude,
            longitude
        )
        SELECT
            gc_id,
            city_name,
            full_name_local,
            full_name_gc,
            latitude,
            longitude
        FROM canonical_cities
        ON CONFLICT (gc_id)
        DO UPDATE SET
            city_name = EXCLUDED.city_name,
            full_name_local = EXCLUDED.full_name_local,
            full_name_gc = EXCLUDED.full_name_gc,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
        WHERE ROW(
            core.dim_city.city_name,
            core.dim_city.full_name_local,
            core.dim_city.full_name_gc,
            core.dim_city.latitude,
            core.dim_city.longitude
        ) IS DISTINCT FROM ROW(
            EXCLUDED.city_name,
            EXCLUDED.full_name_local,
            EXCLUDED.full_name_gc,
            EXCLUDED.latitude,
            EXCLUDED.longitude
        )
        """
    )

    return cursor.rowcount


def upsert_map_source_city(cursor) -> int:
    """Связывает все исходные ID с каноническими городами."""

    cursor.execute(
        LATEST_CITIES_CTE
        + """
        INSERT INTO core.map_source_city (
            source_city_id,
            city_id
        )
        SELECT
            lc.source_id,
            dc.city_id
        FROM latest_cities lc
        JOIN core.dim_city dc
            ON dc.gc_id = (lc.payload->>'gc_id')::BIGINT
        ON CONFLICT (source_city_id)
        DO UPDATE SET
            city_id = EXCLUDED.city_id
        WHERE core.map_source_city.city_id
              IS DISTINCT FROM EXCLUDED.city_id
        """
    )

    return cursor.rowcount


def load_cities_core(
    conn_id: str = "dwh_postgres",
) -> dict:
    """Загружает RAW cities в Core."""

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        with conn:
            with conn.cursor() as cursor:
                validate_cities(cursor)

                dim_city_changed = upsert_dim_city(cursor)

                mapping_changed = upsert_map_source_city(cursor)

                cursor.execute(
                    LATEST_CITIES_CTE
                    + """
                    SELECT COUNT(*)
                    FROM latest_cities
                    """
                )
                source_city_count = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*)
                    FROM core.dim_city
                """)
                dim_city_count = cursor.fetchone()[0]

        return {
            "source_cities": source_city_count,
            "dim_city_total": dim_city_count,
            "dim_city_changed": dim_city_changed,
            "mapping_changed": mapping_changed,
            "status": "SUCCESS",
        }

    finally:
        conn.close()
