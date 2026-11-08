from airflow.providers.postgres.hooks.postgres import PostgresHook


LATEST_BODY_TYPES_CTE = """
    WITH latest_body_types AS (
        SELECT DISTINCT ON (source_id)
            source_id,
            payload
        FROM raw.body_types
        ORDER BY
            source_id,
            extracted_at DESC,
            batch_id DESC
    )
"""



def validate_body_types(cursor) -> None:
    """Проверяет корректность исходных типов кузовов."""

    cursor.execute(
        LATEST_BODY_TYPES_CTE
        + """
        SELECT COUNT(*)
        FROM latest_body_types
        WHERE
            NULLIF(BTRIM(payload->>'std_id'), '') IS NULL
            OR BTRIM(payload->>'std_id') !~ '^-?[0-9]+$'
        """
    )

    invalid_count = cursor.fetchone()[0]

    if invalid_count:
        raise ValueError(
            f"Найдено типов кузова с некорректным std_id: "
            f"{invalid_count}"
        )

    # У каждой стандартизированной группы должно быть название.
    cursor.execute(
        LATEST_BODY_TYPES_CTE
        + """
        SELECT
            (payload->>'std_id')::BIGINT AS std_id
        FROM latest_body_types
        GROUP BY (payload->>'std_id')::BIGINT
        HAVING COUNT(
            NULLIF(BTRIM(payload->>'name'), '')
        ) = 0
        """
    )

    missing_names = cursor.fetchall()

    if missing_names:
        raise ValueError(
            f"У групп отсутствуют названия: {missing_names}"
        )




def upsert_dim_body_group(cursor) -> int:
    """Загружает уникальные стандартизированные группы."""

    cursor.execute(
        LATEST_BODY_TYPES_CTE
        + """
        , canonical_groups AS (
            SELECT DISTINCT ON (
                (payload->>'std_id')::BIGINT
            )
                (payload->>'std_id')::BIGINT AS std_id,
                NULLIF(
                    BTRIM(payload->>'name'), ''
                ) AS group_name
            FROM latest_body_types
            WHERE NULLIF(BTRIM(payload->>'name'), '') IS NOT NULL
            ORDER BY
                (payload->>'std_id')::BIGINT,
                source_id
        )
        INSERT INTO core.dim_body_group (
            std_id,
            group_name
        )
        SELECT
            std_id,
            group_name
        FROM canonical_groups
        ON CONFLICT (std_id)
        DO UPDATE SET
            group_name = EXCLUDED.group_name
        WHERE core.dim_body_group.group_name
              IS DISTINCT FROM EXCLUDED.group_name
        """
    )

    return cursor.rowcount


def upsert_map_source_body_type(cursor) -> int:
    """Сопоставляет исходные типы со стандартизированными группами."""

    cursor.execute(
        LATEST_BODY_TYPES_CTE
        + """
        INSERT INTO core.map_source_body_type (
            source_body_type_id,
            body_group_id,
            source_type_name
        )
        SELECT
            bt.source_id,
            bg.body_group_id,
            NULLIF(BTRIM(bt.payload->>'name'), '')
        FROM latest_body_types bt
        JOIN core.dim_body_group bg
            ON bg.std_id = (bt.payload->>'std_id')::BIGINT
        ON CONFLICT (source_body_type_id)
        DO UPDATE SET
            body_group_id = EXCLUDED.body_group_id,
            source_type_name = EXCLUDED.source_type_name
        WHERE ROW(
            core.map_source_body_type.body_group_id,
            core.map_source_body_type.source_type_name
        ) IS DISTINCT FROM ROW(
            EXCLUDED.body_group_id,
            EXCLUDED.source_type_name
        )
        """
    )

    return cursor.rowcount


def load_body_types_core(
    conn_id: str = "dwh_postgres",
) -> dict:
    """Загружает RAW body_types в Core."""

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        with conn:
            with conn.cursor() as cursor:
                validate_body_types(cursor)

                groups_changed = upsert_dim_body_group(cursor)

                mappings_changed = upsert_map_source_body_type(cursor)

                cursor.execute(
                    LATEST_BODY_TYPES_CTE
                    + """
                    SELECT COUNT(*)
                    FROM latest_body_types
                    """
                )

                source_type_count = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*)
                    FROM core.dim_body_group
                """)

                group_count = cursor.fetchone()[0]

        return {
            "source_types": source_type_count,
            "groups_total": group_count,
            "groups_changed": groups_changed,
            "mappings_changed": mappings_changed,
            "status": "SUCCESS",
        }

    finally:
        conn.close()
