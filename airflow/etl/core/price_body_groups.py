from airflow.providers.postgres.hooks.postgres import PostgresHook


LATEST_PRICES_CTE = """
    WITH latest_prices AS (
        SELECT DISTINCT ON (source_id)
            source_id,
            payload
        FROM raw.prices
        ORDER BY source_id, extracted_at DESC, batch_id DESC
    )
"""

LATEST_LINKS_CTE = """
    latest_links AS (
        SELECT DISTINCT ON (source_id)
            source_id,
            payload
        FROM raw.price_body_types
        ORDER BY source_id, extracted_at DESC, batch_id DESC
    )
"""


def validate_price_body_groups(cursor) -> None:
    """Проверяет исходные данные перед загрузкой bridge."""

    # Проверяем формат prices.body_type.
    cursor.execute(
        LATEST_PRICES_CTE
        + """
        SELECT COUNT(*)
        FROM latest_prices
        WHERE
            NULLIF(BTRIM(payload->>'body_type'), '') IS NOT NULL
            AND BTRIM(payload->>'body_type') !~ '^[0-9]+$'
        """
    )

    invalid_count = cursor.fetchone()[0]

    if invalid_count:
        raise ValueError(
            f"Найдено цен с некорректным body_type: {invalid_count}"
        )

    # Проверяем, что каждый std_id существует в Core.
    cursor.execute(
        "WITH " + LATEST_LINKS_CTE
        + """
        SELECT
            l.source_id,
            l.payload
        FROM latest_links l
        LEFT JOIN core.map_source_body_type m
            ON m.source_body_type_id =
                (l.payload->>'body_type_id')::BIGINT
        WHERE m.source_body_type_id IS NULL
        LIMIT 20
        """
    )

    missing_groups = cursor.fetchall()

    if missing_groups:
        raise ValueError(
            f"Для prices.body_type не найдены группы: "
            f"{missing_groups}"
        )

    # Проверяем корректность ID в price_body_types.
    cursor.execute(
        """
        WITH latest_links AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                payload
            FROM raw.price_body_types
            ORDER BY source_id, extracted_at DESC, batch_id DESC
        )
        SELECT COUNT(*)
        FROM latest_links
        WHERE
            COALESCE(
                NULLIF(BTRIM(payload->>'price_id'), ''),
                ''
            ) !~ '^[0-9]+$'
            OR
            COALESCE(
                NULLIF(BTRIM(payload->>'body_type_id'), ''),
                ''
            ) !~ '^[0-9]+$'
        """
    )

    invalid_links = cursor.fetchone()[0]

    if invalid_links:
        raise ValueError(
            f"Найдено некорректных связей price_body_types: "
            f"{invalid_links}"
        )

    # Проверяем наличие исходных типов в mapping.
    cursor.execute(
        """WITH """ + 
        LATEST_LINKS_CTE
        .replace("    ", "")
        + """
        SELECT
            l.source_id,
            l.payload
        FROM latest_links l
        LEFT JOIN core.map_source_body_type m
            ON m.source_body_type_id =
                (l.payload->>'body_type_id')::BIGINT
        WHERE m.source_body_type_id IS NULL
        LIMIT 20
        """
    )

    missing_mappings = cursor.fetchall()

    if missing_mappings:
        raise ValueError(
            f"Не найдены mapping для price_body_types: "
            f"{missing_mappings}"
        )


def rebuild_price_body_group_bridge(
    conn_id: str = "dwh_postgres",
) -> dict:
    """
    Полностью пересобирает bridge_price_body_group.

    Источники:
    1. prices.body_type -> dim_body_group.std_id
    2. price_body_types.body_type_id -> map_source_body_type
    """

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        with conn:
            with conn.cursor() as cursor:

                validate_price_body_groups(cursor)

                # Удаляем старые связи.
                cursor.execute("""
                    DELETE FROM core.bridge_price_body_group
                """)

                deleted_rows = cursor.rowcount

                # Формируем новые связи из обоих источников.
                cursor.execute(
                    LATEST_PRICES_CTE
                    + ", "
                    + LATEST_LINKS_CTE
                    + """
                    , price_group_links AS (

                        -- Источник 1:
                        -- prices.body_type уже содержит std_id.
                        SELECT
                            p.source_id AS price_id,
                            bg.body_group_id
                        FROM latest_prices p
                        JOIN core.dim_body_group bg
                            ON bg.std_id = NULLIF(
                                                    BTRIM(p.payload->>'body_type'), ''
                                                )::BIGINT
                        WHERE
                            NULLIF(
                                BTRIM(p.payload->>'body_type'),
                                ''
                            ) IS NOT NULL

                        UNION

                        -- Источник 2:
                        -- Исходный body_type_id преобразуем через mapping.
                        SELECT
                            (l.payload->>'price_id')::BIGINT
                                AS price_id,
                            m.body_group_id
                        FROM latest_links l
                        JOIN core.map_source_body_type m
                            ON m.source_body_type_id =
                                (l.payload->>'body_type_id')::BIGINT
                    )

                    INSERT INTO core.bridge_price_body_group (
                        price_id,
                        body_group_id
                    )
                    SELECT DISTINCT
                        links.price_id,
                        links.body_group_id
                    FROM price_group_links links
                    JOIN core.fact_prices fp
                        ON fp.price_id = links.price_id
                    """
                )

                inserted_rows = cursor.rowcount

        return {
            "deleted_rows": deleted_rows,
            "inserted_rows": inserted_rows,
            "status": "SUCCESS",
        }

    finally:
        conn.close()
