from airflow.providers.postgres.hooks.postgres import PostgresHook


def validate_dependencies(cursor) -> None:
    """Проверяет наличие справочников и курсов для загрузки фактов."""

    cursor.execute("SELECT COUNT(*) FROM raw.prices")
    prices_count = cursor.fetchone()[0]

    if prices_count == 0:
        return

    cursor.execute("SELECT COUNT(*) FROM core.dim_city")
    cities_count = cursor.fetchone()[0]

    if cities_count == 0:
        raise ValueError(
            "core.dim_city пустая. Сначала загрузи города в Core."
        )

    cursor.execute("SELECT COUNT(*) FROM raw.exchange_rates")
    rates_count = cursor.fetchone()[0]

    if rates_count == 0:
        raise ValueError(
            "raw.exchange_rates пустая. "
            "Сначала загрузи курсы валют в RAW."
        )

    # Проверяем, нет ли разных курсов для одной валюты и даты.
    cursor.execute("""
        WITH latest_rates AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                payload
            FROM raw.exchange_rates
            ORDER BY source_id, extracted_at DESC, batch_id DESC
        ),
        parsed_rates AS (
            SELECT
                (payload->>'num_code')::INTEGER AS num_code,
                (payload->>'setting_date')::DATE AS setting_date,
                (payload->>'value')::NUMERIC AS rate
            FROM latest_rates
        )
        SELECT
            num_code,
            setting_date,
            COUNT(DISTINCT rate) AS rate_count
        FROM parsed_rates
        GROUP BY num_code, setting_date
        HAVING COUNT(DISTINCT rate) > 1
        LIMIT 20
    """)

    conflicts = cursor.fetchall()

    if conflicts:
        raise ValueError(
            "Найдены разные курсы для одной валюты и даты: "
            f"{conflicts}"
        )


def create_stage_prices(cursor) -> None:
    """
    Создаёт временную таблицу:
    последняя версия prices + города + валютные курсы + DQ-флаг.
    """

    cursor.execute("""
        CREATE TEMP TABLE stage_prices_core
        ON COMMIT DROP
        AS

        WITH latest_prices AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                batch_id,
                payload
            FROM raw.prices
            ORDER BY
                source_id,
                extracted_at DESC,
                batch_id DESC
        ),

        latest_rates AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                payload
            FROM raw.exchange_rates
            ORDER BY
                source_id,
                extracted_at DESC,
                batch_id DESC
        ),

        rate_rows AS (
            SELECT
                source_id,

                CASE
                    WHEN COALESCE(payload->>'num_code', '')
                         ~ '^[0-9]+$'
                    THEN (payload->>'num_code')::INTEGER
                END AS num_code,

                CASE
                    WHEN COALESCE(payload->>'setting_date', '')
                         ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                    THEN (payload->>'setting_date')::DATE
                END AS setting_date,

                CASE
                    WHEN COALESCE(payload->>'value', '')
                         ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN (payload->>'value')::NUMERIC
                END AS rate

            FROM latest_rates
        ),

        rates AS (
            SELECT DISTINCT ON (num_code, setting_date)
                num_code,
                setting_date,
                rate
            FROM rate_rows
            WHERE
                num_code IS NOT NULL
                AND setting_date IS NOT NULL
                AND rate IS NOT NULL
            ORDER BY
                num_code,
                setting_date,
                source_id
        ),

        parsed_prices AS (
            SELECT
                p.source_id AS price_id,
                p.batch_id,
                p.payload,

                CASE
                    WHEN COALESCE(p.payload->>'loading_city_gcid', '')
                         ~ '^[0-9]+$'
                    THEN (p.payload->>'loading_city_gcid')::BIGINT
                END AS loading_city_gcid,

                CASE
                    WHEN COALESCE(p.payload->>'organization_id', '')
                        ~ '^[0-9]+$'
                    THEN (p.payload->>'organization_id')::BIGINT
                END AS organization_id,

                CASE
                    WHEN COALESCE(p.payload->>'unloading_city_gcid', '')
                         ~ '^[0-9]+$'
                    THEN (p.payload->>'unloading_city_gcid')::BIGINT
                END AS unloading_city_gcid,

                NULLIF(p.payload->>'loading_date', '')::DATE
                    AS loading_date,

                NULLIF(p.payload->>'end_loading_date', '')::DATE
                    AS end_loading_date,

                NULLIF(p.payload->>'created_at', '')::TIMESTAMPTZ
                    AS created_at,

                CASE
                    WHEN COALESCE(p.payload->>'distance', '')
                         ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN (p.payload->>'distance')::NUMERIC
                END AS distance,

                CASE
                    WHEN COALESCE(p.payload->>'total_weight', '')
                         ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN (p.payload->>'total_weight')::NUMERIC
                END AS total_weight,

                CASE
                    WHEN COALESCE(p.payload->>'total_volume', '')
                         ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN (p.payload->>'total_volume')::NUMERIC
                END AS total_volume,

                CASE
                    WHEN COALESCE(p.payload->>'price', '')
                         ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN (p.payload->>'price')::NUMERIC
                END AS price,

                CASE
                    WHEN COALESCE(p.payload->>'currency_code', '')
                         ~ '^[0-9]+$'
                    THEN (p.payload->>'currency_code')::INTEGER
                END AS currency_code,

                CASE
                    WHEN COALESCE(p.payload->>'nearest_rate_date', '')
                         ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                    THEN (p.payload->>'nearest_rate_date')::DATE
                END AS nearest_rate_date,

                NULLIF(p.payload->>'price_type', '')::INTEGER
                    AS price_type,

                NULLIF(p.payload->>'is_fact', '')::BOOLEAN
                    AS is_fact,

                COALESCE(
                    NULLIF(p.payload->>'complex', '')::BOOLEAN,
                    FALSE
                ) AS complex,

                COALESCE(
                    NULLIF(p.payload->>'outlier_mark', '')::BOOLEAN,
                    FALSE
                ) AS outlier_mark,

                p.payload->>'weight_category' AS weight_category,
                p.payload->>'volume_category' AS volume_category

            FROM latest_prices p
        ),

        enriched_prices AS (
            SELECT
                p.*,

                loading.city_id AS loading_city_id,
                unloading.city_id AS unloading_city_id,

                r.rate AS exchange_rate

            FROM parsed_prices p

            LEFT JOIN core.dim_city loading
                ON loading.gc_id = p.loading_city_gcid

            LEFT JOIN core.dim_city unloading
                ON unloading.gc_id = p.unloading_city_gcid

            LEFT JOIN rates r
                ON r.num_code = p.currency_code
                AND r.setting_date = p.nearest_rate_date
        )

        SELECT
            e.*,

            CASE
                WHEN e.price IS NOT NULL
                     AND e.exchange_rate IS NOT NULL
                THEN e.price * e.exchange_rate
            END AS price_rub,

            (
                e.price IS NOT NULL
                AND e.price > 0

                AND e.distance IS NOT NULL
                AND e.distance > 0

                AND (
                    e.total_weight IS NULL
                    OR e.total_weight >= 0
                )

                AND (
                    e.total_volume IS NULL
                    OR e.total_volume >= 0
                )

                AND e.loading_city_id IS NOT NULL
                AND e.unloading_city_id IS NOT NULL

                AND e.currency_code IS NOT NULL
                AND e.nearest_rate_date IS NOT NULL

                AND e.exchange_rate IS NOT NULL
                AND e.exchange_rate > 0
            ) AS is_valid

        FROM enriched_prices e
    """)


def load_quarantine(cursor) -> int:
    """Записывает невалидные цены и причины отклонения."""

    cursor.execute("""
        INSERT INTO core.dq_quarantine_prices (
            source_price_id,
            payload,
            reason_code,
            reason_details,
            batch_id
        )

        SELECT
            s.price_id,
            s.payload,
            dq.reason_code,
            dq.reason_details,
            s.batch_id

        FROM stage_prices_core s

        CROSS JOIN LATERAL (
            VALUES
                (
                    'MISSING_PRICE',
                    s.price IS NULL,
                    'Цена отсутствует или не является числом'
                ),
                (
                    'INVALID_PRICE',
                    s.price IS NOT NULL AND s.price <= 0,
                    'Цена должна быть больше нуля'
                ),
                (
                    'INVALID_DISTANCE',
                    s.distance IS NULL OR s.distance <= 0,
                    'Расстояние отсутствует или не больше нуля'
                ),
                (
                    'NEGATIVE_WEIGHT',
                    s.total_weight < 0,
                    'Вес не может быть отрицательным'
                ),
                (
                    'NEGATIVE_VOLUME',
                    s.total_volume < 0,
                    'Объём не может быть отрицательным'
                ),
                (
                    'MISSING_LOADING_CITY',
                    s.loading_city_id IS NULL,
                    'Город загрузки отсутствует или не найден в dim_city'
                ),
                (
                    'MISSING_UNLOADING_CITY',
                    s.unloading_city_id IS NULL,
                    'Город выгрузки отсутствует или не найден в dim_city'
                ),
                (
                    'MISSING_CURRENCY_CODE',
                    s.currency_code IS NULL,
                    'Код валюты отсутствует или некорректен'
                ),
                (
                    'MISSING_EXCHANGE_RATE',
                    s.currency_code IS NOT NULL
                    AND (
                        s.nearest_rate_date IS NULL
                        OR s.exchange_rate IS NULL
                    ),
                    'Не найден точный валютный курс на nearest_rate_date'
                ),
                (
                    'INVALID_EXCHANGE_RATE',
                    s.exchange_rate IS NOT NULL
                    AND s.exchange_rate <= 0,
                    'Валютный курс должен быть больше нуля'
                )
        ) AS dq(reason_code, is_invalid, reason_details)

        WHERE dq.is_invalid

        ON CONFLICT (
            source_price_id,
            batch_id,
            reason_code
        ) DO NOTHING
    """)

    return cursor.rowcount


def delete_invalid_facts(cursor) -> int:
    """Удаляет из фактов цены, последняя версия которых невалидна."""

    cursor.execute("""
        DELETE FROM core.fact_prices f
        USING stage_prices_core s
        WHERE f.price_id = s.price_id
          AND NOT s.is_valid
    """)

    return cursor.rowcount


def upsert_fact_prices(cursor) -> int:
    """Загружает валидные цены в core.fact_prices."""

    cursor.execute("""
        INSERT INTO core.fact_prices (
            price_id,
            organization_id,
            loading_city_id,
            unloading_city_id,
            loading_date,
            end_loading_date,
            created_at,
            distance,
            total_weight,
            total_volume,
            price,
            currency_code,
            price_rub,
            price_type,
            is_fact,
            complex,
            nearest_rate_date,
            outlier_mark,
            weight_category,
            volume_category
        )

        SELECT
            price_id,
            organization_id,
            loading_city_id,
            unloading_city_id,
            loading_date,
            end_loading_date,
            created_at,
            distance,
            total_weight,
            total_volume,
            price,
            currency_code,
            price_rub,
            price_type,
            is_fact,
            complex,
            nearest_rate_date,
            outlier_mark,
            weight_category,
            volume_category

        FROM stage_prices_core
        WHERE is_valid

        ON CONFLICT (price_id)
        DO UPDATE SET
            loading_city_id = EXCLUDED.loading_city_id,
            organization_id = EXCLUDED.organization_id,
            unloading_city_id = EXCLUDED.unloading_city_id,
            loading_date = EXCLUDED.loading_date,
            end_loading_date = EXCLUDED.end_loading_date,
            created_at = EXCLUDED.created_at,
            distance = EXCLUDED.distance,
            total_weight = EXCLUDED.total_weight,
            total_volume = EXCLUDED.total_volume,
            price = EXCLUDED.price,
            currency_code = EXCLUDED.currency_code,
            price_rub = EXCLUDED.price_rub,
            price_type = EXCLUDED.price_type,
            is_fact = EXCLUDED.is_fact,
            complex = EXCLUDED.complex,
            nearest_rate_date = EXCLUDED.nearest_rate_date,
            outlier_mark = EXCLUDED.outlier_mark,
            weight_category = EXCLUDED.weight_category,
            volume_category = EXCLUDED.volume_category,
            loaded_at = NOW()

        WHERE ROW(
            core.fact_prices.organization_id,
            core.fact_prices.loading_city_id,
            core.fact_prices.unloading_city_id,
            core.fact_prices.loading_date,
            core.fact_prices.end_loading_date,
            core.fact_prices.created_at,
            core.fact_prices.distance,
            core.fact_prices.total_weight,
            core.fact_prices.total_volume,
            core.fact_prices.price,
            core.fact_prices.currency_code,
            core.fact_prices.price_rub,
            core.fact_prices.price_type,
            core.fact_prices.is_fact,
            core.fact_prices.complex,
            core.fact_prices.nearest_rate_date,
            core.fact_prices.outlier_mark,
            core.fact_prices.weight_category,
            core.fact_prices.volume_category
        ) IS DISTINCT FROM ROW(
            EXCLUDED.organization_id,
            EXCLUDED.loading_city_id,
            EXCLUDED.unloading_city_id,
            EXCLUDED.loading_date,
            EXCLUDED.end_loading_date,
            EXCLUDED.created_at,
            EXCLUDED.distance,
            EXCLUDED.total_weight,
            EXCLUDED.total_volume,
            EXCLUDED.price,
            EXCLUDED.currency_code,
            EXCLUDED.price_rub,
            EXCLUDED.price_type,
            EXCLUDED.is_fact,
            EXCLUDED.complex,
            EXCLUDED.nearest_rate_date,
            EXCLUDED.outlier_mark,
            EXCLUDED.weight_category,
            EXCLUDED.volume_category
        )
    """)

    return cursor.rowcount


def load_prices_core(
    conn_id: str = "dwh_postgres",
) -> dict:
    """Загружает актуальные цены из RAW в Core."""

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        with conn:
            with conn.cursor() as cursor:

                validate_dependencies(cursor)

                cursor.execute("SELECT COUNT(*) FROM raw.prices")
                raw_rows = cursor.fetchone()[0]

                if raw_rows == 0:
                    return {
                        "source_rows": 0,
                        "valid_rows": 0,
                        "quarantined_prices": 0,
                        "status": "SUCCESS",
                    }

                create_stage_prices(cursor)

                cursor.execute("""
                    SELECT
                        COUNT(*),
                        COUNT(*) FILTER (WHERE is_valid),
                        COUNT(*) FILTER (WHERE NOT is_valid)
                    FROM stage_prices_core
                """)

                source_count, valid_count, invalid_count = cursor.fetchone()

                quarantine_rows = load_quarantine(cursor)

                deleted_facts = delete_invalid_facts(cursor)

                upserted_rows = upsert_fact_prices(cursor)

        return {
            "source_rows": source_count,
            "valid_rows": valid_count,
            "invalid_prices": invalid_count,
            "quarantine_rows": quarantine_rows,
            "deleted_invalid_facts": deleted_facts,
            "upserted_rows": upserted_rows,
            "status": "SUCCESS",
        }

    finally:
        conn.close()
