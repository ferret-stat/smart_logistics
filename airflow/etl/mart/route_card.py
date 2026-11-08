from airflow.providers.postgres.hooks.postgres import PostgresHook


def rebuild_mart_route_card(
    conn_id: str = "dwh_postgres",
) -> dict:
    """Полностью пересобирает mart.mart_route_card"""

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TEMP TABLE stage_mart_route_card
                    ON COMMIT DROP
                    AS

                    WITH monthly AS (

                        SELECT
                            DATE_TRUNC(
                                'month',
                                f.loading_date
                            )::DATE AS month_date,

                            f.loading_city_id AS origin_city_id,

                            f.unloading_city_id
                                AS destination_city_id,

                            b.body_group_id,

                            COALESCE(
                                NULLIF(BTRIM(f.weight_category), ''),
                                'UNKNOWN'
                            ) AS weight_category,

                            COUNT(DISTINCT f.price_id)
                                AS deals_count,

                            COUNT(DISTINCT f.organization_id)
                                AS carriers_count,

                            ROUND(
                                AVG(f.price_rub),
                                2
                            )::NUMERIC(18, 2) AS avg_price_rub,

                            ROUND(
                                (
                                    PERCENTILE_CONT(0.25)
                                    WITHIN GROUP (
                                        ORDER BY
                                            f.price_rub::DOUBLE PRECISION
                                    )
                                )::NUMERIC,
                                2
                            )::NUMERIC(18, 2) AS p25_price_rub,

                            ROUND(
                                (
                                    PERCENTILE_CONT(0.50)
                                    WITHIN GROUP (
                                        ORDER BY
                                            f.price_rub::DOUBLE PRECISION
                                    )
                                )::NUMERIC,
                                2
                            )::NUMERIC(18, 2) AS median_price_rub,

                            ROUND(
                                (
                                    PERCENTILE_CONT(0.75)
                                    WITHIN GROUP (
                                        ORDER BY
                                            f.price_rub::DOUBLE PRECISION
                                    )
                                )::NUMERIC,
                                2
                            )::NUMERIC(18, 2) AS p75_price_rub,

                            ROUND(
                                AVG(
                                    f.price_rub /
                                    NULLIF(f.distance, 0)
                                ),
                                4
                            )::NUMERIC(18, 4) AS avg_km_price,

                            ROUND(
                                STDDEV_POP(f.price_rub)
                                /
                                NULLIF(AVG(f.price_rub), 0),
                                4
                            )::NUMERIC(12, 4) AS cv_price

                        FROM core.fact_prices f

                        JOIN core.bridge_price_body_group b
                            ON b.price_id = f.price_id

                        WHERE
                            f.loading_date IS NOT NULL
                            AND f.price_rub IS NOT NULL
                            AND f.price_rub > 0
                            AND f.distance > 0

                        GROUP BY
                            1, 2, 3, 4, 5
                    ),

                    dynamics AS (

                        SELECT
                            current_month.*,

                            -- Изменение относительно предыдущего
                            -- календарного месяца.
                            ROUND(
                                (
                                    current_month.avg_price_rub
                                    - previous_month.avg_price_rub
                                )
                                /
                                NULLIF(
                                    previous_month.avg_price_rub,
                                    0
                                ) * 100,
                                2
                            )::NUMERIC(12, 2) AS mom_change_pct,

                            -- Изменение относительно того же месяца
                            -- предыдущего календарного года.
                            ROUND(
                                (
                                    current_month.avg_price_rub
                                    - previous_year.avg_price_rub
                                )
                                /
                                NULLIF(
                                    previous_year.avg_price_rub,
                                    0
                                ) * 100,
                                2
                            )::NUMERIC(12, 2) AS yoy_change_pct

                        FROM monthly current_month

                        LEFT JOIN monthly previous_month
                            ON previous_month.month_date =
                                (
                                    current_month.month_date
                                    - INTERVAL '1 month'
                                )::DATE

                            AND previous_month.origin_city_id =
                                current_month.origin_city_id

                            AND previous_month.destination_city_id =
                                current_month.destination_city_id

                            AND previous_month.body_group_id =
                                current_month.body_group_id

                            AND previous_month.weight_category =
                                current_month.weight_category

                        LEFT JOIN monthly previous_year
                            ON previous_year.month_date =
                                (
                                    current_month.month_date
                                    - INTERVAL '1 year'
                                )::DATE

                            AND previous_year.origin_city_id =
                                current_month.origin_city_id

                            AND previous_year.destination_city_id =
                                current_month.destination_city_id

                            AND previous_year.body_group_id =
                                current_month.body_group_id

                            AND previous_year.weight_category =
                                current_month.weight_category
                    )

                    SELECT
                        dynamics.*,

                        CASE
                            WHEN deals_count >= 30
                                AND cv_price <= 0.30
                            THEN 'HIGH'

                            WHEN deals_count >= 10
                                AND cv_price <= 0.50
                            THEN 'MEDIUM'

                            ELSE 'LOW'
                        END AS reliability

                    FROM dynamics
                """)

                cursor.execute("""
                    SELECT
                        COUNT(*),
                        MIN(month_date),
                        MAX(month_date)
                    FROM stage_mart_route_card
                """)

                staged_rows, first_month, last_month = (
                    cursor.fetchone()
                )

                cursor.execute("""
                    SELECT COUNT(*)
                    FROM core.fact_prices
                """)

                fact_count = cursor.fetchone()[0]

                if fact_count > 0 and staged_rows == 0:
                    raise ValueError(
                        "Агрегация Mart вернула 0 строк, "
                        "хотя core.fact_prices не пустая. "
                        "Проверь наличие bridge и загрузку фактов."
                    )

                cursor.execute("""
                    DELETE FROM mart.mart_route_card
                """)

                deleted_rows = cursor.rowcount

                cursor.execute("""
                    INSERT INTO mart.mart_route_card (
                        month_date,
                        origin_city_id,
                        destination_city_id,
                        body_group_id,
                        weight_category,
                        deals_count,
                        carriers_count,
                        avg_price_rub,
                        p25_price_rub,
                        median_price_rub,
                        p75_price_rub,
                        avg_km_price,
                        cv_price,
                        mom_change_pct,
                        yoy_change_pct,
                        reliability
                    )
                    SELECT
                        month_date,
                        origin_city_id,
                        destination_city_id,
                        body_group_id,
                        weight_category,
                        deals_count,
                        carriers_count,
                        avg_price_rub,
                        p25_price_rub,
                        median_price_rub,
                        p75_price_rub,
                        avg_km_price,
                        cv_price,
                        mom_change_pct,
                        yoy_change_pct,
                        reliability
                    FROM stage_mart_route_card
                """)

                inserted_rows = cursor.rowcount

                cursor.execute("""
                    SELECT
                        COUNT(*),
                        COUNT(DISTINCT month_date)
                    FROM mart.mart_route_card
                """)

                mart_rows, months_count = cursor.fetchone()

                cursor.execute("""
                    SELECT COUNT(*)
                    FROM core.fact_prices f
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM core.bridge_price_body_group b
                        WHERE b.price_id = f.price_id
                    )
                """)

                facts_without_body_group = cursor.fetchone()[0]

        return {
            "deleted_rows": deleted_rows,
            "inserted_rows": inserted_rows,
            "mart_rows": mart_rows,
            "months_count": months_count,
            "first_month": first_month.isoformat()
                if first_month else None,
            "last_month": last_month.isoformat()
                if last_month else None,
            "facts_without_body_group": facts_without_body_group,
            "status": "SUCCESS",
        }

    finally:
        conn.close()