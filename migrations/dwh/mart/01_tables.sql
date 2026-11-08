
CREATE TABLE IF NOT EXISTS mart.mart_route_card (
    month_date          DATE NOT NULL,

    origin_city_id      BIGINT NOT NULL,
    destination_city_id BIGINT NOT NULL,
    body_group_id       BIGINT NOT NULL,
    weight_category     TEXT NOT NULL,

    deals_count         BIGINT NOT NULL DEFAULT 0,
    carriers_count      BIGINT NOT NULL DEFAULT 0, v  

    avg_price_rub       NUMERIC(18, 2),
    p25_price_rub       NUMERIC(18, 2),
    median_price_rub    NUMERIC(18, 2),
    p75_price_rub       NUMERIC(18, 2),

    avg_km_price        NUMERIC(18, 4),
    cv_price            NUMERIC(12, 4),

    mom_change_pct      NUMERIC(12, 2),
    yoy_change_pct      NUMERIC(12, 2),

    reliability         TEXT NOT NULL,
    calculated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        month_date,
        origin_city_id,
        destination_city_id,
        body_group_id,
        weight_category
    )
);


CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    prediction_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    price_id            BIGINT NOT NULL,
    loading_date        DATE NOT NULL,

    dataset_type        TEXT NOT NULL,
    predicted_price_rub NUMERIC(18, 2),

    actual_price_rub    NUMERIC(18, 2),
    abs_error           NUMERIC(18, 2),
    ape                 NUMERIC(12, 4),

    model_version       TEXT NOT NULL,
    predicted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (price_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_predictions_loading_date
    ON mart.ml_price_predictions (loading_date);