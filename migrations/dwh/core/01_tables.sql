-- Города. gc_id в источнике не уникален,
-- поэтому в Core используем отдельный surrogate key.

CREATE TABLE IF NOT EXISTS core.dim_city (
    city_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_city_id  BIGINT NOT NULL,
    gc_id           BIGINT NOT NULL,
    city_name       TEXT,
    region_name     TEXT,
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),

    UNIQUE (source_city_id)
);

CREATE INDEX IF NOT EXISTS idx_core_city_gcid
    ON core.dim_city (gc_id);

-- Стандартизированные группы кузовов.
CREATE TABLE IF NOT EXISTS core.dim_body_group (
    body_group_id   INTEGER PRIMARY KEY,
    group_name      TEXT NOT NULL UNIQUE
);


-- Факты сделок.
CREATE TABLE IF NOT EXISTS core.fact_prices (
    price_id            BIGINT PRIMARY KEY,

    loading_city_id     BIGINT REFERENCES core.dim_city(city_id),
    unloading_city_id   BIGINT REFERENCES core.dim_city(city_id),

    loading_date        DATE,
    end_loading_date    DATE,
    created_at          TIMESTAMPTZ,

    distance            NUMERIC(12, 2),
    total_weight        NUMERIC(12, 2),
    total_volume        NUMERIC(12, 2),

    price               NUMERIC(18, 2),
    currency_code       INTEGER,
    price_rub           NUMERIC(18, 2),

    price_type          INTEGER,
    is_fact             BOOLEAN,
    complex             BOOLEAN DEFAULT FALSE,

    nearest_rate_date   DATE,
    outlier_mark        BOOLEAN DEFAULT FALSE,

    weight_category     TEXT,
    volume_category    TEXT,

    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_prices_loading_date
    ON core.fact_prices (loading_date);

CREATE INDEX IF NOT EXISTS idx_core_prices_route
    ON core.fact_prices (loading_city_id, unloading_city_id);


-- Связь сделок с группами кузова.
-- Одна сделка может относиться к нескольким группам.
CREATE TABLE IF NOT EXISTS core.bridge_price_body_group (
    price_id        BIGINT NOT NULL
        REFERENCES core.fact_prices(price_id) ON DELETE CASCADE,

    body_group_id   INTEGER NOT NULL
        REFERENCES core.dim_body_group(body_group_id),

    PRIMARY KEY (price_id, body_group_id)
);


-- Карантин невалидных записей.
CREATE TABLE IF NOT EXISTS core.dq_quarantine_prices (
    quarantine_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_price_id BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    reason_code     TEXT NOT NULL,
    reason_details  TEXT,
    batch_id        UUID NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantine_batch
    ON core.dq_quarantine_prices (batch_id);
