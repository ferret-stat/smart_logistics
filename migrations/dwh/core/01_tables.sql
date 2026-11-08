CREATE SCHEMA IF NOT EXISTS core;


--CITIES
-- Один gc_id соответствует одному городу в Core
CREATE TABLE IF NOT EXISTS core.dim_city (
    city_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gc_id           BIGINT NOT NULL UNIQUE,
    city_name       TEXT,
    full_name_local TEXT,
    full_name_gc    TEXT,
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7)
);

CREATE TABLE IF NOT EXISTS core.map_source_city (
    source_city_id BIGINT PRIMARY KEY,
    city_id        BIGINT NOT NULL
        REFERENCES core.dim_city(city_id)
);

CREATE INDEX IF NOT EXISTS idx_map_source_city_city_id
    ON core.map_source_city(city_id);


--BODY GROUPS
-- Стандартизированные группы кузовов.
CREATE TABLE IF NOT EXISTS core.dim_body_group (
    body_group_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    std_id        BIGINT NOT NULL UNIQUE,
    group_name    TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS core.map_source_body_type (
    source_body_type_id BIGINT PRIMARY KEY,
    body_group_id       BIGINT NOT NULL
        REFERENCES core.dim_body_group(body_group_id),
    source_type_name    TEXT
);

CREATE INDEX IF NOT EXISTS idx_map_body_type_group
    ON core.map_source_body_type(body_group_id);


-- FACT TABLE
CREATE TABLE IF NOT EXISTS core.fact_prices (
    price_id            BIGINT PRIMARY KEY,

    loading_city_id     BIGINT
        REFERENCES core.dim_city(city_id),

    unloading_city_id   BIGINT
        REFERENCES core.dim_city(city_id),

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
    volume_category     TEXT,

    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_prices_loading_date
    ON core.fact_prices(loading_date);

CREATE INDEX IF NOT EXISTS idx_core_prices_route
    ON core.fact_prices(loading_city_id, unloading_city_id);


-- BRIDGE TABLE
CREATE TABLE IF NOT EXISTS core.bridge_price_body_group (
    price_id        BIGINT NOT NULL
        REFERENCES core.fact_prices(price_id) ON DELETE CASCADE,

    body_group_id   BIGINT NOT NULL
        REFERENCES core.dim_body_group(body_group_id),

    PRIMARY KEY (price_id, body_group_id)
);


-- DQ QUARANTINE
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
    ON core.dq_quarantine_prices(batch_id);


CREATE UNIQUE INDEX IF NOT EXISTS uq_dq_quarantine_price_batch_reason
ON core.dq_quarantine_prices (
    source_price_id,
    batch_id,
    reason_code
);