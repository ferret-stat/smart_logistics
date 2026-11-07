CREATE TABLE IF NOT EXISTS raw.prices (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id        UUID NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_prices_batch
    ON raw.prices (batch_id);

CREATE TABLE IF NOT EXISTS raw.cities (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id        UUID NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);

CREATE TABLE IF NOT EXISTS raw.body_types (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id        UUID NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);

CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id        UUID NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);
