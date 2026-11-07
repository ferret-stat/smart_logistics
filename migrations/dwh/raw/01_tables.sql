CREATE TABLE IF NOT EXISTS raw.etl_batches (
    batch_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS raw.prices (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id UUID NOT NULL REFERENCES raw.etl_batches(batch_id),
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_prices_batch
    ON raw.prices (batch_id);

CREATE TABLE IF NOT EXISTS raw.cities (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id UUID NOT NULL REFERENCES raw.etl_batches(batch_id),
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);

CREATE TABLE IF NOT EXISTS raw.body_types (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id UUID NOT NULL REFERENCES raw.etl_batches(batch_id),
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);

CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    source_id       BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    batch_id UUID NOT NULL REFERENCES raw.etl_batches(batch_id),
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, batch_id)
);
