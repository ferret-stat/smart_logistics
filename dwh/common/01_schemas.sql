CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

COMMENT ON SCHEMA raw IS 'Слой сырых данных';
COMMENT ON SCHEMA core IS 'Чистые и нормализованные данные';
COMMENT ON SCHEMA mart IS 'Витрины данных';