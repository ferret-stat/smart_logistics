\set ON_ERROR_STOP on

\echo 'Initializing DWH schemas...'
\ir common/01_schemas.sql

\echo 'Creating RAW tables...'
\ir raw/01_tables.sql

\echo 'Creating CORE tables...'
\ir core/01_tables.sql

\echo 'Creating MART tables...'
\ir mart/01_tables.sql

\echo 'DWH initialization completed.'