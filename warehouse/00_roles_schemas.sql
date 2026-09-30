-- WAREHOUSE: roles and schemas.
--
-- Three accounts, three jobs (least privilege):
--   loader      writes raw.* and ops.*; the ingestion code connects as this
--   dbt_user    reads raw.*, owns staging.* and marts.*; dbt connects as this
--   superset_ro reads marts.* and ops.*; Superset connects as this
--
-- Layers:
--   raw      untouched copies of source rows, append-only change log
--   staging  cleaned, typed, de-duplicated views (dbt)
--   marts    business-facing tables (dbt)
--   ops      the platform's own bookkeeping: watermarks, audit, data quality
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''UTC''', current_database());
END $$;
SET timezone TO 'UTC';

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'loader')      THEN CREATE ROLE loader      LOGIN PASSWORD 'loader';      END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dbt_user')    THEN CREATE ROLE dbt_user    LOGIN PASSWORD 'dbt_user';    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset_ro') THEN CREATE ROLE superset_ro LOGIN PASSWORD 'superset_ro'; END IF;
END $$;

-- Fail fast in BI instead of hanging the warehouse for ten minutes.
ALTER ROLE superset_ro SET statement_timeout = '60s';

CREATE SCHEMA IF NOT EXISTS raw     AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS ops     AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION dbt_user;
CREATE SCHEMA IF NOT EXISTS marts   AUTHORIZATION dbt_user;

-- dbt reads what the loader writes.
GRANT USAGE ON SCHEMA raw TO dbt_user;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw GRANT SELECT ON TABLES TO dbt_user;

-- Superset reads business tables and the platform's own health tables.
GRANT USAGE ON SCHEMA marts, ops TO superset_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_user IN SCHEMA marts GRANT SELECT ON TABLES TO superset_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE loader   IN SCHEMA ops   GRANT SELECT ON TABLES TO superset_ro;
