-- The metadata Postgres hosts two databases: `superset` (created by the image from
-- POSTGRES_DB) and `airflow`. One server, two tenants, fewer containers for a laptop.
CREATE ROLE airflow LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
