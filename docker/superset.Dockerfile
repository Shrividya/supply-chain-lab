# Superset 6.x images use `uv` and a virtualenv at /app/.venv, so `pip install`
# would put packages somewhere Superset never looks. Bump the tag when you
# want a newer release, and read UPDATING.md in the Superset repo first.
FROM apache/superset:6.1.0

USER root

# psycopg2-binary: the metadata DB and the warehouse are both Postgres.
# Add more drivers here when you connect other engines, for example
# clickhouse-connect, snowflake-sqlalchemy, trino, pymssql.
RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache-dir psycopg2-binary

USER superset
