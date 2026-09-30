"""Ingestion and data-quality code for the shop data platform.

Standard library only. All database work goes through the `psql` client
(see db.py for why). Modules:

    config        which tables to load and how
    db            thin psql wrappers, including the source -> warehouse COPY pipe
    extract_load  incremental, idempotent, atomic loads into the raw layer
    schema_drift  compare source columns with raw columns
    reconcile     data quality checks, results written to ops.dq_results
    cli           `python -m pipeline ...`
"""
