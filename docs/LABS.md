# Failure labs

Each lab breaks something on purpose so you can watch the platform notice. Start from a healthy state: `make up && make demo`. Every outcome below is pinned by an automated test (`tests/test_transformations.py`, `tests/test_ingestion.py`), so what you see should match. Write down what you observe; it is your material.

| Lab | Command | What should happen | Fix |
|---|---|---|---|
| Negative prices | `make lab-bad-amount`, then `make load dbt-build` | `assert_no_negative_amounts` fails | Correct the rows in the source, load, build. The marts heal without a full refresh |
| Missing channel | `make lab-null-channel` | `not_null(stg_orders.channel)` fails, and the reconcile null-rate check fails | Fix upstream |
| Orphan orders | `make lab-orphan` | `relationships(stg_orders.customer_id)` and `not_null(fct_orders.country)` fail | Decide: fix the source or quarantine |
| Late events inside the window | `sim.inject('late_events', 2)` via `make psql-source` | Build stays green | None needed |
| Late events beyond the window | `make lab-late` | `assert_funnel_matches_events` fails | `make dbt-full` |
| Source adds a column | `make lab-drift-add` | Load succeeds, a `warn` row appears in `ops.schema_drift` | Decide whether to add it to raw |
| Source renames a column | `make lab-drift-rename` | Load stops before writing, exit code 1, watermark unchanged | `make lab-heal` (or update raw), then load again |

## Questions to answer after each lab

1. Which layer caught it, and why that one?
2. What would a user of the dashboard have seen if nothing had caught it?
3. How would you alert on it in a real team?
