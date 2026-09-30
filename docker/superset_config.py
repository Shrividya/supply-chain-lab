"""
Superset Lab configuration (Superset 6.1).

This file is bind-mounted into every Superset container, so you can edit it and
just restart:   docker compose restart superset superset-worker

Everything here is a plain Superset setting. Comments explain *why* each one is
set, because the defaults are the source of most "why is Superset slow / weird /
insecure" questions.
"""
import os

from celery.schedules import crontab  # noqa: F401  (handy when you add beat tasks)
from cachelib.redis import RedisCache
from flask_appbuilder.security.manager import AUTH_DB

# Secrets and metadata database
# Superset refuses to start in non-debug mode with its built-in example key.
# Every encrypted value in the metadata DB (database passwords, for one) is
# tied to this key. Lose it or change it and saved connections stop working.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}".format(
    user=os.environ["METADATA_DB_USER"],
    password=os.environ["METADATA_DB_PASSWORD"],
    host=os.environ["METADATA_DB_HOST"],
    port=os.environ["METADATA_DB_PORT"],
    name=os.environ["METADATA_DB_NAME"],
)

AUTH_TYPE = AUTH_DB

# Redis: cache, filter state, async query results, Celery
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")


def _redis_cache(db: int, prefix: str, timeout: int) -> dict:
    return {
        "CACHE_TYPE": "RedisCache",
        "CACHE_DEFAULT_TIMEOUT": timeout,
        "CACHE_KEY_PREFIX": prefix,
        "CACHE_REDIS_URL": f"redis://{REDIS_HOST}:{REDIS_PORT}/{db}",
    }


# Metadata cache (table lists, column info, and so on).
CACHE_CONFIG = _redis_cache(db=2, prefix="superset_meta_", timeout=300)

# Chart query results. This is the one that makes dashboards feel fast.
# Precedence, highest first: chart cache timeout, dataset timeout, database
# timeout, then this default. 0 would mean "never expire", -1 means "do not cache".
DATA_CACHE_CONFIG = _redis_cache(db=3, prefix="superset_data_", timeout=600)

# Dashboard filter state and Explore form data. Without a real backend these
# fall back to the metadata DB or process memory and behave oddly across workers.
FILTER_STATE_CACHE_CONFIG = _redis_cache(db=4, prefix="superset_filter_", timeout=86400)
EXPLORE_FORM_DATA_CACHE_CONFIG = _redis_cache(db=5, prefix="superset_explore_", timeout=86400)

# Where async SQL Lab results wait until the browser fetches them.
RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=int(REDIS_PORT),
    db=6,
    key_prefix="superset_results_",
    default_timeout=3600,
)


class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/1"
    imports = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
        "superset.tasks.thumbnails",
        "superset.tasks.cache",
    )
    worker_prefetch_multiplier = 1
    task_acks_late = False
    task_annotations = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
    }


CELERY_CONFIG = CeleryConfig

# Query guard rails
ROW_LIMIT = 50_000                # rows a chart may pull into the browser
SQL_MAX_ROW = 100_000             # hard ceiling for SQL Lab result sets
DEFAULT_SQLLAB_LIMIT = 1_000      # what the "LIMIT" dropdown starts on
SQLLAB_TIMEOUT = 120              # seconds, synchronous SQL Lab queries
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600 # seconds, queries run on the Celery worker
SUPERSET_WEBSERVER_TIMEOUT = 120  # keep this >= SQLLAB_TIMEOUT or gunicorn kills the request first

# Features
FEATURE_FLAGS = {
    # Jinja in SQL Lab, virtual datasets and chart filters:
    #   {{ current_username() }}, {{ filter_values('channel') }}, {{ from_dttm }}
    "ENABLE_TEMPLATE_PROCESSING": True,
    # Per-dashboard role access (Dashboard > Edit properties > Access).
    "DASHBOARD_RBAC": True,
}

# Branding (6.1 moved this into the theme system; APP_NAME no longer sets the
# browser tab title, and CUSTOM_FONT_URLS is gone).
THEME_DEFAULT = {
    "algorithm": "default",
    "token": {
        "brandAppName": "Superset Lab",
    },
}

# Local-development conveniences. Do NOT copy these into anything shared.
# Talisman sets CSP/HSTS headers; it needs HTTPS in front of it to be useful.
TALISMAN_ENABLED = False
# Keep the API usable from the provisioning container over plain HTTP.
SESSION_COOKIE_SECURE = False
# Superset already blocks SQLite and similar "unsafe" connection strings by
# default. Leave it that way.
PREVENT_UNSAFE_DB_CONNECTIONS = True
