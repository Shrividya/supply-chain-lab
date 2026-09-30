#!/usr/bin/env bash
# One-shot bootstrap. Runs in the `superset-init` container on every `make up`,
# and every step is safe to repeat.
set -euo pipefail

echo ">> Migrating the metadata database"
superset db upgrade

echo ">> Creating the admin user (${SUPERSET_ADMIN_USER})"
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  || echo "   (admin already exists, skipping)"

# A second, locked-down account for the row-level security / roles exercises.
# Gamma can build charts on datasets it has been granted, and nothing else.
echo ">> Creating the analyst user (Gamma role)"
superset fab create-user \
  --role Gamma \
  --username analyst \
  --firstname Ana \
  --lastname Lyst \
  --email analyst@example.com \
  --password analyst \
  || echo "   (analyst already exists, skipping)"

echo ">> Syncing roles and permissions"
superset init

echo ">> Done"
