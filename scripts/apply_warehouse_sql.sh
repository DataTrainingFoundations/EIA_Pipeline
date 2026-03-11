#!/usr/bin/env bash
set -euo pipefail

for migration in warehouse/migrations/*.sql; do
  name="$(basename "${migration}")"
  echo "Applying warehouse/migrations/${name}..."
  docker compose exec -T postgres psql -U "${POSTGRES_USER:-platform}" -d "${POSTGRES_DB:-platform}" -f "/docker-entrypoint-initdb.d/${name}"
done