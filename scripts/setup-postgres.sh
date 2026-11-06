#!/bin/sh
set -eu

: "${POSTGRES_SEEDS:?}" "${POSTGRES_USER:?}" "${SQL_PASSWORD:?}"

for db in temporal temporal_visibility; do
  echo "Initializing $db"
  temporal-sql-tool --plugin postgres12 --ep "$POSTGRES_SEEDS" -p "${DB_PORT:-5432}" -u "$POSTGRES_USER" --db "$db" create || true
  temporal-sql-tool --plugin postgres12 --ep "$POSTGRES_SEEDS" -p "${DB_PORT:-5432}" -u "$POSTGRES_USER" --db "$db" setup-schema -v 0.0 || true
  if [ "$db" = temporal ]; then
    schema_dir=/etc/temporal/schema/postgresql/v12/temporal/versioned
  else
    schema_dir=/etc/temporal/schema/postgresql/v12/visibility/versioned
  fi
  temporal-sql-tool --plugin postgres12 --ep "$POSTGRES_SEEDS" -p "${DB_PORT:-5432}" -u "$POSTGRES_USER" --db "$db" update-schema -d "$schema_dir"
done
