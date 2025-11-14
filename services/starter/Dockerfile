FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY services/starter/build/registry_start_snapshot.sql.gz /app/registry_start_snapshot.sql.gz
COPY services/starter/restore_snapshot.sh /app/restore_snapshot.sh
COPY services/starter/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh /app/restore_snapshot.sh

ENV SNAPSHOT_PATH=/app/registry_start_snapshot.sql.gz \
    STARTER_MAX_WAIT=180 \
    FORCE=0 \
    POSTGRES_HOST=postgres_registry \
    POSTGRES_PORT=5432 \
    POSTGRES_DB=registry \
    POSTGRES_USER=registry

ENTRYPOINT ["/entrypoint.sh"]
