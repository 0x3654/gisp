# syntax=docker/dockerfile:1.6
# Starter service — restores DB snapshot and waits for postgres to be ready
# Pure shell: Alpine + bash + postgresql-client, no Python needed

FROM alpine:3.21

ARG VERSION=1.0

# Metadata labels
LABEL maintainer="0x3654"
LABEL service="starter-dump"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Set working directory
WORKDIR /app

# Install runtime dependencies only (bash + postgresql-client for pg_restore/psql)
RUN apk add --no-cache \
    bash \
    postgresql-client \
    ca-certificates \
    && addgroup -g 10001 appuser \
    && adduser -D -u 10001 -G appuser -h /app -s /bin/bash appuser

# Copy entrypoint with correct permissions in one layer (no extra RUN chmod needed)
COPY --chown=appuser:appuser --chmod=755 src/starter/entrypoint.sh /entrypoint.sh

# Copy snapshot (readable by all) — runtime data baked into image at build time
COPY services/starter/build/registry_start_snapshot.sql.gz /app/

# Run as non-root user
USER appuser

ENTRYPOINT ["/entrypoint.sh"]
