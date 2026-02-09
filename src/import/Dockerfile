# ===================================
# BUILDER STAGE - Compile dependencies
# ===================================
FROM python:3.12-alpine AS builder

# Build arguments
ARG VERSION=1.0

# Install build dependencies for psycopg2 compilation
RUN apk add --no-cache \
    gcc \
    musl-dev \
    postgresql-dev

# Install Python packages to /install directory
# These will be copied to final stage, build tools stay behind
RUN pip install --no-cache-dir --target=/install \
    psycopg2==2.9.10 \
    requests==2.32.3

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-alpine

# Build arguments
ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="import"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Environment variables
ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    CRON_IMPORT_SCHEDULE="5 0 * * *" \
    CRON_MAINTENANCE_SCHEDULE="0 5 * * *"

# Set working directory
WORKDIR /app

# Install ONLY runtime dependencies (no gcc, no build tools!)
RUN apk add --no-cache \
    postgresql-client \
    bash \
    curl \
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && addgroup -g 10001 appuser && adduser -D -u 10001 -G appuser appuser

# Copy compiled Python packages from builder
COPY --from=builder /install /usr/local/lib/python3.12/site-packages

# Copy application code (with ownership)
COPY --chown=appuser:appuser ./src/import/scripts /scripts
COPY --chown=appuser:appuser ./src/import/entrypoint.sh /usr/local/bin/entrypoint.sh

# Configure cron and set permissions
RUN chmod +x /scripts/* /usr/local/bin/entrypoint.sh \
    && mkdir -p /etc/cron.d \
    && cat <<'EOF' > /etc/cron.d/registry-cron \
    && chmod 0644 /etc/cron.d/registry-cron \
    && touch /var/log/cron.log \
    && chmod 0644 /var/log/cron.log \
    && chown appuser:appuser /var/log/cron.log
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
${CRON_IMPORT_SCHEDULE:-5 0 * * *} appuser /scripts/run_import.sh >> /var/log/cron.log 2>&1
${CRON_MAINTENANCE_SCHEDULE:-0 5 * * *} appuser /scripts/run_pg_maintenance.sh >> /var/log/cron.log 2>&1
EOF

# Healthcheck (will be overridden by compose.yaml, but useful for local runs)
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD pg_isready -h postgres_registry -U postgres && pidof crond >/dev/null

# Run as non-root user
USER appuser

# Use entrypoint for proper initialization and signal handling
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
