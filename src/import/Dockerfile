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
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install ONLY runtime dependencies
RUN apk add --no-cache \
    postgresql-client \
    bash \
    curl \
    ca-certificates \
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && addgroup -g 10001 appuser && adduser -D -u 10001 -G appuser appuser

# Copy compiled Python packages from builder
COPY --from=builder /install /usr/local/lib/python3.12/site-packages

# Copy application code
COPY --chown=appuser:appuser ./src/import/scripts /scripts

# Create directories
RUN mkdir -p /home/appuser /var/log/registry && \
    chown appuser:appuser /home/appuser /var/log/registry && \
    chmod +x /scripts/*.sh

# Run as appuser
USER appuser

# Default command (can be overridden by compose run)
CMD ["/scripts/run_import.sh"]
