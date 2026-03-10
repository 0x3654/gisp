# syntax=docker/dockerfile:1.6
# ===================================
# BUILDER STAGE - Compile dependencies
# ===================================
FROM python:3.12-alpine AS builder

ARG VERSION=1.0

# Install build deps for psycopg2 (stay in builder only)
RUN apk add --no-cache \
    gcc \
    musl-dev \
    postgresql-dev

# Create virtual environment and install packages
RUN python -m venv /venv \
    && /venv/bin/pip install --no-cache-dir \
        psycopg2==2.9.10 \
        requests==2.32.3

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-alpine

ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="import"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Environment variables
# PATH includes /venv/bin so python from venv is used automatically
ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Runtime: libpq for psycopg2, bash for shell scripts, curl for healthchecks in scripts
RUN apk add --no-cache \
    libpq \
    bash \
    curl \
    ca-certificates \
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && addgroup -g 10001 appuser \
    && adduser -D -u 10001 -G appuser appuser \
    && mkdir -p /home/appuser /var/log/registry \
    && chown appuser:appuser /home/appuser /var/log/registry

# Copy virtual environment from builder (no gcc, no musl-dev, no postgresql-dev)
COPY --from=builder /venv /venv

# Copy application scripts
COPY --chown=appuser:appuser --chmod=755 ./src/import/scripts /scripts

# Run as appuser
USER appuser

# Default command (can be overridden by compose run)
CMD ["/scripts/run_import.sh"]
