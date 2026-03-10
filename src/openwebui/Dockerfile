# syntax=docker/dockerfile:1.6
# OpenWebUI sync service - sync reestr data to OpenWebUI functions
# Python 3.12-slim with multistage build

# ===================================
# BUILDER STAGE - Install dependencies
# ===================================
FROM python:3.12-slim AS builder

ARG VERSION=1.0

# Create virtual environment and install packages
# requests is pure Python — no compilation needed, but venv keeps it clean
RUN python -m venv /venv \
    && /venv/bin/pip install --no-cache-dir \
        requests==2.32.3

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-slim

ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="openwebui-sync"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Set working directory
WORKDIR /app

# Environment variables
# PATH includes /venv/bin so python/scripts from venv are used automatically
ENV TZ=Europe/Moscow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/venv/bin:$PATH"

# Non-root user (no extra packages needed — requests is pure Python)
RUN groupadd -g 10001 appuser \
    && useradd -u 10001 -g appuser -s /sbin/nologin -d /app appuser

# Copy virtual environment from builder
COPY --from=builder /venv /venv

# Copy application code (with ownership)
COPY --chown=appuser:appuser ./src/openwebui /app

# Run as non-root user
USER appuser

# Default entrypoint (can be overridden in compose.yaml)
ENTRYPOINT ["python", "/app/reestr_sync.py"]

# Default command shows help
CMD ["--help"]
