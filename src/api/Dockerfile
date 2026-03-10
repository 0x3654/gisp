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

# Create virtual environment and install all packages
RUN python -m venv /venv
RUN /venv/bin/pip install --no-cache-dir \
    fastapi==0.115.6 \
    uvicorn[standard]==0.34.0 \
    psycopg2==2.9.10 \
    aiofiles==24.1.0 \
    requests==2.32.3

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-alpine

ARG VERSION=1.0

# Metadata labels
LABEL maintainer="0x3654"
LABEL service="api"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Environment variables
# PATH includes /venv/bin so uvicorn/python from venv are used automatically
ENV TZ=Europe/Moscow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Runtime: only libpq is needed for psycopg2 (NOT postgresql-client with psql/pg_dump)
# No curl needed — healthcheck uses python's urllib built-in
RUN apk add --no-cache libpq \
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && addgroup -g 10001 appuser \
    && adduser -D -u 10001 -G appuser appuser

# Copy virtual environment from builder (no gcc, no musl-dev, no postgresql-dev)
COPY --from=builder /venv /venv

# Copy application code (with ownership)
COPY --chown=appuser:appuser ./src/api /app

# Healthcheck using python's stdlib — no curl needed
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request, os; urllib.request.urlopen('http://127.0.0.1:' + os.getenv('API_PORT','8000') + '/docs', timeout=4)"

# Expose port
EXPOSE 8000

# Run as non-root user
USER appuser

# Run application (uvicorn is on PATH via /venv/bin)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
