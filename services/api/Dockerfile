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
    fastapi==0.115.0 \
    uvicorn[standard]==0.32.0 \
    psycopg2==2.9.9 \
    aiofiles==24.1.0 \
    requests==2.32.3

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-alpine

# Build arguments
ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="api"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Environment variables
ENV TZ=Europe/Moscow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install ONLY runtime dependencies (no gcc, no build tools!)
RUN apk add --no-cache \
    postgresql-client \
    curl \
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && addgroup -g 10001 appuser && adduser -D -u 10001 -G appuser appuser

# Copy compiled Python packages from builder
COPY --from=builder /install /usr/local/lib/python3.12/site-packages

# Copy application (with ownership)
COPY --chown=appuser:appuser . /app/

# Expose port
EXPOSE 8000

# Run as non-root user
USER appuser

# Run application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
