# syntax=docker/dockerfile:1.6
# OpenWebUI sync service - sync reestr data to OpenWebUI functions
# Python 3.12 with minimal dependencies

FROM python:3.12-slim

# Build arguments
ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="openwebui-sync"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Set working directory
WORKDIR /app

# Environment variables
ENV TZ=Europe/Moscow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir requests

# Copy application code
COPY ./src/openwebui /app

# Default entrypoint (can be overridden in compose.yaml)
ENTRYPOINT ["python", "/app/reestr_sync.py"]

# Default command shows help
CMD ["--help"]
