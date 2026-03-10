# ===================================
# BUILDER STAGE - Compile dependencies
# ===================================
FROM python:3.12-alpine AS builder

# Build arguments
ARG VERSION=1.0

# Install Python packages to /install directory
# These will be copied to final stage, build tools stay behind
RUN pip install --no-cache-dir --target=/install \
    requests==2.32.3 \
    pysocks==1.7.1 \
    urllib3==2.6.3

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-alpine

# Build arguments
ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="downloader"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Environment variables
ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    FILES_DIR=/files \
    MAX_CSV_FILES=7

# Set working directory
WORKDIR /app

# Install ONLY runtime dependencies
RUN apk add --no-cache \
    bash \
    curl \
    ca-certificates \
    openssh-client \
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && addgroup -g 10001 appuser && adduser -D -u 10001 -G appuser appuser

# Copy compiled Python packages from builder
COPY --from=builder /install /usr/local/lib/python3.12/site-packages

# Copy application code
COPY --chown=appuser:appuser src/downloader/scripts /scripts
COPY --chown=appuser:appuser src/downloader/entrypoint.sh /usr/local/bin/entrypoint.sh

# Create directories
RUN mkdir -p /home/appuser /files && \
    chown appuser:appuser /home/appuser /files && \
    chmod +x /scripts/* /usr/local/bin/entrypoint.sh

# Run as appuser
USER appuser

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
