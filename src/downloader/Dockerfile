# syntax=docker/dockerfile:1.6
# ===================================
# BUILDER STAGE - Install dependencies
# ===================================
FROM python:3.12-alpine AS builder

ARG VERSION=1.0

# Create virtual environment and install packages
# requests/pysocks/urllib3 are pure Python — no compilation needed,
# but builder stage keeps pip and wheel out of the final image
RUN python -m venv /venv \
    && /venv/bin/pip install --no-cache-dir \
        requests==2.32.3 \
        pysocks==1.7.1 \
        urllib3==2.6.3

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-alpine

ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="downloader"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Environment variables
# PATH includes /venv/bin so python from venv is used automatically
ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/venv/bin:$PATH" \
    FILES_DIR=/files \
    MAX_CSV_FILES=7

# Set working directory
WORKDIR /app

# openssh-client is required for the SSH tunnel download mode (ssh -D SOCKS proxy)
RUN apk add --no-cache \
    bash \
    curl \
    ca-certificates \
    openssh-client \
    && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && addgroup -g 10001 appuser \
    && adduser -D -u 10001 -G appuser appuser \
    && mkdir -p /home/appuser /files \
    && chown appuser:appuser /home/appuser /files

# Copy virtual environment from builder
COPY --from=builder /venv /venv

# Copy application scripts and entrypoint
COPY --chown=appuser:appuser --chmod=755 src/downloader/scripts /scripts
COPY --chown=appuser:appuser --chmod=755 src/downloader/entrypoint.sh /usr/local/bin/entrypoint.sh

# Run as appuser
USER appuser

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
