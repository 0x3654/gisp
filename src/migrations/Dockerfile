# syntax=docker/dockerfile:1.6
# Тонкий контейнер для ad-hoc миграций. Использует тот же стек, что и api (psycopg2 + python).
FROM python:3.12-alpine

ARG VERSION=1.0

LABEL maintainer="0x3654"
LABEL service="gisp_migrations"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

ENV TZ=Europe/Moscow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/venv/bin:$PATH"

WORKDIR /app

RUN apk add --no-cache libpq gcc musl-dev postgresql-dev \
    && python -m venv /venv \
    && /venv/bin/pip install --no-cache-dir psycopg2==2.9.10 \
    && apk del gcc musl-dev postgresql-dev \
    && addgroup -g 10001 appuser \
    && adduser -D -u 10001 -G appuser appuser

COPY --chown=appuser:appuser ./src/migrations /app
# DDL-файлы лежат в services/migrations/, образ их не несёт — монтируем через volume в compose.

USER appuser

ENTRYPOINT ["python", "/app/run_migration.py"]
