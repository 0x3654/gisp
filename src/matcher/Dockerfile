# syntax=docker/dockerfile:1.6
FROM python:3.12-alpine AS builder

ARG VERSION=1.0

RUN apk add --no-cache gcc musl-dev postgresql-dev \
    && python -m venv /venv \
    && /venv/bin/pip install --no-cache-dir \
        fastapi==0.115.6 \
        "uvicorn[standard]==0.34.0" \
        psycopg2==2.9.10 \
        requests==2.32.3 \
        "pydantic>=2.6,<3" \
        "anthropic>=0.40,<1.0"

FROM python:3.12-alpine

ARG VERSION=1.0
LABEL maintainer="0x3654"
LABEL service="gisp_matcher"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

ENV TZ=Europe/Moscow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/venv/bin:$PATH"

WORKDIR /app

RUN apk add --no-cache libpq \
    && addgroup -g 10001 appuser \
    && adduser -D -u 10001 -G appuser appuser

COPY --from=builder /venv /venv
COPY --chown=appuser:appuser ./src/matcher /app

EXPOSE 8030

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import socket, os; s=socket.socket(); s.connect(('127.0.0.1', int(os.getenv('MATCHER_PORT','8030')))); s.close()"

USER appuser

CMD ["uvicorn", "matcher_service:app", "--host", "0.0.0.0", "--port", "8030"]
