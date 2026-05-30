# syntax=docker/dockerfile:1.6
FROM python:3.12-alpine

RUN apk add --no-cache && \
    pip install --no-cache-dir fastapi==0.115.6 "uvicorn[standard]==0.34.0" httpx==0.27.2

WORKDIR /app
COPY proxy.py /app/proxy.py

ENV PYTHONUNBUFFERED=1 \
    UPSTREAM=http://openwebui:8080 \
    PORT=8080

EXPOSE 8080
CMD ["uvicorn", "proxy:app", "--host", "0.0.0.0", "--port", "8080"]
