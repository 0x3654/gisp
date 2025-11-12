FROM python:3.12-slim
WORKDIR /app

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    bash \
    cron \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY ./scripts /scripts
RUN chmod +x /scripts/*

RUN pip install --no-cache-dir psycopg2-binary requests

# Cron tasks
RUN cat <<'EOF' > /etc/cron.d/registry-cron \
    && chmod 0644 /etc/cron.d/registry-cron
5 0 * * * root bash -l -c '/scripts/run_import.sh >> /var/log/cron.log 2>&1'
0 5 * * * root bash -l -c '/scripts/run_pg_maintenance.sh >> /var/log/cron.log 2>&1'
EOF

RUN touch /var/log/cron.log

# Запуск cron и наблюдение за логами
CMD bash -c "printenv | grep -v 'no_proxy' > /etc/environment && ( /scripts/run_import.sh & ) && service cron start && tail -f /var/log/cron.log"
