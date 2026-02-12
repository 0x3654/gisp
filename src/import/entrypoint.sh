#!/bin/bash
set -e

# Trap signals for graceful shutdown
trap 'echo "Received signal, shutting down..."; kill $(jobs -p); exit 0' SIGTERM SIGINT

# Generate crontab from environment variables
echo "Generating crontab from environment..."
cat > /etc/crontabs/appuser << CRON_EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
${CRON_IMPORT_SCHEDULE:-5 0 * * *} /scripts/run_import.sh >> /var/log/cron.log 2>&1
${CRON_MAINTENANCE_SCHEDULE:-0 5 * * *} /scripts/run_pg_maintenance.sh >> /var/log/cron.log 2>&1
CRON_EOF

# Run initial import in background
echo "Starting initial import..."
/scripts/run_import.sh &
IMPORT_PID=$!

# Start cron in background (Alpine uses busybox crond)
echo "Starting cron service..."
crond -f -l 2 -L /var/log/cron.log &
CRON_PID=$!

# Function to check if background processes are still running
check_processes() {
    if ! kill -0 $IMPORT_PID 2>/dev/null; then
        echo "Import process has completed"
        IMPORT_PID=""
    fi

    if ! kill -0 $CRON_PID 2>/dev/null; then
        echo "Cron process has died, exiting..."
        exit 1
    fi
}

# Monitor processes and tail logs
echo "Monitoring logs..."
tail -f /var/log/cron.log &
TAIL_PID=$!

# Wait for processes
while [ -n "$IMPORT_PID" ] || kill -0 $CRON_PID 2>/dev/null; do
    check_processes
    sleep 5
done

# Cleanup
kill $TAIL_PID 2>/dev/null || true
echo "All processes completed, shutting down..."
