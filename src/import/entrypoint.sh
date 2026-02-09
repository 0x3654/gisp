#!/bin/bash
set -e

# Trap signals for graceful shutdown
trap 'echo "Received signal, shutting down..."; kill $(jobs -p); exit 0' SIGTERM SIGINT

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
