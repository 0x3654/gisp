FROM alpine:3.21

# Build arguments
ARG VERSION=1.0

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="starter"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"

# Set working directory
WORKDIR /app

# Install runtime dependencies only (bash + postgresql-client)
RUN apk add --no-cache \
    bash \
    postgresql-client \
    ca-certificates \
    && addgroup -g 10001 appuser \
    && adduser -D -u 10001 -G appuser -h /app -s /bin/bash appuser

# Copy entrypoint script (includes restore logic - combined into single file)
COPY --chown=appuser:appuser src/starter/entrypoint.sh /entrypoint.sh

# Copy snapshot (readable by all) - runtime data from services/starter/build/
COPY services/starter/build/registry_start_snapshot.sql.gz /app/

# Make entrypoint executable
RUN chmod +x /entrypoint.sh

# Run as non-root user
USER appuser

ENTRYPOINT ["/entrypoint.sh"]
