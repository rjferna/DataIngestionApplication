# Dockerfile for PostgreSQL
FROM postgres:13

# Set environment variables for PostgreSQL
ENV POSTGRES_USER=metadata_user \
    POSTGRES_PASSWORD=admin \
    POSTGRES_DB=metadata_utilities

# Update package lists and install necessary tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

# Copy Spark scripts
COPY ddls /app/ddls
COPY statements /app/statements

WORKDIR /app

# Optional: Add health check for the container
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD pg_isready -U $POSTGRES_USER -d $POSTGRES_DB || exit 1

# Expose port
EXPOSE 5432