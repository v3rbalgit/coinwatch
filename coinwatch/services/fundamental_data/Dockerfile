# Use Python 3.12.8 as the base image
FROM python:3.12.8-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy shared package
COPY ./shared /app/shared

# Install shared package
RUN pip install -e /app/shared

# Install service dependencies
COPY ./services/fundamental_data/requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Copy service code
COPY ./services/fundamental_data/src /app/src

# Expose port for health checks
EXPOSE 8002

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# Command to run the service
CMD ["python", "-m", "src.main"]
