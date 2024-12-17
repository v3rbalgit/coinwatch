# Build stage
FROM python:3.12.8-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install service requirements (excluding shared package for now)
COPY ./services/monitor/requirements.txt /requirements.txt
RUN grep -v "../../shared" requirements.txt > requirements_clean.txt && \
    pip install --no-cache-dir -r requirements_clean.txt

# Runtime stage
FROM python:3.12.8-slim

# Install system monitoring dependencies
RUN apt-get update && apt-get install -y \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create non-root user
RUN useradd -m -u 1000 coinwatch

# Set working directory
WORKDIR /app

# Copy shared package and install it
COPY ./shared /app/shared
RUN pip install -e /app/shared

# Copy application code
COPY ./services/monitor/src ./src

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create logs directory and set permissions
RUN mkdir -p /app/logs && \
    chown -R coinwatch:coinwatch /app/logs

# Switch to non-root user
USER coinwatch

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8003/health || exit 1

# Expose monitor service port
EXPOSE 8003

# Run the application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8003"]
