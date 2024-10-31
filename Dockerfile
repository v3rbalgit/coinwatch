FROM python:3.12.6-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*


# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install supervisor

# Copy project files
COPY . .

# Add environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

# Create directory for logs
RUN mkdir -p /app/logs && chmod 777 /app/logs