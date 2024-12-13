#!/bin/bash

echo "Starting Coinwatch development environment..."

# Start infrastructure services
echo "Starting infrastructure services..."
docker-compose up -d rabbitmq postgres

# Wait for infrastructure
echo "Waiting for RabbitMQ..."
timeout 60s bash -c 'until nc -z localhost 5672; do sleep 1; done'

echo "Waiting for PostgreSQL..."
timeout 60s bash -c 'until nc -z localhost 5432; do sleep 1; done'

# Run database migrations
echo "Running database migrations..."
docker-compose run --rm market_data alembic upgrade head

# Start application services
echo "Starting application services..."
docker-compose up -d market_data fundamental_data monitor api

echo "Waiting for services to be ready..."
sleep 5

# Check service health
echo "Checking service health..."
services=("market_data:8001" "fundamental_data:8002" "monitor:8003" "api:8000")

for service in "${services[@]}"; do
    IFS=':' read -r name port <<< "$service"
    echo "Checking $name on port $port..."
    timeout 30s bash -c "until curl -s http://localhost:$port/health > /dev/null; do sleep 1; done"
    if [ $? -eq 0 ]; then
        echo "✓ $name is healthy"
    else
        echo "✗ $name failed health check"
    fi
done

echo "Development environment is ready!"
echo "Access services at:"
echo "  - Market Data:      http://localhost:8001"
echo "  - Fundamental Data: http://localhost:8002"
echo "  - Monitor:         http://localhost:8003"
echo "  - API Gateway:     http://localhost:8000"
echo "  - RabbitMQ UI:     http://localhost:15672"
