#!/bin/bash

echo "Stopping Coinwatch development environment..."

# Stop application services first (reverse order of start)
echo "Stopping application services..."
docker-compose stop api monitor fundamental_data market_data

# Stop infrastructure services
echo "Stopping infrastructure services..."
docker-compose stop rabbitmq postgres

# Optional cleanup (uncomment if needed)
# echo "Cleaning up containers..."
# docker-compose down

echo "All services stopped successfully!"
echo "To remove all containers and networks, run: docker-compose down"
echo "To remove all data volumes, run: docker-compose down -v"
