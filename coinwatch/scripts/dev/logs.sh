#!/bin/bash

# Default to showing all services if none specified
SERVICE=${1:-all}

case $SERVICE in
    "market")
        echo "Showing Market Data Service logs..."
        docker-compose logs -f market_data
        ;;
    "fundamental")
        echo "Showing Fundamental Data Service logs..."
        docker-compose logs -f fundamental_data
        ;;
    "monitor")
        echo "Showing Monitor Service logs..."
        docker-compose logs -f monitor
        ;;
    "api")
        echo "Showing API Gateway logs..."
        docker-compose logs -f api
        ;;
    "rabbit" | "rabbitmq")
        echo "Showing RabbitMQ logs..."
        docker-compose logs -f rabbitmq
        ;;
    "db" | "postgres")
        echo "Showing PostgreSQL logs..."
        docker-compose logs -f postgres
        ;;
    "all")
        echo "Showing all service logs..."
        docker-compose logs -f
        ;;
    *)
        echo "Usage: ./logs.sh [service]"
        echo "Available services:"
        echo "  - market      (Market Data Service)"
        echo "  - fundamental (Fundamental Data Service)"
        echo "  - monitor     (Monitor Service)"
        echo "  - api         (API Gateway)"
        echo "  - rabbit      (RabbitMQ)"
        echo "  - db          (PostgreSQL)"
        echo "  - all         (All Services)"
        exit 1
        ;;
esac
