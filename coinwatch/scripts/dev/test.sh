#!/bin/bash

# Default to running all tests if no service specified
SERVICE=${1:-all}
COVERAGE=${2:-false}

run_tests() {
    local service=$1
    local coverage=$2

    echo "Running tests for $service..."

    if [ "$coverage" = "true" ]; then
        CMD="pytest --cov=src --cov-report=term-missing"
    else
        CMD="pytest"
    fi

    case $service in
        "market")
            docker-compose run --rm market_data $CMD tests/
            ;;
        "fundamental")
            docker-compose run --rm fundamental_data $CMD tests/
            ;;
        "monitor")
            docker-compose run --rm monitor $CMD tests/
            ;;
        "api")
            docker-compose run --rm api $CMD tests/
            ;;
        "all")
            echo "Running all tests..."
            for svc in market_data fundamental_data monitor api; do
                echo "Testing $svc..."
                docker-compose run --rm $svc $CMD tests/
            done
            ;;
        *)
            echo "Usage: ./test.sh [service] [coverage]"
            echo "Available services:"
            echo "  - market      (Market Data Service)"
            echo "  - fundamental (Fundamental Data Service)"
            echo "  - monitor     (Monitor Service)"
            echo "  - api         (API Gateway)"
            echo "  - all         (All Services)"
            echo ""
            echo "Coverage:"
            echo "  true/false    (Generate coverage report)"
            exit 1
            ;;
    esac
}

# Ensure services are running
echo "Checking infrastructure services..."
if ! nc -z localhost 5672 || ! nc -z localhost 5432; then
    echo "Starting required services..."
    docker-compose up -d rabbitmq postgres
    sleep 5  # Wait for services to be ready
fi

# Run the tests
run_tests $SERVICE $COVERAGE
