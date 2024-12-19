#!/bin/bash

# Create log directories for each service
mkdir -p logs/market_data
mkdir -p logs/fundamental_data
mkdir -p logs/monitor

# Set permissions to allow container user (UID 1000) to write
chown -R 1000:1000 logs/market_data
chown -R 1000:1000 logs/fundamental_data
chown -R 1000:1000 logs/monitor

# Set directory permissions
chmod 755 logs/market_data
chmod 755 logs/fundamental_data
chmod 755 logs/monitor

echo "Log directories created and permissions set"
