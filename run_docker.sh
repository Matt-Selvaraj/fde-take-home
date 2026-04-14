#!/bin/bash

# Configuration
PROJECT_NAME="risk-alert-system"

# 1. Build and Run using Docker Compose
echo "Starting services with Docker Compose..."
docker compose up -d --build

echo "----------------------------------------"
echo "Services are running in the background."
echo "Access the Risk Alert API at http://localhost:8000"
echo "Access the Mock Slack API at http://localhost:5001"
echo ""
echo "Use 'docker compose logs -f' to view logs."
echo "Use 'docker compose stop' to stop the containers."
echo "Use 'docker compose down' to stop and remove everything."
