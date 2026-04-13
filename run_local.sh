#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Configuration
MOCK_SLACK_PORT=5001
API_PORT=8000

echo "=== Risk Alert Service: Local Runner ==="

# 1. Dependency Installation
echo "Installing/updating dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# 2. Environment Variables
# Set SLACK_WEBHOOK_BASE_URL to point to our mock service
export SLACK_WEBHOOK_BASE_URL="http://127.0.0.1:$MOCK_SLACK_PORT/slack/webhook"
export DATABASE_URL="sqlite:///./risk_alerts.db"

# 3. Start Mock Slack Service
echo "Starting Mock Slack Service on port $MOCK_SLACK_PORT..."
# Using uvicorn for the mock service too, as it's a FastAPI app
uvicorn mock_slack.server:app --host 127.0.0.1 --port $MOCK_SLACK_PORT > mock_slack.log 2>&1 &
MOCK_SLACK_PID=$!

# Function to kill background processes on exit
cleanup() {
    echo ""
    echo "Shutting down services..."
    kill $MOCK_SLACK_PID 2>/dev/null || true
    echo "Cleanup complete."
}

# Trap SIGINT (Ctrl+C) and SIGTERM
trap cleanup EXIT

# 4. Start Main Risk Alert Service
echo "Starting Risk Alert Service on port $API_PORT..."
echo "Access the API at http://127.0.0.1:$API_PORT"
echo "API Docs: http://127.0.0.1:$API_PORT/docs"
echo "Mock Slack Logs: mock_slack.log"
echo "----------------------------------------"

uvicorn app.main:app --host 127.0.0.1 --port $API_PORT --reload
