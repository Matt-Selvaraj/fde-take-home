#!/bin/bash

# Configuration
IMAGE_NAME="risk-alert-service"
CONTAINER_NAME="risk-alert-service-container"
PORT=8000

# 1. Build the Docker image
echo "Building Docker image: $IMAGE_NAME..."
docker build -t $IMAGE_NAME .

# 2. Stop and remove existing container if it exists
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping and removing existing container: $CONTAINER_NAME..."
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# 3. Run the container
# Mapping port 8000 to 8000.
# The app in the Docker container listens on 0.0.0.0:8000.
echo "Running Docker container: $CONTAINER_NAME..."
echo "Access the API at http://localhost:$PORT"
docker run -d \
  --name $CONTAINER_NAME \
  -p $PORT:8000 \
  -e DATABASE_URL="sqlite:///./risk_alerts.db" \
  $IMAGE_NAME

echo "Container $CONTAINER_NAME is running in the background."
echo "Use 'docker logs -f $CONTAINER_NAME' to view logs."
echo "Use 'docker stop $CONTAINER_NAME' to stop the container."
