#! /bin/bash

echo "Stopping localstack..."
docker rm -f lk 2>/dev/null

echo "Stopping rabbitMQ..."
docker rm -f rb 2>/dev/null

echo "quit application..."
lsof -ti tcp:8080 2>/dev/null | xargs kill 2>/dev/null
