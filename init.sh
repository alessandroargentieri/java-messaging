#! /bin/bash

echo "Starting localstack..."
docker run -it --rm -d -p 8081:8081 -p 4566-4599:4566-4599 --name lk localstack/localstack

echo "Starting rabbitMQ..."
docker run -it --rm -d -p 15672:15672 -e RABBITMQ_USERNAME=myuser -e RABBIT_MQ_PASSWORD=password --name rb bitnami/rabbitmq

echo "Starting activeMQ..."
docker run -it --rm -d -p 61616:61616 -p 8161:8161 --name acmq rmohr/activemq:5.15.9