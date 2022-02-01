#! /bin/bash

echo "Starting localstack..."
docker run -it --rm -d -p 8081:8081 -p 4566-4599:4566-4599 --name lk localstack/localstack

echo "Starting rabbitMQ..."
# Ui available at http://localhost:15672/
# this image doesn't work with MacOs M1 chip. I need a centos based one
# check at https://github.com/CentOS/CentOS-Dockerfiles/tree/master/rabbitmq/centos7
# docker run -it --rm -d -p 15672:15672 -e RABBITMQ_USERNAME=myuser -e RABBIT_MQ_PASSWORD=password --name rb bitnami/rabbitmq
docker run -it --rm -d -p 5672:5672 -p 15672:15672 -e RABBITMQ_USER=rabbitmq_user -e RABBITMQ_PASS=rabbitmq_password --name rb alessandroargentieri/rabbitmq
# vromero/activemq-artemis

echo "Starting activeMQ..."
# UI available at http://localhost:8161/
docker run -it --rm -d -p 61616:61616 -p 8161:8161 --name acmq rmohr/activemq:5.15.9