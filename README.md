# Java Event Driven Application

Example application on how to use Java in a event driver fashion.

## Summary

This codebase contains some simple examples how of to engage an event driven architecture in Java.
The `EventProducer<T>` interface provide a way to define a generic notification producer.

The implementation used in this codebase are three:
- `EndpointEventProducer`: though which the notification is sent to a REST endpoint;
- `SnsEventProducer`: through which the event is notified on `AWS SNS` service, by creating a new `topic` for that specific event;
- `RabbitMqEventProducer`: though which the event is notified on `RabbitMQ` by creating a new `exchange`.
- `ActiveMqEventProducer`: though which the event is notified on `Apache ActiveMQ` by creating a new `topic`.
- `KafkaEventProducer`: though which the event is notified on `Apache Kafka` by creating a new `topic`.

The codebase provides a way to consume these three events with their respective consumer:
- an endpoint in the `RestController` class which consumes the events sent via `EndpointEventProducer`;
- a `SqsConsumer` class which creates an `AWS SQS queue` and automatically suscribes it to the given `AWS SNS topic`;
- a `RabbitMqConsumer` class which creates a `queue` and registers it to the given `exchange` to consume the messages.
- an `ActiveMqConsumer` class which subscribe directly to the `ActiveMqEventProducer` `topic` to consume the messages.
- a `KafkaEventConsumer` class which reads the records coming from the `KafkaEventProducer` producer `topic`.

## Run locally

To start the application locally you must have `localstack` (which emulates AWS services locally) and `rabitMQ` started locally in Docker containers.
To automatize this phase an `init` script has been provided:

```bash
your/project/folder/ $ ./init.sh
```
Then you can start your application though your IDE or by exporting the jar artifact:
```bash
your/project/folder/ $ mvn clean install
your/project/folder/ $ java -jar ./target/notificationdemo-0.0.1.jar
```
The process will start an HTTP server on port 8080, so pay attention to let it free before starting the process.

To stop the processes you can take advantage of the `quit.sh` script:

```bash
your/project/folder/ $ ./quit.sh
```

## Insights on Localstack in docker

Localstack is a useful way to have your AWS services running locally without having to link your app to your AWS cloud account.

### Localstack Cheatsheet
```bash
# Run docker image of localstack
docker run -it --rm -d -p 8081:8081 -p 4566-4599:4566-4599 --name lk localstack/localstack

# Health check
curl http://localhost:4566/health | jq
{
  "features": {
    "initScripts": "initialized"
  },
  "services": {
    "acm": "available",
    "apigateway": "available",
    "cloudformation": "available",
    "cloudwatch": "available",
    "config": "available",
    "dynamodb": "available",
    "dynamodbstreams": "available",
    "ec2": "available",
    "es": "available",
    "events": "available",
    "firehose": "available",
    "iam": "available",
    "kinesis": "available",
    "kms": "available",
    "lambda": "available",
    "logs": "available",
    "opensearch": "available",
    "redshift": "available",
    "resource-groups": "available",
    "resourcegroupstaggingapi": "available",
    "route53": "available",
    "route53resolver": "available",
    "s3": "running",
    "secretsmanager": "available",
    "ses": "available",
    "sns": "running",
    "sqs": "running",
    "ssm": "available",
    "stepfunctions": "available",
    "sts": "available",
    "support": "available",
    "swf": "available"
  },
  "version": "0.13.3"
}

# s3 service

# create s3 bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://mybucket
# list s3 files
aws --endpoint-url=http://localhost:4566 s3 ls
# upload file into s3 bucket
echo 'hello world!' > text-test.txt
aws --endpoint-url=http://localhost:4566 s3 cp text-test.txt s3://mybucket
# list s3 files
aws --endpoint-url=http://localhost:4566 s3 ls s3://mybucket

# SNS and SQS

# create topic
aws --endpoint-url=http://localhost:4566 sns create-topic --name mytopic
    {"TopicArn": "arn:aws:sns:eu-west-1:000000000000:mytopic"}
# create queue
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name myqueue001
    {"QueueUrl": "http://localhost:4566/000000000000/myqueue001"}
# subscribe queue to topic
aws --endpoint-url=http://localhost:4566 sns subscribe --topic-arn arn:aws:sns:eu-west-1:000000000000:mytopic --protocol sqs --notification-endpoint http://localhost:4566/queue/myqueue001
    {"SubscriptionArn": "arn:aws:sns:eu-west-1:000000000000:mytopic:20ed1612-26cb-4297-882d-e33158ab6130"}

# list topics
aws --endpoint-url=http://localhost:4566 sns list-topics    
# list queues
aws --endpoint-url=http://localhost:4566 sqs list-queues
# list subscriptions
aws --endpoint-url=http://localhost:4566 sns list-subscriptions

# send notifications to topic
aws --endpoint-url=http://localhost:4566 sns publish --topic-arn arn:aws:sns:eu-west-1:000000000000:mytopic --message "Hi"
aws --endpoint-url=http://localhost:4566 sns publish --topic-arn arn:aws:sns:eu-west-1:000000000000:mytopic --message file://file.json

# read message on queue
aws --endpoint-url=http://localhost:4566 sqs receive-message --queue-url http://localhost:4566/000000000000/myqueue001

### send message on queue
aws --endpoint-url=http://localhost:4566 sqs send-message --queue-url http://localhost:4566/000000000000/myqueue001 --message-body 'Welcome to SQS queue myqueue001'

### read message from queue
aws --endpoint-url=http://localhost:4566 sqs receive-message --queue-url http://localhost:4566/000000000000/myqueue001

### delete queue
aws --endpoint-url=http://localhost:4566 sqs delete-queue --queue-url http://localhost:4566/000000000000/myqueue001
```