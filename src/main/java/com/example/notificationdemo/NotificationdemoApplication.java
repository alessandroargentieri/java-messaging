package com.example.notificationdemo;

import com.example.notificationdemo.notifications.EventProducer;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.notifications.consumers.ActiveMqConsumer;
import com.example.notificationdemo.notifications.consumers.KafkaEventConsumer;
import com.example.notificationdemo.notifications.consumers.RabbitMqConsumer;
import com.example.notificationdemo.notifications.consumers.SqsConsumer;
import com.example.notificationdemo.notifications.producers.*;
import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.jms.JMSException;
import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
public class NotificationdemoApplication {

	public static void main(String[] args) throws URISyntaxException, NotificationException, InterruptedException, IOException, TimeoutException, JMSException, OperationNotSupportedException {

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SQS-SNS test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		SnsEventProducer<String> snsProducer = SnsEventProducer.createProducer("sns-sqs-test");

		SqsConsumer sqsConsumer0 = SqsConsumer.create("sns-sqs-test", snsProducer.getTopicArn());
		SqsConsumer sqsConsumer1 = SqsConsumer.create("sns-sqs-test", snsProducer.getTopicArn());

		sqsConsumer0.onReadStart(message -> consumeSqsMessage(sqsConsumer0, message));
		sqsConsumer1.onReadStart(message -> consumeSqsMessage(sqsConsumer1, message));

		snsProducer.issue("Get the message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ RabbitMQ test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		RabbitMqEventProducer<String> rabbitMqProducer = RabbitMqEventProducer.create("rabbitmq-test");

		RabbitMqConsumer rabbitMqConsumer1 = RabbitMqConsumer.create("rabbitmq-test", rabbitMqProducer.getExchange());
		RabbitMqConsumer rabbitMqConsumer2 = RabbitMqConsumer.create("rabbitmq-test", rabbitMqProducer.getExchange());

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			System.out.println(String.format("Message received from RabbitMQ consumerTag %s: ", consumerTag));
			System.out.println(message);
    	};
		rabbitMqConsumer1.onReadMessage(deliverCallback);
		rabbitMqConsumer2.onReadMessage(deliverCallback);

		rabbitMqProducer.issue("Will you get this message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ActiveMQ test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		ActiveMqEventProducer<String> activeMqProducer = new ActiveMqEventProducer<>("activemq-test");

		ActiveMqConsumer activeMqConsumer0 = new ActiveMqConsumer("activemq-test", activeMqProducer.getTopicName());
		ActiveMqConsumer activeMqConsumer1 = new ActiveMqConsumer("activemq-test", activeMqProducer.getTopicName());

		activeMqConsumer0.onReadStart(message -> consumeActiveMqMessage(activeMqConsumer0, message));
		activeMqConsumer1.onReadStart(message -> consumeActiveMqMessage(activeMqConsumer1, message));

		activeMqProducer.issue("Hey u, d'ya get the message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Kafka test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		KafkaEventProducer<String> kafkaProducer = new KafkaEventProducer<>("kafka-test");

		KafkaEventConsumer kafkaConsumer0 = KafkaEventConsumer.create("kafka-test", kafkaProducer.getTopic());
		KafkaEventConsumer kafkaConsumer1 = KafkaEventConsumer.create("kafka-test", kafkaProducer.getTopic());

		kafkaConsumer0.onReadStart(message -> consumeKafkaMessage(kafkaConsumer0, message));
		kafkaConsumer1.onReadStart(message -> consumeKafkaMessage(kafkaConsumer1, message));

		Thread.sleep(20000);
		kafkaProducer.issue("First message on Kafka");
		kafkaProducer.issue("Second message on Kafka");
		kafkaProducer.issue("Third message on Kafka");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Endpoint test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		EndpointEventProducer.Endpoint endpoint = new EndpointEventProducer.Endpoint(new URL("http://localhost:8080/callback"));
		EventProducer<String> endpointEventProducer = new EndpointEventProducer<>(endpoint);

		Executors.newSingleThreadExecutor().submit(
				() -> {
					try {
						// let's wait 10s so the RestController bean is instantiated
						Thread.sleep(10000);
						endpointEventProducer.issue("Here is the message!");
					} catch (InterruptedException | NotificationException e) {
						e.printStackTrace();
					}
				}
		);

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting Spring Web ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		SpringApplication.run(NotificationdemoApplication.class, args);
	}

	private static void consumeSqsMessage(SqsConsumer sqsConsumer, Message message) {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(String.format("Message received from SQS %s:", sqsConsumer.getQueueEndpoint()));
		System.out.println(message.body());
	}

	private static void consumeActiveMqMessage(ActiveMqConsumer activeMqConsumer, String message) {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(String.format("ActiveMQ message received from clientId %s: %s", activeMqConsumer.getClientId(), message));
	}

	private static void consumeKafkaMessage(KafkaEventConsumer kafkaEventConsumer, ConsumerRecord<String, String> record) {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("Kafka consumer " + kafkaEventConsumer.getConsumerName() + " - received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
	}




}
