package com.example.notificationdemo;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.notifications.consumers.ActiveMqConsumer;
import com.example.notificationdemo.notifications.consumers.KafkaStreamConsumer;
import com.example.notificationdemo.notifications.consumers.RabbitMqConsumer;
import com.example.notificationdemo.notifications.consumers.SqsConsumer;
import com.example.notificationdemo.notifications.producers.*;
import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@SpringBootApplication
public class NotificationdemoApplication {

	public static void main(String[] args) throws URISyntaxException, NotificationException, InterruptedException, IOException, TimeoutException, JMSException {

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SQS-SNS test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		SnsNotification<String> snsNotification = new SnsNotification<>("sns-sqs-test");

		SqsConsumer sqsConsumer0 = new SqsConsumer("sns-sqs-test", snsNotification.getTopicArn());
		SqsConsumer sqsConsumer1 = new SqsConsumer("sns-sqs-test", snsNotification.getTopicArn());

		sqsConsumer0.onReadStart(message -> consumeSqsMessage(sqsConsumer0, message));
		sqsConsumer1.onReadStart(message -> consumeSqsMessage(sqsConsumer1, message));

		snsNotification.issue("Get the message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ RabbitMQ test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		RabbitMqNotification<String> rabbitMqNotification = new RabbitMqNotification<>("rabbitmq-test");

		RabbitMqConsumer rabbitMqConsumer1 = new RabbitMqConsumer("rabbitmq-test", rabbitMqNotification.getExchange());
		RabbitMqConsumer rabbitMqConsumer2 = new RabbitMqConsumer("rabbitmq-test", rabbitMqNotification.getExchange());

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			System.out.println(String.format("Message received from RabbitMQ consumerTag %s: ", consumerTag));
			System.out.println(message);
    	};
		rabbitMqConsumer1.onReadMessage(deliverCallback);
		rabbitMqConsumer2.onReadMessage(deliverCallback);

		rabbitMqNotification.issue("Will you get this message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ActiveMQ test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		ActiveMqNotification<String> activeMqNotification = new ActiveMqNotification<>("activemq-test");

		ActiveMqConsumer activeMqConsumer0 = new ActiveMqConsumer("activemq-test", activeMqNotification.getTopicName());
		ActiveMqConsumer activeMqConsumer1 = new ActiveMqConsumer("activemq-test", activeMqNotification.getTopicName());

		activeMqConsumer0.onReadStart(message -> consumeActiveMqMessage(activeMqConsumer0, message));
		activeMqConsumer1.onReadStart(message -> consumeActiveMqMessage(activeMqConsumer1, message));

		activeMqNotification.issue("Hey u, d'ya get the message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Kafka test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		KafkaNotification<String> kafkaNotification = new KafkaNotification<>("kafka-test");

		KafkaStreamConsumer kafkaStreamConsumer0 = KafkaStreamConsumer.createConsumer("kafka-test", kafkaNotification.getTopic());
		KafkaStreamConsumer kafkaStreamConsumer1 = KafkaStreamConsumer.createConsumer("kafka-test", kafkaNotification.getTopic());

		kafkaStreamConsumer0.onReadStart(message -> consumeKafkaMessage(kafkaStreamConsumer0, message));
		kafkaStreamConsumer1.onReadStart(message -> consumeKafkaMessage(kafkaStreamConsumer1, message));

		Thread.sleep(20000);
		kafkaNotification.issue("First message on Kafka");
		kafkaNotification.issue("Second message on Kafka");
		kafkaNotification.issue("Third message on Kafka");


		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Endpoint test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		EndpointNotification.Endpoint endpoint = new EndpointNotification.Endpoint(new URL("http://localhost:8080/callback"));
		Notification<String> endpointNotification = new EndpointNotification<>("endpoint-test", endpoint);

		Executors.newSingleThreadExecutor().submit(
				() -> {
					try {
						// let's wait 10s so the RestController bean is instantiated
						Thread.sleep(10000);
						endpointNotification.issue("Here is the message!");
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
		System.out.println(String.format("Message received from SQS %s:", sqsConsumer.getQueue()));
		System.out.println(message.body());
	}

	private static void consumeActiveMqMessage(ActiveMqConsumer activeMqConsumer, String message) {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(String.format("ActiveMQ message received from clientId %s: %s", activeMqConsumer.getClientId(), message));
	}

	private static void consumeKafkaMessage(KafkaStreamConsumer kafkaStreamConsumer, ConsumerRecord<String, String> record) {
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("Kafka consumer " + kafkaStreamConsumer.getConsumerName() + " - received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
	}




}
