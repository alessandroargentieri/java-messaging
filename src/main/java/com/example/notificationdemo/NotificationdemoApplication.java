package com.example.notificationdemo;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.notifications.consumers.ActiveMqConsumer;
import com.example.notificationdemo.notifications.consumers.KafkaStreamConsumer;
import com.example.notificationdemo.notifications.consumers.RabbitMqConsumer;
import com.example.notificationdemo.notifications.consumers.SqsConsumer;
import com.example.notificationdemo.notifications.producers.*;
import com.rabbitmq.client.DeliverCallback;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class NotificationdemoApplication {

	public static void main(String[] args) throws URISyntaxException, NotificationException, InterruptedException, IOException, TimeoutException, JMSException {

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SQS-SNS test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		SnsNotification<String> snsNotification = new SnsNotification<>("sns-sqs-test");

		SqsConsumer sqsConsumer0 = new SqsConsumer("sns-sqs-test", snsNotification.getTopicArn());
		SqsConsumer sqsConsumer1 = new SqsConsumer("sns-sqs-test", snsNotification.getTopicArn());

		sqsConsumerOnRead(sqsConsumer0);
		sqsConsumerOnRead(sqsConsumer1);

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
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    	};
		rabbitMqConsumer1.onReadMessage(deliverCallback);
		rabbitMqConsumer2.onReadMessage(deliverCallback);

		rabbitMqNotification.issue("Will you get this message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ActiveMQ test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		ActiveMqNotification<String> activeMqNotification = new ActiveMqNotification<>("activemq-test");

		ActiveMqConsumer activeMqConsumer0 = new ActiveMqConsumer("activemq-test", activeMqNotification.getTopicName());
		ActiveMqConsumer activeMqConsumer1 = new ActiveMqConsumer("activemq-test", activeMqNotification.getTopicName());

		activeMqConsumerOnRead(activeMqConsumer0);
		activeMqConsumerOnRead(activeMqConsumer1);

		activeMqNotification.issue("Hey u, d'ya get the message?");

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Kafka test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		KafkaNotification<String> kafkaNotification = new KafkaNotification<>("kafka-test");
		KafkaStreamConsumer kafkaStreamConsumer1 = KafkaStreamConsumer.createConsumer("kafka-test", kafkaNotification.getTopic());
		kafkaStreamConsumer1.start();
		KafkaStreamConsumer kafkaStreamConsumer2 = KafkaStreamConsumer.createConsumer("kafka-test", kafkaNotification.getTopic());
		kafkaStreamConsumer2.start();

		System.out.println("Sleep 20 seconds");
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

	private static void activeMqConsumerOnRead(ActiveMqConsumer activeMqConsumer) {
		Executors.newSingleThreadExecutor().submit(
				() -> {
					boolean exit = false;
					while(!exit) {
						String message = null;
						try {
							message = activeMqConsumer.readMessage();
						} catch (JMSException e) {
							e.printStackTrace();
						}
						if (message != null) {
							System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
							System.out.println(String.format("ActiveMQ message received from clientId %s: %s", activeMqConsumer.getClientId(), message));
							System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
							exit = true;
						}
					}
				}
		);
	}

	private static void sqsConsumerOnRead(SqsConsumer sqsConsumer) {
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.submit(() -> {
			AtomicBoolean received = new AtomicBoolean(false);
			while(!received.get()) {
				sqsConsumer.readMessages().forEach(
						message -> {
							if (message != null) {
								System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
								System.out.println(String.format("Message received from SQS %s:", sqsConsumer.getQueue()));
								System.out.println(message.body());
								System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
								received.set(true);
							}
						}
				);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

}
