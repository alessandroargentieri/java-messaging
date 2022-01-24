package com.example.notificationdemo;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.notifications.consumers.RabbitMqConsumer;
import com.example.notificationdemo.notifications.consumers.SqsConsumer;
import com.example.notificationdemo.notifications.producers.EndpointNotification;
import com.example.notificationdemo.notifications.producers.RabbitMqNotification;
import com.example.notificationdemo.notifications.producers.SnsNotification;
import com.rabbitmq.client.DeliverCallback;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class NotificationdemoApplication {

	public static void main(String[] args) throws URISyntaxException, NotificationException, InterruptedException, IOException, TimeoutException {

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SQS-SNS test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		SnsNotification<String> snsNotification = new SnsNotification<>("sns-sqs-test");
		SqsConsumer sqsConsumer = new SqsConsumer("sns-sqs-test", snsNotification.getTopicArn());

		snsNotification.issue("Get the message?");

		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.submit(() -> {
			AtomicBoolean received = new AtomicBoolean(false);
			while(!received.get()) {
				sqsConsumer.readMessages().forEach(
						message -> {
							if (message != null) {
								System.out.println("Message received from SQS: ");
								System.out.println(message.body());
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

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ RabbitMQ test ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		RabbitMqNotification<String> rabbitMqNotification = new RabbitMqNotification<>("rabbitmq-test");

		RabbitMqConsumer rabbitMqConsumer = new RabbitMqConsumer("rabbitmq-test", rabbitMqNotification.getExchange());

		rabbitMqNotification.issue("Will you get this message?");

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        	String message = new String(delivery.getBody(), "UTF-8");
        	System.out.println("Message received from RabbitMQ queue: ");
			System.out.println(message);
    	};
		rabbitMqConsumer.onReadMessage(deliverCallback);


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

}
