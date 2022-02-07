package com.example.notificationdemo.notifications.consumers;

import com.example.notificationdemo.notifications.producers.RabbitMqChannel;
import com.example.notificationdemo.utils.Properties;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * This class acts as a consumer for the {@link RabbitMqChannel}.
 * It creates a queue by specifying the notification id and the exchange name of the related RabbitMQ exchange.
 * The new queue is then subscribed to the given RabbitMQ exchange in order to receive and buffer all the incoming messages.
 * Every RabbitMqConsumer instance for a specific notification id has its own copy of the messages.
 */
public class RabbitMqConsumer {

    private String eventName;
    private Channel channel;
    private String queue;
    private String exchange;
    private static int queueNumber = -1;

    // when used in the same application we can reuse the channel and get the input data from a given notification producer
    public RabbitMqConsumer(final RabbitMqChannel producer) throws IOException {
        this(producer.id(), producer.getExchange(), producer.getChannel());
    }

    public RabbitMqConsumer(String eventName, String exchange) throws IOException, TimeoutException {
        this.eventName = eventName;
        this.channel = channel();  // creates a new channel
        this.queue = createQueue(eventName, exchange);
    }

    private RabbitMqConsumer(String eventName, String exchange, final Channel channel) throws IOException {
        this.eventName = eventName;
        this.channel = channel;   // reuses the channel got from the notification given in input
        this.queue = createQueue(eventName, exchange);
    }

    private Channel channel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(Properties.get("rabbitmq.host"));
        factory.setPort(Integer.parseInt(Properties.get("rabbitmq.port")));
        factory.setUsername(Properties.get("rabbitmq.username"));
        factory.setPassword(Properties.get("rabbitmq.password"));

        return factory.newConnection().createChannel();
    }

    private String createQueue(String id, String exchange) throws IOException {
        queueNumber++;
        queue = id+"-queue"+queueNumber+"";
        this.channel.queueDeclare(queue, false, false, false, null);
        this.channel.queueBind(queue, exchange, "");
        return queue;
    }

    /**
     * Registers a callback when a new message is received in the queue
     * @param deliverCallback the action that must be performed at the reading of a new message from the queue
     * @throws IOException
     */
    public void onReadMessage(DeliverCallback deliverCallback) throws IOException {
        this.channel.basicConsume(this.queue, true, deliverCallback, consumerTag -> { });
    }

}
