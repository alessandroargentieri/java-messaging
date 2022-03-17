package com.example.notificationdemo.notifications.consumers;

import com.example.notificationdemo.notifications.producers.RabbitMqEventProducer;
import com.example.notificationdemo.utils.Properties;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * This class acts as a consumer for the {@link RabbitMqEventProducer}.
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

    /**
     * Returns an instance of {@link RabbitMqConsumer}.
     * It links to or create a RabbitMQ queue named '<event-name>-queue'.
     *
     * @param eventName the event name
     * @param exchangeName the RabbitMQ exchange name
     * @return the RabbitMqConsumer
     * @throws IOException
     * @throws TimeoutException
     */
    public static RabbitMqConsumer create(String eventName, String exchangeName) throws IOException, TimeoutException {
        queueNumber++;
        return RabbitMqConsumer.create(eventName, exchangeName, eventName+"-queue"+queueNumber+"");
    }

    /**
     * Returns an instance of {@link RabbitMqConsumer}.
     * It links to the given RabbitMQ queue.
     *
     * @param eventName the event name
     * @param exchangeName the RabbitMQ exchange name
     * @param queueName the RabbitMQ queue name
     * @return the RabbitMqConsumer
     * @throws IOException
     * @throws TimeoutException
     */
    public static RabbitMqConsumer create(String eventName, String exchangeName, String queueName) throws IOException, TimeoutException {
        return new RabbitMqConsumer(eventName, exchangeName, queueName, channel());
    }

    /**
     * Returns an instance of {@link RabbitMqConsumer} from the given {@link RabbitMqEventProducer}.
     * It links to or create a RabbitMq queue named '<event-name>-queue'.
     *
     * @param producer the instance of {@link RabbitMqEventProducer}
     * @return the RabbitMqConsumer
     * @throws IOException
     */
    public static RabbitMqConsumer createConsumerFromProducer(final RabbitMqEventProducer producer) throws IOException {
        queueNumber++;
        return RabbitMqConsumer.createConsumerFromProducer(producer, producer.getEventName()+"-queue"+queueNumber+"");
    }

    // when used in the same application we can reuse the channel and get the input data from a given notification producer

    /**
     * Returns an instance of {@link RabbitMqConsumer} from the given {@link RabbitMqEventProducer}.
     * It links to or create a RabbitMq queue with the given name.
     *
     * @param producer the instance of {@link RabbitMqEventProducer}
     * @param queueName the name of a RabbitMQ queue to link to, or to create
     * @return the RabbitMqConsumer
     * @throws IOException
     */
    public static RabbitMqConsumer createConsumerFromProducer(final RabbitMqEventProducer producer, String queueName) throws IOException {
        return new RabbitMqConsumer(producer.getEventName(), producer.getExchange(), queueName, producer.getChannel());
    }

    private RabbitMqConsumer(String eventName, String exchangeName, String queueName, final Channel channel) throws IOException {
        this.eventName = eventName;
        this.exchange = exchangeName;
        this.channel = channel;
        this.queue = queueName;
        if (Boolean.TRUE.equals(Boolean.parseBoolean(Properties.get("rabbitmq.enable.queue.create")))) {
            createQueue();
        }
        bindQueue();
    }

    private static Channel channel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Properties.get("rabbitmq.host"));
        factory.setPort(Integer.parseInt(Properties.get("rabbitmq.port")));
        factory.setUsername(Properties.get("rabbitmq.username"));
        factory.setPassword(Properties.get("rabbitmq.password"));
        return factory.newConnection().createChannel();
    }

    private void createQueue() throws IOException {
        this.channel.queueDeclare(this.queue, false, false, false, null);
    }

    private void bindQueue() throws IOException {
        this.channel.queueBind(this.queue, this.exchange, "");
    }

    /**
     * Registers a callback when a new message is received in the queue.
     *
     * @param deliverCallback the action that must be performed at the reading of a new message from the queue
     * @throws IOException
     */
    public void onReadMessage(DeliverCallback deliverCallback) throws IOException {
        this.channel.basicConsume(this.queue, true, deliverCallback, consumerTag -> { });
    }

}
