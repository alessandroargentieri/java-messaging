package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.EventProducer;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.utils.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * This class emits a {@link EventProducer} by publishing a message
 * on a RabbitMQ Exchange.
 * It automatically creates an Exchange (if it doesn't exist yet)
 * and publish the notification.
 *
 * @param <T> the body of the message passed as a JSON String
 */
public class RabbitMqEventProducer<T> implements EventProducer<T> {

    private final String eventName;
    private String exchange;
    private com.rabbitmq.client.Channel channel;
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Returns a new {@link RabbitMqEventProducer} by specifying the event name.
     * The class attempts creating a new exchange or attaching to an existing one
     * named "<event-name>-exchange".
     *
     * @param eventName the event name
     * @return the RabbitMqChannel
     * @throws IOException
     * @throws TimeoutException
     */
    public static RabbitMqEventProducer create(String eventName) throws IOException, TimeoutException {
        return RabbitMqEventProducer.create(eventName, eventName+"-exchange");
    }

    /**
     * Returns a new {@link RabbitMqEventProducer} by specifying the event name and the exchange name.
     * The class attempts creating a new exchange or attaching to an existing one.
     *
     * @param eventName the event name
     * @param exchangeName the exchange name
     * @return the RabbitMqChannel
     * @throws IOException
     * @throws TimeoutException
     */
    public static RabbitMqEventProducer create(String eventName, String exchangeName) throws IOException, TimeoutException {
        return new RabbitMqEventProducer(eventName, exchangeName, channel());
    }

    private RabbitMqEventProducer(String eventName, String exchangeName, final com.rabbitmq.client.Channel channel) throws IOException {
        this.eventName = eventName;
        this.channel = channel;
        this.exchange = exchangeName;
        if (Boolean.TRUE.equals(Boolean.parseBoolean(Properties.get("rabbitmq.enable.exchange.create")))) {
            createExchange();
        }
    }

    /**
     * Returns the exchange name.
     *
     * @return the exchange name
     */
    public String getExchange() {
        return this.exchange;
    }

    /**
     * Returns the {@link com.rabbitmq.client.Channel}
     *
     * @return the rabbitmq channel
     */
    public com.rabbitmq.client.Channel getChannel() {
        return this.channel;
    }

    private static com.rabbitmq.client.Channel channel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(Properties.get("rabbitmq.host"));
        factory.setPort(Integer.parseInt(Properties.get("rabbitmq.port")));
        factory.setUsername(Properties.get("rabbitmq.username"));
        factory.setPassword(Properties.get("rabbitmq.password"));

        return factory.newConnection().createChannel();
    }

    private void createExchange() throws IOException {
        this.channel.exchangeDeclare(this.exchange, BuiltinExchangeType.FANOUT, true, false, null);
    }

    /**
     * Returns the event name.
     *
     * @return the event name
     */
    public String getEventName() {
        return this.eventName;
    }

    @Override
    public void issue(T body) throws NotificationException {
        if (body == null) throw new NotificationException("Body is null");
        if (this.exchange == null)  throw new NotificationException("RabbitMQ exchange is null");

        try {
            this.channel.basicPublish(this.exchange, "", null, mapper.writeValueAsString(body).getBytes("UTF-8"));
        } catch (IOException e) {
            throw new NotificationException(e.getMessage());
        }
    }
}
