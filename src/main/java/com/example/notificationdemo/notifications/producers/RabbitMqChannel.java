package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Channel;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.utils.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * This class emits a {@link Channel} by publishing a message
 * on a RabbitMQ Exchange.
 * It automatically creates an Exchange (if it doesn't exist yet)
 * and publish the notification.
 * @param <T> the body of the message passed as a JSON String
 */
public class RabbitMqChannel<T> implements Channel<T> {

    private final String eventName;
    private String exchange;
    private com.rabbitmq.client.Channel channel;
    private ObjectMapper mapper = new ObjectMapper();

    public RabbitMqChannel(String eventName) throws IOException, TimeoutException {
        this.eventName = eventName;
        this.channel = channel();
        this.exchange = createExchange(eventName);
    }

    public String getExchange() {
        return this.exchange;
    }

    public com.rabbitmq.client.Channel getChannel() {
        return this.channel;
    }

    private com.rabbitmq.client.Channel channel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(Properties.get("rabbitmq.host"));
        factory.setPort(Integer.parseInt(Properties.get("rabbitmq.port")));
        factory.setUsername(Properties.get("rabbitmq.username"));
        factory.setPassword(Properties.get("rabbitmq.password"));

        return factory.newConnection().createChannel();
    }

    private String createExchange(String id) throws IOException, TimeoutException {
        String exchange = id+"-exchange";
        this.channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true, false, null);
        return exchange;
    }

    public String id() {
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
