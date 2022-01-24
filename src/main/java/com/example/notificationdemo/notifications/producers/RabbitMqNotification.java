package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.utils.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * This class emits a {@link Notification} by publishing a message
 * on a RabbitMQ Exchange.
 * It automatically creates an Exchange (if it doesn't exist yet)
 * and publish the notification.
 * @param <T> the body of the message passed as a JSON String
 */
public class RabbitMqNotification<T> implements Notification<T> {

    private String id;
    private String exchange;
    private Channel channel;
    private ObjectMapper mapper = new ObjectMapper();

    public RabbitMqNotification(String id) throws IOException, TimeoutException {
        this.id = id;
        this.channel = channel();
        this.exchange = createExchange(id);
    }

    public String getExchange() {
        return this.exchange;
    }

    public Channel getChannel() {
        return this.channel;
    }

    private Channel channel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(Properties.get("rabbitmq.host"));
        factory.setPort(Integer.parseInt(Properties.get("rabbitmq.port")));
        factory.setUsername(Properties.get("rabbitmq.username"));
        factory.setPassword(Properties.get("rabbitmq.password"));

        return factory.newConnection().createChannel();
    }

    private String createExchange(String id) throws IOException, TimeoutException {
        String exchange = id+"-exchange";
        this.channel.exchangeDeclare(this.exchange, BuiltinExchangeType.FANOUT, true, false, null);
        return exchange;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public void issue(T body) throws NotificationException {
        if (body == null || exchange == null) throw new NotificationException("");
        try {
            this.channel.basicPublish(this.exchange, "", null, mapper.writeValueAsString(body).getBytes("UTF-8"));
        } catch (IOException e) {
            throw new NotificationException(e.getMessage());
        }
    }
}
