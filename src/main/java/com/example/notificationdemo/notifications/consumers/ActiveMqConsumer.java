package com.example.notificationdemo.notifications.consumers;

import com.example.notificationdemo.notifications.producers.ActiveMqEventProducer;
import com.example.notificationdemo.utils.ContinuousJob;
import com.example.notificationdemo.utils.Properties;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.function.Consumer;

/**
 * This class acts as a consumer for the {@link ActiveMqEventProducer}.
 * It creates a subscriber for the producer's topic by specifying the notification id and the topic name.
 * Every ActiveMqConsumer instance for a specific notification id has its own copy of the messages.
 * It can be started as a {@link ContinuousJob}
 */
public class ActiveMqConsumer extends ContinuousJob {

    private String eventName;
    private Connection connection;
    private Session session;
    private Topic topic;
    private MessageConsumer consumer;
    private String clientId;
    private Consumer<String> onReadConsumer;

    private static int clientIdIndex = -1;

    /**
     * Basic constructor for {@link ActiveMqConsumer}.
     *
     * @param eventName the event name
     * @param topicName the ActiveMQ topic name
     * @throws JMSException
     */
    public ActiveMqConsumer(String eventName, String topicName) throws JMSException {
        this.eventName = eventName;
        this.connection = connection(eventName);
        this.session = session(this.connection);
        this.topic = this.session.createTopic(topicName);
        this.consumer = this.session.createConsumer(topic);
    }

    /**
     * Constructor for {@link ActiveMqConsumer}.
     * It take advantage of an existing {@link ActiveMqEventProducer} to reuse connection, session and topic.
     *
     * @param producer the ActiveMqChannel
     * @throws JMSException
     */
    public ActiveMqConsumer(final ActiveMqEventProducer producer) throws JMSException {
        this.eventName = producer.getEventName();
        this.connection = producer.getConnection();
        this.session = producer.getSession();
        this.topic = producer.getTopic();
        this.consumer = this.session.createConsumer(topic);
    }

    /**
     * Returns a text message (a JSON string if the message was a DTO) read from the topic.
     *
     * @return received text message
     * @throws JMSException
     */
    public String readMessage() throws JMSException {
        Message message = this.consumer.receive(5L);
        if (message != null) {
            return ((TextMessage) message).getText();
        }
        return null;
    }

    private Connection connection(String id) throws JMSException {
        clientIdIndex++;
        this.clientId = id+"-consumer"+clientIdIndex+"";

        Connection connection = new ActiveMQConnectionFactory(Properties.get("activemq.host"))
                .createConnection(Properties.get("activemq.username"), Properties.get("activemq.password"));
        connection.setClientID(clientId);
        connection.start();
        return connection;
    }

    private Session session(final Connection connection) throws JMSException {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Returns the ActiveMq clientId of the consumer.
     *
     * @return the cliendId
     */
    public String getClientId() {
        return this.clientId;
    }

    /**
     * The job to be repeated at the various intervals.
     */
    @Override
    public void doWork() {
        try {
            String message = this.readMessage();
            if (message != null) {
                this.onReadConsumer.accept(message);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts listening and reacting to the messages.
     * Gets a {@link Consumer} to consume the read messages.
     *
     * @param consumer the action to be performed on the read message
     */
    public void onReadStart(Consumer<String> consumer) {
        this.onReadConsumer = consumer;
        this.start();
    }
}


