package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import com.example.notificationdemo.utils.Properties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * This class emits a {@link Notification} by publishing a message
 * on a ActiveMQ topic.
 * It automatically creates a topic (if it doesn't exist yet)
 * and publish the notification.
 * @param <T> the body of the message passed as a JSON String
 */
public class ActiveMqNotification<T> implements Notification<T> {

    private final String id;
    private String topicName;
    private Connection connection;
    private Session session;
    private Topic topic;
    private MessageProducer producer;
    private ObjectMapper mapper = new ObjectMapper();

    private static int clientIdIndex = 0;

    public ActiveMqNotification(String id) throws JMSException {
        this.id = id;
        this.connection = connection();
        this.session = session(this.connection);
        this.topicName = id+"-topic";
        this.topic = this.session.createTopic(this.topicName);
        this.producer = session.createProducer(topic);
    }

    /**
     * Closes the JMS Session
     * @throws JMSException
     */
    public void closeSession() throws JMSException {
       this.connection.close();
    }

    /**
     * Closes the JMS Connection
     * @throws JMSException
     */
    public void closeConnection() throws JMSException {
        this.connection.close();
    }

    public Connection getConnection() {
        return connection;
    }

    public Session getSession() {
        return session;
    }

    public Topic getTopic() {
        return topic;
    }

    public String getTopicName() throws JMSException {
        return topic.getTopicName();
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public void issue(T body) throws NotificationException {
        if (body == null) throw new NotificationException("Body is null");
        if (this.session == null)  throw new NotificationException("ActiveMQ session is null");
        if (this.producer == null)  throw new NotificationException("ActiveMQ producer exchange is null");

        try {
            mapper.writeValueAsString(body);
            TextMessage message = session.createTextMessage(mapper.writeValueAsString(body));
            this.producer.send(message);
        } catch (JsonProcessingException | JMSException e) {
            throw new NotificationException(e.getMessage());
        }
    }

    private Connection connection() throws JMSException {
        clientIdIndex++;
        String clientId = this.getClass().getSimpleName()+clientIdIndex+"";

        Connection connection = new ActiveMQConnectionFactory(Properties.get("activemq.host"))
                .createConnection(Properties.get("activemq.username"), Properties.get("activemq.password"));
        connection.setClientID(clientId);
        connection.start();
        return connection;
    }

    private Session session(final Connection connection) throws JMSException {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

}
