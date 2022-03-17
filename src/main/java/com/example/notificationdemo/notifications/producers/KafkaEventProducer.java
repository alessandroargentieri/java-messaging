package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Channel;
import com.example.notificationdemo.notifications.NotificationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * KafkaChannel class is the Kafka implementation for the {@link Channel} interface.
 * It creates a topic or link to an existing one.
 *
 * @param <T> the payload object type issued
 */
public class KafkaChannel<T> implements Channel<T> {

    private final String eventName;
    private String topic;
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper = new ObjectMapper();

    private String kafkaUrl = String.format("%s:%s", com.example.notificationdemo.utils.Properties.get("kafka.host"), com.example.notificationdemo.utils.Properties.get("kafka.port"));

    /**
     * Basic constructor for {@link KafkaChannel}.
     * It creates or link to a Kafka topic named '<event-name>-topic'.
     *
     * @param eventName the event name
     */
    public KafkaChannel(String eventName) {
        this(eventName, eventName+"-topic");
    }

    /**
     * Constructor for {@link KafkaChannel}.
     * It creates or link to the givem Kafka topic.
     *
     * @param eventName the event name
     * @param topicName the Kafka topic name
     */
    public KafkaChannel(String eventName, String topicName) {
        this.eventName = eventName;
        this.producer = initProducer();
        this.topic = topicName;
        if (Boolean.TRUE.equals(Boolean.parseBoolean(com.example.notificationdemo.utils.Properties.get("kafka.enable.topic.create")))) {
            // only if a specific flag is enabled the application has the priviledges to create a topic
            // else it must be attached to the topicName resource specified which already exists
            createTopic(topicName);
        }
    }

    private void createTopic(String topicName) {
        int maxIdleConn = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.connection.max_idle"));
        int reqTimeout = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.request.timeout"));
        int numPartitions = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.topic.num_partitions"));
        short numReplications = (short) Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.topic.num_replications"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, maxIdleConn);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, reqTimeout);
        try (AdminClient client = AdminClient.create(props)) {
            // return if topic already exists
            if (client.listTopics().names().get().stream().anyMatch(t -> topicName.equals(t))) return;
            // create otherwise
            client.createTopics(Arrays.asList(new NewTopic(topicName, numPartitions, numReplications)));
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Emits the notification in JSON String format.
     *
     * @param body the object to be sent as payload
     * @throws NotificationException
     */
    @Override
    public void issue(T body) throws NotificationException {
        if (body == null) throw new NotificationException("Body is null");
        if (this.topic == null) throw new NotificationException("Kafka Topic is null");
        if (this.producer == null) throw new NotificationException("KafkaProducer is null");

        try {
            this.producer.send(new ProducerRecord<String, String>(this.topic, UUID.randomUUID().toString(), mapper.writeValueAsString(body)));
        } catch (JsonProcessingException e) {
            throw new NotificationException(e.getMessage());
        }
    }

    /**
     * Returns the Kafka topic name.
     *
     * @return the Kafka topic name
     */
    public String getTopic() {
        return topic;
    }

    private KafkaProducer<String, String> initProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }

    /**
     * Closes the Kafka producer.
     */
    public void closeProducer() {
        this.producer.close();
    }
}
