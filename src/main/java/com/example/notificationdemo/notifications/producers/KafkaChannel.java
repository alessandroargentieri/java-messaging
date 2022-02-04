package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Channel;
import com.example.notificationdemo.notifications.NotificationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaChannel<T> implements Channel<T> {

    private final String id;
    private String topic;
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper = new ObjectMapper();

    public KafkaChannel(String id) {
        this.id = id;
        this.producer = initProducer();
        this.topic = topic(id);
    }

    private String topic(String id) {
        String topic = id+"-topic";

        String kafkaUrl = String.format("%s:%s",
                com.example.notificationdemo.utils.Properties.get("kafka.host"),
                com.example.notificationdemo.utils.Properties.get("kafka.port"));

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
            if (client.listTopics().names().get().stream().anyMatch(t -> topic.equals(t))) return topic;
            CreateTopicsResult result = client.createTopics(Arrays.asList(new NewTopic(topic, numPartitions, numReplications)));
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
        return topic;
    }

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

    public String getTopic() {
        return topic;
    }

    private KafkaProducer<String, String> initProducer() {
        String kafkaUrl = String.format("%s:%s",
                com.example.notificationdemo.utils.Properties.get("kafka.host"),
                com.example.notificationdemo.utils.Properties.get("kafka.port"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }

    /**
     * Closes the kafka producer.
     */
    public void closeProducer() {
        this.producer.close();
    }
}
