package com.example.notificationdemo.notifications.producers;

import com.example.notificationdemo.notifications.Notification;
import com.example.notificationdemo.notifications.NotificationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;

import java.util.Properties;

public class KafkaNotification<T> implements Notification<T> {

    private final String id;
    private String topic;
    private KafkaProducer producer;
    private ObjectMapper mapper = new ObjectMapper();

    public KafkaNotification(String id) {
        this.id = id;
        this.topic = topic(id);
        this.producer = initProducer();
    }

    private String topic(String id) {
        String topic = id+"-topic";

        KafkaZkClient zkClient = kafkaZkClient();
        if (zkClient.topicExists(topic)) return topic;

        int partitions = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.topic.num_partitions"));
        int replication = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.topic.num_replications"));
        Properties topicConfig = new Properties();
        topicConfig.put("cleanup.policy","delete");
        new AdminZkClient(zkClient).createTopic(topic,partitions,replication,topicConfig, RackAwareMode.Disabled$.MODULE$);

        return topic;
    }

    private KafkaZkClient kafkaZkClient() {

        String zookeperUrl = String.format("%s:%s",
                com.example.notificationdemo.utils.Properties.get("kafka.zookeeper.host"),
                com.example.notificationdemo.utils.Properties.get("kafka.zookeeper.port"));

        boolean isSucre = false;
        int sessionTimeoutMs = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.session.timeout"));
        int connectionTimeoutMs = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.connection.timeout"));
        int maxInFlightRequests = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.max_inflight_requests"));
        String metricGroup = com.example.notificationdemo.utils.Properties.get("kafka.metric.group");
        String metricType = com.example.notificationdemo.utils.Properties.get("kafka.metric.type");

        return KafkaZkClient.apply(zookeperUrl, isSucre, sessionTimeoutMs,
                connectionTimeoutMs, maxInFlightRequests, Time.SYSTEM, metricGroup,metricType);
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public void issue(T body) throws NotificationException {
        if (body == null) throw new NotificationException("Body is null");
        if (this.producer == null) throw new NotificationException("KafkaProducer is null");

        try {
            this.producer.send(new ProducerRecord<String, String>(this.topic, mapper.writeValueAsString(body)));
        } catch (JsonProcessingException e) {
            throw new NotificationException(e.getMessage());
        }
    }

    private void ciao() {
        String kafkaUrl = String.format("%s:%s",
                com.example.notificationdemo.utils.Properties.get("kafka.host"),
                com.example.notificationdemo.utils.Properties.get("kafka.port"));

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUrl);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //create producer
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        //send messages to my-topic
        for(int i = 0; i < 100; i++) {
            ProducerRecord producerRecord = new ProducerRecord<Integer, String>(this.topic, i, "Test Message #" + Integer.toString(i));
            producer.send(producerRecord);
        }

        //close producer
        producer.close();
    }

    private KafkaProducer initProducer() {
        String kafkaUrl = String.format("%s:%s",
                com.example.notificationdemo.utils.Properties.get("kafka.host"),
                com.example.notificationdemo.utils.Properties.get("kafka.port"));

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUrl);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer(props);
    }

    /**
     * Closes the kafka producer.
     */
    public void closeProducer() {
        this.producer.close();
    }
}
