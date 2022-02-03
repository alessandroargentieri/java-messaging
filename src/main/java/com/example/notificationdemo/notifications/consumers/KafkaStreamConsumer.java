package com.example.notificationdemo.notifications.consumers;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaStreamConsumer extends ShutdownableThread {

    private final String id;
    private String topic;
    private KafkaConsumer<String, String> consumer;
    private String consumerName;

    private static int consumerNumber = -1;

    public static KafkaStreamConsumer createConsumer(String id, String topic) {
        consumerNumber++;
        return new KafkaStreamConsumer(id, id+"-consumer"+consumerNumber+"", topic);
    }

    private KafkaStreamConsumer(String id, String consumerName, String topic) {
        super(consumerName, false);
        this.id = id;
        this.consumerName = consumerName;
        this.topic = topic;
        this.consumer = kafkaConsumer(topic);
    }

    private KafkaConsumer<String, String> kafkaConsumer(String topic) {

        String kafkaUrl = String.format("%s:%s",
                com.example.notificationdemo.utils.Properties.get("kafka.host"),
                com.example.notificationdemo.utils.Properties.get("kafka.port"));

        int sessionTimeoutMs = Integer.parseInt(com.example.notificationdemo.utils.Properties.get("kafka.session.timeout"));
        String enableAutoCommit = com.example.notificationdemo.utils.Properties.get("kafka.enable.autocommit");
        String autocommitInterval = com.example.notificationdemo.utils.Properties.get("kafka.autocommit.interval");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerName);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autocommitInterval);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    @Override
    public void doWork() {
        ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(5000));
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println("Kafka consumer " + this.consumerName + " - received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
    }
}
