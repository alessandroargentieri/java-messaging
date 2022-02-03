package com.example.notificationdemo.notifications.consumers;

import com.example.notificationdemo.utils.ContinuousRunnable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * KafkaStreamConsumer is a consumer for the incoming Kafka messages. It can be started as a {@link Thread}.
 */
public class KafkaStreamConsumer extends ContinuousRunnable {

    private final String id;
    private String topic;
    private KafkaConsumer<String, String> consumer;
    private String consumerName;
    private Consumer<ConsumerRecord<String, String>> onReadConsumer;

    private static int consumerNumber = -1;

    public static KafkaStreamConsumer createConsumer(String id, String topic) {
        consumerNumber++;
        return new KafkaStreamConsumer(id, id+"-consumer"+consumerNumber+"", topic);
    }

    private KafkaStreamConsumer(String id, String consumerName, String topic) {
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

    /**
     * Returns the Kafka consumer name.
     * @return the kafka consumer name
     */
    public String getConsumerName() {
        return consumerName;
    }

    @Override
    protected void doWork() {
        ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(5000));
        for (ConsumerRecord<String, String> record : records) {
            this.onReadConsumer.accept(record);
        }
    }

    /**
     * Starts listening and reacting to the messages.
     * Gets a {@link Consumer} to consume the read messages.
     * @param consumer the action to be performed on the read message
     */
    public void onReadStart(Consumer<ConsumerRecord<String, String>> consumer) {
        this.onReadConsumer = consumer;
        Executors.newSingleThreadExecutor().submit(this);
    }
}
