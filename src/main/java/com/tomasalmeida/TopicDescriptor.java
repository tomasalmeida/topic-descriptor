package com.tomasalmeida;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.utils.Bytes.BYTES_LEXICO_COMPARATOR;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

public class TopicDescriptor {

    private final Set<byte[]> keys = new TreeSet<>(BYTES_LEXICO_COMPARATOR);
    private long totalRecords;

    public static void main(String[] args) throws Exception {
        TopicDescriptor topicDescriptor = new TopicDescriptor();
        topicDescriptor.run(args);
    }

    private void run(final String[] args) throws Exception {
        final Properties properties = buildProperties(args[0]);

        consumeAllMessages(properties, args[1]);
        printTopicConfigs(properties, args[1]);
    }

    private void printTopicConfigs(Properties properties, String topic) throws ExecutionException, InterruptedException {
        final AdminClient admin = KafkaAdminClient.create(properties);
        printMessage("Config of the topic [%s]", topic);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        Config configs = admin
                .describeConfigs(List.of(configResource))
                .values()
                .get(configResource)
                .get();

        configs.entries()
                .forEach(entry -> printMessage("Config [%s] = [%s]", entry.name(), entry.value()));
    }

    private void consumeAllMessages(final Properties properties, final String topic) {
        final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        int zeroMessagesCounter = 0;
        try {
            consumer.subscribe(List.of(topic));
            printMessage("Verifying topic [%s]", topic);
            while (zeroMessagesCounter < 2) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                zeroMessagesCounter = records.count() == 0 ? ++zeroMessagesCounter : 0;
                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    totalRecords++;
                    keys.add(record.key());
                }
                System.out.print('.');
            }
        } finally {
            consumer.close();
        }
        System.out.println("");
        printMessage("Total number of records: %d", totalRecords);
        printMessage("Total number of distinct keys: %d", keys.size());
    }

    private void printMessage(final String format, final Object... args) {
        final String message = String.format(format, args);
        System.out.println(message);
    }

    private Properties buildProperties(final String configPath) throws IOException {
        final Properties properties = PropertiesLoader.load(configPath);
        // override configuration to be able to consume the whole topic
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }
}