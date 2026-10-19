package com.niranjankagri.orders;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Reads a topic from "now" on, like a downstream service would, so a test sees only the records produced after the
 * probe was opened. Not part of any consumer group: it never takes messages away from the service.
 */
public final class KafkaProbe implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    /** Assigns all partitions of the topic and moves to their end, so only records produced from now on are seen. */
    public KafkaProbe(String bootstrapServers, String topic) {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.CLIENT_ID_CONFIG, "probe-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position); // resolve the end offsets now, before the test produces anything
    }

    /** Waits until a record matching the condition arrives (at most 15 seconds) and returns it. */
    public ConsumerRecord<String, String> awaitRecord(Predicate<ConsumerRecord<String, String>> condition) {
        return await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ZERO).until(() -> {
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
            return seen.stream().filter(condition).findFirst().orElse(null);
        }, record -> record != null);
    }

    /** The values of every record read so far. */
    public List<String> seenValues() {
        return seen.stream().map(ConsumerRecord::value).toList();
    }

    /** Closes the consumer; try-with-resources calls it. */
    @Override
    public void close() {
        consumer.close();
    }
}
