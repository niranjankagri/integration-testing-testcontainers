package com.niranjankagri.orders.events;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Lag of the payment consumer group: how many records of payments.completed are not yet committed by this service.
 *
 * The Kafka client's own lag metric (kafka_consumer_fetch_manager_records_lag_max) is measured by the consumer while it
 * fetches, so a stuck or paused listener stops reporting. This gauge asks the broker instead: log-end offsets minus
 * the group's committed offsets, which keeps growing while nothing is processed.
 */
@Component
public class ConsumerLagMetrics implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagMetrics.class);
    private static final long TIMEOUT_SECONDS = 5;

    private final Admin admin;
    private final String groupId;

    public ConsumerLagMetrics(
            KafkaAdmin kafkaAdmin,
            MeterRegistry registry,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {
        this.admin = Admin.create(kafkaAdmin.getConfigurationProperties());
        this.groupId = groupId;
        registry.gauge("orders.payment.consumer.lag", this, ConsumerLagMetrics::paymentLag);
    }

    /**
     * Records of payments.completed the group has not committed yet, summed over all partitions. A partition the group
     * never committed counts from its beginning (the listener starts there: auto-offset-reset=earliest). NaN when Kafka
     * cannot answer, so a dashboard shows a gap instead of a misleading zero.
     */
    public double paymentLag() {
        try {
            List<TopicPartition> partitions = admin.describeTopics(List.of(Topics.PAYMENTS_COMPLETED))
                    .allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get(Topics.PAYMENTS_COMPLETED).partitions().stream()
                    .map(info -> new TopicPartition(Topics.PAYMENTS_COMPLETED, info.partition()))
                    .toList();
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<TopicPartition, Long> end = offsets(partitions, OffsetSpec.latest());
            Map<TopicPartition, Long> start = offsets(partitions, OffsetSpec.earliest());
            long lag = 0;
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata done = committed.get(partition);
                long position = done != null ? done.offset() : start.get(partition);
                lag += Math.max(0, end.get(partition) - position);
            }
            return lag;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return Double.NaN;
        } catch (Exception failure) {
            log.debug("Could not read the consumer lag of group {}", groupId, failure);
            return Double.NaN;
        }
    }

    private Map<TopicPartition, Long> offsets(List<TopicPartition> partitions, OffsetSpec spec) throws Exception {
        Map<TopicPartition, ListOffsetsResultInfo> result = admin.listOffsets(
                        partitions.stream().collect(Collectors.toMap(partition -> partition, partition -> spec)))
                .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    /** Closes the admin client with the application. */
    @Override
    public void destroy() {
        admin.close();
    }
}
