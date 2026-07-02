package com.amazonaws.samples.offsettranslator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.mirror.OffsetSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Reads an MSK Replicator (or MM2) {@code offset-syncs} topic from the beginning to the current end
 * and builds an in-memory map of offset translations keyed by (topic, partition).
 *
 * <p>Records are the standard {@link OffsetSync} format produced by MirrorSourceConnector /
 * MSK Replicator. Each record gives a single sample mapping {@code upstreamOffset -> downstreamOffset}
 * for one topic-partition.
 *
 * <p>For each partition we keep all unique samples sorted by upstream offset, which lets the
 * translator find the largest sample with {@code upstream <= sourceOffset} via binary search.
 */
public class OffsetSyncStore {

    private static final Logger log = LoggerFactory.getLogger(OffsetSyncStore.class);

    /** Sorted list of (upstream, downstream) samples per partition. */
    private final Map<TopicPartition, NavigableMap<Long, Long>> samples = new HashMap<>();

    /**
     * Fully consume {@code offsetSyncsTopic} from the target cluster identified by
     * {@code consumerProps}. Blocks until end-of-log on every partition has been observed.
     */
    public void load(String offsetSyncsTopic, Properties consumerProps) {
        Properties props = new Properties();
        props.putAll(consumerProps);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "offset-translator-" + UUID.randomUUID());

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> parts = consumer.partitionsFor(offsetSyncsTopic);
            if (parts == null || parts.isEmpty()) {
                throw new IllegalArgumentException("offset-syncs topic not found or empty: " + offsetSyncsTopic);
            }
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo p : parts) tps.add(new TopicPartition(p.topic(), p.partition()));
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            Map<TopicPartition, Long> ends = consumer.endOffsets(tps);

            int recordsRead = 0;
            while (!isCaughtUp(consumer, ends)) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    OffsetSync sync = OffsetSync.deserializeRecord(toCr(r));
                    samples.computeIfAbsent(sync.topicPartition(), k -> new TreeMap<>())
                           .put(sync.upstreamOffset(), sync.downstreamOffset());
                    recordsRead++;
                }
            }
            log.info("Loaded {} offset-sync records covering {} partitions from {}",
                     recordsRead, samples.size(), offsetSyncsTopic);
        }
    }

    /**
     * Translate a single source offset for {@code tp} into the equivalent target offset.
     * Uses the same algorithm as MM2's OffsetSyncStore: pick the largest sample with
     * {@code upstream <= sourceOffset} and add the delta.
     *
     * @return Optional of translated offset, empty if the partition has no samples or the requested
     *         offset is older than every sample we have (translation undefined).
     */
    public OptionalLong translate(TopicPartition tp, long sourceOffset) {
        NavigableMap<Long, Long> partitionSamples = samples.get(tp);
        if (partitionSamples == null || partitionSamples.isEmpty()) return OptionalLong.empty();
        Map.Entry<Long, Long> floor = partitionSamples.floorEntry(sourceOffset);
        if (floor == null) return OptionalLong.empty();
        long delta = sourceOffset - floor.getKey();
        return OptionalLong.of(floor.getValue() + delta);
    }

    /** Number of partitions that have at least one sample. */
    public int coveredPartitionCount() { return samples.size(); }

    private static boolean isCaughtUp(KafkaConsumer<?, ?> consumer, Map<TopicPartition, Long> ends) {
        for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
            if (consumer.position(e.getKey()) < e.getValue()) return false;
        }
        return true;
    }

    /** {@link OffsetSync#deserializeRecord} expects a Kafka ConsumerRecord; pass through unchanged. */
    private static ConsumerRecord<byte[], byte[]> toCr(ConsumerRecord<byte[], byte[]> r) { return r; }
}
