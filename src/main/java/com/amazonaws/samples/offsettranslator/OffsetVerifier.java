package com.amazonaws.samples.offsettranslator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Optional sanity check for the translator. For each translated
 * {@code (topic-partition, sourceOffset, targetOffset)} it reads exactly one record from each
 * cluster at that offset and compares them by Kafka {@code CreateTime}, key bytes, and value bytes.
 *
 * <p>The verdict is one of:
 * <ul>
 *   <li>{@code EXACT} - both records exist and have identical key/value/timestamp. The translation
 *       points at the same logical record on the target. Phase-2 reads will resume with no
 *       overlap and no gap.</li>
 *   <li>{@code BEHIND_BY_MS} - the target record exists but its timestamp is older than the
 *       source record's. The translation is conservative (at-least-once). Phase-2 will re-read a
 *       small number of records.</li>
 *   <li>{@code AHEAD_BY_MS} - the target record's timestamp is newer than the source record's.
 *       Treat as a bug or a topic-recreation event - phase-2 would skip data.</li>
 *   <li>{@code TARGET_OFFSET_OUT_OF_RANGE} - the requested target offset is past target's
 *       high-water-mark. Translation extrapolated past the replication tip; phase-2 will idle
 *       until replication catches up.</li>
 *   <li>{@code SOURCE_OFFSET_OUT_OF_RANGE} - the source offset is past source's HWM (shouldn't
 *       happen for a Spark-committed checkpoint).</li>
 *   <li>{@code TIMEOUT} - did not receive a record within the per-partition fetch budget.</li>
 * </ul>
 */
public class OffsetVerifier {

    private static final Logger log = LoggerFactory.getLogger(OffsetVerifier.class);

    public enum Verdict {
        EXACT,
        BEHIND_BY_MS,
        AHEAD_BY_MS,
        TARGET_OFFSET_OUT_OF_RANGE,
        SOURCE_OFFSET_OUT_OF_RANGE,
        TIMEOUT
    }

    /** Per-partition outcome plus enough context to print a useful summary. */
    public static class Result {
        public final TopicPartition tp;
        public final long sourceOffset;
        public final long targetOffset;
        public Verdict verdict;
        public Long deltaMs;        // target.ts - source.ts, when both records were fetched
        public String detail;       // free-form explanation, especially on errors

        public Result(TopicPartition tp, long sourceOffset, long targetOffset) {
            this.tp = tp; this.sourceOffset = sourceOffset; this.targetOffset = targetOffset;
        }
    }

    public List<Result> verify(Map<TopicPartition, Long> sourceOffsets,
                               Map<TopicPartition, Long> translatedOffsets,
                               Properties sourceConsumerProps,
                               Properties targetConsumerProps,
                               Duration perFetchTimeout) {
        List<Result> out = new ArrayList<>();

        try (KafkaConsumer<byte[], byte[]> src = newConsumer(sourceConsumerProps);
             KafkaConsumer<byte[], byte[]> tgt = newConsumer(targetConsumerProps)) {

            for (Map.Entry<TopicPartition, Long> e : sourceOffsets.entrySet()) {
                TopicPartition tp = e.getKey();
                long srcOff = e.getValue();
                Long tgtOffBoxed = translatedOffsets.get(tp);
                if (tgtOffBoxed == null) continue; // unmapped partitions are reported elsewhere
                long tgtOff = tgtOffBoxed;

                Result r = new Result(tp, srcOff, tgtOff);
                try {
                    ConsumerRecord<byte[], byte[]> sRec = fetchOne(src, tp, srcOff, perFetchTimeout);
                    ConsumerRecord<byte[], byte[]> tRec = fetchOne(tgt, tp, tgtOff, perFetchTimeout);
                    decide(r, sRec, tRec);
                } catch (RuntimeException ex) {
                    r.verdict = Verdict.TIMEOUT;
                    r.detail = ex.getMessage();
                }
                out.add(r);
            }
        }
        return out;
    }

    private static KafkaConsumer<byte[], byte[]> newConsumer(Properties base) {
        Properties p = new Properties();
        p.putAll(base);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "offset-verifier-" + UUID.randomUUID());
        // Fetch single records, no waiting for batch fill.
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        return new KafkaConsumer<>(p);
    }

    /**
     * Assigns the partition, seeks to {@code offset}, and polls until one record is returned or
     * the timeout elapses. Returns null if the offset is past HWM (out of range).
     */
    private static ConsumerRecord<byte[], byte[]> fetchOne(KafkaConsumer<byte[], byte[]> c,
                                                           TopicPartition tp,
                                                           long offset,
                                                           Duration timeout) {
        c.assign(Collections.singletonList(tp));
        Map<TopicPartition, Long> end = c.endOffsets(Collections.singletonList(tp));
        Long hwm = end.get(tp);
        if (hwm != null && offset >= hwm) return null;
        c.seek(tp, offset);

        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(500));
            if (!recs.isEmpty()) return recs.iterator().next();
        }
        throw new RuntimeException("no record fetched within " + timeout + " for " + tp + "@" + offset);
    }

    private static void decide(Result r,
                               ConsumerRecord<byte[], byte[]> srcRec,
                               ConsumerRecord<byte[], byte[]> tgtRec) {
        if (srcRec == null) {
            r.verdict = Verdict.SOURCE_OFFSET_OUT_OF_RANGE;
            r.detail = "source HWM <= " + r.sourceOffset;
            return;
        }
        if (tgtRec == null) {
            r.verdict = Verdict.TARGET_OFFSET_OUT_OF_RANGE;
            r.detail = "target HWM <= " + r.targetOffset + " (replication has not caught up)";
            return;
        }
        long delta = tgtRec.timestamp() - srcRec.timestamp();
        r.deltaMs = delta;
        boolean keysEqual = Arrays.equals(
                srcRec.key() == null ? new byte[0] : srcRec.key(),
                tgtRec.key() == null ? new byte[0] : tgtRec.key());
        boolean valsEqual = Arrays.equals(
                srcRec.value() == null ? new byte[0] : srcRec.value(),
                tgtRec.value() == null ? new byte[0] : tgtRec.value());

        if (delta == 0 && keysEqual && valsEqual) {
            r.verdict = Verdict.EXACT;
            r.detail = "key+value+timestamp identical";
        } else if (delta < 0) {
            r.verdict = Verdict.BEHIND_BY_MS;
            r.detail = "target ts is " + (-delta) + " ms older; expect duplicate reads on resume";
        } else {
            // delta > 0 OR (delta==0 but bytes differ). Either way the records do not match.
            r.verdict = Verdict.AHEAD_BY_MS;
            r.detail = "target ts is " + delta + " ms newer or content differs"
                    + " keysEqual=" + keysEqual + " valuesEqual=" + valsEqual
                    + " srcKey=" + safe(srcRec.key()) + " tgtKey=" + safe(tgtRec.key());
        }
    }

    private static String safe(byte[] b) {
        if (b == null) return "<null>";
        return new String(b, StandardCharsets.UTF_8);
    }
}
