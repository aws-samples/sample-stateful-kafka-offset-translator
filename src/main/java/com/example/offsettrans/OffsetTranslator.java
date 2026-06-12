package com.example.offsettrans;

import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Pure translation logic. Takes a source-cluster offset map and produces the target-cluster
 * equivalent, plus a per-partition diagnostic report.
 */
public class OffsetTranslator {

    private final OffsetSyncStore store;

    public OffsetTranslator(OffsetSyncStore store) { this.store = store; }

    /** Result of translating one batch of offsets. */
    public static class Result {
        public final Map<TopicPartition, Long> translated = new LinkedHashMap<>();
        public final Map<TopicPartition, String> unmapped = new LinkedHashMap<>();
        public boolean isComplete() { return unmapped.isEmpty(); }
    }

    public Result translate(Map<TopicPartition, Long> sourceOffsets) {
        Result out = new Result();
        for (Map.Entry<TopicPartition, Long> e : sourceOffsets.entrySet()) {
            OptionalLong t = store.translate(e.getKey(), e.getValue());
            if (t.isPresent()) {
                out.translated.put(e.getKey(), t.getAsLong());
            } else {
                out.unmapped.put(e.getKey(),
                        "no offset-sync sample <= " + e.getValue() + " for " + e.getKey());
            }
        }
        return out;
    }
}
