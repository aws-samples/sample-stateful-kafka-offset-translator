package com.amazonaws.samples.offsettranslator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Parses and rewrites Spark Structured Streaming checkpoint <code>offsets/&lt;batchId&gt;</code>
 * files. The on-disk format is text, line-oriented:
 * <pre>
 *   v1
 *   {"batchWatermarkMs":...,"batchTimestampMs":...,"conf":{...}}
 *   {"topicA":{"0":12345,"1":67890},"topicB":{"0":111}}
 * </pre>
 *
 * <p>The third line is the Kafka source's offsets (KafkaSourceOffset JSON). For multi-source
 * queries Spark writes one file per source under different sub-paths; this class focuses on
 * a single file at a time. The first two lines are preserved verbatim so we don't disturb
 * watermark or conf state.
 */
public class SparkCheckpoint {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String versionLine;
    private final String metadataLine;
    private final Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
    /** Preserves topic order so we can write back deterministically. */
    private final List<String> topicOrder = new ArrayList<>();

    private SparkCheckpoint(String versionLine, String metadataLine) {
        this.versionLine = versionLine;
        this.metadataLine = metadataLine;
    }

    public static SparkCheckpoint parse(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        String[] lines = text.split("\n", -1);
        if (lines.length < 3) {
            throw new IllegalArgumentException("Not a Spark offsets file: expected at least 3 lines, got " + lines.length);
        }
        SparkCheckpoint cp = new SparkCheckpoint(lines[0], lines[1]);
        try {
            JsonNode root = MAPPER.readTree(lines[2]);
            if (!root.isObject()) {
                throw new IllegalArgumentException("offsets line is not a JSON object");
            }
            Iterator<Map.Entry<String, JsonNode>> topicIt = root.fields();
            while (topicIt.hasNext()) {
                Map.Entry<String, JsonNode> topicEntry = topicIt.next();
                String topic = topicEntry.getKey();
                cp.topicOrder.add(topic);
                Iterator<Map.Entry<String, JsonNode>> partIt = topicEntry.getValue().fields();
                while (partIt.hasNext()) {
                    Map.Entry<String, JsonNode> pe = partIt.next();
                    cp.offsets.put(new TopicPartition(topic, Integer.parseInt(pe.getKey())), pe.getValue().asLong());
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse Spark offsets JSON line: " + e.getMessage(), e);
        }
        return cp;
    }

    public Map<TopicPartition, Long> offsets() { return Collections.unmodifiableMap(offsets); }

    /** Produce a new file with translated offsets, preserving v-line and metadata line. */
    public byte[] rewriteWithOffsets(Map<TopicPartition, Long> translated) {
        Map<String, ObjectNode> byTopic = new LinkedHashMap<>();
        for (String t : topicOrder) byTopic.put(t, MAPPER.createObjectNode());
        for (Map.Entry<TopicPartition, Long> e : translated.entrySet()) {
            String t = e.getKey().topic();
            byTopic.computeIfAbsent(t, k -> MAPPER.createObjectNode())
                   .put(String.valueOf(e.getKey().partition()), e.getValue());
        }
        ObjectNode root = MAPPER.createObjectNode();
        for (Map.Entry<String, ObjectNode> e : byTopic.entrySet()) root.set(e.getKey(), e.getValue());
        try {
            String json = MAPPER.writeValueAsString(root);
            return (versionLine + "\n" + metadataLine + "\n" + json + "\n").getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize translated offsets", e);
        }
    }
}
