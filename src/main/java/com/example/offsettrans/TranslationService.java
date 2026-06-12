package com.example.offsettrans;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Library entry point. Wires together the {@link OffsetSyncStore},
 * {@link OffsetTranslator}, and a {@link CheckpointStore} and produces a translated
 * Spark checkpoint offsets file at a new path.
 */
public class TranslationService {

    private static final Logger log = LoggerFactory.getLogger(TranslationService.class);

    public static class Request {
        public String checkpointDir;       // input: Spark checkpoint root (contains offsets/, commits/, ...)
        public String outputCheckpointDir; // output: new dir to write translated files into
        public String offsetSyncsTopic;    // e.g. mskr-offset-syncs.<id>.internal
        public Properties targetClusterConsumerProps; // bootstrap + IAM/SASL config for target cluster
        /** If non-null, only this batch id is translated; otherwise the latest batch is used. */
        public Long batchId;

        // Optional verification mode. When sourceClusterConsumerProps is non-null the service
        // also reads one record from each cluster at the (source, translated) offsets and reports
        // whether they match. This is a sanity check, not a correctness requirement.
        public Properties sourceClusterConsumerProps;
        public Duration verifyPerFetchTimeout = Duration.ofSeconds(15);
    }

    public static class Response {
        public String writtenPath;
        public Map<TopicPartition, Long> sourceOffsets;
        public Map<TopicPartition, Long> translatedOffsets;
        public Map<TopicPartition, String> unmapped;
        /** Populated only when verification was requested. */
        public List<OffsetVerifier.Result> verification;
    }

    public Response translate(Request req) throws IOException {
        CheckpointStore inFs = CheckpointStore.forPath(req.checkpointDir);
        CheckpointStore outFs = CheckpointStore.forPath(req.outputCheckpointDir);

        String offsetsDir = joinPath(req.checkpointDir, "offsets");
        long batch = req.batchId != null ? req.batchId : findLatestBatch(inFs, offsetsDir);
        String inFile = joinPath(offsetsDir, String.valueOf(batch));
        log.info("Reading Spark checkpoint offsets file: {}", inFile);
        SparkCheckpoint cp = SparkCheckpoint.parse(inFs.read(inFile));

        OffsetSyncStore store = new OffsetSyncStore();
        store.load(req.offsetSyncsTopic, req.targetClusterConsumerProps);

        OffsetTranslator translator = new OffsetTranslator(store);
        OffsetTranslator.Result result = translator.translate(cp.offsets());

        if (!result.isComplete()) {
            log.warn("{} partitions could not be translated:", result.unmapped.size());
            result.unmapped.forEach((tp, why) -> log.warn("  {}: {}", tp, why));
        }

        byte[] newBytes = cp.rewriteWithOffsets(result.translated);
        String outFile = joinPath(joinPath(req.outputCheckpointDir, "offsets"), String.valueOf(batch));
        outFs.write(outFile, newBytes);

        Response resp = new Response();
        resp.writtenPath = outFile;
        resp.sourceOffsets = cp.offsets();
        resp.translatedOffsets = result.translated;
        resp.unmapped = result.unmapped;

        if (req.sourceClusterConsumerProps != null && !result.translated.isEmpty()) {
            log.info("Verifying {} translated offsets by fetching one record on each cluster",
                     result.translated.size());
            resp.verification = new OffsetVerifier().verify(
                    cp.offsets(), result.translated,
                    req.sourceClusterConsumerProps, req.targetClusterConsumerProps,
                    req.verifyPerFetchTimeout);
        }
        return resp;
    }

    private static long findLatestBatch(CheckpointStore fs, String offsetsDir) throws IOException {
        long max = -1;
        for (String child : fs.list(offsetsDir)) {
            String name = child.substring(child.lastIndexOf('/') + 1);
            if (name.matches("\\d+")) max = Math.max(max, Long.parseLong(name));
        }
        if (max < 0) throw new IOException("No batch files found under " + offsetsDir);
        return max;
    }

    private static String joinPath(String dir, String child) {
        return dir.endsWith("/") ? dir + child : dir + "/" + child;
    }
}
