package com.example.offsettrans.cli;

import com.example.offsettrans.TranslationService;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * CLI wrapper around {@link TranslationService}. Defaults assume IAM auth against an MSK cluster;
 * the SASL settings can be overridden with {@code --consumer-prop key=val} for SASL/SCRAM, mTLS, etc.
 */
@Command(name = "offset-translator", mixinStandardHelpOptions = true,
        description = "Translate Spark Structured Streaming Kafka checkpoint offsets via MSK Replicator offset-syncs.")
public class Main implements Callable<Integer> {

    @Option(names = "--checkpoint-dir", required = true,
            description = "Source Spark checkpoint root (local path or s3://...).")
    String checkpointDir;

    @Option(names = "--output-dir", required = true,
            description = "Where to write the translated checkpoint (local path or s3://...).")
    String outputDir;

    @Option(names = "--bootstrap-servers", required = true,
            description = "Bootstrap servers of the target MSK cluster (the one hosting offset-syncs).")
    String bootstrap;

    @Option(names = "--offset-syncs-topic", required = true,
            description = "Name of the offset-syncs topic, e.g. mskr-offset-syncs.<id>.internal.")
    String offsetSyncsTopic;

    @Option(names = "--batch-id",
            description = "Specific batch id to translate. Defaults to the latest in offsets/.")
    Long batchId;

    @Option(names = "--auth", defaultValue = "IAM",
            description = "Auth mode: IAM | SASL_SCRAM | MTLS | PLAINTEXT. Default: ${DEFAULT-VALUE}.")
    String auth;

    @Option(names = "--consumer-prop", description = "Override or add a consumer property: key=value.")
    java.util.List<String> consumerProps;

    @Option(names = "--verify",
            description = "After translating, fetch one record from each cluster at the (source, "
                    + "translated) offset pair and report whether the records match. Requires "
                    + "--source-bootstrap-servers.")
    boolean verify;

    @Option(names = "--source-bootstrap-servers",
            description = "Bootstrap servers of the SOURCE cluster (only needed for --verify).")
    String sourceBootstrap;

    @Option(names = "--source-auth",
            description = "Auth mode for the source cluster. Defaults to the same value as --auth.")
    String sourceAuth;

    @Option(names = "--source-consumer-prop",
            description = "Override or add a SOURCE consumer property: key=value.")
    java.util.List<String> sourceConsumerProps;

    @Override
    public Integer call() throws Exception {
        Properties targetProps = new Properties();
        targetProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        applyAuth(targetProps, auth);
        applyOverrides(targetProps, consumerProps);

        TranslationService.Request req = new TranslationService.Request();
        req.checkpointDir = checkpointDir;
        req.outputCheckpointDir = outputDir;
        req.offsetSyncsTopic = offsetSyncsTopic;
        req.targetClusterConsumerProps = targetProps;
        req.batchId = batchId;

        if (verify) {
            if (sourceBootstrap == null) {
                throw new IllegalArgumentException("--verify requires --source-bootstrap-servers");
            }
            Properties sourceProps = new Properties();
            sourceProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootstrap);
            applyAuth(sourceProps, sourceAuth != null ? sourceAuth : auth);
            applyOverrides(sourceProps, sourceConsumerProps);
            req.sourceClusterConsumerProps = sourceProps;
        }

        TranslationService.Response resp = new TranslationService().translate(req);
        System.out.println("Wrote translated checkpoint: " + resp.writtenPath);
        System.out.println("Source offsets:");
        resp.sourceOffsets.forEach((tp, off) -> System.out.println("  " + tp + " -> " + off));
        System.out.println("Translated offsets:");
        resp.translatedOffsets.forEach((tp, off) -> System.out.println("  " + tp + " -> " + off));
        if (!resp.unmapped.isEmpty()) {
            System.out.println("Unmapped (no offset-sync sample yet):");
            resp.unmapped.forEach((tp, why) -> System.out.println("  " + tp + ": " + why));
        }

        boolean verifyHadIssues = false;
        if (resp.verification != null) {
            System.out.println("Verification (one fetch per cluster per partition):");
            for (com.example.offsettrans.OffsetVerifier.Result v : resp.verification) {
                String ts = v.deltaMs == null ? "" : (" deltaMs=" + v.deltaMs);
                System.out.println("  " + v.tp + " src=" + v.sourceOffset + " tgt=" + v.targetOffset
                        + " -> " + v.verdict + ts + " (" + v.detail + ")");
                if (v.verdict != com.example.offsettrans.OffsetVerifier.Verdict.EXACT
                        && v.verdict != com.example.offsettrans.OffsetVerifier.Verdict.BEHIND_BY_MS) {
                    verifyHadIssues = true;
                }
            }
        }

        if (!resp.unmapped.isEmpty()) return 2;
        if (verifyHadIssues) return 3;
        return 0;
    }

    private static void applyOverrides(Properties target, java.util.List<String> overrides) {
        if (overrides == null) return;
        for (String kv : overrides) {
            int eq = kv.indexOf('=');
            if (eq < 0) throw new IllegalArgumentException("expected key=value, got: " + kv);
            target.put(kv.substring(0, eq), kv.substring(eq + 1));
        }
    }

    private static void applyAuth(Properties p, String mode) {
        String m = mode.toUpperCase();
        switch (m) {
            case "IAM":
                p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                p.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
                p.put(SaslConfigs.SASL_JAAS_CONFIG,
                        "software.amazon.msk.auth.iam.IAMLoginModule required;");
                p.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
                        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
                break;
            case "PLAINTEXT":
                p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
                break;
            case "SASL_SCRAM":
            case "MTLS":
                throw new UnsupportedOperationException(mode + " auth is not wired in v0.1; supply --consumer-prop overrides.");
            default:
                throw new IllegalArgumentException("Unknown auth mode: " + mode);
        }
    }

    public static void main(String[] args) { System.exit(new CommandLine(new Main()).execute(args)); }
}
