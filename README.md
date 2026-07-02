# stateful-offset-translator

A CLI tool that translates [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) Kafka checkpoint offsets from a source cluster to a target cluster using [MSK Replicator](https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator.html) offset-sync records.

When you use MSK Replicator to replicate topics across clusters, consumer group offsets are tracked in an internal `offset-syncs` topic. This tool reads those sync records to map your Spark checkpoint's source-cluster offsets to their equivalent target-cluster offsets, enabling you to resume a Spark Structured Streaming job on the target cluster without reprocessing or data loss.

## How it works

1. Reads the Spark checkpoint's latest (or specified) batch offset file from a local path or S3.
2. Consumes the MSK Replicator `offset-syncs` topic on the target cluster to build a per-partition translation map.
3. Writes a new checkpoint offset file with translated offsets to the specified output directory.
4. Optionally verifies the translation by fetching one record per partition from both clusters and comparing them.

## Migration workflow

This tool is intended to be run as part of a controlled cluster migration:

1. **Stop your Spark Structured Streaming job** on the source cluster and note the last committed batch ID.
2. **Ensure MSK Replicator has caught up** — the offset-syncs topic must contain a sample for every topic-partition at or after the checkpoint's offsets. If not, exit code `2` will be returned and you should wait and retry.
3. **Run this tool** to translate the checkpoint offsets file and write it to a new location.
4. **Copy the rest of your checkpoint** to the same output directory. The tool only writes `offsets/<batchId>` — you must copy `commits/`, `metadata`, and any other subdirectories yourself:
   ```bash
   aws s3 sync s3://my-bucket/spark-checkpoint/ s3://my-bucket/spark-checkpoint-translated/ \
     --exclude "offsets/*"
   ```
5. **Optionally run with `--verify`** to confirm the translated offsets point at the same records on the target cluster.
6. **Start your Spark job on the target cluster**, pointing it at the translated checkpoint directory.

## Prerequisites

- Java 11+
- Maven (to build from source)
- Network access to the target MSK cluster's bootstrap servers
- IAM permissions to consume from the target cluster and the offset-syncs topic (see [IAM permissions](#iam-permissions))

## IAM permissions

When using `--auth IAM` (the default), the executing principal needs the following permissions:

**Target MSK cluster** (reading the offset-syncs topic):
```json
{
  "Action": [
    "kafka-cluster:Connect",
    "kafka-cluster:DescribeTopic",
    "kafka-cluster:ReadData"
  ],
  "Resource": [
    "arn:aws:kafka:<region>:<account>:cluster/<target-cluster-name>/<cluster-id>",
    "arn:aws:kafka:<region>:<account>:topic/<target-cluster-name>/<cluster-id>/mskr-offset-syncs.*"
  ]
}
```

**S3** (when checkpoint paths are `s3://...`):
```json
{
  "Action": [
    "s3:GetObject",
    "s3:ListBucket"
  ],
  "Resource": "<source-checkpoint-bucket-and-prefix>"
}
```
```json
{
  "Action": [
    "s3:PutObject"
  ],
  "Resource": "<output-checkpoint-bucket-and-prefix>"
}
```

When using `--verify`, the same `kafka-cluster:Connect`, `kafka-cluster:DescribeTopic`, and `kafka-cluster:ReadData` permissions are also required on the **source** cluster and the replicated topic(s).

## Building

```bash
mvn package -q
```

This produces `target/offset-translator-<version>-all.jar` (a shaded uber-jar).

## Usage

```
java -jar target/offset-translator-*-all.jar \
  --checkpoint-dir   <source-checkpoint-path>   \
  --output-dir       <output-checkpoint-path>   \
  --bootstrap-servers <target-cluster-bootstrap> \
  --offset-syncs-topic mskr-offset-syncs.<replicator-id>.internal
```

### Options

| Option | Required | Description |
|--------|----------|-------------|
| `--checkpoint-dir` | Yes | Source Spark checkpoint root — local path or `s3://...` |
| `--output-dir` | Yes | Where to write the translated checkpoint — local path or `s3://...` |
| `--bootstrap-servers` | Yes | Bootstrap servers of the **target** MSK cluster |
| `--offset-syncs-topic` | Yes | Offset-syncs topic name, e.g. `mskr-offset-syncs.<id>.internal` |
| `--batch-id` | No | Specific batch ID to translate; defaults to the latest in `offsets/` |
| `--auth` | No | Auth mode: `IAM` (default), `PLAINTEXT` |
| `--consumer-prop` | No | Override or add a consumer property: `key=value` (repeatable) |
| `--verify` | No | After translating, fetch one record per partition from each cluster and compare |
| `--source-bootstrap-servers` | With `--verify` | Bootstrap servers of the **source** cluster |
| `--source-auth` | No | Auth mode for the source cluster; defaults to `--auth` |
| `--source-consumer-prop` | No | Override or add a **source** consumer property: `key=value` (repeatable) |

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success — all partitions translated |
| `2` | Some partitions could not be mapped (no offset-sync sample found yet — MSK Replicator has not caught up) |
| `3` | Translation succeeded but `--verify` found mismatches |

### Verification verdicts

When `--verify` is used, each partition is assigned one of the following verdicts:

| Verdict | Meaning | Safe to proceed? |
|---------|---------|-----------------|
| `EXACT` | Source and target records have identical key, value, and timestamp. The job will resume with no overlap and no gap. | Yes |
| `BEHIND_BY_MS` | Target record timestamp is older than source. Translation is conservative (at-least-once). The job will re-read a small number of records on resume. | Yes — expect minor duplicates |
| `AHEAD_BY_MS` | Target record timestamp is newer than source, or key/value differs. Likely a topic-recreation or compaction event. The job would skip data. | No — investigate before proceeding |
| `TARGET_OFFSET_OUT_OF_RANGE` | The translated offset is past the target cluster's high-water mark. Replication has not yet caught up to this offset. | No — wait for replication to catch up |
| `SOURCE_OFFSET_OUT_OF_RANGE` | The source offset is past the source cluster's high-water mark. Should not happen for a committed Spark checkpoint. | No — investigate |
| `TIMEOUT` | No record was returned within the per-partition fetch budget (default 15 s). | Retry or investigate connectivity |

### Examples

**Translate latest checkpoint using IAM auth (default):**

```bash
java -jar offset-translator-*-all.jar \
  --checkpoint-dir s3://my-bucket/spark-checkpoint \
  --output-dir s3://my-bucket/spark-checkpoint-translated \
  --bootstrap-servers b-1.my-target-cluster.abc123.kafka.us-east-1.amazonaws.com:9098 \
  --offset-syncs-topic mskr-offset-syncs.abcd1234.internal
```

**Translate and verify:**

```bash
java -jar offset-translator-*-all.jar \
  --checkpoint-dir s3://my-bucket/spark-checkpoint \
  --output-dir /tmp/translated-checkpoint \
  --bootstrap-servers b-1.target.abc.kafka.us-east-1.amazonaws.com:9098 \
  --offset-syncs-topic mskr-offset-syncs.abcd1234.internal \
  --verify \
  --source-bootstrap-servers b-1.source.xyz.kafka.us-east-1.amazonaws.com:9098
```

**Use PLAINTEXT auth (e.g., local testing):**

```bash
java -jar offset-translator-*-all.jar \
  --checkpoint-dir /tmp/spark-checkpoint \
  --output-dir /tmp/translated \
  --bootstrap-servers localhost:9092 \
  --offset-syncs-topic mskr-offset-syncs.test.internal \
  --auth PLAINTEXT
```

## Limitations

- **One offsets file per run.** The tool translates a single `offsets/<batchId>` file. For Spark queries with multiple Kafka sources, Spark writes one offsets file per source under different sub-paths — run the tool once for each source's checkpoint path.
- **Output is a partial checkpoint.** Only `offsets/<batchId>` is written to `--output-dir`. You must copy the rest of the checkpoint directory (`commits/`, `metadata`, etc.) separately before pointing a Spark job at the output. See [Migration workflow](#migration-workflow).
- **Translation requires at least one offset-sync sample.** If MSK Replicator has not yet synced a given partition at or beyond the checkpoint offset, that partition will be reported as unmapped (exit code `2`). Wait for replication to advance and re-run.

## References

- [MSK Migration Lab](https://catalog.workshops.aws/msk-migration-lab) — step-by-step workshop for spinning up two MSK clusters, configuring MSK Replicator for bidirectional replication, and migrating consumer workloads between clusters. The `demo-app/` in this repository is built against the lab environment.

## Demo app

`demo-app/` contains a full end-to-end demo that exercises the tool against a live bidirectional MSK Replicator lab environment. See [`demo-app/README.md`](demo-app/README.md) for the complete runbook.

**What's included:**

| Path | Description |
|------|-------------|
| `cfn/infra-base.yaml` | CloudFormation stack — S3 bucket, IAM role for Glue, Glue NETWORK connection into the MSK VPC |
| `cfn/infra-jobs.yaml` | CloudFormation stack — two Glue Streaming jobs (deploy after uploading the script) |
| `glue/stream_kafka_to_s3.py` | Spark Structured Streaming script used by both jobs; reads from Kafka via IAM auth and writes Parquet to S3 |
| `producer/clickstream_producer.py` | Constant-rate synthetic clickstream producer (IAM auth); generates `user_id`-keyed JSON events |

**Demo flow summary:**

1. Deploy the base CFN stack (bucket, IAM role, Glue connection).
2. Upload the Glue script and MSK IAM auth jar to S3.
3. Deploy the jobs CFN stack.
4. Run the producer on a KafkaClientEC2 instance to generate traffic.
5. Run the Phase 1 Glue job (reads from the **source** cluster, writes a real Spark checkpoint to S3).
6. Run the translator to rewrite the checkpoint offsets for the **target** cluster.
7. Run the Phase 2 Glue job (resumes from the translated checkpoint on the **target** cluster).
8. Compare `output/phase1/` and `output/phase2/` in S3 to confirm no gap and minimal overlap.

## Support

Please open a [GitHub issue](../../issues) to report bugs, request features, or ask questions.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to this project.

## Security

If you discover a potential security issue, please notify AWS/Amazon Security via the [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/) rather than opening a public GitHub issue.

## License

This project is licensed under the MIT No Attribution License. See the [LICENSE](LICENSE) file for details.
