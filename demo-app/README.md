# offset-translator demo

End-to-end test for `offset-translator` against the bidirectional MSK Replicator
lab. Phase 1 produces a real Spark checkpoint by reading from the **source**
cluster. The translator rewrites that checkpoint to **target**-cluster offsets
using the `mskr-offset-syncs.*.internal` topic. Phase 2 resumes from the
translated checkpoint against the target cluster and produces equivalent output.

## Layout

```
demo-app/
├── cfn/infra-base.yaml           # bucket, IAM role, Glue connection
├── cfn/infra-jobs.yaml           # the two Glue Streaming jobs (deploy AFTER uploading the script)
├── glue/stream_kafka_to_s3.py    # the Spark Structured Streaming script
└── producer/clickstream_producer.py
```

The split exists because CloudFormation early validation refuses to create a
`AWS::Glue::Job` whose `ScriptLocation` does not yet exist in S3. The base stack
creates the bucket; you upload the script; the jobs stack consumes it.

## Discovered values (defaults baked into the template)

| Item              | Value                                                        |
|-------------------|--------------------------------------------------------------|
| Region            | `us-east-1`                                                  |
| VPC               | `vpc-01884526e588ec0c0`                                      |
| Glue subnet / AZ  | `subnet-04fa4ab4df830ab64` / `us-east-1a`                    |
| Security group    | `sg-0f8e1d0791497d064` (already allows 9098 in-VPC)          |
| Source cluster    | `Source-MSKCluster-msk-labs-default`                         |
| Target cluster    | `Target-MSKCluster-msk-labs-default`                         |
| Offset-syncs topic| `mskr-offset-syncs.-PZfZLdsRweF9wBoTPlGNQ-1ac2ecfe52ce.internal` (on target) |
| Demo Kafka topic  | `demo-clickstream` (3 partitions, replication factor 3) — auto-replicated by `.*` filter |

## Runbook

### 1. Deploy the base stack

```
aws cloudformation deploy \
  --region us-east-1 \
  --stack-name offset-translator-demo-base \
  --template-file demo-app/cfn/infra-base.yaml \
  --capabilities CAPABILITY_NAMED_IAM
```

Capture the bucket name:

```
BUCKET=$(aws cloudformation describe-stacks --region us-east-1 \
  --stack-name offset-translator-demo-base \
  --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text)
```

### 2. Upload the Glue script and the MSK IAM auth jar

```
aws s3 cp demo-app/glue/stream_kafka_to_s3.py s3://$BUCKET/scripts/

# aws-msk-iam-auth shaded jar - pinned to v2.2.0. Verify SHA-256 before upload.
EXPECTED_SHA256=735dd058b81d088e767770d6cc4e7f1a1beea815ced18b66a23b35fd87651e77
curl -L -o /tmp/aws-msk-iam-auth-2.2.0-all.jar \
  https://github.com/aws/aws-msk-iam-auth/releases/download/v2.2.0/aws-msk-iam-auth-2.2.0-all.jar
echo "${EXPECTED_SHA256}  /tmp/aws-msk-iam-auth-2.2.0-all.jar" | shasum -a 256 -c -
aws s3 cp /tmp/aws-msk-iam-auth-2.2.0-all.jar s3://$BUCKET/jars/
```

If you need a different version, update both the URL and `EXPECTED_SHA256`
(also update `IamAuthJarKey` in `cfn/infra-jobs.yaml`).

### 2b. Deploy the jobs stack

```
aws cloudformation deploy \
  --region us-east-1 \
  --stack-name offset-translator-demo-jobs \
  --template-file demo-app/cfn/infra-jobs.yaml
```

### 3. Start the producer on KafkaClientEC2Instance1

SSM into the EC2 instance, then:

```
pip install --user kafka-python aws-msk-iam-sasl-signer-python
python clickstream_producer.py \
  --bootstrap "$BOOTSTRAP" \
  --topic demo-clickstream \
  --rate 50
```

Leave it running in the background while phase 1 executes.

### 4. Phase 1 — capture a real checkpoint on the source cluster

```
aws glue start-job-run --region us-east-1 --job-name offset-translator-demo-phase1-source
```

Let it run for 5–10 minutes so `offsets/` has multiple batch files and the
target cluster's `mskr-offset-syncs` topic accumulates samples for any
partitions you'll touch. Then stop it:

```
RUN=$(aws glue get-job-runs --region us-east-1 --job-name offset-translator-demo-phase1-source \
  --query 'JobRuns[0].Id' --output text)
aws glue batch-stop-job-run --region us-east-1 \
  --job-name offset-translator-demo-phase1-source --job-run-ids $RUN
```

### 5. Translate the checkpoint

Build the translator JAR from the project root and run it locally (or on the
EC2 instance) against S3:

```
mvn -q -DskipTests package

java -jar target/offset-translator-0.1.0-SNAPSHOT-all.jar \
  --checkpoint-dir s3://$BUCKET/checkpoints/phase1/ \
  --output-dir    s3://$BUCKET/checkpoints/phase2-translated/ \
  --bootstrap-servers "boot-irn.targetmskclustermsklab.bdetn6.c9.kafka.us-east-1.amazonaws.com:9098,boot-1hu.targetmskclustermsklab.bdetn6.c9.kafka.us-east-1.amazonaws.com:9098,boot-5a2.targetmskclustermsklab.bdetn6.c9.kafka.us-east-1.amazonaws.com:9098" \
  --offset-syncs-topic 'mskr-offset-syncs.-PZfZLdsRweF9wBoTPlGNQ-1ac2ecfe52ce.internal' \
  --auth IAM
```

The translator writes a new `offsets/<batchId>` file. Copy the rest of the
checkpoint state so phase 2 has a complete, valid checkpoint:

```
aws s3 sync s3://$BUCKET/checkpoints/phase1/  s3://$BUCKET/checkpoints/phase2-translated/ \
  --exclude 'offsets/*'
```

(Once `TranslationService` learns to copy `commits/`, `state/`, and `metadata`
on its own, this `s3 sync` step goes away.)

### 6. Phase 2 — resume on the target cluster

```
aws glue start-job-run --region us-east-1 --job-name offset-translator-demo-phase2-target
```

Phase 2 should resume reading from the translated offset on the target cluster
and continue producing Parquet under `output/phase2/`.

### 7. Verify

Compare `output/phase1/` and `output/phase2/`. Records that were committed in
phase 1 should not appear (or only appear as a small overlap, since MM2's
translation is conservative / at-least-once) in phase 2. Records produced after
the cutover should appear once in phase 2 only.

```
aws s3 ls s3://$BUCKET/output/phase1/ --recursive --summarize
aws s3 ls s3://$BUCKET/output/phase2/ --recursive --summarize
```

## Cleanup

```
aws cloudformation delete-stack --region us-east-1 --stack-name offset-translator-demo-jobs
aws cloudformation wait stack-delete-complete --region us-east-1 --stack-name offset-translator-demo-jobs
aws s3 rm s3://$BUCKET --recursive
aws cloudformation delete-stack --region us-east-1 --stack-name offset-translator-demo-base
aws cloudformation wait stack-delete-complete --region us-east-1 --stack-name offset-translator-demo-base
# bucket has DeletionPolicy: Retain - delete it explicitly if desired
aws s3 rb s3://$BUCKET
```
