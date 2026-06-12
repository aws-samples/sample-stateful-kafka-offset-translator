"""
Glue Streaming demo script for the offset-translator.

Reads a single Kafka topic from an MSK cluster using IAM auth, writes the raw
records as Parquet to S3, and persists Spark Structured Streaming checkpoints
to S3. The same script is used for both phases of the demo:

  Phase 1: --bootstrap-servers points at the SOURCE cluster, --checkpoint-location
           is a fresh prefix. Run for a few minutes to accumulate offsets.

  Phase 2: --bootstrap-servers points at the TARGET cluster, --checkpoint-location
           is the OUTPUT of the offset-translator (translated source-cluster
           offsets rewritten to target-cluster offsets).

The MSK IAM auth jar (software.amazon.msk:aws-msk-iam-auth) must be on the
driver+executor classpath. The CloudFormation stack passes it via --extra-jars,
which the README runbook expects at s3://<bucket>/jars/.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp


# Glue jobs declare arguments with leading hyphens (e.g. --bootstrap-servers),
# but getResolvedOptions returns them with hyphens converted to underscores in
# the result dict. Pass the hyphenated names here and read with underscores below.
REQUIRED_ARGS = [
    "JOB_NAME",
    "bootstrap-servers",
    "topic",
    "checkpoint-location",
    "output-location",
    "starting-offsets",
]


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    iam_jaas = "software.amazon.msk.auth.iam.IAMLoginModule required;"
    iam_handler = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args["bootstrap_servers"])
        .option("subscribe", args["topic"])
        .option("startingOffsets", args["starting_offsets"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option("kafka.sasl.jaas.config", iam_jaas)
        .option("kafka.sasl.client.callback.handler.class", iam_handler)
        .option("failOnDataLoss", "false")
        .load()
    )

    enriched = (
        stream.selectExpr(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
        )
        .withColumn("ingest_ts", current_timestamp())
    )

    query = (
        enriched.writeStream.format("parquet")
        .option("path", args["output_location"])
        .option("checkpointLocation", args["checkpoint_location"])
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
