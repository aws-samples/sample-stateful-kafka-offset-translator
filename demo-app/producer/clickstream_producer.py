"""
Constant-rate clickstream producer for the demo. Run on KafkaClientEC2Instance1
where the role already has kafka-cluster:WriteData and IAM auth is configured.

Usage:
    pip install --user kafka-python aws-msk-iam-sasl-signer-python
    python clickstream_producer.py \
        --bootstrap b-1.sourcemskclustermsklab....:9098,... \
        --topic ExampleTopicClickStream \
        --rate 50

Records are small JSON blobs with a synthetic user_id key, so partitioning is
deterministic across phases.
"""

import argparse
import json
import random
import socket
import time
import uuid

from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


class IamTokenProvider(AbstractTokenProvider):
    def __init__(self, region: str) -> None:
        self.region = region

    def token(self) -> str:
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


def build_producer(bootstrap: str, region: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=IamTokenProvider(region),
        client_id=socket.gethostname(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=10,
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic", default="demo-clickstream")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--rate", type=int, default=20, help="records/sec")
    args = p.parse_args()

    producer = build_producer(args.bootstrap, args.region)
    period = 1.0 / max(args.rate, 1)
    pages = ["/", "/about", "/checkout", "/product", "/search"]
    sent = 0
    while True:
        user_id = f"u-{random.randint(1, 10_000)}"
        record = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "page": random.choice(pages),
            "ts": time.time(),
        }
        producer.send(args.topic, key=user_id, value=record)
        sent += 1
        if sent % 100 == 0:
            print(f"sent={sent}")
        time.sleep(period)


if __name__ == "__main__":
    main()
