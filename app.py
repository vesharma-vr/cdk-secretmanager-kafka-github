#!/usr/bin/env python3
"""
CDK app entry point for the kafka-simple-no-msk learning example.

Region is fixed to us-east-1 per the spec. Account is taken from the CDK
CLI / environment (CDK_DEFAULT_ACCOUNT) if present.

Unlike a from-scratch-VPC design, this stack performs a LIVE lookup of the
account's default VPC at synth time (`ec2.Vpc.from_lookup(is_default=True)`
in kafka_cdk/kafka_cluster_stack.py) -- so `cdk synth` requires real AWS
credentials for the target account/region to succeed. The AMI is still
resolved via an SSM dynamic reference at deploy time, not at synth time.
"""
import os

import aws_cdk as cdk

from kafka_cdk.kafka_cluster_stack import KafkaSimpleCdkStack

app = cdk.App()

KafkaSimpleCdkStack(
    app,
    "KafkaSimpleCdkStack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region="us-east-1",
    ),
    description=(
        "Single-node, single-broker, KRaft-mode Apache Kafka on EC2 in the "
        "default VPC (learning example, not MSK). "
        "Connect via SSM: aws ssm start-session --target <instance-id>"
    ),
)

app.synth()
