"""
Unit tests for KafkaSimpleCdkStack -- run by the "Unit tests" step of the
GitHub Actions CI/CD workflow.

These synthesize the stack fully offline. The default-VPC lookup is satisfied
from the committed cdk.context.json (the same cache `cdk synth` uses in CI), so
no AWS credentials are needed and the template under test matches the one
that gets deployed.
"""

import json
import pathlib

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from kafka_cdk.kafka_cluster_stack import (
    SASL_MECHANISM,
    SECRET_NAME,
    KafkaSimpleCdkStack,
    build_user_data,
)

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
REGION = "us-east-1"


def _cached_context():
    """Return (account, context) from the committed cdk.context.json."""
    context = json.loads((PROJECT_ROOT / "cdk.context.json").read_text())
    vpc_keys = [k for k in context if k.startswith("vpc-provider:")]
    assert vpc_keys, "cdk.context.json has no cached default-VPC lookup"
    # Key shape: vpc-provider:account=<id>:filter.isDefault=true:region=...
    account = vpc_keys[0].split("account=", 1)[1].split(":", 1)[0]
    return account, context


@pytest.fixture(scope="module")
def template():
    account, context = _cached_context()
    app = cdk.App(context=context)
    stack = KafkaSimpleCdkStack(
        app,
        "KafkaSimpleCdkStack",
        env=cdk.Environment(account=account, region=REGION),
    )
    return Template.from_stack(stack)


def test_single_small_broker_without_ssh_key(template):
    template.resource_count_is("AWS::EC2::Instance", 1)
    template.has_resource_properties(
        "AWS::EC2::Instance",
        {
            "InstanceType": "t3.small",
            "KeyName": Match.absent(),
            "BlockDeviceMappings": [
                Match.object_like(
                    {"Ebs": Match.object_like({"VolumeSize": 20, "VolumeType": "gp3"})}
                )
            ],
        },
    )


def test_only_kafka_client_port_is_open(template):
    sgs = template.find_resources("AWS::EC2::SecurityGroup")
    assert len(sgs) == 1
    ingress = next(iter(sgs.values()))["Properties"]["SecurityGroupIngress"]
    assert [(r["FromPort"], r["ToPort"]) for r in ingress] == [(9092, 9092)]
    # No standalone ingress rules sneaking in a port 22 / 9093 opening.
    template.resource_count_is("AWS::EC2::SecurityGroupIngress", 0)


def test_secret_password_is_generated_not_authored(template):
    template.has_resource_properties(
        "AWS::SecretsManager::Secret",
        {
            "Name": SECRET_NAME,
            "SecretString": Match.absent(),
            "GenerateSecretString": {
                "SecretStringTemplate": json.dumps({"username": "kafka-admin"}),
                "GenerateStringKey": "password",
                "PasswordLength": 32,
                "ExcludePunctuation": True,
            },
        },
    )


def test_instance_role_reads_only_the_broker_secret(template):
    secret_id = next(iter(template.find_resources("AWS::SecretsManager::Secret")))
    policies = template.find_resources("AWS::IAM::Policy")
    statements = [
        s
        for p in policies.values()
        for s in p["Properties"]["PolicyDocument"]["Statement"]
    ]
    assert statements == [
        {
            "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
            "Effect": "Allow",
            "Resource": {"Ref": secret_id},
        }
    ]
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "ManagedPolicyArns": [
                Match.object_like(
                    {"Fn::Join": ["", Match.array_with([":iam::aws:policy/AmazonSSMManagedInstanceCore"])]}
                )
            ]
        },
    )


def test_user_data_bootstraps_sasl_kafka_without_leaking_password():
    script = build_user_data(
        secret_arn="arn:aws:secretsmanager:us-east-1:111122223333:secret:test", region=REGION
    )
    lines = script.splitlines()

    # Tracing must be off for the whole window in which the password is in a
    # shell variable, or cloud-init-output.log captures it.
    trace_off = lines.index("set +x")
    fetch = next(i for i, l in enumerate(lines) if "get-secret-value" in l)
    unset = lines.index("unset KAFKA_PASSWORD")
    trace_on = lines.index("set -x")
    assert trace_off < fetch < unset < trace_on
    assert not any(
        l.strip() == "set -x" or "set -euxo" in l for l in lines[trace_off + 1 : unset]
    )

    assert "listeners=SASL_PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" in script
    assert f"sasl.enabled.mechanisms={SASL_MECHANISM}" in script
    assert f'--add-scram "{SASL_MECHANISM}=[name=$KAFKA_USERNAME,password=$KAFKA_PASSWORD]"' in script
    assert "--ignore-formatted" in script
    assert "chmod 600 /opt/kafka/config/server.properties /opt/kafka/config/client.properties" in script
