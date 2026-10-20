# kafka-simple-cdk

Python CDK (aws-cdk-lib v2) app that deploys the smallest, most readable "Apache Kafka on
EC2, not MSK" example: **one EC2 instance, one Kafka broker, KRaft mode (no Zookeeper)**, in
the AWS account's existing **default VPC** (no custom VPC/subnet/internet gateway/route table
is created).

Stack id: `KafkaSimpleCdkStack`.

There is **no SSH key pair** in this stack. The only way to reach the instance is via AWS
Systems Manager Session Manager (SSM), using the IAM instance profile attached to it.

The broker's client listener is **SASL-authenticated**, using a password that this stack has
**AWS Secrets Manager generate automatically at deploy time** — see
[Secrets Manager integration](#secrets-manager-integration) below.

## What this stack creates

- 1 Secrets Manager secret (`kafka-simple-cdk-broker-credentials`) holding the broker's
  SASL/SCRAM-SHA-512 username and an **auto-generated** 32-character password
- 1 security group allowing ingress on TCP 9092 from `0.0.0.0/0` (wide open for
  learning-example simplicity — scope this to a specific CIDR, e.g. your own IP, for anything
  beyond a throwaway test) and no SSH ingress rule
- 1 IAM role (trusted by `ec2.amazonaws.com`) with `AmazonSSMManagedInstanceCore` attached
  plus read access scoped to that one secret, wrapped in an instance profile
- 1 `t3.small` EC2 instance (`kafka-simple-broker`) with a 20 GiB gp3 root volume and a public
  IP, bootstrapping Apache Kafka 4.3.1 in single-node KRaft mode via user data (cluster id
  `zsvUnJ-LSWW-5vb3PEALHQ`, combined broker+controller, its own and only quorum voter)

## Secrets Manager integration

The point of this variant: **no credential is ever authored by a human**. The stack asks
Secrets Manager to mint the password, and the broker reads it back at boot using its instance
role.

**1. Automatic creation.** The `Secret` construct uses `generate_secret_string`, which
synthesizes into a CloudFormation `GenerateSecretString` block:

```json
"GenerateSecretString": {
  "SecretStringTemplate": "{\"username\": \"kafka-admin\"}",
  "GenerateStringKey": "password",
  "PasswordLength": 32,
  "ExcludePunctuation": true
}
```

Only the non-secret `username` is in the template. CloudFormation hands this to Secrets
Manager at deploy time and Secrets Manager generates the password, so the password exists in
**no** source file, **no** `cdk.context.json`, and **no** synthesized template. The stored
value ends up as `{"username": "kafka-admin", "password": "<generated>"}`.

`ExcludePunctuation` keeps the password alphanumeric. That is deliberate: the password has to
pass through three quoting layers intact on the instance (the shell, the Kafka properties
file, and the JAAS config line). 32 alphanumeric characters is still ~190 bits of entropy.

**2. Automatic usage.** `secret.grant_read(role)` scopes `secretsmanager:GetSecretValue` to
this one secret's ARN — not a blanket `secretsmanager:*`. The user-data script then:

1. Calls `aws secretsmanager get-secret-value` with the instance role and parses it with `jq`.
2. Writes `server.properties` with a `SASL_PLAINTEXT` listener on 9092 using
   `SCRAM-SHA-512`, with the credential in the listener's JAAS config line.
3. Seeds the SCRAM credential into the KRaft metadata log via
   `kafka-storage.sh format --add-scram`. This is the KRaft-native bootstrap: without it there
   would be no credential to authenticate with, and no authenticated path over which to create
   one.
4. Also writes `/opt/kafka/config/client.properties` so the bundled CLI tools can authenticate.

Both files are `chmod 600`, owned by `kafka`. The script runs `set +x` around the whole
secret-handling section — with `set -x` left on, the shell would echo the password straight
into `/var/log/cloud-init-output.log`, defeating the entire point.

**What changed on the wire:** port 9092 was `PLAINTEXT` (anonymous) and is now
`SASL_PLAINTEXT` + `SCRAM-SHA-512`. Clients must present credentials. The `CONTROLLER`
listener on 9093 stays `PLAINTEXT` — it is the node talking to itself for the KRaft quorum,
and no security-group rule exposes it.

**Note on transport:** `SASL_PLAINTEXT` authenticates but does not encrypt. SCRAM never puts
the password on the wire (it's a challenge-response exchange), but message data travels in
the clear. For anything beyond a throwaway test, use `SASL_SSL` with a TLS certificate.

Deliberately out of scope here: a customer-managed KMS key for the secret (the AWS-managed
`aws/secretsmanager` key is used), automatic rotation, and a Kafka `authorizer` with ACLs —
with no authorizer configured, any successfully authenticated client is authorized.

**Why this stack needs live AWS credentials just to synthesize:** unlike a from-scratch-VPC
stack, this one performs a live lookup of the account's default VPC
(`ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)`) at synth time — CDK actually
queries your account before generating the CloudFormation template. CloudFormation itself
can't do this (see the `cloudformation/` README for why it needs explicit `VpcId`/`SubnetId`
parameters instead), and this is the one genuine mechanical difference between the three
implementations, not just a styling choice.

## Prerequisites

- Python 3.9+
- Node.js (required by the AWS CDK Toolkit / jsii runtime)
- AWS credentials configured for `us-east-1` (e.g. `export AWS_PROFILE=<your-profile>`)

## Step 1 — Set up the Python environment

```bash
cd cdk-secretmanager-kafka-github
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate.bat
pip install -r requirements.txt
```

## Step 2 — Bootstrap CDK (first time only, per account/region)

Skip this if the account/region has already been CDK-bootstrapped (check for a
`CDKToolkit` CloudFormation stack):

```bash
aws cloudformation describe-stacks --stack-name CDKToolkit --region us-east-1 \
  --query 'Stacks[0].StackStatus' --output text
```

If that errors (stack not found), bootstrap:

```bash
npx aws-cdk@2 bootstrap aws://<ACCOUNT_ID>/us-east-1
```

## Step 3 — Synthesize (requires real AWS credentials)

```bash
npx aws-cdk@2 synth
```

This performs the live default-VPC lookup, renders the CloudFormation template to `cdk.out/`,
and is a good sanity check before deploying. The lookup result is cached into
`cdk.context.json`, which is intentionally committed so subsequent synths/deploys are
reproducible without re-querying the account — delete it and re-run `synth` if the account's
default VPC configuration ever changes.

## Step 4 — Deploy

```bash
npx aws-cdk@2 deploy --require-approval never
```

Deployment takes roughly 3 minutes (IAM role/instance profile creation has an inherent
propagation delay before the EC2 instance can launch with it attached). On success, the
stack prints its outputs: instance ID, public IP, private IP, security group ID, and the
broker credentials secret's ARN and name.

To read the generated credentials from your local machine:

```bash
aws secretsmanager get-secret-value --region us-east-1 \
  --secret-id kafka-simple-cdk-broker-credentials \
  --query SecretString --output text
```

Expect `{"username":"kafka-admin","password":"<32 alphanumeric chars>"}`.

## Step 5 — Wait for the Kafka bootstrap to finish

`cdk deploy` finishing only means the EC2 instance was launched — the user-data script
(install Java, download Kafka, format storage, start the systemd service) still needs to run
on first boot. Poll for the SSM agent to register:

```bash
INSTANCE_ID=<InstanceId from the deploy output>

aws ssm describe-instance-information --region us-east-1 \
  --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
  --query 'InstanceInformationList[0].PingStatus' --output text
```

Wait until this prints `Online`, then give it another ~30-60 seconds for the bootstrap script
itself to finish.

## Step 6 — Connect via SSM (no SSH key needed)

```bash
aws ssm start-session --target <instance-id>
```

## Step 7 — Confirm Kafka is running

On the instance:

```bash
sudo systemctl status kafka
```

Expect `Active: active (running)`. If it isn't, check `sudo journalctl -u kafka -n 50` and
`cat /var/log/cloud-init-output.log` for bootstrap errors.

## Step 8 — Create a topic, produce, and consume a test message

Still on the instance. Every command now needs the client config written at boot from the
Secrets Manager credential — `--command-config` for admin tools, `--producer.config` /
`--consumer.config` for the producer and consumer:

```bash
CFG=/opt/kafka/config/client.properties

sudo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config $CFG \
  --create --topic verify-topic --partitions 1 --replication-factor 1

sudo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config $CFG \
  --describe --topic verify-topic

echo "hello kafka" | sudo /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --producer.config $CFG --topic verify-topic

sudo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --consumer.config $CFG \
  --topic verify-topic --from-beginning --timeout-ms 5000
```

Expected: the `--describe` shows `PartitionCount: 1 ReplicationFactor: 1` with `Leader: 1`,
and the consumer prints `hello kafka` before it times out.

**Confirm the authentication is actually being enforced.** Run the same command *without*
`--command-config`:

```bash
sudo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

This should now **fail** (it hangs then errors with a disconnect/timeout rather than listing
topics) — the broker drops the unauthenticated connection. If it succeeds and prints topics,
SASL is not in effect; check `grep -c SASL /opt/kafka/config/server.properties` and
`sudo journalctl -u kafka -n 50`.

> Note: the end-to-end run recorded in the original README predates this change and covered
> the anonymous-PLAINTEXT path. The SASL path here is validated by `cdk synth` and by shell
> and template inspection, but has not been re-run against a live deployment.

## Step 9 — Clean up

Exit the SSM session (`exit`), then from your local machine:

```bash
npx aws-cdk@2 destroy --force
```

This tears down the EC2 instance, security group, IAM role/instance profile, and the broker
credentials secret created by this stack. It does NOT touch the account's default VPC or its
subnets — they were only ever looked up, never owned by this stack.

**Secrets Manager gotcha — read this before redeploying.** Deleting a secret does not delete
it immediately: Secrets Manager schedules it for deletion with a **30-day recovery window**,
and the name stays reserved for that whole window. Because this stack uses a fixed secret name
(`kafka-simple-cdk-broker-credentials`), a `destroy` followed by a `deploy` inside those 30
days fails with *"You can't create this secret because a secret with this name is already
scheduled for deletion."* To redeploy immediately, force-delete it first:

```bash
aws secretsmanager delete-secret --region us-east-1 \
  --secret-id kafka-simple-cdk-broker-credentials \
  --force-delete-without-recovery
```

(The alternative is to drop `secret_name=SECRET_NAME` from the `Secret` construct and let CDK
generate a unique name per deploy — at the cost of a less discoverable name.)

**Confirm the cleanup actually completed:**

```bash
aws cloudformation list-stacks --region us-east-1 \
  --query "StackSummaries[?StackName=='KafkaSimpleCdkStack'].StackStatus" --output text
```

Should show `DELETE_COMPLETE` (or nothing at all once it ages out of the list). Also
double-check no EC2 instance from this stack is still running:

```bash
aws ec2 describe-instances --region us-east-1 \
  --filters "Name=tag:Project,Values=kafka-simple-no-msk" "Name=tag:Iac,Values=cdk" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
```

Empty output means it's fully torn down. And confirm the secret is gone (or at least scheduled
for deletion):

```bash
aws secretsmanager describe-secret --region us-east-1 \
  --secret-id kafka-simple-cdk-broker-credentials \
  --query 'DeletedDate' --output text
```

A `ResourceNotFoundException` means it's fully deleted; a timestamp means it's in the recovery
window (see the force-delete note above).

Optionally clean up the local Python environment and synth output:

```bash
deactivate
rm -rf .venv cdk.out
```

(Keep `cdk.context.json` — it's meant to be committed.)

## CI/CD with GitHub Actions

Two workflows run on GitHub-hosted `ubuntu-latest` runners:

- [`.github/workflows/ci-cd.yml`](.github/workflows/ci-cd.yml) runs the unit tests on every PR
  into `main`, every push to `main`, and on demand (*Run workflow*). It needs no AWS
  credentials and never deploys. Deploys are done by hand (Steps 1–8 above).
- [`.github/workflows/destroy.yml`](.github/workflows/destroy.yml) tears the stack down (Step 9).
  It only runs when you start it by hand.

Both share the [`.github/actions/setup`](.github/actions/setup/action.yml) composite action. That
action installs Python 3.13, `requirements-dev.txt`, Node 24, a pinned AWS CDK CLI (2.1145.0),
and, when given a role, AWS credentials through **GitHub OIDC**, so no long-lived AWS keys are
stored in GitHub.

| Step | What it does | Fails when |
|---|---|---|
| Setup | `requirements-dev.txt` (pytest, cfn-lint), CDK CLI | install error |
| Unit tests | [`tests/`](tests/test_kafka_cluster_stack.py): offline synth from `cdk.context.json`; asserts 1×t3.small, no key pair, only 9092 open, generated secret, secret-scoped IAM, no password in traced user data | any assertion fails |

The JUnit report is uploaded as the `reports` artifact.

**Destroy inputs**

| Input | Meaning |
|---|---|
| `confirm_stack_name` | must be exactly `KafkaSimpleCdkStack`, or nothing is destroyed |
| `force_delete_secret` | also purge the broker secret, so you can redeploy immediately (see the Secrets Manager gotcha above) |

Destroy runs `cdk destroy`, checks that the stack is gone, and optionally purges the secret. It
only runs from `main` and needs the `production` environment's approval.

### GitHub setup (Destroy workflow only)

CI needs no setup. The Destroy workflow needs AWS access:

1. **OIDC provider in AWS** (once per account). Skip this if
   `token.actions.githubusercontent.com` already exists under IAM → Identity providers:

   ```bash
   aws iam create-open-id-connect-provider \
     --url https://token.actions.githubusercontent.com \
     --client-id-list sts.amazonaws.com
   ```

2. **Role.** Create an IAM role that GitHub can assume. Its trust policy, with
   `<ACCOUNT_ID>` and `<OWNER>/<REPO>` filled in:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Principal": { "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com" },
       "Action": "sts:AssumeRoleWithWebIdentity",
       "Condition": {
         "StringEquals": {
           "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
           "token.actions.githubusercontent.com:sub": "repo:<OWNER>/<REPO>:environment:production"
         }
       }
     }]
   }
   ```

   The role needs:

   - `sts:AssumeRole` on `arn:aws:iam::<ACCOUNT_ID>:role/cdk-hnb659fds-*-<ACCOUNT_ID>-us-east-1`
     (CDK's bootstrap roles, which do the actual delete)
   - `cloudformation:DescribeStacks` (post-destroy check)
   - `secretsmanager:DeleteSecret` on `kafka-simple-cdk-broker-credentials-*`, only if you use
     `force_delete_secret`

3. **Repository variable** `AWS_ROLE_ARN` (Settings → Secrets and variables → Actions →
   *Variables*) = the role's ARN.

4. **Environment** (Settings → Environments → *New environment* `production`). Add **Required
   reviewers**; this is the approval gate. Also restrict *Deployment branches* to `main`.

To run the smoke test after a local deploy:

```bash
npx aws-cdk@2 deploy --require-approval never --outputs-file cdk-outputs.json
scripts/smoke_test.sh cdk-outputs.json
```

## Known gotchas (already handled in this stack, worth knowing)

**SCRAM credentials must be bootstrapped at format time.** In KRaft mode, SCRAM users live in
the metadata log, and there is a chicken-and-egg problem: you need an authenticated connection
to create a SASL user, but you can't authenticate until a user exists. `kafka-storage.sh
format --add-scram` is the escape hatch — it writes the first credential directly into the log
before the broker ever starts. Because the stack also passes `--ignore-formatted` (so reboots
are a no-op), `--add-scram` only takes effect on the instance's **first** boot. Rotating the
secret therefore will not reconfigure a running broker; that needs a separate
`kafka-configs.sh --alter --add-config 'SCRAM-SHA-512=[...]'` against the live cluster.

**Kafka version layout.** Kafka 3.x shipped a `config/kraft/server.properties` template alongside the legacy
`config/server.properties` (Zookeeper-mode) file. Kafka 4.x removed Zookeeper mode entirely
and flattened the layout — there's no `config/kraft/` subdirectory anymore, just
`config/server.properties` directly. If you're adapting this stack for a different Kafka
version, double-check that path against the actual tarball contents rather than trusting
older docs/tutorials.
