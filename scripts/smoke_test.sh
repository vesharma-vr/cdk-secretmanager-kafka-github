#!/usr/bin/env bash
#
# Post-deploy verification for KafkaSimpleCdkStack. Run it by hand after a
# deploy:
#
#   scripts/smoke_test.sh cdk-outputs.json
#
# `cdk deploy` returning only means the EC2 instance launched; the user-data
# bootstrap (Java, Kafka download, storage format, systemd) runs afterwards.
# This script waits for that to finish and then proves the broker works the
# way the README claims, end to end:
#
#   1. the SSM agent registers (it is the only way onto the box -- no SSH),
#   2. cloud-init finished without error and kafka.service is listening on 9092,
#   3. an AUTHENTICATED client (the Secrets Manager credential) can create a
#      topic, produce, and consume a message back,
#   4. an UNAUTHENTICATED client is rejected.
#
# Everything runs on the instance through SSM Run Command, because the broker
# advertises its private IP and is not reachable as a full client from outside
# the VPC. Requires: aws CLI v2, jq, and credentials allowed to call
# ssm:SendCommand / ssm:GetCommandInvocation / ssm:DescribeInstanceInformation.

set -euo pipefail

OUTPUTS_FILE="${1:-cdk-outputs.json}"
STACK_NAME="${STACK_NAME:-KafkaSimpleCdkStack}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
SSM_ONLINE_TIMEOUT="${SSM_ONLINE_TIMEOUT:-600}"
BOOTSTRAP_TIMEOUT="${BOOTSTRAP_TIMEOUT:-900}"

log() { echo "[smoke $(date -u +%H:%M:%S)] $*"; }
die() { echo "[smoke $(date -u +%H:%M:%S)] FAIL: $*" >&2; exit 1; }

[ -f "$OUTPUTS_FILE" ] || die "outputs file '$OUTPUTS_FILE' not found (cdk deploy --outputs-file ...)"
INSTANCE_ID=$(jq -r --arg s "$STACK_NAME" '.[$s].InstanceId // empty' "$OUTPUTS_FILE")
[ -n "$INSTANCE_ID" ] || die "no InstanceId output for stack $STACK_NAME in $OUTPUTS_FILE"
log "target instance: $INSTANCE_ID ($AWS_REGION)"

# run_remote <timeout-seconds> <comment> <script>
#   Runs <script> on the broker via AWS-RunShellScript, prints its stdout (and
#   stderr to our stderr), and returns 0 only if the remote script succeeded.
run_remote() {
  local timeout="$1" comment="$2" script="$3"
  local params cmd_id status deadline

  params=$(jq -n --arg s "$script" --arg t "$timeout" '{commands: [$s], executionTimeout: [$t]}')
  cmd_id=$(aws ssm send-command \
    --instance-ids "$INSTANCE_ID" \
    --document-name AWS-RunShellScript \
    --comment "$comment" \
    --parameters "$params" \
    --query Command.CommandId --output text)

  deadline=$((SECONDS + timeout + 60))
  while :; do
    # get-command-invocation 404s for a moment right after send-command.
    status=$(aws ssm get-command-invocation --command-id "$cmd_id" --instance-id "$INSTANCE_ID" \
      --query Status --output text 2>/dev/null || echo Pending)
    case "$status" in
      Success | Failed | Cancelled | TimedOut) break ;;
    esac
    if ((SECONDS >= deadline)); then
      status="TimedOut (gave up polling)"
      break
    fi
    sleep 5
  done

  aws ssm get-command-invocation --command-id "$cmd_id" --instance-id "$INSTANCE_ID" \
    --query StandardOutputContent --output text 2>/dev/null || true
  local err
  err=$(aws ssm get-command-invocation --command-id "$cmd_id" --instance-id "$INSTANCE_ID" \
    --query StandardErrorContent --output text 2>/dev/null || true)
  [ -z "$err" ] || printf '%s\n' "$err" >&2

  if [ "$status" != "Success" ]; then
    log "remote command '$comment' ($cmd_id) ended with status: $status"
    return 1
  fi
}

# ---------------------------------------------------------------------------
# 1. SSM agent online
# ---------------------------------------------------------------------------
log "waiting up to ${SSM_ONLINE_TIMEOUT}s for the SSM agent to report Online"
deadline=$((SECONDS + SSM_ONLINE_TIMEOUT))
until [ "$(aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
  --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null)" = "Online" ]; do
  ((SECONDS < deadline)) || die "SSM agent on $INSTANCE_ID never came Online (instance profile / outbound HTTPS?)"
  sleep 10
done
log "SSM agent is Online"

# ---------------------------------------------------------------------------
# 2. User-data bootstrap finished and the broker is listening
# ---------------------------------------------------------------------------
log "waiting up to ${BOOTSTRAP_TIMEOUT}s for cloud-init and kafka.service"
run_remote "$BOOTSTRAP_TIMEOUT" "smoke: wait for kafka bootstrap" "$(cat <<'REMOTE'
cloud-init status --wait >/dev/null
rc=$?
cloud-init status --long || true
# 0 = done, 2 = done with recoverable warnings, 1 = user-data failed.
if [ "$rc" -eq 1 ]; then
  echo "user-data FAILED -- tail of /var/log/cloud-init-output.log:"
  tail -n 40 /var/log/cloud-init-output.log
  exit 1
fi
for _ in $(seq 1 60); do
  if systemctl is-active --quiet kafka && ss -ltn 'sport = :9092' | grep -q 9092; then
    echo "kafka.service active, port 9092 listening"
    exit 0
  fi
  sleep 3
done
echo "kafka never came up:"
systemctl status kafka --no-pager || true
journalctl -u kafka -n 40 --no-pager || true
exit 1
REMOTE
)" || die "Kafka bootstrap did not complete"

# ---------------------------------------------------------------------------
# 3 + 4. Authenticated round-trip succeeds, unauthenticated client is rejected
# ---------------------------------------------------------------------------
TOPIC="ci-smoke-${GITHUB_RUN_NUMBER:-local}-$(date +%s)"
log "running authenticated produce/consume on topic $TOPIC, then an unauthenticated probe"
run_remote 300 "smoke: SASL round-trip" "TOPIC='$TOPIC'
$(cat <<'REMOTE'
set -euo pipefail
K=/opt/kafka/bin
CFG=/opt/kafka/config/client.properties
BS=localhost:9092
MSG="hello from CI $(date -u +%FT%TZ)"

$K/kafka-topics.sh --bootstrap-server $BS --command-config $CFG \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

echo "$MSG" | $K/kafka-console-producer.sh --bootstrap-server $BS \
  --producer.config $CFG --topic "$TOPIC"

GOT=$($K/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $CFG \
  --topic "$TOPIC" --from-beginning --max-messages 1 --timeout-ms 30000 2>/dev/null || true)

$K/kafka-topics.sh --bootstrap-server $BS --command-config $CFG --delete --topic "$TOPIC" || true

if [ "$GOT" != "$MSG" ]; then
  echo "FAIL: round-trip mismatch -- expected '$MSG', got '$GOT'"
  exit 1
fi
echo "PASS: authenticated produce/consume round-trip"

# No SASL settings at all: the broker should drop the connection, so the
# admin call fails or times out instead of listing topics.
NOAUTH=$(mktemp)
printf 'request.timeout.ms=5000\ndefault.api.timeout.ms=15000\n' > "$NOAUTH"
if timeout 60 $K/kafka-topics.sh --bootstrap-server $BS --command-config "$NOAUTH" --list >/dev/null 2>&1; then
  rm -f "$NOAUTH"
  echo "FAIL: unauthenticated client listed topics -- SASL is NOT enforced"
  exit 1
fi
rm -f "$NOAUTH"
echo "PASS: unauthenticated client rejected"
REMOTE
)" || die "functional Kafka checks failed"

log "all smoke tests passed"
