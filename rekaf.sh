#!/bin/bash

set -euo pipefail

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP_SERVER="localhost:9092"
CONFIG="/opt/kafka/config/client.conf"

TOPIC=""
PRODUCER=""
CONSUMER=""
CONS_GROUP=""

usage() {
  cat <<EOF
Usage:
  $0 --topic <topic> [--producer <producer>] [--consumer <consumer> --group <group>]

Examples:

  Producer only:
    $0 --topic orders --producer orders-producer

  Consumer only:
    $0 --topic orders --consumer orders-consumer --group orders-group

  Producer + Consumer:
    $0 \
      --topic orders \
      --producer orders-producer \
      --consumer orders-consumer \
      --group orders-group
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

# --------------------------------------------------
# Parameters
# --------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --topic)
      [[ $# -ge 2 ]] || die "--topic requires a value"
      [[ "$2" != --* ]] || die "--topic requires a value"
      TOPIC="$2"
      shift 2
      ;;

    --producer)
      [[ $# -ge 2 ]] || die "--producer requires a value"
      [[ "$2" != --* ]] || die "--producer requires a value"
      PRODUCER="$2"
      shift 2
      ;;

    --consumer)
      [[ $# -ge 2 ]] || die "--consumer requires a value"
      [[ "$2" != --* ]] || die "--consumer requires a value"
      CONSUMER="$2"
      shift 2
      ;;

    --group)
      [[ $# -ge 2 ]] || die "--group requires a value"
      [[ "$2" != --* ]] || die "--group requires a value"
      CONS_GROUP="$2"
      shift 2
      ;;

    -h|--help)
      usage
      exit 0
      ;;

    *)
      die "Unknown parameter: $1"
      ;;
  esac
done

# --------------------------------------------------
# Validation
# --------------------------------------------------

[[ -n "$TOPIC" ]] || die "--topic is required"

if [[ -z "$PRODUCER" && -z "$CONSUMER" ]]; then
  die "Specify at least --producer or --consumer"
fi

if [[ -n "$CONSUMER" && -z "$CONS_GROUP" ]]; then
  die "--group is required when --consumer is specified"
fi

if [[ -z "$CONSUMER" && -n "$CONS_GROUP" ]]; then
  die "--group cannot be used without --consumer"
fi

[[ -x "$KAFKA_BIN/kafka-topics.sh" ]] \
  || die "kafka-topics.sh not found: $KAFKA_BIN/kafka-topics.sh"

[[ -x "$KAFKA_BIN/kafka-acls.sh" ]] \
  || die "kafka-acls.sh not found: $KAFKA_BIN/kafka-acls.sh"

[[ -f "$CONFIG" ]] \
  || die "Kafka config not found: $CONFIG"

# --------------------------------------------------
# Common Kafka options
# --------------------------------------------------

COMMON_OPTIONS=(
  --bootstrap-server "$BOOTSTRAP_SERVER"
  --command-config "$CONFIG"
)

# --------------------------------------------------
# Topic
# --------------------------------------------------

echo "Checking topic '$TOPIC'..."

if "$KAFKA_BIN/kafka-topics.sh" \
    "${COMMON_OPTIONS[@]}" \
    --list |
    grep -Fxq -- "$TOPIC"
then
  echo "OK: topic '$TOPIC' already exists"
else
  echo "Creating topic '$TOPIC'..."

  "$KAFKA_BIN/kafka-topics.sh" \
    "${COMMON_OPTIONS[@]}" \
    --create \
    --topic "$TOPIC"

  echo "OK: topic '$TOPIC' created"
fi

# --------------------------------------------------
# ACL helper functions
# --------------------------------------------------

topic_acl_exists() {
  local principal="$1"
  local operation="$2"

  "$KAFKA_BIN/kafka-acls.sh" \
    "${COMMON_OPTIONS[@]}" \
    --list \
    --topic "$TOPIC" 2>/dev/null |
    grep -F "principal=$principal" |
    grep -Fq "operation=$operation"
}

group_acl_exists() {
  local principal="$1"
  local operation="$2"
  local group="$3"

  "$KAFKA_BIN/kafka-acls.sh" \
    "${COMMON_OPTIONS[@]}" \
    --list \
    --group "$group" 2>/dev/null |
    grep -F "principal=$principal" |
    grep -Fq "operation=$operation"
}

# --------------------------------------------------
# Consumer ACL
#
# Consumer:
#   READ topic
#   READ group
# --------------------------------------------------

if [[ -n "$CONSUMER" ]]; then

  CONSUMER_PRINCIPAL="User:$CONSUMER"

  echo
  echo "Checking consumer ACL:"
  echo "  principal: $CONSUMER_PRINCIPAL"
  echo "  topic:     $TOPIC"
  echo "  group:     $CONS_GROUP"

  if topic_acl_exists "$CONSUMER_PRINCIPAL" "READ"; then
    echo "OK: topic READ already exists"
  else
    echo "Adding topic READ..."

    "$KAFKA_BIN/kafka-acls.sh" \
      "${COMMON_OPTIONS[@]}" \
      --add \
      --allow-principal "$CONSUMER_PRINCIPAL" \
      --operation READ \
      --topic "$TOPIC"

    echo "OK: topic READ added"
  fi

  if group_acl_exists "$CONSUMER_PRINCIPAL" "READ" "$CONS_GROUP"; then
    echo "OK: group READ already exists"
  else
    echo "Adding group READ..."

    "$KAFKA_BIN/kafka-acls.sh" \
      "${COMMON_OPTIONS[@]}" \
      --add \
      --allow-principal "$CONSUMER_PRINCIPAL" \
      --operation READ \
      --group "$CONS_GROUP"

    echo "OK: group READ added"
  fi
fi

# --------------------------------------------------
# Producer ACL
#
# Producer:
#   WRITE topic
#
# CREATE intentionally NOT granted.
# Topic is created above using administrative credentials.
# --------------------------------------------------

if [[ -n "$PRODUCER" ]]; then

  PRODUCER_PRINCIPAL="User:$PRODUCER"

  echo
  echo "Checking producer ACL:"
  echo "  principal: $PRODUCER_PRINCIPAL"
  echo "  topic:     $TOPIC"

  if topic_acl_exists "$PRODUCER_PRINCIPAL" "WRITE"; then
    echo "OK: topic WRITE already exists"
  else
    echo "Adding topic WRITE..."

    "$KAFKA_BIN/kafka-acls.sh" \
      "${COMMON_OPTIONS[@]}" \
      --add \
      --allow-principal "$PRODUCER_PRINCIPAL" \
      --operation WRITE \
      --topic "$TOPIC"

    echo "OK: topic WRITE added"
  fi
fi

echo
echo "Done."
