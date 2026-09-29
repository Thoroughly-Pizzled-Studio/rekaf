# User guide

[Русский](user-guide.ru.md) · [README](../README.md) · [Developer guide](developer-guide.en.md)

## Requirements

- Linux or another Unix-like environment with Bash.
- An Apache Kafka installation with executable utilities:
  - `/opt/kafka/bin/kafka-topics.sh`;
  - `/opt/kafka/bin/kafka-acls.sh`.
- Kafka available at `localhost:9092`.
- Client configuration at `/opt/kafka/config/client_jenkins.conf`.
- Configuration credentials that can list and create topics, and list and modify ACLs.
- The `grep` utility.

## Installation

```bash
git clone https://github.com/Eksterulo/rekaf.git
cd rekaf
chmod +x rekaf.sh
```

## Syntax

```text
./rekaf.sh --topic <topic> [--producer <producer>] [--consumer <consumer> --group <group>]
```

| Parameter | Required | Purpose |
| --- | --- | --- |
| `--topic <topic>` | Always | Kafka topic name |
| `--producer <producer>` | At least producer or consumer | Producer principal name without the `User:` prefix |
| `--consumer <consumer>` | At least producer or consumer | Consumer principal name without the `User:` prefix |
| `--group <group>` | With `--consumer` | Consumer group |
| `-h`, `--help` | No | Display help |

`--group` cannot be used without `--consumer`.

## Examples

Producer only:

```bash
./rekaf.sh --topic orders --producer orders-producer
```

Consumer only:

```bash
./rekaf.sh --topic orders --consumer orders-consumer --group orders-group
```

Producer and consumer:

```bash
./rekaf.sh --topic orders --producer orders-producer --consumer orders-consumer --group orders-group
```

## Output and repeated runs

The script prints each check and change. If its checks find an existing topic or ACL, creation is skipped. Argument and prerequisite errors exit with a nonzero status. A failed topic creation or ACL addition also stops execution. A failed existence check may be treated as a missing resource and lead to a creation attempt.

## Environment configuration

The following values are set at the beginning of `rekaf.sh`:

```bash
KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP_SERVER="localhost:9092"
CONFIG="/opt/kafka/config/client_jenkins.conf"
```

Edit these values in the script for a different environment. There are currently no command-line or environment-variable overrides. The filename `client_jenkins.conf` is the current client configuration path; the script runs as an ordinary terminal command.

## Limitations

- Kafka paths, the bootstrap server, and the configuration path are hard-coded.
- ACL checks parse text output from `kafka-acls.sh` using `grep`; output format changes may require adjustments.
- Checks match principal and operation as substrings, without separately validating permission type or host. A match does not guarantee a suitable allow ACL.
- Resource-listing failures may be treated as a missing topic or ACL; ACL-listing error messages are suppressed.
- The script only adds permissions and does not remove extra ACLs.
- Topics are created without explicit `--partitions` or `--replication-factor`, using broker defaults.

## Safety

The script changes Kafka state. Before running in production, verify the cluster address, client configuration, principal names, topic, and consumer group. Check behavior on a test cluster first.

Do not commit Kafka secrets or client configuration. Store the configuration separately and restrict access to it.
