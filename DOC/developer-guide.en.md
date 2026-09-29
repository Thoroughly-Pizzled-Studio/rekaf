# Developer guide

[Русский](developer-guide.ru.md) · [README](../README.md) · [User guide](user-guide.en.md)

## Repository structure

| Path | Purpose |
| --- | --- |
| `rekaf.sh` | Topic and ACL administration script |
| `README.md`, `README.ru.md` | Project overview in two languages |
| `DOC/user-guide.en.md`, `DOC/user-guide.ru.md` | User documentation |
| `DOC/developer-guide.en.md`, `DOC/developer-guide.ru.md` | Developer documentation |
| `.gitignore` | Git exclusions |
| `tests/test_rekaf.py` | CLI regression tests with Kafka command doubles |

## Script design

- `set -euo pipefail` enables handling of unset variables and command and pipeline failures, subject to Bash rules for conditional constructs.
- Arguments are parsed with a `while` loop and `case`.
- Before accessing Kafka, the script validates parameter combinations, executable Kafka utilities, and the presence of the client configuration file.
- Shared Kafka arguments are stored in the `COMMON_OPTIONS` array.
- Topic existence is checked with `kafka-topics.sh --list` and an exact line match.
- `topic_acl_exists` and `group_acl_exists` delegate to `acl_exists`, which checks query success explicitly and matches an exact ACL entry in the complete output.
- Following the checks, the script creates the topic, adds consumer ACLs, and adds producer ACLs as needed.
- Clients are not granted `CREATE`: the topic is created using administrative credentials.

## Configuration and limitations

`KAFKA_BIN`, `BOOTSTRAP_SERVER`, and `CONFIG` are set at the beginning of the script. See the [user guide](user-guide.en.md#environment-configuration) for current values and instructions.

- Kafka paths, the bootstrap server, and the configuration path are hard-coded.
- ACL checks match complete entry lines from the text output of `kafka-acls.sh`; output format changes may require adjustments.
- Checks require the exact principal and operation, `permissionType=ALLOW`, and `host=*` on the requested literal resource. Other hosts, wildcard principals, broader operations, and prefixed resource patterns are not treated as the same entry. This is not an effective-authorization check: existing DENY rules can still block access.
- A failed resource-listing command stops execution with a nonzero status and preserves Kafka diagnostics. Earlier successful changes are not rolled back.
- The script only adds permissions and does not remove extra ACLs.
- Topics are created without explicit `--partitions` or `--replication-factor`, using broker defaults.

Existence checks run in `if` conditions. Do not rely on `set -e` to guarantee termination on errors inside these checks.

## Validating changes

Run these minimum checks from the repository root:

```bash
bash -n rekaf.sh
shellcheck rekaf.sh
```

The second command requires ShellCheck to be installed.

Run integration checks on a test Kafka cluster:

1. Run the script for a new topic.
2. Verify that the topic and ACLs were created.
3. Repeat the same command.
4. Verify that the repeated run does not create duplicates.
5. Check producer-only, consumer-only, and combined scenarios.
6. Check errors for missing and incompatible arguments.

## Automated regression tests

Run from the repository root with Python 3.9 or newer:

```bash
python3 -m unittest discover -s tests -v
```

Tests run a temporary copy of the script with stateful Kafka command doubles. They require Bash and Python, but no Kafka cluster, administrative privileges, or third-party Python packages. Only paths in the temporary copy are rewritten; no files under `/opt/kafka` are touched.

Coverage includes missing and existing resources, repeated runs, exact ACL fields, similar principal names, large listings, query failures with partial output, mutation failures, and argument validation. These tests do not replace validation against a real Kafka cluster.

## Development rules

- Preserve Bash compatibility; arrays and `[[ ... ]]` are not supported by POSIX `sh`.
- Quote variable expansions.
- Account for `pipefail` and conditional error handling when adding pipelines.
- Do not commit secrets, client configurations, or credentials.
- Validate ACL changes on a separate Kafka cluster before production use.
- Update both documentation languages when behavior changes.
