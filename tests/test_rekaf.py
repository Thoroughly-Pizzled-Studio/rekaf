"""CLI regression tests using isolated, stateful Kafka command doubles."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
FAKE_KAFKA = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

path = Path(os.environ["REKAF_TEST_STATE"])
state = json.loads(path.read_text())
args = sys.argv[1:]
is_topics = Path(sys.argv[0]).name == "kafka-topics.sh"
kind = "topics" if is_topics else ("group" if "--group" in args else "topic")
action = "list" if "--list" in args else ("create" if is_topics else "add")
key = kind + ":" + action
state["calls"].append([key, args])
path.write_text(json.dumps(state))
if state.get("fail") == key:
    print(state.get("partial", ""))
    print("Kafka test diagnostic: " + key, file=sys.stderr)
    sys.exit(7)
if action == "list":
    if is_topics:
        print("\n".join(state["topics"]))
    else:
        print("Current ACLs for resource ResourcePattern:")
        print("\n".join(state[kind]))
else:
    if is_topics:
        state["topics"].append(args[args.index("--topic") + 1])
    else:
        assert args[args.index("--resource-pattern-type") + 1] == "literal"
        assert args[args.index("--allow-host") + 1] == "*"
        principal = args[args.index("--allow-principal") + 1]
        operation = args[args.index("--operation") + 1]
        state[kind].append(f"\t(principal={principal}, host=*, operation={operation}, permissionType=ALLOW)")
    path.write_text(json.dumps(state))
'''


def acl(principal="alice", operation="WRITE", host="*", permission="ALLOW"):
    return (f"\t(principal=User:{principal}, host={host}, "
            f"operation={operation}, permissionType={permission})")


class RekafTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="rekaf-test-")
        self.addCleanup(self.temp.cleanup)
        directory = Path(self.temp.name)
        self.state_file = directory / "state.json"
        config = directory / "client.conf"
        config.touch()
        for name in ("kafka-topics.sh", "kafka-acls.sh"):
            command = directory / name
            command.write_text(FAKE_KAFKA)
            command.chmod(0o755)
        # Rewrite only a temporary copy: production configuration stays unchanged.
        source = (ROOT / "rekaf.sh").read_text()
        source = source.replace('KAFKA_BIN="/opt/kafka/bin"', f'KAFKA_BIN="{directory}"')
        source = source.replace('CONFIG="/opt/kafka/config/client.conf"', f'CONFIG="{config}"')
        self.script = directory / "rekaf.sh"
        self.script.write_text(source)
        self.save(topics=["orders"], topic=[], group=[], calls=[])

    def save(self, **state):
        self.state_file.write_text(json.dumps(state))

    def state(self):
        return json.loads(self.state_file.read_text())

    def configure(self, **changes):
        self.save(**(self.state() | changes))

    def run_script(self, *args):
        return subprocess.run(
            ["bash", str(self.script), *args], text=True, capture_output=True,
            env=dict(os.environ, REKAF_TEST_STATE=str(self.state_file)), timeout=10,
        )

    def producer(self, name="alice"):
        return self.run_script("--topic", "orders", "--producer", name)

    def mutations(self):
        return [key for key, _ in self.state()["calls"] if not key.endswith(":list")]

    def test_missing_resources_and_repeated_run(self):
        self.configure(topics=[])
        args = ("--topic", "orders", "--producer", "alice", "--consumer", "bob", "--group", "workers")
        first = self.run_script(*args)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(self.mutations(), ["topics:create", "topic:add", "group:add", "topic:add"])
        self.configure(calls=[])
        second = self.run_script(*args)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.mutations(), [])

    def test_existing_exact_acl(self):
        self.configure(topic=[acl()])
        result = self.producer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.mutations(), [])
        for key, args in self.state()["calls"]:
            if key == "topic:list":
                self.assertEqual(args[args.index("--resource-pattern-type") + 1], "literal")

    def test_nonmatching_acl_fields_do_not_skip_addition(self):
        for entry in (acl(principal="alice-admin"), acl(operation="WRITE_EXTRA"),
                      acl(permission="DENY"), acl(host="127.0.0.1"),
                      acl(principal="*"), acl(operation="ALL")):
            with self.subTest(entry=entry):
                self.configure(topic=[entry], calls=[])
                result = self.producer()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.mutations(), ["topic:add"])

    def test_fields_from_different_entries_are_not_combined(self):
        self.configure(topic=[acl(operation="READ"), acl(principal="other")])
        result = self.producer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.mutations(), ["topic:add"])

    def test_principal_regex_characters_are_literal(self):
        self.configure(topic=[acl(principal="aliceXadmin")])
        result = self.producer("alice.admin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.mutations(), ["topic:add"])
        self.configure(calls=[])
        self.assertEqual(self.producer("alice.admin").returncode, 0)
        self.assertEqual(self.mutations(), [])

    def test_topic_name_is_exact(self):
        self.configure(topics=["orders-archive"])
        self.assertEqual(self.producer().returncode, 0)
        self.assertEqual(self.mutations(), ["topics:create", "topic:add"])

    def test_large_topic_listing(self):
        self.configure(topics=["orders"] + [f"topic-{i}" for i in range(20000)], topic=[acl()])
        result = self.producer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.mutations(), [])

    def test_topic_query_failure_even_with_matching_partial_output(self):
        self.configure(fail="topics:list", partial="orders")
        result = self.producer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Kafka test diagnostic", result.stderr)
        self.assertIn("Failed to list topics", result.stderr)
        self.assertEqual(self.mutations(), [])

    def test_acl_query_failure_even_with_matching_partial_output(self):
        self.configure(fail="topic:list", partial=acl())
        result = self.producer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Kafka test diagnostic", result.stderr)
        self.assertIn("Failed to list ACLs", result.stderr)
        self.assertEqual(self.mutations(), [])

    def test_group_query_failure_stops_before_producer(self):
        self.configure(fail="group:list", topic=[acl(principal="bob", operation="READ")])
        result = self.run_script("--topic", "orders", "--consumer", "bob", "--group", "workers", "--producer", "alice")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Kafka test diagnostic", result.stderr)
        self.assertEqual(self.mutations(), [])
        self.assertEqual(self.state()["calls"][-1][0], "group:list")

    def test_consumer_only(self):
        result = self.run_script("--topic", "orders", "--consumer", "bob", "--group", "workers")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.mutations(), ["topic:add", "group:add"])

    def test_mutation_failure_stops_execution(self):
        for failure in ("topics:create", "topic:add", "group:add"):
            with self.subTest(failure=failure):
                self.configure(topics=[] if failure == "topics:create" else ["orders"],
                               topic=[], group=[], calls=[], fail=failure)
                result = self.run_script("--topic", "orders", "--consumer", "bob", "--group", "workers", "--producer", "alice")
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.state()["calls"][-1][0], failure)
                self.assertNotIn("Done.", result.stdout)

    def test_invalid_arguments_never_call_kafka(self):
        for args in ((), ("--topic", "orders"), ("--topic", "orders", "--consumer", "bob"),
                     ("--topic", "orders", "--producer", "alice", "--group", "workers")):
            with self.subTest(args=args):
                self.assertNotEqual(self.run_script(*args).returncode, 0)
                self.assertEqual(self.state()["calls"], [])


if __name__ == "__main__":
    unittest.main()
