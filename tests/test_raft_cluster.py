import os
import re
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

import grpc

import inventory_pb2
import inventory_pb2_grpc
import raft_node
import raft_pb2


ROOT = Path(__file__).resolve().parents[1]
LEADER_LINE = re.compile(
    r"\[(?P<node>[ABC]) term=(?P<term>\d+) LEADER\] "
    r"ELECTED LEADER \((?P<votes>\d+)/3 votes\)"
)


class RaftTermTests(unittest.TestCase):
    def test_votes_are_limited_to_one_candidate_per_term(self):
        node = raft_node.RaftNode("B")

        first = node.RequestVote(
            raft_pb2.VoteRequest(term=1, candidate_id="A"),
            None,
        )
        second = node.RequestVote(
            raft_pb2.VoteRequest(term=1, candidate_id="C"),
            None,
        )
        stale = node.RequestVote(
            raft_pb2.VoteRequest(term=0, candidate_id="C"),
            None,
        )

        self.assertTrue(first.vote_granted)
        self.assertFalse(second.vote_granted)
        self.assertFalse(stale.vote_granted)
        self.assertEqual(stale.term, 1)

    def test_stale_heartbeat_is_rejected_and_higher_term_is_accepted(self):
        node = raft_node.RaftNode("B")
        node.current_term = 2

        stale = node.AppendEntries(
            raft_pb2.HeartbeatRequest(term=1, leader_id="A"),
            None,
        )
        current = node.AppendEntries(
            raft_pb2.HeartbeatRequest(term=3, leader_id="C"),
            None,
        )

        self.assertFalse(stale.success)
        self.assertEqual(stale.term, 2)
        self.assertTrue(current.success)
        self.assertEqual(node.current_term, 3)
        self.assertEqual(node.state, "FOLLOWER")
        self.assertEqual(node.leader_id, "C")


class RaftClusterProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.processes = {}
        cls.output = {name: [] for name in raft_node.PEERS}
        cls.all_output = []
        cls.condition = threading.Condition()
        cls.readers = []

        for name in raft_node.PEERS:
            process = subprocess.Popen(
                [sys.executable, "-u", "raft_node.py", name],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=os.environ.copy(),
            )
            cls.processes[name] = process
            reader = threading.Thread(
                target=cls._read_output,
                args=(name, process),
                daemon=True,
            )
            cls.readers.append(reader)
            reader.start()

        if not cls._wait_for(
            lambda: all(
                any(f"Raft coordinator {name} listening" in line for line in cls.output[name])
                for name in raft_node.PEERS
            ),
            timeout=8,
        ):
            cls._stop_processes()
            raise AssertionError("Not all Raft node processes started")

    @classmethod
    def tearDownClass(cls):
        cls._stop_processes()

    @classmethod
    def _read_output(cls, name, process):
        for line in process.stdout:
            with cls.condition:
                line = line.rstrip()
                cls.output[name].append(line)
                cls.all_output.append((name, line))
                cls.condition.notify_all()

    @classmethod
    def _wait_for(cls, predicate, timeout):
        deadline = time.monotonic() + timeout
        with cls.condition:
            while not predicate():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                cls.condition.wait(min(remaining, 0.2))
            return True

    @classmethod
    def _leader_events(cls, since=0):
        with cls.condition:
            events = []
            for _, line in cls.all_output[since:]:
                match = LEADER_LINE.search(line)
                if match:
                    event = match.groupdict()
                    events.append(
                        (event["node"], int(event["term"]), int(event["votes"]))
                    )
            return events

    @classmethod
    def _stop_processes(cls):
        for process in getattr(cls, "processes", {}).values():
            if process.poll() is None:
                process.terminate()
        for process in getattr(cls, "processes", {}).values():
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    def test_election_stability_failover_and_leader_gated_coordinator(self):
        self.assertTrue(
            self._wait_for(lambda: bool(self._leader_events()), timeout=12),
            "No leader elected within 12 seconds",
        )

        def followers_observed_leader():
            events = self._leader_events()
            if not events:
                return False
            elected_node, elected_term, _ = events[-1]
            return all(
                any(f"Received heartbeat from {elected_node}" in line for line in self.output[name])
                for name in self.output
                if name != elected_node
            )

        self.assertTrue(
            self._wait_for(followers_observed_leader, timeout=5),
            "Followers did not recognize the elected leader",
        )

        leader, old_term, votes = self._leader_events()[-1]
        self.assertGreaterEqual(votes, 2)
        self.assertEqual(len({node for node, term, _ in self._leader_events() if term == old_term}), 1)

        self._assert_coordinator_access(leader, old_term)

        with self.condition:
            stable_markers = {name: len(lines) for name, lines in self.output.items()}
            failover_marker = len(self.all_output)
            stable_leader_events = len(self._leader_events())
        time.sleep(10)
        with self.condition:
            new_elections = [
                line
                for name, lines in self.output.items()
                for line in lines[stable_markers[name]:]
                if "Starting election" in line
            ]
            stable_heartbeats = {
                name: sum(
                    f"Received heartbeat from {leader}" in line
                    for line in self.output[name][stable_markers[name]:]
                )
                for name in self.output
                if name != leader
            }
        self.assertEqual(new_elections, [], "A new election occurred during healthy heartbeats")
        self.assertEqual(len(self._leader_events()), stable_leader_events)
        self.assertTrue(
            all(count >= 10 for count in stable_heartbeats.values()),
            f"Followers did not continue receiving heartbeats: {stable_heartbeats}",
        )

        survivor_names = set(self.processes) - {leader}
        survivor_markers = {name: len(self.output[name]) for name in survivor_names}
        self.processes[leader].terminate()
        self.processes[leader].wait(timeout=5)

        def replacement_elected():
            return any(
                node != leader and term > old_term
                for node, term, _ in self._leader_events(since=failover_marker)
            )

        self.assertTrue(
            self._wait_for(replacement_elected, timeout=8),
            "Surviving nodes did not elect a higher-term replacement",
        )
        new_leader, new_term, new_votes = next(
            event
            for event in reversed(self._leader_events(since=failover_marker))
            if event[0] != leader and event[1] > old_term
        )
        self.assertGreater(new_term, old_term)
        self.assertGreaterEqual(new_votes, 2)
        for term in {term for _, term, _ in self._leader_events()}:
            leaders_in_term = {
                node for node, event_term, _ in self._leader_events() if event_term == term
            }
            self.assertLessEqual(len(leaders_in_term), 1)

        follower = next(iter(survivor_names - {new_leader}))
        self.assertTrue(
            self._wait_for(
                lambda: any(
                    f"Received heartbeat from {new_leader}" in line
                    for line in self.output[follower][survivor_markers[follower]:]
                ),
                timeout=4,
            ),
            "Surviving follower did not recognize the replacement leader",
        )
        self._assert_coordinator_access(new_leader, new_term, follower)

    def _assert_coordinator_access(self, leader, term, follower=None):
        follower = follower or next(name for name in self.processes if name != leader)
        with grpc.insecure_channel(raft_node.PEERS[leader]) as channel:
            stub = inventory_pb2_grpc.MutexServiceStub(channel)
            response = stub.RequestAccess(
                inventory_pb2.AccessRequest(node_id=7, timestamp=term),
                timeout=2,
            )
        self.assertEqual(response.node_id, 7)
        self.assertGreater(response.timestamp, term)

        with grpc.insecure_channel(raft_node.PEERS[follower]) as channel:
            stub = inventory_pb2_grpc.MutexServiceStub(channel)
            with self.assertRaises(grpc.RpcError) as error:
                stub.RequestAccess(
                    inventory_pb2.AccessRequest(node_id=8, timestamp=term),
                    timeout=2,
                )
        self.assertEqual(error.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
        self.assertIn(f"leader_id={leader}", error.exception.details())


if __name__ == "__main__":
    unittest.main()