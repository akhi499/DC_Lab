"""Three-node Raft election demo with a leader-gated mutex coordinator."""

import random
import sys
import threading
import time
from concurrent import futures

import grpc

import inventory_pb2
import inventory_pb2_grpc
import raft_pb2
import raft_pb2_grpc


PEERS = {
    "A": "localhost:60401",
    "B": "localhost:60402",
    "C": "localhost:60403",
}
HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
RPC_TIMEOUT = 0.5
MAJORITY = len(PEERS) // 2 + 1


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id):
        if node_id not in PEERS:
            raise ValueError(f"Unknown node {node_id!r}; expected A, B, or C")

        self.node_id = node_id
        self.peers = {name: address for name, address in PEERS.items() if name != node_id}
        self.state = "FOLLOWER"
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.monotonic()
        self.lock = threading.RLock()
        self._election_deadline = self.last_heartbeat + self._random_timeout()
        self._votes = set()
        self._coordinator_lock = threading.Lock()
        self._coordinator_timestamp = 0
        self._stop_event = threading.Event()
        self._threads = []

    def _random_timeout(self):
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def log(self, message):
        with self.lock:
            print(
                f"[{self.node_id} term={self.current_term} {self.state}] {message}",
                flush=True,
            )

    def _reset_election_deadline_locked(self):
        now = time.monotonic()
        self.last_heartbeat = now
        self._election_deadline = now + self._random_timeout()

    def _step_down_locked(self, term):
        self.current_term = term
        self.state = "FOLLOWER"
        self.voted_for = None
        self.leader_id = None
        self._votes.clear()
        self._reset_election_deadline_locked()

    def start(self):
        self._threads = [
            threading.Thread(target=self._election_loop, name=f"{self.node_id}-election", daemon=True),
            threading.Thread(target=self._heartbeat_loop, name=f"{self.node_id}-heartbeat", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self):
        self._stop_event.set()

    def RequestVote(self, request, context):
        granted = False
        stepped_down = False
        with self.lock:
            if request.term < self.current_term:
                reply_term = self.current_term
            else:
                if request.term > self.current_term:
                    self._step_down_locked(request.term)
                    stepped_down = True

                if self.voted_for is None or self.voted_for == request.candidate_id:
                    self.state = "FOLLOWER"
                    self.leader_id = None
                    self.voted_for = request.candidate_id
                    self._reset_election_deadline_locked()
                    granted = True
                reply_term = self.current_term

        if stepped_down:
            self.log(f"Higher term detected; stepped down for term {request.term}")
        outcome = "granted" if granted else "denied"
        self.log(f"Vote {outcome} to {request.candidate_id} for term {request.term}")
        return raft_pb2.VoteReply(term=reply_term, vote_granted=granted)

    def AppendEntries(self, request, context):
        stepped_down = False
        with self.lock:
            if request.term < self.current_term:
                self.log(
                    f"Rejected stale heartbeat from {request.leader_id} "
                    f"(term {request.term})"
                )
                return raft_pb2.HeartbeatReply(term=self.current_term, success=False)

            if request.term > self.current_term:
                self._step_down_locked(request.term)
                stepped_down = True
            elif self.state != "FOLLOWER":
                self.state = "FOLLOWER"
                self._votes.clear()

            self.leader_id = request.leader_id
            self._reset_election_deadline_locked()
            reply_term = self.current_term

        if stepped_down:
            self.log(f"Higher term detected; stepped down for term {request.term}")
        self.log(f"Received heartbeat from {request.leader_id}")
        return raft_pb2.HeartbeatReply(term=reply_term, success=True)

    def _election_loop(self):
        while not self._stop_event.wait(0.05):
            start_election = False
            failed_votes = 0
            with self.lock:
                if self.state != "LEADER" and time.monotonic() >= self._election_deadline:
                    if self.state == "CANDIDATE":
                        failed_votes = len(self._votes)
                        self.state = "FOLLOWER"
                        self.leader_id = None
                        self._reset_election_deadline_locked()
                    else:
                        start_election = True

            if failed_votes:
                self.log(f"Election failed ({failed_votes}/{len(PEERS)} votes); returning to follower")
            if start_election:
                self._begin_election()

    def _begin_election(self):
        with self.lock:
            if self.state == "LEADER":
                return
            self.current_term += 1
            term = self.current_term
            self.state = "CANDIDATE"
            self.voted_for = self.node_id
            self.leader_id = None
            self._votes = {self.node_id}
            self._reset_election_deadline_locked()

        self.log("Starting election")
        if len(self._votes) >= MAJORITY:
            self._become_leader(term)
            return

        for peer_id, address in self.peers.items():
            threading.Thread(
                target=self._request_vote,
                args=(peer_id, address, term),
                name=f"{self.node_id}-vote-{peer_id}-term-{term}",
                daemon=True,
            ).start()

    def _request_vote(self, peer_id, address, term):
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                reply = stub.RequestVote(
                    raft_pb2.VoteRequest(term=term, candidate_id=self.node_id),
                    timeout=RPC_TIMEOUT,
                )
        except grpc.RpcError as error:
            self.log(f"Peer {peer_id} unavailable for vote: {error.code().name}")
            return

        became_leader = False
        stepped_down = False
        with self.lock:
            if reply.term > self.current_term:
                self._step_down_locked(reply.term)
                stepped_down = True
            elif (
                reply.vote_granted
                and reply.term == term
                and self.current_term == term
                and self.state == "CANDIDATE"
            ):
                self._votes.add(peer_id)
                became_leader = len(self._votes) >= MAJORITY

        if stepped_down:
            self.log(f"Higher term {reply.term} detected from {peer_id}; leadership lost")
        elif reply.vote_granted:
            self.log(f"Vote received from {peer_id} ({len(self._votes)}/{len(PEERS)})")
            if became_leader:
                self._become_leader(term)
        else:
            self.log(f"Vote denied by {peer_id} for term {term}")

    def _become_leader(self, term):
        with self.lock:
            if self.current_term != term or self.state != "CANDIDATE":
                return
            votes = len(self._votes)
            if votes < MAJORITY:
                return
            self.state = "LEADER"
            self.leader_id = self.node_id
            self._reset_election_deadline_locked()
        self.log(f"ELECTED LEADER ({votes}/{len(PEERS)} votes)")

    def _heartbeat_loop(self):
        while not self._stop_event.wait(HEARTBEAT_INTERVAL):
            with self.lock:
                if self.state != "LEADER":
                    continue
                term = self.current_term

            for peer_id, address in self.peers.items():
                if self._stop_event.is_set():
                    break
                self._send_heartbeat(peer_id, address, term)

    def _send_heartbeat(self, peer_id, address, term):
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                reply = stub.AppendEntries(
                    raft_pb2.HeartbeatRequest(term=term, leader_id=self.node_id),
                    timeout=RPC_TIMEOUT,
                )
        except grpc.RpcError as error:
            self.log(f"Peer {peer_id} unavailable for heartbeat: {error.code().name}")
            return

        stepped_down = False
        with self.lock:
            if reply.term > self.current_term:
                self._step_down_locked(reply.term)
                stepped_down = True

        if stepped_down:
            self.log(f"Higher term {reply.term} detected from {peer_id}; leadership lost")
        elif reply.success:
            self.log(f"Heartbeat sent to {peer_id}")


class CoordinatorService(inventory_pb2_grpc.MutexServiceServicer):
    """Expose the existing RequestAccess contract only through the leader."""

    def __init__(self, node):
        self.node = node

    def RequestAccess(self, request, context):
        node = self.node
        with node.lock:
            if node.state != "LEADER":
                leader = node.leader_id or "unknown"
                node.log(f"Coordination request rejected; leader is {leader}")
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"NOT_LEADER: leader_id={leader}",
                )

            with node._coordinator_lock:
                node._coordinator_timestamp = max(
                    node._coordinator_timestamp,
                    request.timestamp,
                ) + 1
                response = inventory_pb2.AccessReply(
                    node_id=request.node_id,
                    timestamp=node._coordinator_timestamp,
                )

        node.log(f"Coordination request accepted from node {request.node_id}")
        return response


def serve(node_id):
    node = RaftNode(node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(node, server)
    inventory_pb2_grpc.add_MutexServiceServicer_to_server(
        CoordinatorService(node),
        server,
    )
    bound_port = server.add_insecure_port(PEERS[node_id])
    if not bound_port:
        raise RuntimeError(f"Could not bind {PEERS[node_id]}")

    server.start()
    node.start()
    print(f"Raft coordinator {node_id} listening on {PEERS[node_id]}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        server.stop(0).wait()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in PEERS:
        print("Usage: python raft_node.py <A|B|C>", file=sys.stderr)
        sys.exit(2)
    serve(sys.argv[1])