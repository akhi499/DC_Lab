# Experiment 9: Raft-Based Leader Election and Failover

This experiment runs a three-node coordinator using Raft-style leader election. Nodes A, B, and C run independent gRPC servers on ports 60401, 60402, and 60403. Each node begins as a follower, elections use randomized 1.5–3.0 second timeouts and a two-node majority, and leaders send heartbeats every 0.5 seconds.

Every server registers both `RaftService` and the repository's existing `MutexService` on the same port. `MutexService.RequestAccess` is accepted only by the current leader. Followers return gRPC `FAILED_PRECONDITION` with `NOT_LEADER: leader_id=<id>` in the status details; this keeps the existing `AccessReply` message unchanged. The existing contract has no release RPC, so this demo serializes and acknowledges the coordination request itself. It does not implement a durable or replicated lock state, replicated log, or production-grade consensus.

## Experiment 9 Files

```text
proto/raft.proto          Raft vote and heartbeat RPC definitions
raft_pb2.py               Generated Raft protobuf messages
raft_pb2_grpc.py           Generated Raft gRPC services
raft_node.py               Raft election and leader-gated coordinator
tests/test_raft_cluster.py Unit and three-process failover tests
proto/inventory.proto      Existing MutexService contract
inventory_pb2*.py          Existing generated coordinator messages/services
```

## Prerequisites

The project requires Python and the dependencies already listed in `requirements.txt` (`grpcio` and `grpcio-tools` are included). From the repository root, install them into the existing virtual environment:

```powershell
\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Regenerate the Raft stubs after changing `proto/raft.proto`:

```powershell
\.venv\Scripts\python.exe -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/raft.proto
```

## Run the Cluster

Open three terminals at the repository root and run one command in each:

```powershell
\.venv\Scripts\python.exe raft_node.py A
```

```powershell
\.venv\Scripts\python.exe raft_node.py B
```

```powershell
\.venv\Scripts\python.exe raft_node.py C
```

Watch the console output for `ELECTED LEADER (2/3 votes)` to identify the leader. Followers report `Received heartbeat from <id>`. To observe failover, terminate the leader's terminal process with Ctrl+C or close that terminal. One of the remaining nodes should win a higher-term election automatically, and the surviving follower should begin logging heartbeats from it.

Typical election and failover output (node names and terms vary):

```text
[A term=1 CANDIDATE] Starting election
[B term=1 FOLLOWER] Vote granted to A for term 1
[A term=1 LEADER] ELECTED LEADER (2/3 votes)
[C term=1 FOLLOWER] Received heartbeat from A
... terminate node A ...
[B term=2 CANDIDATE] Starting election
[B term=2 LEADER] ELECTED LEADER (2/3 votes)
[C term=2 FOLLOWER] Received heartbeat from B
```

Run the tests from the repository root. The process test starts all three servers, checks stable heartbeats for 10 seconds, tests leader/follower coordinator responses, terminates the elected leader process, and verifies higher-term failover:

```powershell
\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The tests require the three ports to be available. All child processes are terminated by the test cleanup.

---

# Existing Mini gRPC Flask Project

This project is a small version of the experiment shown in the presentation:

- A Flask app acts like the order service.
- A gRPC server acts like the inventory service.
- A `.proto` file defines the RPC methods.
- The Flask app calls the gRPC service before confirming the order.

## Project structure

```text
grpc-shop-demo/
|-- app.py
|-- client_order.py
|-- inventory_server.py
|-- inventory_pb2.py
|-- inventory_pb2_grpc.py
|-- proto/
|   `-- inventory.proto
|-- templates/
|   `-- index.html
`-- requirements.txt
```

## Run the project

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Generate the gRPC Python files:

```bash
python -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/inventory.proto
```

4. Start the gRPC inventory server:

```bash
python inventory_server.py
```

5. Open a new terminal, then start the Flask app:

```bash
python app.py
```

6. Open `http://127.0.0.1:5000` and place an order.

## What to observe for your experiment

- The Flask app sends a request to the gRPC inventory service.
- `CheckStock()` runs first.
- If stock is available, `ReserveStock()` runs next.
- The browser shows the response returned by the gRPC server.
