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

# Experiment 11: Distributed File System Storage

This repository now includes the chunked, replicated DFS lab described in the walkthrough.

- Protocol: [dfs_lab/proto/dfs.proto](dfs_lab/proto/dfs.proto)
- Metadata server: [dfs_lab/dfs_metadata.py](dfs_lab/dfs_metadata.py)
- Storage nodes: [dfs_lab/dfs_storage.py](dfs_lab/dfs_storage.py)
- Client: [dfs_lab/dfs_client.py](dfs_lab/dfs_client.py)
- Repair script: [dfs_lab/dfs_repair.py](dfs_lab/dfs_repair.py)
- Full step-by-step guide: [dfs_lab/docs/dfs_experiment_11.md](dfs_lab/docs/dfs_experiment_11.md)
- Lab folder: [dfs_lab/README.md](dfs_lab/README.md)

The guide includes screenshot references for the cluster architecture, post-upload status, and repair/failover flow.

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

---

# Experiment 10 — Kubernetes Deployment and Load Simulation

## Objective

This experiment packages the existing gRPC-based distributed system into a single local Docker image and deploys the Raft cluster plus worker replicas into Docker Desktop Kubernetes. The goal is to prove the lab can run in Kubernetes with Service DNS names, leader election/failover, and real distributed traffic across worker replicas.

## Architecture

- Raft node A: `raft-a` Service, port `60401`
- Raft node B: `raft-b` Service, port `60402`
- Raft node C: `raft-c` Service, port `60403`
- Worker service: headless `worker` Service, port `60201`, 3 replicas
- Load-test Job: runs `python load_test.py` with `TARGET_ADDR=dns:///worker:60201`

The worker replicas are stateless and each logs when it handles a request. The gRPC channel uses the Kubernetes Service DNS endpoint plus `grpc.lb_policy_name=round_robin` so traffic is spread across multiple pods instead of staying pinned to a single long-lived connection.

## Files added or changed

- `Dockerfile`
- `load_test.py`
- `worker_server.py`
- `raft_node.py`
- `load_balancer.py`
- `k8s/app.yaml`
- `k8s/load-test-job.yaml`
- `README.md`

## Prerequisites

- Docker Desktop with Kubernetes enabled
- `kubectl` available in PATH
- Python environment with dependencies from `requirements.txt`
- Local Docker image build permission

### Linux/macOS

```bash
python -m pip install -r requirements.txt
```

### Windows PowerShell

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Docker Desktop Kubernetes setup

1. Enable Kubernetes in Docker Desktop.
2. Ensure the local cluster is ready:

```bash
kubectl version --client
kubectl get nodes
```

```powershell
kubectl version --client
kubectl get nodes
```

3. Build the shared image:

```bash
docker build -t myapp-backend:latest .
```

```powershell
docker build -t myapp-backend:latest .
```

> The image must be built locally and the manifests must use `imagePullPolicy: Never`.

## Kubernetes deployment

Deploy the Raft cluster and worker service:

```bash
kubectl apply -f k8s/app.yaml
```

```powershell
kubectl apply -f k8s/app.yaml
```

Check the pods:

```bash
kubectl get pods -o wide
kubectl get svc
kubectl get deployments
```

```powershell
kubectl get pods -o wide
kubectl get svc
kubectl get deployments
```

Expected state:

- 3 Raft pods (A, B, C)
- 3 worker pods
- `raft-a`, `raft-b`, `raft-c` Services
- headless `worker` Service

## Log verification commands

Inspect the Raft logs:

```bash
kubectl logs deployment/raft-a
kubectl logs deployment/raft-b
kubectl logs deployment/raft-c
```

```powershell
kubectl logs deployment/raft-a
kubectl logs deployment/raft-b
kubectl logs deployment/raft-c
```

Inspect the worker logs:

```bash
kubectl logs deployment/worker
```

```powershell
kubectl logs deployment/worker
```

Look for:

- `Starting election`
- `Vote granted to ...`
- `ELECTED LEADER`
- `Received heartbeat from ...`
- `Handling request ...`

## How to identify the leader

The lab is designed so that any Raft node may become leader. Check each deployment log and look for the leader announcement:

```bash
kubectl logs deployment/raft-a | tail -n 40
kubectl logs deployment/raft-b | tail -n 40
kubectl logs deployment/raft-c | tail -n 40
```

```powershell
kubectl logs deployment/raft-a | Select-Object -Last 40
kubectl logs deployment/raft-b | Select-Object -Last 40
kubectl logs deployment/raft-c | Select-Object -Last 40
```

Find the node log containing:

```text
ELECTED LEADER
```

That node is the current leader.

## How to perform forced failover

Do not delete the leader pod directly if you want a clear failover demonstration. Kubernetes may recreate it too quickly and hide the election. Instead, scale the current leader Deployment to zero:

```bash
kubectl scale deployment raft-a --replicas=0
```

```powershell
kubectl scale deployment raft-a --replicas=0
```

If the leader is `raft-b` or `raft-c`, substitute that Deployment name.

Then wait and inspect the surviving logs:

```bash
kubectl get pods
kubectl logs deployment/raft-b
kubectl logs deployment/raft-c
```

```powershell
kubectl get pods
kubectl logs deployment/raft-b
kubectl logs deployment/raft-c
```

You should observe:

- the old leader disappears
- a higher-term election starts
- one surviving node becomes the new leader
- the remaining follower starts receiving the new leader heartbeat

## How to restore the node

After verifying the failover, restore the original leader deployment:

```bash
kubectl scale deployment raft-a --replicas=1
```

```powershell
kubectl scale deployment raft-a --replicas=1
```

The returning pod should rejoin the cluster and not immediately trigger a new election.

## Run the load-test Job

Apply the Job:

```bash
kubectl apply -f k8s/load-test-job.yaml
kubectl wait --for=condition=complete job/load-test --timeout=90s
kubectl logs job/load-test
```

```powershell
kubectl apply -f k8s/load-test-job.yaml
kubectl wait --for=condition=complete job/load-test --timeout=90s
kubectl logs job/load-test
```

The Job runs inside the cluster and targets:

```text
TARGET_ADDR=dns:///worker:60201
```

The job uses concurrent requests and prints each successful response with the actual worker name.

## How to rerun the Job

Delete the previous Job first:

```bash
kubectl delete job load-test
```

```powershell
kubectl delete job load-test
```

If `load_test.py` changed, rebuild the container image before rerunning:

```bash
docker build -t myapp-backend:latest .
kubectl apply -f k8s/load-test-job.yaml
```

```powershell
docker build -t myapp-backend:latest .
kubectl apply -f k8s/load-test-job.yaml
```

## Verify request distribution

After the load test, count the requests seen by each pod:

```bash
kubectl get pods -l app=worker -o name
```

```powershell
kubectl get pods -l app=worker -o name
```

For each pod:

```bash
kubectl logs <pod-name> | grep -o "Handling request" | wc -l
```

```powershell
kubectl logs <pod-name> | Select-String -Pattern "Handling request" | Measure-Object
```

The total across all replicas should roughly match the number of successful requests. It does not need to be perfectly equal. A healthy cluster should show multiple worker pods receiving traffic.

This is the reason the experiment uses a headless Service and gRPC round-robin instead of a standard ClusterIP Service. A normal Service can keep one HTTP/2/TCP connection alive, which can skew traffic to a single replica even when all pods appear healthy.

## Cleanup commands

```bash
kubectl delete job load-test
kubectl delete -f k8s/app.yaml
```

```powershell
kubectl delete job load-test
kubectl delete -f k8s/app.yaml
```

If the worker Service was changed during development from a normal Service to a headless Service, recreate it as part of the deployment:

```bash
kubectl delete service worker
kubectl apply -f k8s/app.yaml
```

```powershell
kubectl delete service worker
kubectl apply -f k8s/app.yaml
```

## Troubleshooting

### 1. `ErrImagePull` / `ImagePullBackOff`

Cause: the manifest uses the local `myapp-backend:latest` image but the container is requesting a pull instead of using the local image.

Fix:

```bash
docker build -t myapp-backend:latest .
```

```powershell
docker build -t myapp-backend:latest .
```

Ensure all manifests include `imagePullPolicy: Never`.

### 2. Pods are running but there is no gRPC communication

Cause: a service is bound to `localhost` instead of all interfaces.

Fix: bind to `0.0.0.0` or `[::]`.

### 3. Pods connect to themselves instead of their peers

Cause: the code uses `localhost` for peer addresses.

Fix: use Kubernetes Service DNS names like `raft-a:60401` and `raft-b:60402`.

### 4. `kubectl logs` shows nothing for a while

Cause: Python output is buffered.

Fix: configure `PYTHONUNBUFFERED=1` in the Dockerfile and environment.

### 5. Deleting the leader pod does not show failover

Cause: Kubernetes recreates the pod too quickly.

Fix: scale the leader Deployment to zero instead of deleting the pod, then wait for the timeout and election.

### 6. Job already exists

Fix:

```bash
kubectl delete job load-test
```

```powershell
kubectl delete job load-test
```

### 7. Load test runs old code

Cause: the Docker image was not rebuilt after changes.

Fix:

```bash
docker build -t myapp-backend:latest .
```

```powershell
docker build -t myapp-backend:latest .
```

### 8. Only one worker receives most gRPC calls

Cause: a normal Kubernetes Service keeps a long-lived gRPC connection.

Fix: use a headless Service with `dns:///` and `grpc.lb_policy_name=round_robin`.

### 9. Pods are Running but the experiment is not actually working

Fix: inspect the logs and verify the real Raft election and worker request handling output. Running alone is not proof.

## Final verification checklist

Before finishing, confirm all of the following are true:

- Dockerfile exists
- image builds successfully
- `RUN_ENV=kubernetes` is assigned to the container runtime
- `PYTHONUNBUFFERED=1` is enabled
- Raft and worker gRPC services bind to all interfaces
- Raft nodes communicate using Service DNS names
- worker Deployment has 3 replicas
- worker Service is headless
- `load_test.py` exists and uses concurrency
- Job runs with `TARGET_ADDR=dns:///worker:60201`
- leader election and failover can be observed in logs
- multiple workers process real requests

---

The repo is now ready for a Docker Desktop Kubernetes demonstration using the local image `myapp-backend:latest`.
