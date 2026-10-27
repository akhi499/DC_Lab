# Experiment 10: Kubernetes Deployment and Load Simulation

**Run date:** 28 October 2026  
**Project:** DC Lab  
**Purpose:** Deploy the Raft coordination service and replicated gRPC worker service to Kubernetes, verify communication and failover, and send concurrent load to the workers.

## Executive summary

The experiment completed successfully on Docker Desktop Kubernetes. The cluster ran three Raft pods and three worker pods. Raft elected a leader, recovered after the leader Deployment was scaled to zero, and the returning node rejoined as a follower. The load-test Job completed all 20 requests successfully and reached all three worker replicas.

The request distribution was uneven: the workers handled 14, 4, and 2 requests. This confirms that traffic reached every replica, while also showing that this run did not distribute the requests evenly.

## Environment and deployment

- Kubernetes cluster: Docker Desktop, `kind` mode
- Kubernetes node: `desktop-control-plane`, `Ready`, Kubernetes v1.36.1
- Container runtime: containerd 2.3.1
- Application image: `myapp-backend:latest`
- Raft nodes: `raft-a` on port 60401, `raft-b` on port 60402, and `raft-c` on port 60403
- Worker Service: headless `worker` Service on port 60201 with three replicas
- Load test: Kubernetes Job named `load-test`, targeting `dns:///worker:60201`

The local image was built and made available to the Docker Desktop kind node. The Kubernetes manifests use `imagePullPolicy: Never`.

## Pod status

All application pods were `Running` with zero restarts. The load-test pod completed successfully.

| Component | Pod | Status | Restarts |
|---|---|---|---:|
| Raft A | `raft-a-557bfd79c7-xxjqq` | Running | 0 |
| Raft B | `raft-b-69778d687-gn2qn` | Running | 0 |
| Raft C | `raft-c-85dc84dc7c-66cm2` | Running | 0 |
| Worker | `worker-69688748b6-5bg6l` | Running | 0 |
| Worker | `worker-69688748b6-6j8wk` | Running | 0 |
| Worker | `worker-69688748b6-fr5dn` | Running | 0 |
| Load test | `load-test-ffhlm` | Completed | 0 |

## Raft coordination and failover

In the final verified run, node B became leader at term 2. I scaled its Deployment to zero. Node C then won an election at term 3 with two of three votes, and node A followed C. When B was restored, it joined as a follower and received C's heartbeats.

Observed log excerpts:

```text
[B term=2 LEADER] ELECTED LEADER (2/3 votes)

[C term=3 CANDIDATE] Starting election
[C term=3 CANDIDATE] Vote received from A (2/3)
[C term=3 LEADER] ELECTED LEADER (2/3 votes)
[C term=3 LEADER] Peer B unavailable for heartbeat: UNAVAILABLE
[C term=3 LEADER] Heartbeat sent to A

[B term=3 FOLLOWER] Higher term detected; stepped down for term 3
[B term=3 FOLLOWER] Received heartbeat from C
```

The `UNAVAILABLE` message for B is expected while its pod is scaled down. C continued heartbeating A and became the leader for the surviving pair.

## Concurrent load test

The Job sent 20 concurrent gRPC requests to the headless worker Service. All requests succeeded in 4.38 seconds.

```text
[LOAD] Starting 20 concurrent requests against dns:///worker:60201
[LOAD] Successful requests: 20/20
[LOAD] Failed requests: 0/20
[LOAD] Workers reached: ['Worker-60201-worker-69688748b6-5bg6l', 'Worker-60201-worker-69688748b6-6j8wk', 'Worker-60201-worker-69688748b6-fr5dn']
[LOAD] Total duration: 4.38 seconds
```

Request counts from the worker logs:

| Worker pod | Requests handled |
|---|---:|
| `worker-69688748b6-5bg6l` | 14 |
| `worker-69688748b6-6j8wk` | 4 |
| `worker-69688748b6-fr5dn` | 2 |
| **Total** | **20** |

Every replica handled at least one request. Distribution was skewed toward the first pod in this run, so the result demonstrates multi-replica traffic but not an even split.

## Commands used to reproduce or collect evidence

Build and deploy:

```powershell
docker build -t myapp-backend:latest .
kubectl apply -f k8s/app.yaml
kubectl get pods -o wide
kubectl get services
```

Inspect Raft logs:

```powershell
kubectl logs deployment/raft-a
kubectl logs deployment/raft-b
kubectl logs deployment/raft-c
```

Force failover and restore the node that was leader in this run:

```powershell
kubectl scale deployment/raft-b --replicas=0
# Wait for the surviving nodes to elect a leader; inspect their logs.
kubectl logs deployment/raft-c
kubectl logs deployment/raft-a
kubectl scale deployment/raft-b --replicas=1
```

Run and inspect the load-test Job:

```powershell
kubectl apply -f k8s/load-test-job.yaml
kubectl wait --for=condition=complete job/load-test --timeout=90s
kubectl logs job/load-test
```

Count requests in each worker's logs:

```powershell
kubectl get pods -l app=worker -o name
kubectl logs <worker-pod-name> | Select-String -Pattern "Handling request" | Measure-Object
```

The Job cannot be applied again under the same name until the completed Job is deleted:

```powershell
kubectl delete job load-test
```

## Implementation note

During execution, the Raft heartbeat loop was corrected so it continues sending heartbeats while a node is leader. Peer gRPC channels are reused and the RPC timeout was increased to accommodate initial channel connection time. These changes are in [`raft_node.py`](../raft_node.py).

## Conclusion

The Kubernetes deployment, Raft election, forced failover, follower rejoin, and concurrent load request all completed. The Job achieved a 100% request success rate and reached all three worker replicas. Worker traffic was spread unevenly (14/4/2), which should be reported as an observed limitation of this particular run.
