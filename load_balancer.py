import threading
import time

import grpc

import service_pb2
import service_pb2_grpc


BACKENDS = ["localhost:60201", "localhost:60202", "localhost:60203"]
NUM_REQUESTS = 9

active = [0] * len(BACKENDS)
lock = threading.Lock()


def pick_least_connections():
    """Core strategy: pick the backend with the fewest active
    connections right now, and increment its count immediately so
    the very next request sees the updated picture."""
    with lock:
        idx = active.index(min(active))
        active[idx] += 1

        print(f"[LB] Routing -> {BACKENDS[idx]} (active connections now: {active})")

        return idx


def release(idx):
    with lock:
        active[idx] -= 1
        print(f"[LB] Released {BACKENDS[idx]} (active connections now: {active})")


def handle_request(req_id):
    idx = pick_least_connections()
    addr = BACKENDS[idx]

    try:
        with grpc.insecure_channel(addr) as channel:
            stub = service_pb2_grpc.WorkerServiceStub(channel)
            reply = stub.HandleRequest(service_pb2.WorkRequest(request_id=req_id))
            print(f"[LB] Request {req_id} -> handled by {reply.handled_by}")
    finally:
        release(idx)


def main():
    print(f"[LB] Load Balancer starting. Backends: {BACKENDS}")

    threads = []

    for req_id in range(1, NUM_REQUESTS + 1):
        t = threading.Thread(target=handle_request, args=(req_id,))
        threads.append(t)
        t.start()

        time.sleep(0.15)

    for t in threads:
        t.join()

    print("\n[LB] All requests processed.")
    print(f"[LB] Final active connection counts (should all be 0): {active}")


if __name__ == "__main__":
    main()
