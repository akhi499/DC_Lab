import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

import service_pb2
import service_pb2_grpc


TARGET_ADDR = os.environ.get("TARGET_ADDR", "localhost:60201")
NUM_REQUESTS = int(os.environ.get("NUM_REQUESTS", "20"))
REQUEST_TIMEOUT = 15


def run_request(request_id, channel):
    stub = service_pb2_grpc.WorkerServiceStub(channel)
    try:
        reply = stub.HandleRequest(
            service_pb2.WorkRequest(request_id=request_id),
            timeout=REQUEST_TIMEOUT,
        )
        print(f"[LOAD] Request {request_id} succeeded -> {reply.handled_by}", flush=True)
        return True, reply.handled_by
    except Exception as exc:  # pragma: no cover - debugging output for lab use
        print(f"[LOAD] Request {request_id} failed -> {exc}", flush=True)
        return False, str(exc)


def main():
    print(f"[LOAD] Starting {NUM_REQUESTS} concurrent requests against {TARGET_ADDR}", flush=True)
    start = time.perf_counter()
    successes = 0
    failures = 0
    workers = set()

    options = [("grpc.lb_policy_name", "round_robin")]
    with grpc.insecure_channel(TARGET_ADDR, options=options) as channel:
        with ThreadPoolExecutor(max_workers=min(NUM_REQUESTS, 32)) as executor:
            future_map = {
                executor.submit(run_request, request_id, channel): request_id
                for request_id in range(1, NUM_REQUESTS + 1)
            }

            for future in as_completed(future_map):
                success, worker = future.result()
                if success:
                    successes += 1
                    workers.add(worker)
                else:
                    failures += 1

    total_duration = time.perf_counter() - start
    print(f"[LOAD] Successful requests: {successes}/{NUM_REQUESTS}", flush=True)
    print(f"[LOAD] Failed requests: {failures}/{NUM_REQUESTS}", flush=True)
    print(f"[LOAD] Workers reached: {sorted(workers) if workers else 'none'}", flush=True)
    print(f"[LOAD] Total duration: {total_duration:.2f} seconds", flush=True)


if __name__ == "__main__":
    main()
