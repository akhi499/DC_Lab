import os
import sys
import time
import random

from concurrent import futures

import grpc

import service_pb2
import service_pb2_grpc


DEFAULT_PORT = int(os.environ.get("WORKER_SERVICE_PORT", "60201"))
RUN_ENV = os.environ.get("RUN_ENV", "local").lower()


def current_bind_host():
    return "0.0.0.0" if RUN_ENV == "kubernetes" else "localhost"


class WorkerServicer(service_pb2_grpc.WorkerServiceServicer):
    def __init__(self, name):
        self.name = name

    def HandleRequest(self, request, context):
        work_time = random.uniform(0.5, 2.5)
        print(
            f"[{self.name}] Handling request {request.request_id} "
            f"(will take {work_time:.1f}s)",
            flush=True,
        )
        time.sleep(work_time)
        print(f"[{self.name}] Finished request {request.request_id}", flush=True)

        return service_pb2.WorkReply(
            request_id=request.request_id,
            handled_by=self.name,
            status="done",
        )


def serve(port=None):
    port = int(port if port is not None else DEFAULT_PORT)
    name = f"Worker-{port}-{os.environ.get('HOSTNAME', 'unknown')}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    service_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerServicer(name), server)
    listen_address = f"{current_bind_host()}:{port}"
    server.add_insecure_port(listen_address)
    server.start()

    print(f"{name} listening on {listen_address}", flush=True)

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    serve(port)
