import json
import sys

import grpc
import redis

import inventory_pb2
import inventory_pb2_grpc

REDIS_HOST = "localhost"
REDIS_PORT = 6379
CACHE_TTL_SECONDS = 10
BACKEND_SERVICE_ADDR = "localhost:50051"
STALE_KEY_PREFIX = "stale:item:"
FRESH_KEY_PREFIX = "fresh:item:"


def get_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        socket_connect_timeout=1,
        socket_timeout=1,
    )


def fetch_from_backend(item_id):
    with grpc.insecure_channel(BACKEND_SERVICE_ADDR) as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)
        response = stub.GetItem(
            inventory_pb2.ItemRequest(item_id=item_id, lamport_timestamp=0),
            timeout=2,
        )
        return {"item_id": response.item_id, "data": response.data}


def get_item(item_id):
    fresh_key = f"{FRESH_KEY_PREFIX}{item_id}"
    stale_key = f"{STALE_KEY_PREFIX}{item_id}"

    try:
        r = get_redis()
        cached = r.get(fresh_key)
        if cached:
            print(f"[Gateway] CACHE HIT for item {item_id}")
            return json.loads(cached), "cache-hit"
        print(f"[Gateway] CACHE MISS for item {item_id} -- querying backend")
    except redis.exceptions.RedisError as e:
        print(f"[Gateway] REDIS UNAVAILABLE ({e}) -- falling back to backend directly")
        r = None

    try:
        data = fetch_from_backend(item_id)
        print(f"[Gateway] Fetched item {item_id} from BACKEND")
        if r is not None:
            try:
                r.set(fresh_key, json.dumps(data), ex=CACHE_TTL_SECONDS)
                r.set(stale_key, json.dumps(data))
            except redis.exceptions.RedisError:
                print("[Gateway] Redis write failed, continuing without caching this result")
        return data, "backend"
    except grpc.RpcError as e:
        print(f"[Gateway] BACKEND UNAVAILABLE ({e.code()}) -- checking for stale cache")
        if r is not None:
            try:
                stale = r.get(stale_key)
                if stale:
                    print(f"[Gateway] Serving STALE cached copy for item {item_id}")
                    return json.loads(stale), "stale-fallback"
            except redis.exceptions.RedisError:
                pass
        raise RuntimeError(f"Item {item_id} unavailable: both backend and cache failed")


if __name__ == "__main__":
    item_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    data, source = get_item(item_id)
    print(f"\nResult (source={source}): {data}")
