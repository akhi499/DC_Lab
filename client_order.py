import grpc

import inventory_pb2
import inventory_pb2_grpc
from lamport_clock import LamportClock


# Lamport logical clock for the Order Service
clock = LamportClock()


def place_order(
    product_id: str,
    quantity: int,
    host: str = "localhost:50051"
) -> dict:

    with grpc.insecure_channel(host) as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)

        # -----------------------------------------
        # Event 1: Client sends CheckStock request
        # -----------------------------------------
        request_timestamp = clock.tick()

        print(
            f"[CLIENT] Sending CheckStock | "
            f"Lamport timestamp={request_timestamp}"
        )

        stock_response = stub.CheckStock(
            inventory_pb2.ProductRequest(
                product_id=product_id,
                quantity=quantity,
                lamport_timestamp=request_timestamp,
            )
        )

        # -----------------------------------------
        # Event 2: Client receives CheckStock response
        # -----------------------------------------
        updated_time = clock.update(
            stock_response.lamport_timestamp
        )

        print(
            f"[CLIENT] Received CheckStock response | "
            f"Server timestamp={stock_response.lamport_timestamp} | "
            f"Client clock={updated_time}"
        )

        if not stock_response.available:
            return {
                "success": False,
                "stage": "check_stock",
                "message": stock_response.message,
                "current_stock": stock_response.current_stock,
                "lamport_timestamp": clock.get_time(),
            }

        # -----------------------------------------
        # Event 3: Client sends ReserveStock request
        # -----------------------------------------
        request_timestamp = clock.tick()

        print(
            f"[CLIENT] Sending ReserveStock | "
            f"Lamport timestamp={request_timestamp}"
        )

        reserve_response = stub.ReserveStock(
            inventory_pb2.ProductRequest(
                product_id=product_id,
                quantity=quantity,
                lamport_timestamp=request_timestamp,
            )
        )

        # -----------------------------------------
        # Event 4: Client receives ReserveStock response
        # -----------------------------------------
        updated_time = clock.update(
            reserve_response.lamport_timestamp
        )

        print(
            f"[CLIENT] Received ReserveStock response | "
            f"Server timestamp={reserve_response.lamport_timestamp} | "
            f"Client clock={updated_time}"
        )

        return {
            "success": reserve_response.success,
            "stage": "reserve_stock",
            "message": reserve_response.message,
            "remaining_stock": reserve_response.remaining_stock,
            "lamport_timestamp": clock.get_time(),
        }


if __name__ == "__main__":
    result = place_order("book", 2)
    print("\n[CLIENT] Final result:")
    print(result)