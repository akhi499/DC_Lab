from concurrent import futures
import threading

import grpc

import inventory_pb2
import inventory_pb2_grpc
from lamport_clock import LamportClock


class InventoryService(inventory_pb2_grpc.InventoryServiceServicer):

    def __init__(self):
        self._lock = threading.Lock()
        self._stock = {
            "book": 10,
            "pen": 25,
            "notebook": 15,
        }

        # Lamport logical clock for the Inventory Service
        self._clock = LamportClock()

    def CheckStock(self, request, context):
        # Receive event: update Lamport clock using client's timestamp
        current_time = self._clock.update(request.lamport_timestamp)

        print(
            f"[SERVER] CheckStock received | "
            f"Client timestamp={request.lamport_timestamp} | "
            f"Server clock={current_time}"
        )

        current_stock = self._stock.get(request.product_id, 0)
        available = current_stock >= request.quantity

        message = (
            f"{request.product_id} is available."
            if available
            else f"Only {current_stock} item(s) left for {request.product_id}."
        )

        # Increment for the response event
        response_time = self._clock.tick()

        print(
            f"[SERVER] CheckStock response | "
            f"Lamport timestamp={response_time}"
        )

        return inventory_pb2.StockResponse(
            available=available,
            current_stock=current_stock,
            message=message,
            lamport_timestamp=response_time,
        )

    def ReserveStock(self, request, context):
        # Receive event: update Lamport clock using client's timestamp
        current_time = self._clock.update(request.lamport_timestamp)

        print(
            f"[SERVER] ReserveStock received | "
            f"Client timestamp={request.lamport_timestamp} | "
            f"Server clock={current_time}"
        )

        with self._lock:
            current_stock = self._stock.get(request.product_id, 0)

            if current_stock < request.quantity:
                response_time = self._clock.tick()

                print(
                    f"[SERVER] Reservation failed | "
                    f"Lamport timestamp={response_time}"
                )

                return inventory_pb2.ReserveResponse(
                    success=False,
                    remaining_stock=current_stock,
                    message=(
                        f"Reservation failed. "
                        f"Only {current_stock} item(s) available."
                    ),
                    lamport_timestamp=response_time,
                )

            self._stock[request.product_id] = (
                current_stock - request.quantity
            )

            remaining_stock = self._stock[request.product_id]

            # Increment for the response event
            response_time = self._clock.tick()

            print(
                f"[SERVER] ReserveStock response | "
                f"Lamport timestamp={response_time}"
            )

            return inventory_pb2.ReserveResponse(
                success=True,
                remaining_stock=remaining_stock,
                message=(
                    f"Reserved {request.quantity} "
                    f"{request.product_id}(s) successfully."
                ),
                lamport_timestamp=response_time,
            )

    def UpdateStock(self, request, context):
        # Receive event: update Lamport clock using client's timestamp
        current_time = self._clock.update(request.lamport_timestamp)

        print(
            f"[SERVER] UpdateStock received | "
            f"Client timestamp={request.lamport_timestamp} | "
            f"Server clock={current_time}"
        )

        with self._lock:
            current_stock = self._stock.get(request.product_id, 0)
            updated_stock = max(
                0,
                current_stock + request.quantity_change
            )

            self._stock[request.product_id] = updated_stock

            # Increment for the response event
            response_time = self._clock.tick()

            print(
                f"[SERVER] UpdateStock response | "
                f"Lamport timestamp={response_time}"
            )

            return inventory_pb2.UpdateStockResponse(
                success=True,
                updated_stock=updated_stock,
                message=f"Stock updated for {request.product_id}.",
                lamport_timestamp=response_time,
            )


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(
        InventoryService(),
        server
    )

    server.add_insecure_port("[::]:50051")
    server.start()

    print("Inventory gRPC server running on port 50051")
    print("Lamport logical clock enabled.")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()