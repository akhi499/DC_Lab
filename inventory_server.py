from concurrent import futures
import threading

import grpc

import inventory_pb2
import inventory_pb2_grpc


class InventoryService(inventory_pb2_grpc.InventoryServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._stock = {
            "book": 10,
            "pen": 25,
            "notebook": 15,
        }

    def CheckStock(self, request, context):
        current_stock = self._stock.get(request.product_id, 0)
        available = current_stock >= request.quantity
        message = (
            f"{request.product_id} is available."
            if available
            else f"Only {current_stock} item(s) left for {request.product_id}."
        )
        return inventory_pb2.StockResponse(
            available=available,
            current_stock=current_stock,
            message=message,
        )

    def ReserveStock(self, request, context):
        with self._lock:
            current_stock = self._stock.get(request.product_id, 0)
            if current_stock < request.quantity:
                return inventory_pb2.ReserveResponse(
                    success=False,
                    remaining_stock=current_stock,
                    message=f"Reservation failed. Only {current_stock} item(s) available.",
                )

            self._stock[request.product_id] = current_stock - request.quantity
            remaining_stock = self._stock[request.product_id]
            return inventory_pb2.ReserveResponse(
                success=True,
                remaining_stock=remaining_stock,
                message=f"Reserved {request.quantity} {request.product_id}(s) successfully.",
            )

    def UpdateStock(self, request, context):
        with self._lock:
            current_stock = self._stock.get(request.product_id, 0)
            updated_stock = max(0, current_stock + request.quantity_change)
            self._stock[request.product_id] = updated_stock
            return inventory_pb2.UpdateStockResponse(
                success=True,
                updated_stock=updated_stock,
                message=f"Stock updated for {request.product_id}.",
            )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Inventory gRPC server running on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
