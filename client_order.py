import grpc

import inventory_pb2
import inventory_pb2_grpc


def place_order(product_id: str, quantity: int, host: str = "localhost:50051") -> dict:
    with grpc.insecure_channel(host) as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)

        stock_response = stub.CheckStock(
            inventory_pb2.ProductRequest(product_id=product_id, quantity=quantity)
        )

        if not stock_response.available:
            return {
                "success": False,
                "stage": "check_stock",
                "message": stock_response.message,
                "current_stock": stock_response.current_stock,
            }

        reserve_response = stub.ReserveStock(
            inventory_pb2.ProductRequest(product_id=product_id, quantity=quantity)
        )
        return {
            "success": reserve_response.success,
            "stage": "reserve_stock",
            "message": reserve_response.message,
            "remaining_stock": reserve_response.remaining_stock,
        }
        
if __name__ == "__main__":
    result = place_order("book", 2)
    print(result)
