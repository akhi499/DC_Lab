# Mini gRPC Flask Project

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
