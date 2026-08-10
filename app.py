from flask import Flask, jsonify, render_template, request

from client_order import place_order

app = Flask(__name__)


@app.get("/")
def index():
    sample_products = [
        {"id": "book", "label": "Book", "stock_hint": 10},
        {"id": "pen", "label": "Pen", "stock_hint": 25},
        {"id": "notebook", "label": "Notebook", "stock_hint": 15},
    ]
    return render_template("index.html", products=sample_products)


@app.post("/order")
def order():
    product_id = request.form.get("product_id", "").strip()
    quantity = int(request.form.get("quantity", "1"))
    result = place_order(product_id, quantity)
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
