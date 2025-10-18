from datetime import datetime, timedelta, timezone
import time
import json
import random
import uuid

products = [
    {"id": "p1", "name": "Laptop", "price": 1200},
    {"id": "p2", "name": "Phone", "price": 800},
    {"id": "p3", "name": "Tablet", "price": 600},
    {"id": "p4", "name": "Monitor", "price": 300},
    {"id": "p5", "name": "Keyboard", "price": 100}
]

user_ids = [f'u{i}' for i in range(1, 11)]
locations = ["New York", "San Francisco", "Chicago", "Austin", "Boston"]

def emit_orders():
    while True:
        product = random.choice(products)
        order = {
            "order_id": str(uuid.uuid4()),
            "user_id": random.choice(user_ids),
            "product_id": product["id"],
            "product_name": product["name"],
            "price": product["price"],
            "quantity": random.choice([1, 1, 2, 3]),  # skewed toward 1
            "location": random.choice(locations),
            "timestamp": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        }
        with open("stream_buffer.json", "a") as f:
            f.write(json.dumps(order) + "\n")
        time.sleep(1)

if __name__ == "__main__":
    emit_orders()
