from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sales_data =[
    {"sale_id": 1, "product": "Laptop", "quantity": 2, "price": 1000, "date": "2024-03-01"},
    {"sale_id": 2, "product": "Phone", "quantity": 5, "price": 500, "date": "2024-03-02"},
    {"sale_id": 3, "product": "Tablet", "quantity": 3, "price": 700, "date": "2024-03-03"},
    {"sale_id": 4, "product": "Laptop", "quantity": 1, "price": 1200, "date": "2024-03-04"},
    {"sale_id": 5, "product": "Phone", "quantity": 2, "price": 600, "date": "2024-03-05"}

]

for sale in sales_data:
    producer.send('sales_topic', value=sale)
    print(f"Sent: {sale}")
    time.sleep(2)