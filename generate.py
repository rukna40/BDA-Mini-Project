from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'market_data'

while True:
    data = {
        "lineID": random.randint(1, 10000),
        "day": random.randint(1, 7),
        "pid": random.randint(1000, 20000),
        "adFlag": random.randint(0, 1),
        "availability": random.randint(1, 10),
        "competitorPrice": round(random.uniform(1.0, 20.0), 2),
        "click": random.randint(0, 10),
        "basket": random.randint(0, 5),
        "order": random.randint(0, 3),
        "price": round(random.uniform(1.0, 20.0), 2),
        "revenue": round(random.uniform(0.0, 50.0), 2)
    }
    
    producer.send(topic, value=data)
    print(f"Sent: {data}")
    time.sleep(1)