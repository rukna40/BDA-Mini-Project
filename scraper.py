import requests
from bs4 import BeautifulSoup
import json
import time
from kafka import KafkaProducer
import random

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic = 'market_data'

def scrape_data():
    """Scrapes market data from the books.toscrape.com website."""
    url = 'https://books.toscrape.com/catalogue/page-1.html'  # URL for the first page of books
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all book containers on the page
        for product in soup.select('.product_pod'):
            try:
                title = product.h3.a['title']
                price = float(product.select_one('.price_color').text.replace('£', ''))
                availability = product.select_one('.availability').text.strip()
                adFlag = random.randint(0, 1)  # Random ad flag
                lineID = random.randint(1, 10000)  # Random line ID
                pid = random.randint(1000, 20000)  # Random product ID
                competitorPrice = round(price * random.uniform(0.8, 1.2), 2)  # Random competitor price
                click = random.randint(0, 10)  # Random clicks
                basket = random.randint(0, 5)  # Random basket count
                order = random.randint(0, 3)  # Random order count
                revenue = round(price * order, 2)  # Revenue based on price and order

                data = {
                    "lineID": lineID,
                    "day": random.randint(1, 7),  # Random day of the week
                    "pid": pid,
                    "adFlag": adFlag,
                    "availability": 1 if "in stock" in availability else 0,
                    "competitorPrice": competitorPrice,
                    "click": click,
                    "basket": basket,
                    "order": order,
                    "price": price,
                    "revenue": revenue
                }
                
                producer.send(topic, value=data)
                print(f"Sent: {data}")
                time.sleep(1)  # Pause between sends

            except Exception as e:
                print(f"Error parsing product: {e}")
    else:
        print("Failed to fetch data from the website")

if __name__ == "__main__":
    try:
        while True:
            scrape_data()
            time.sleep(60)  # Scrape every minute
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()
