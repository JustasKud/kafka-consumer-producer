# cd "C:\Users\37061\OneDrive\Stalinis kompiuteris\Code\13_job_offers\03_pubsub" && source ./venv/Scripts/activate && clear
# python consumer.py
import atexit

from confluent_kafka import Consumer
from typing import Dict

config: Dict[str, str] = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "consumer-1"
}

def exit_handler(consumer):
    consumer.close()
    print('My application is ending!')

if __name__ == "__main__":
    print("Starting Kafka Consumer\n")
    consumer = Consumer(config)
    atexit.register(exit_handler, consumer)
    # consumer.subscribe(["records"])
    consumer.subscribe(["filtered"])
    while True:
        msg = consumer.poll(0)
        if msg == None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            break
        print(msg.key().decode("utf-8"))
        # print(msg.value().decode("utf-8"))



