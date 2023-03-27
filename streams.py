# cd "C:\Users\37061\OneDrive\Stalinis kompiuteris\Code\13_job_offers\03_pubsub" && source ./venv/Scripts/activate && clear
# python streams.py

import atexit
from confluent_kafka import Consumer, Producer
from typing import Dict

consumer_config: Dict[str, str] = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "consumer-2"
}

producer_config: Dict[str, str] = {
    "bootstrap.servers": "localhost:29092",
}

unique_IDs = {}

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), err))
        return
    # print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
    #     msg.key(), msg.topic(), msg.partition(), msg.offset()))

def exit_handler(consumer):
    consumer.close()
    print('My application is ending!')

if __name__ == "__main__":
    print("Starting Kafka Streams\n")
    consumer = Consumer(consumer_config)
    atexit.register(exit_handler, consumer)
    consumer.subscribe(["records"])
    producer = Producer(producer_config)
    producer.poll(0)
    while True:
        msg = consumer.poll(0)
        if msg == None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            break
        if msg.key().decode("utf-8") in unique_IDs:
            # print(msg.key().decode("utf-8"))
            # print(f"The above key already exists in topic \"filtered\" and has not been pushed. The current count of unique IDs is {len(unique_IDs)}.")
            print(f"The current count of unique IDs is {len(unique_IDs)}.")
            continue
        # print(msg.key().decode("utf-8"))
        print(f"The current count of unique IDs is {len(unique_IDs)}.")
        unique_IDs[msg.key().decode("utf-8")] = None
        producer.produce("filtered", value=msg.value(), key=msg.key(), on_delivery=delivery_report)
        producer.flush()
        # print(msg.value().decode("utf-8"))
