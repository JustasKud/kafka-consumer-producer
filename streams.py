from confluent_kafka import Consumer, Producer
from config import SERVER
import atexit

consumer_config = {
    "bootstrap.servers": SERVER,
    "group.id": "streams-consumer",
    "auto.offset.reset": "earliest"
}


producer_config = {
    "bootstrap.servers": SERVER,
}


existing_messages = {}


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for Message: {msg.key()} : {err}")
        return
    # print((
    #     f'Message: {msg.key()} successfully produced '
    #     f'to Topic: {msg.topic()} '
    #     f'Partition: [{msg.partition()}] '
    #     f'at offset {msg.offset()}'
    #     ))


def exit_handler(consumer):
    consumer.close()
    print('Kafka Consumer got closed!')


if __name__ == "__main__":
    print("Starting Kafka Streams.")

    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    atexit.register(exit_handler, consumer)
    consumer.subscribe(["records"])

    while True:
        msg = consumer.poll(1.0)

        if msg == None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            break

        if msg.key().decode("utf-8") in existing_messages:
            print(f"The current count of unique IDs is {len(existing_messages)}.")
            continue

        print(f"The current count of unique IDs is {len(existing_messages)}.")

        existing_messages[msg.key().decode("utf-8")] = 0
        producer.produce("unique_records", value=msg.value(), key=msg.key(), on_delivery=delivery_report)
        producer.flush()

    print("Stopping Kafka Streams.")
