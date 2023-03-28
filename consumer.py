from confluent_kafka import Consumer
from config import SERVER
import atexit

consumer_config = {
    "bootstrap.servers": SERVER,
    "group.id": "final-consumer",
    "auto.offset.reset": "earliest"
}


def exit_handler(consumer):
    consumer.close()
    print('Kafka Consumer stopped listening and got closed!')


if __name__ == "__main__":
    print("Starting Kafka Consumer.")

    consumer = Consumer(consumer_config)
    consumer.subscribe(["unique_records"])

    atexit.register(exit_handler, consumer)

    while True:
        msg = consumer.poll(1.0)

        if msg == None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            break

        print(msg.key().decode("utf-8"))

    print("Stopping Kafka Consumer.")



