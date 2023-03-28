import sys
from confluent_kafka import Producer
from config import PATH, SERVER
import pandas as pd

producer_config = {
    "bootstrap.servers": SERVER
}


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for Message: {msg.key()} : {err}")

        return

    print((
        f'Message: {msg.key()} successfully produced '
        f'to Topic: {msg.topic()} '
        f'Partition: [{msg.partition()}] '
        f'at offset {msg.offset()}'
        ))


if __name__ == "__main__":
    rows_count = 500

    # TODO: Consider improving user input validation (look into argparse)
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        rows_count = int(sys.argv[1])

    print("Starting Kafka Producer.")

    producer = Producer(producer_config)

    with pd.read_csv(PATH, chunksize=1, nrows=rows_count) as reader:
        for chunk in reader:
            try:
                producer.produce(
                    "records",
                    value=chunk.to_json(orient = "values").encode("utf-8"),
                    key=str(chunk.iloc[0]["ID"]),
                    on_delivery=delivery_report
                )
                producer.flush()
            except Exception as e:
                print(f"Exception happend: {e}")

    print("Stopping Kafka Producer.")
