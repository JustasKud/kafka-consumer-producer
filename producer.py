# deactivate && source ./venv/Scripts/activate && clear && python consumer.py

import pandas as pd
from confluent_kafka import Producer
from typing import Dict
from config import CHUNK_SIZE, PATH

config: Dict[str, str] = {
    "bootstrap.servers": "localhost:29092"
}

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), err))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


if __name__ == "__main__":
    print("Starting Kafka Producer\n")
    producer = Producer(config)
    producer.poll(0)
    with pd.read_csv(PATH, chunksize=CHUNK_SIZE, nrows=2000) as reader:
        for chunk in reader:
            try:
                producer.produce("records", value=chunk.to_json(orient = "values").encode("utf-8"), key=str(chunk.iloc[0]["ID"]), on_delivery=delivery_report)
                producer.flush()
            except Exception as ex:
                print("Exception happend: ", ex)

    print("\n Stopping Kafka Producer")
