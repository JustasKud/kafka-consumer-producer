from confluent_kafka.admin import AdminClient, NewTopic
import time

# TODO: refactor to one file with options (sys.argv)
config = {
    "bootstrap.servers": "localhost:29092"
}

topic_config = {
    "retention.ms": "3000"
}

admin = AdminClient(config)

fs = admin.delete_topics(["records", "filtered"])

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} deleted".format(topic))
    except Exception as e:
        print("Failed to delete topic {}: {}".format(topic, e))

# TODO: refactor to not use sleep
time.sleep(0.3)

# topic = [NewTopic("records", num_partitions=3, replication_factor=1, config=topic_config)]
topic = [NewTopic("records", num_partitions=3, replication_factor=1), NewTopic("filtered", num_partitions=3, replication_factor=1)]
fs_2 = admin.create_topics(topic)

# Wait for each operation to finish.
for topic, f in fs_2.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
