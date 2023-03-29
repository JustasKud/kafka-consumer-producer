import time
from confluent_kafka.admin import AdminClient, NewTopic
from config import SERVER


admin_config = {
    "bootstrap.servers": SERVER
}

# topic_config = {
#     "retention.ms": "3000"
# }


if __name__ == "__main__":
    print("Starting Kafka Admin.")

    admin = AdminClient(admin_config)
    topic_metadata = admin.list_topics()

    topics = ["records", "unique_records"]
    existing = {topic: topic_metadata.topics.get(topic) for topic in topics}

    for topic in topics:
        if existing[topic] != None:
            admin.delete_topics([topic])
            print(f"Topic {topic} deleted")

    # Waiting for topics marked for deletion to be actually deleted.
    time.sleep(0.3)

    topics_to_create = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    res = admin.create_topics(topics_to_create, validate_only=False)

    for topic, f in res.items():
        f.result()

    print(f"Topics {', '.join(topics)} created")
    print("Stopping Kafka Admin.")
