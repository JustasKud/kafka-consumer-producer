import time
from confluent_kafka.admin import AdminClient, NewTopic
from config import SERVER


admin_config = {
    "bootstrap.servers": SERVER
}


# topic_config = {
#     "retention.ms": "3000"
# }


# 😭
# def wait_until_deleted(admin, topic_list):
#     while True:
#         topic_metadata = admin.list_topics()
#         print(topic_metadata.topics.get(topic_list[0]))
#         if all(topic_metadata.topics.get(topic) == None for topic in topic_list):
#             return
#         print("Waiting for topics to be deleted.")


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
    # wait_until_deleted(admin, topics)
    time.sleep(0.3)

    topics_to_create = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    res = admin.create_topics(topics_to_create, validate_only=False)

    for topic, f in res.items():
        f.result()

    print(f"Topics {', '.join(topics)} created")
    print("Stopping Kafka Admin.")
