import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")

def ensure_pubsub_resources():
    if not PROJECT_ID or not TOPIC_ID or not SUBSCRIPTION_ID:
        raise ValueError(
            "PROJECT_ID, TOPIC_ID, dan SUBSCRIPTION_ID belum di-set di env."
        )

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"Topic '{TOPIC_ID}' berhasil dibuat.")
    except AlreadyExists:
        print(f"Topic '{TOPIC_ID}' sudah ada, skip.")

    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        print(f"Subscription '{SUBSCRIPTION_ID}' berhasil dibuat.")
    except AlreadyExists:
        print(f"Subscription '{SUBSCRIPTION_ID}' sudah ada, skip.")


if __name__ == "__main__":
    ensure_pubsub_resources()
    print("Pub/Sub resources siap digunakan.")