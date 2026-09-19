import json
import os
import time
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")
GBFS_URL = os.getenv("STATION_STATUS_URL")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def fetch_and_publish():
  try:
    print(f"Mengambil data batch dari {GBFS_URL}...")
    response = requests.get(GBFS_URL, timeout=15)

    if response.status_code == 200:
      raw_data = response.json()
      message_bytes = json.dumps(raw_data).encode("utf-8")

      future = publisher.publish(topic_path, data=message_bytes)
      message_id = future.result()

      print(f"Berhasil publish ke Pub/Sub. Message ID: {message_id}")
    else:
      print(f"Gagal mengambil data. Status code: {response.status_code}")
  except Exception as e:
    print(f"Terjadi kesalahan pada publisher: {e}")


if __name__ == "__main__":
  while True:
    fetch_and_publish()
    time.sleep(60)