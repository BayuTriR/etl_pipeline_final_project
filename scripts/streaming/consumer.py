import json
import os
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, lit

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")
DATASET_STAGING = os.getenv("DATASET_STAGING")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
TARGET_TABLE_ID = "station_status_streaming"
POLL_INTERVAL_SECONDS = 300

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def send_slack_alert(error_message):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL belum di-set...")
        return

    message = (
        f":red_circle: *Consumer GBFS Station Gagal*\n"
        f"*Waktu:* {datetime.now(timezone.utc).isoformat()}\n"
        f"*Alasan:* ```{error_message}```"
    )

    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as e:
        print(f"Gagal kirim alert Slack: {e}")

def process_batch(spark):
    print(f"Menarik pesan dari Pub/Sub subscription: {subscription_path}")

    try:
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 10},
            timeout=30.0,
        )
    except Exception as e:
        if "DeadlineExceeded" in str(e) or "504" in str(e):
            print("Pub/Sub pull timeout (antrean kosong/lambat), lanjut siklus berikutnya...")
            return
        raise e

    json_data_list = []
    ack_ids = []

    for received_message in response.received_messages:
        payload = received_message.message.data.decode("utf-8")
        json_data_list.append(payload)
        ack_ids.append(received_message.ack_id)

    if not json_data_list:
        print("Tidak ada pesan baru di Pub/Sub.")
        return

    print(f"Berhasil menarik {len(json_data_list)} pesan.")

    ingestion_ts = datetime.now(timezone.utc).isoformat()

    rdd = spark.sparkContext.parallelize(json_data_list)
    raw_df = spark.read.json(rdd)

    if "data" not in raw_df.columns:
        raise ValueError("Struktur data tidak sesuai (kolom 'data' tidak ditemukan).")

    flattened_df = raw_df.select(
        col("last_updated").alias("feed_last_updated"),
        explode(col("data.stations")).alias("station"),
    ).select(
        lit(ingestion_ts).alias("ingestion_at"),
        col("feed_last_updated"),
        col("station.station_id").alias("station_id"),
        col("station.num_bikes_available").alias("num_bikes_available"),
        col("station.num_bikes_disabled").alias("num_bikes_disabled"),
        col("station.num_docks_available").alias("num_docks_available"),
        col("station.num_docks_disabled").alias("num_docks_disabled"),
        col("station.is_installed").alias("is_installed"),
        col("station.is_renting").alias("is_renting"),
        col("station.is_returning").alias("is_returning"),
        from_unixtime(col("station.last_reported")).alias("last_reported"),
        col("station.num_ebikes_available").alias("num_ebikes_available"),
        col("station.num_scooters_available").alias("num_scooters_available"),
        col("station.num_scooters_unavailable").alias("num_scooters_unavailable"),
    )

    bigquery_table = f"{PROJECT_ID}.{DATASET_STAGING}.{TARGET_TABLE_ID}"
    print(f"Menulis ke BigQuery: {bigquery_table}...")

    writer = (
        flattened_df.write.format("bigquery")
        .option("table", bigquery_table)
        .option("writeMethod", "direct")
    )

    writer.mode("append").save()
    print("Berhasil dimuat ke BigQuery!")

    subscriber.acknowledge(
        request={"subscription": subscription_path, "ack_ids": ack_ids}
    )
    print("Pesan telah di-acknowledge.")

def main():
    spark = (
        SparkSession.builder.appName("GBFSBatchPubSubToBigQueryENV")
        .config(
            "spark.jars",
            "/opt/spark-jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    while True:
        try:
            process_batch(spark)
        except Exception as e:
            print(f"Terjadi kesalahan pada consumer: {e}")
            send_slack_alert(str(e))

        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()