from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, trim, date_format, explode, current_timestamp

def transform_citibike_data(local_csv_path, batch_month):
    # 1. Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("CitiBikeFullPipeline") \
        .getOrCreate()

    # 2. Baca file CSV bulanan ke PySpark DataFrame
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(local_csv_path)

    # 3. Transformasi Data (Cleaning, Konversi Tipe, Hitung Durasi)
    print("Melakukan transformasi data...")
    df_transformed = df_raw \
        .withColumn("started_at", to_timestamp(col("started_at"))) \
        .withColumn("ended_at", to_timestamp(col("ended_at"))) \
        .withColumn("start_station_name", trim(col("start_station_name"))) \
        .withColumn("end_station_name", trim(col("end_station_name"))) \
        .withColumn("start_lat", col("start_lat").cast("double")) \
        .withColumn("start_lng", col("start_lng").cast("double")) \
        .withColumn("end_lat", col("end_lat").cast("double")) \
        .withColumn("end_lng", col("end_lng").cast("double")) \
        .withColumn("date_str", date_format(col("started_at"), "yyyyMMdd")) \
        .withColumn("source_month", lit(batch_month)) \
        .withColumn("source_data", lit('Batch')) \
        .select(
            "source_month",
            "source_data",
            "ride_id",
            "rideable_type",
            "started_at",
            "ended_at",
            "start_station_id",
            "start_station_name",
            "end_station_id",
            "end_station_name",
            "start_lat",
            "end_lat",
            "start_lng",
            "end_lng",
            "member_casual",
            "date_str",
        )

    # 4. Ambil daftar tanggal unik
    unique_dates = [
        row.date_str for row in df_transformed.select("date_str").distinct().collect() 
        if row.date_str is not None
    ]

    return df_transformed, unique_dates

def transform_gbfs_stations(local_json_path):
    # 1. Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("GBFSStationsPipeline") \
        .getOrCreate()

    # 2. Baca file JSON GBFS ke PySpark DataFrame
    df_raw = spark.read \
        .option("multiline", "true") \
        .json(local_json_path)

    # 3. Transformasi Data (Ekstrak array stations, casting tipe data, dan cleaning)
    print("Melakukan transformasi data GBFS stations...")

    # Ekstrak array dari path data.stations
    df_stations = df_raw.select(explode(col("data.stations")).alias("station"))

    df_transformed = df_stations \
        .withColumn("station_id", trim(col("station.station_id").cast("string"))) \
        .withColumn("station_name", trim(col("station.name").cast("string"))) \
        .withColumn("short_name", trim(col("station.short_name").cast("string"))) \
        .withColumn("lon", col("station.lon").cast("double")) \
        .withColumn("lat", col("station.lat").cast("double")) \
        .withColumn("region_id", col("station.region_id").cast("string")) \
        .withColumn("capacity", col("station.capacity").cast("integer")) \
        .withColumn("rental_uri_android", col("station.rental_uris.android").cast("string")) \
        .withColumn("rental_uri_ios", col("station.rental_uris.ios").cast("string")) \
        .withColumn("extracted_at", current_timestamp()) \
        .select(
            "station_id",
            "station_name",
            "short_name",
            "lon",
            "lat",
            "region_id",
            "capacity",
            "rental_uri_android",
            "rental_uri_ios",
            "extracted_at",
        )

    return df_transformed