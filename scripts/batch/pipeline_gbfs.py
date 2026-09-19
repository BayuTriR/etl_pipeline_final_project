from config import load_configurations
from extract import download_gbfs_json
from transform import transform_gbfs_stations
from load import upload_gbfs_to_gcs_and_bq

def main():
    print("Memulai Pipeline ELT GBFS Stations...")

    # 1. Load konfigurasi & validasi environment
    cfg = load_configurations()
    station_info_url = cfg["station_info_url"]

    # 2. Extract data mentah dari sumber
    local_json_path = download_gbfs_json(station_info_url)

    # 3. Transformasi data menggunakan PySpark
    df_transformed = transform_gbfs_stations(local_json_path)

    # 4. Load data ke GCS & BigQuery
    upload_gbfs_to_gcs_and_bq(df_transformed, cfg)

    print("Pipeline ELT GBFS Stations Selesai Sepenuhnya!")

if __name__ == "__main__":
    main()