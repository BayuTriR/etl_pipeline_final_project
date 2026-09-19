from config import load_configurations
from extract import download_citibike_data
from transform import transform_citibike_data
from load import upload_citibike_to_gcs_and_bq

def main():
    print("Memulai Pipeline ELT CitiBike Batch...")
    
    # 1. Load konfigurasi & validasi environment
    cfg = load_configurations()
    template_url = cfg["citibike_history_url"]

    target_months = ["202601", "202602", "202603", "202604", "202605", "202606", "202607", "202608"]

    # 2. Extract data mentah dari sumber
    for ym in target_months:
        url = template_url.format(year_month=ym)

        local_csv_path = download_citibike_data(url)
    
        # 3. Transformasi data menggunakan PySpark
        df_transformed, unique_dates = transform_citibike_data(local_csv_path, batch_month=ym)
        
        # 4. Load data harian ke GCS & BigQuery
        upload_citibike_to_gcs_and_bq(df_transformed, unique_dates, cfg, batch_month=ym)
    
    print("Pipeline ELT Selesai Sepenuhnya!")

if __name__ == "__main__":
    main()