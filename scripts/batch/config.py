import os
from dotenv import load_dotenv

def load_configurations():
    # Load environment variables dari file .env
    load_dotenv()

    # Ambil konfigurasi dari .env
    config = {
        "output_dir": os.getenv("OUTPUT_DIR", "/opt/airflow/output/citibike_output"),
        "output_station_info": os.getenv("OUTPUT_STATION_INFO", "/opt/airflow/output/station_info"),
        "gcs_bucket_name": os.getenv("GCS_BUCKET_NAME"),
        "gcs_folder_prefix": os.getenv("GCS_FOLDER_PREFIX", "citibike_output"),
        "project_id": os.getenv("PROJECT_ID"),
        "dataset_staging": os.getenv("DATASET_STAGING"),
        "citibike_history_url": os.getenv("CITIBIKE_TEMPLATE_URL"),
        "station_info_url": os.getenv("STATION_INFO_URL")
    }
    
    # Validasi wajib untuk mencegah kegagalan di tengah jalan
    if not config["gcs_bucket_name"]:
        raise ValueError("GCS_BUCKET_NAME belum diset di file .env!")
    if not config["citibike_history_url"]:
        raise ValueError("CITIBIKE_TEMPLATE_URL belum diset di file .env!")
    if not config["station_info_url"]:
        raise ValueError("STATION_INFO_URL belum diset di file .env!")
    if not config["project_id"] or not config["dataset_staging"]:
        raise ValueError("PROJECT_ID atau DATASET_STAGING belum diset di file .env!")
        
    return config