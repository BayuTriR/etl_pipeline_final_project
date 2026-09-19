import os
import zipfile
import urllib.request
import tempfile
from urllib.parse import urlparse
from urllib.error import HTTPError

def download_citibike_data(url):
    urls_to_try = [url]
    if url.endswith(".csv.zip"):
        urls_to_try.append(url.replace(".csv.zip", ".zip"))
    elif url.endswith(".zip") and not url.endswith(".csv.zip"):
        urls_to_try.append(url.replace(".zip", ".csv.zip"))
    else:
        urls_to_try = [f"{url}.csv.zip", f"{url}.zip"]

    tmp_dir = tempfile.gettempdir()
    extract_target_dir = os.path.join(tmp_dir, "citibike_extracted")
    os.makedirs(extract_target_dir, exist_ok=True)

    success = False
    local_zip_path = ""
    zip_file_name = ""

    for target_url in urls_to_try:
        parsed_url = urlparse(target_url)
        zip_file_name = os.path.basename(parsed_url.path)
        local_zip_path = os.path.join(tmp_dir, zip_file_name)

        try:
            print(f"Mencoba mendownload dari: {target_url}...")
            urllib.request.urlretrieve(target_url, local_zip_path)
            success = True
            print(f"Berhasil diakses! Menggunakan link: {target_url}")
            break
        except HTTPError as e:
            print(f"Link {target_url} tidak bisa diakses (Error {e.code}), otomatis beralih ke format .zip...")

    if not success:
        raise Exception(f"Gagal total: URL tidak dapat diakses untuk semua variasi ekstensi.")

    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_target_dir)
        extracted_files = zip_ref.namelist()

    csv_files = [f for f in extracted_files if f.endswith('.csv') and not f.startswith('__MACOSX')]

    if not csv_files:
        raise ValueError(f"Tidak ditemukan file .csv di dalam arsip {zip_file_name}!")

    local_csv_path = os.path.join(extract_target_dir, csv_files[0])
    
    return local_csv_path

def download_gbfs_json(url):
    tmp_dir = tempfile.gettempdir()
    extract_target_dir = os.path.join(tmp_dir, "gbfs_extracted")
    os.makedirs(extract_target_dir, exist_ok=True)
    
    local_json_path = os.path.join(extract_target_dir, "station_information.json")
    
    try:
        print(f"Mendownload GBFS JSON dari: {url}...")
        urllib.request.urlretrieve(url, local_json_path)
        print("Berhasil mendownload GBFS station_information.json")
        return local_json_path
    except HTTPError as e:
        raise Exception(f"Gagal mendownload GBFS JSON (Error {e.code})")