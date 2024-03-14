import os
import zipfile
# Imports the Google Cloud client library
from google.cloud import storage

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def upload_to_gcs(target_path, bucket, path):
    #create a blob object per file
    blob = bucket.blob(target_path)
    #upload
    blob.upload_from_filename(path)
    print(f"File uploaded to bucket {bucket} in folder {target_path}.")

def unzip_files(path):
    with zipfile.ZipFile(path, 'r') as zip_ref:
        extract_dir = os.path.join(CURRENT_DIR, 'raw')
        zip_ref.extractall(extract_dir)

def upload_csvs_to_gcs(client):
    zip_file_path = os.path.join(CURRENT_DIR, 'raw', 'tiller.zip')
    unzip_files(zip_file_path)

    file_names = ['order_data', 'order_line', 'payment_data', 'store_data']
    for file in file_names:
        file_path = os.path.join(CURRENT_DIR, f"raw/tiller/{file}.csv")
        #get bucket name
        bucket_name = os.environ["RESTAURANTS_BUCKET"]
        bucket = client.bucket(bucket_name)
        target_path = f"restaurant_csvs/{file}.csv"
        upload_to_gcs(target_path, bucket, file_path)

if __name__ == '__main__':
    #instantiate the client
    storage_client = storage.Client()
    upload_csvs_to_gcs(storage_client)
