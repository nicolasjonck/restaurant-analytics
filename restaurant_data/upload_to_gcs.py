import os, json
import zipfile
# Imports the Google Cloud client library
from google.cloud import storage, bigquery

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_to_bigquery(uri, table_id, schema):
    client = bigquery.Client()

    print(schema)

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(**field) for field in schema],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1
    )
    
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def prepare_bq_load_config():
    schemas = []
    json_files = ['order_data', 'payment_data', 'order_line_data', 'store_data']
    for file in json_files:
        with open(f"{CURRENT_DIR}/schemas/{file}.json", 'r') as file:
            schema = json.load(file)
            schemas.append(schema)

    gcs_uris = [
        "gs://restaurant_data_raw/restaurant_csvs/order_data.csv",
        "gs://restaurant_data_raw/restaurant_csvs/payment_data.csv",
        "gs://restaurant_data_raw/restaurant_csvs/order_line.csv",
        "gs://restaurant_data_raw/restaurant_csvs/store_data.csv",
    ]

    dataset = "restaurant-analytics-417114.restaurant_tables"
    tables = ['orders', 'payments', 'order_details', 'stores']

    for uri, table, schema in zip(gcs_uris, tables, schemas): 
        table_id = dataset + '.' + table
        print(table_id)
        load_to_bigquery(uri, table_id, schema)

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
    # storage_client = storage.Client()
    # upload_csvs_to_gcs(storage_client)
    prepare_bq_load_config()
