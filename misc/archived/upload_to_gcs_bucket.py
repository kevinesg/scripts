import os
from dotenv import load_dotenv
import argparse

from google.cloud import storage


load_dotenv()
service_account_creds = os.getenv("SERVICE_ACCOUNT_CREDS")
os.chdir(os.path.expanduser('~'))

parser = argparse.ArgumentParser()
parser.add_argument("--upload_file", type=str, help="/file/to/upload")
parser.add_argument("--name", type=str, help="uploaded file name in GCS bucket")
parser.add_argument("--bucket_name", type=str, help="GCS bucket name")
args = parser.parse_args()

upload_file = args.upload_file
name = args.name
bucket_name = args.bucket_name

client = storage.Client.from_service_account_json(service_account_creds)
bucket = client.lookup_bucket(bucket_name)
if bucket is None:
    bucket = client.create_bucket(bucket_name)
    print(f'[INFO] Bucket {bucket.name} created.')

blob = bucket.blob(name)
with open(upload_file, "rb") as f:
    blob.upload_from_file(f, content_type="text/html")

print(f"[INFO] Uploaded {upload_file} to GCS bucket {bucket_name} as {name}.")