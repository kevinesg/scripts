import os
import datetime as dt
from google.cloud import storage
from dotenv import load_dotenv




load_dotenv()
service_account_creds = os.getenv("SERVICE_ACCOUNT_CREDS")
log_path = os.getenv("AIRFLOW_LOGS_PATH")
bucket_name = os.getenv("AIRFLOW_LOGS_BUCKET_NAME")

files = []
for root, dirs, filenames in os.walk(log_path):
    for filename in filenames:
        files.append(os.path.join(root, filename))
files_to_cleanup = []
for file in files:
    timestamp = os.path.getmtime(file)
    utc_timestamp = dt.datetime.utcfromtimestamp(timestamp)
    if utc_timestamp < (dt.datetime.now() - dt.timedelta(days=30)):
        files_to_cleanup.append(file)
print(f'[INFO] files to cleanup: {files_to_cleanup}')


# Create a storage client
client = storage.Client.from_service_account_json(service_account_creds)

# Check if bucket already exists
bucket = client.bucket(bucket_name)
if not bucket.exists():
    # Create the bucket
    bucket = client.create_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created successfully")
bucket = client.bucket(bucket_name)

if files_to_cleanup == []:
    print(f'[INFO] Nothing to cleanup; exiting...')
    exit(0)

for local_file in files_to_cleanup:
    bucket_file = local_file.replace(log_path, "")
    # Create a blob object in the bucket
    blob = bucket.blob(bucket_file)

    # Upload the file to GCS
    blob.upload_from_filename(local_file)

    print(f"Uploaded {bucket_file} to GCS")

    os.remove(local_file)
    print(f'[INFO] Deleted {bucket_file} from local storage')

# delete empty folders
for root, dirs, files in os.walk(log_path, topdown=False):
    for dir_name in dirs:
        dir_path = os.path.join(root, dir_name)
        # Check if the directory is empty
        if not os.listdir(dir_path):
            # Delete the empty directory
            os.rmdir(dir_path)
            print(f"[INFO] Deleted empty folder: {dir_path}")

print(f'[INFO] Done!')