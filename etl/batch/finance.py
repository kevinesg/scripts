import argparse
import gspread as gs
import pandas as pd
import datetime as dt
import json
import io
from google.cloud import storage, bigquery
from io import StringIO
import os
from dotenv import load_dotenv




load_dotenv()
service_account_creds = os.getenv("SERVICE_ACCOUNT_CREDS")
project_id = os.getenv("PROJECT_ID")
dataset = os.getenv("RAW_DATASET")
table = os.getenv("FINANCE_RAW_TABLE")
bucket_name = os.getenv("FINANCE_BUCKET")
folder_name = os.getenv("FINANCE_BUCKET_FOLDER")
gsheet_url = os.getenv("FINANCE_GSHEET_URL")
sheet_name = os.getenv("FINANCE_GSHEET_SHEET_NAME")

# Parse command line arguments
parser = argparse.ArgumentParser(prog='finance ETL')
parser.add_argument('--step', action='store', required=True, choices=['extract', 'load'])
args = parser.parse_args()

client = storage.Client.from_service_account_json(service_account_creds)
bq_client = bigquery.Client.from_service_account_json(service_account_creds)

schema = bq_client.schema_from_json('/home/kevinesg/github/scripts/etl/batch/schema/finance__ledger.json')

if args.step == 'extract':

    bucket = client.lookup_bucket(bucket_name)
    if bucket is None:
        bucket = client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created.')

    blobs = bucket.list_blobs(prefix=f'{folder_name}/')
    blobs_list = [blob.name for blob in blobs]

    if f'{folder_name}/' not in blobs_list:
        bucket.blob(f'{folder_name}/').upload_from_string('')
        print(f'{folder_name}/ folder created.')

    print(f'[INFO] Processing table {table}...')

    from google.cloud.exceptions import NotFound
    try:
        bq_client.get_table(f"{project_id}.{dataset}.{table}")
        query = f"""
            SELECT MAX(updated_at) AS max_updated_at
            FROM `{project_id}.{dataset}.{table}`
        """
        rows = bq_client.query(query).result()
        #rows.result()
        for row in rows:
            latest_update = row["max_updated_at"]
            break
        if latest_update is None:
            adjusted_updated_at = dt.datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        else:
            adjusted_updated_at = latest_update
    
    except NotFound:
        bq_table = bigquery.Table(f"{project_id}.{dataset}.{table}", schema=schema)
        bq_client.create_table(bq_table, exists_ok=True)
        adjusted_updated_at = dt.datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    print(f'[INFO] {table} adjusted updated_at: {adjusted_updated_at}')

    print(f'[INFO] Extracting raw data...')
    gc = gs.service_account(filename=service_account_creds)
    sh = gc.open_by_url(gsheet_url)
    ws = sh.worksheet(sheet_name)
    df = pd.DataFrame(ws.get_all_records())
    df = df[pd.to_datetime(df['updated_at']) > adjusted_updated_at]

    # save raw df to GCS
    blob = bucket.blob(f'{folder_name}/raw_data.csv')
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f'[INFO] Done saving raw data to GCS bucket.')

    # Get the ids of newly-deleted rows from the source
    source_ids = set(pd.DataFrame(ws.get_all_records())['id'])
    query = f"""
        SELECT id
        FROM `{project_id}.{dataset}.{table}`
        WHERE NOT _is_deleted
        """
    bq_ids = bq_client.query(query).result()
    bq_ids_set = set()
    for row in bq_ids:
        bq_ids_set.add(row["id"])
    deleted_ids = list(bq_ids_set.difference(source_ids))
    df_deleted = pd.DataFrame(deleted_ids, columns=['deleted_ids'])
    blob_deleted = bucket.blob(f'{table}__deleted_ids.csv')
    blob_deleted.upload_from_string(df_deleted.to_csv(index=False), 'text/csv')
    print(f'[INFO] Saved {table} deleted row ids to GCS bucket.')




if args.step == 'load':

    try:
        bucket = client.lookup_bucket(bucket_name)
        data_blob = bucket.get_blob(f'{folder_name}/raw_data.csv')
        data_bytes = data_blob.download_as_bytes()
        data_buffer = io.BytesIO(data_bytes)
        df = pd.read_csv(data_buffer)
    except Exception as e:
        print(f'[ERROR] Error reading raw data: {e}')
        exit(1)
    
    try:
        deleted_blob = bucket.get_blob(f'{table}__deleted_ids.csv')
        data_str = deleted_blob.download_as_string()
        data_file = io.StringIO(data_str.decode('utf-8'))
        df_deleted = pd.read_csv(data_file)
    except Exception as e:
        print(f'[ERROR] Error reading deleted row ids: {e}')
        exit(1)

    df['_inserted_at'] = dt.datetime.now(dt.UTC)
    df['_is_deleted'] = False

    print(f'[INFO] {df.shape[0]} new/updated rows for table {table}')

    try:
        print(f'[INFO] {df.shape[0]} new/updated rows being upserted in table {table}...')
        temporary_table_id = f'{dataset}.{table}__temp_table'
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job_config.schema = schema
        bq_client.load_table_from_dataframe(df, temporary_table_id, job_config=job_config).result()

        merge_query = f"""
        MERGE INTO `{dataset}.{table}` AS target
        USING `{temporary_table_id}` AS source
        ON target.id = source.id
        WHEN MATCHED THEN
        UPDATE SET {', '.join([f"target.`{col}` = source.`{col}`" for col in df.columns])}
        WHEN NOT MATCHED THEN
        INSERT ({', '.join([f"`{col}`" for col in df.columns])})
        VALUES ({', '.join([f"source.`{col}`" for col in df.columns])})
        """
        print(merge_query)
        job = bq_client.query(merge_query)
        job.result()

        bq_client.delete_table(temporary_table_id)

    except Exception as e:
        print(f'[ERROR] Error upserting new/updated rows; {e}')
        exit(1)
        
    values_deleted = df_deleted['deleted_ids'].tolist()
    if values_deleted != []:
        print(f'[INFO] {len(values_deleted)} deleted rows being updated in table {table}...')
        # remove comma if only one element; logic can probably be improved/simplified
        if len(values_deleted) == 1:
            delete_ids = str(tuple(df_deleted['deleted_ids'])).replace(',', '')
        else:
            delete_ids = tuple(df_deleted['deleted_ids'])
        # Update is_deleted for newly-deleted rows
        update_query_deleted = f"""
            UPDATE {project_id}.{dataset}.{table}
            SET
                _is_deleted = TRUE,
                uploaded_to_bq = CURRENT_TIMESTAMP
            WHERE id IN {delete_ids}
            """
        bq_client.query(update_query_deleted)

    try:
        blob_name = f'{folder_name}/raw_data.csv'
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Delete the blob
        blob.delete()
        print(f"[INFO] Deleted temp file: {folder_name}/raw_data.csv")

    except Exception as e:
        print(f"[INFO] Error deleting file {folder_name}/raw_data.csv: {e}")
    
    try:
        del_blob_name = f'{table}__deleted_ids.csv'
        bucket = client.bucket(bucket_name)
        del_blob = bucket.blob(del_blob_name)

        # Delete the blob
        del_blob.delete()
        print(f"[INFO] Deleted temp file: {table}__deleted_ids.csv")

    except Exception as e:
        print(f"[INFO] Error deleting file {table}__deleted_ids.csv: {e}")
    
    print(f'[INFO] Done updating table {table}!')