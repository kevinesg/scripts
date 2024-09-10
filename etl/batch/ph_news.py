import argparse
import requests
import pandas as pd
import pandas_gbq
from google.cloud import storage, bigquery
from io import StringIO
from google.oauth2 import service_account
import os
from dotenv import load_dotenv




load_dotenv()
service_account_creds = os.getenv("SERVICE_ACCOUNT_CREDS")
project_id = os.getenv("PROJECT_ID")
dataset = os.getenv("RAW_DATASET")
bucket_name = os.getenv("PH_NEWS_BUCKET")
raw_folder_name = 'data/raw'
transformed_folder_name = 'data/transformed'
dataset = os.getenv("RAW_DATASET")
table = os.getenv("PH_NEWS_TABLE")
min_date = '2023-06-01 00:00:00+08:00'
query_params = {
    'access_key': os.getenv("PH_NEWS_ACCESS_KEY"),
    'countries': 'ph',
    'language': 'en',
    'sort': 'published_desc',
    'limit': 100,
}

# Parse command line arguments
parser = argparse.ArgumentParser(prog='PH News ETL')
parser.add_argument('--step', action='store', required=True, choices=['extract', 'transform', 'load'])
args = parser.parse_args()


def convert_timezone(ts, target_tz):
    if ts.tzinfo is None:
        return ts.tz_localize('UTC').tz_convert(target_tz)
    else:
        return ts.tz_convert(target_tz)


if args.step == "extract":

    with open(query_params['access_key'], 'r') as f:
        api_key:str = f.readline()
        query_params['access_key'] = api_key

    url:str = 'http://api.mediastack.com/v1/news'
    response = requests.get(url, params=query_params)
    data:list = response.json()['data']
    df:pd.DataFrame = pd.json_normalize(data)

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    # Check if bucket already exists
    bucket = client.lookup_bucket(str(bucket_name))

    if bucket is None:
        bucket = client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created.')

    blobs = bucket.list_blobs(prefix=f'{raw_folder_name}/')
    blobs_list = [blob.name for blob in blobs]

    if f'{raw_folder_name}/' not in blobs_list:

        bucket.blob(f'{raw_folder_name}/').upload_from_string('')
        print(f'{raw_folder_name}/ folder created.')

    latest_batch:str = 'latest_batch.csv'
    new_data:str = 'new_data.csv'
    if bucket is None:
        bucket = client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created.')
        df_new:pd.DataFrame = df
    else:
        blob = bucket.blob(f'{raw_folder_name}/{latest_batch}')
        if blob.exists():
            data_blob = bucket.get_blob(f'{raw_folder_name}/{latest_batch}')
            data_str = data_blob.download_as_string()
            data_file = StringIO(data_str.decode('utf-8'))
            df_prev:pd.DataFrame = pd.read_csv(data_file)
            df_new:pd.DataFrame = df[~df['url'].isin(df_prev['url'])]
        else:
            df_new:pd.DataFrame = df
    
    bucket.blob(f'{raw_folder_name}/{latest_batch}').upload_from_string(df.to_csv(index=False), 'text/csv')
    bucket.blob(f'{raw_folder_name}/{new_data}').upload_from_string(df_new.to_csv(index=False), 'text/csv')




if args.step == "transform":

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    bucket = client.get_bucket(bucket_name)
    data_blob = bucket.get_blob(f'{raw_folder_name}/new_data.csv')
    data_str = data_blob.download_as_string()
    data_file = StringIO(data_str.decode('utf-8'))
    df = pd.read_csv(data_file)

    if df.shape[0] > 0:

        # data cleaning and filtering
        df.drop(columns=['language', 'country'], inplace=True)

        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
        df['published_at'] = df['published_at'].apply(lambda x: convert_timezone(x, 'Asia/Manila'))
        df.dropna(subset=['published_at'], inplace=True)

        df = df[df['published_at'] >= min_date] # only relevant for the first couple of runs
    
    df_transformed_name:str = f'{transformed_folder_name}/df_transformed.csv'
    bucket.blob(df_transformed_name).upload_from_string(df.to_csv(index=False), 'text/csv')




if args.step == "load":

    credentials = service_account.Credentials.from_service_account_file(service_account_creds)
    pandas_gbq.context.credentials = credentials

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    bucket = client.get_bucket(bucket_name)
    data_blob = bucket.get_blob(f'{transformed_folder_name}/df_transformed.csv')
    data_str = data_blob.download_as_string()
    data_file = StringIO(data_str.decode('utf-8'))
    df = pd.read_csv(data_file)

    if df.shape[0] == 0:
        print('[INFO] No new rows to be ingested.')
        exit(0)

    # Create a bigquery client
    gbq_client = bigquery.Client.from_service_account_json(service_account_creds)
    datasets = list(gbq_client.list_datasets())
    dataset_list = [dataset.dataset_id for dataset in datasets]
    if dataset not in dataset_list:
        gbq_client.create_dataset(bigquery.Dataset(gbq_client.dataset(dataset)))
    
    tables = list(gbq_client.list_tables(dataset))
    table_list = [table.table_id for table in tables]

    if table not in table_list:
        gbq_client.create_table(bigquery.Table(f'{project_id}.{dataset}.{table}'))

    print(f'[INFO] Ingesting new data to GBQ...')
    df.to_gbq(destination_table=f'{dataset}.{table}', project_id=project_id, if_exists='append')
    print(f'[INFO] Done ingesting new data to GBQ.')    
