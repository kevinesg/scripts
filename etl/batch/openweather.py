import argparse
import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
import pandas_gbq
from google.cloud import storage, bigquery
from io import StringIO
import os
from dotenv import load_dotenv





load_dotenv()
service_account_creds = os.getenv("SERVICE_ACCOUNT_CREDS")
project_id = os.getenv("PROJECT_ID")
dataset = os.getenv("STAGING_DATASET")
table = os.getenv("OPENWEATHER_TABLE")
bucket_name = os.getenv("OPENWEATHER_BUCKET")
raw_folder_name = os.getenv("OPENWEATHER_BUCKET_RAW_FOLDER")
transformed_folder_name = os.getenv("OPENWEATHER_BUCKET_TRANSFORMED_FOLDER")

query_params = {
    'lat': os.getenv("OPENWEATHER_LATITUDE"),
    'lon': os.getenv("OPENWEATHER_LONGITUDE"),
    'api_key': os.getenv("OPENWEATHER_API_KEY")
}

# Parse command line arguments
parser = argparse.ArgumentParser(prog='finance ETL')
parser.add_argument('--step', action='store', required=True, choices=['extract', 'transform', 'load'])
args = parser.parse_args()

if args.step == "extract":

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    # Check if bucket already exists
    bucket = client.lookup_bucket(bucket_name)
    raw_df_name:str = f'{datetime.now()}__raw_data.csv'

    if bucket is None:
        bucket = client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created.')

    blobs = bucket.list_blobs(prefix=f'{raw_folder_name}/')
    blobs_list = [blob.name for blob in blobs]

    if f'{raw_folder_name}/' not in blobs_list:

        bucket.blob(f'{raw_folder_name}/').upload_from_string('')
        print(f'{raw_folder_name}/ folder created.')

    # extract data from API
    print(f'[INFO] Extracting {datetime.now()} data...')

    with open(query_params['api_key'], 'r') as f:
        api_key:str = f.readline()
        query_params['api_key'] = api_key

    base_url = 'https://api.openweathermap.org/data/2.5/weather'
    url = base_url + '?'
    for param in query_params:
        url += f'{param}={query_params[param]}'
        if param == list(query_params.keys())[-1]:
            continue
        else:
            url += '&'
    
    response = requests.get(url)
    json_data = response.json()
    raw_df:pd.DataFrame = pd.json_normalize(json_data)

    # save raw df to GCS
    blob = bucket.blob(f'{raw_folder_name}/{raw_df_name}')
    blob.upload_from_string(raw_df.to_csv(index=False), 'text/csv')
    print(f'[INFO] Done saving {raw_df_name} to GCS bucket.')

'''
if args.step == "transform":

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    bucket = client.lookup_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f'{transformed_folder_name}/')
    blobs_list = [blob.name for blob in blobs]

    if f'{transformed_folder_name}/' not in blobs_list:

        bucket.blob(f'{transformed_folder_name}/').upload_from_string('')
        print(f'{transformed_folder_name}/ folder created.')

    raw_blobs = bucket.list_blobs(prefix=f'{raw_folder_name}/')
    raw_blobs_list = [
        blob.name for blob in raw_blobs if blob.name != f'{raw_folder_name}/'
    ]
    for filename in raw_blobs_list:

        day = filename.split('/')[-1][:10]
        transformed_df_name = f'{day}__transformed_data.csv'
        if f'{transformed_folder_name}/{transformed_df_name}' in blobs_list:
            print(f'[INFO] {transformed_df_name} already in GCS bucket. Skipping.')
        
        else:
            blob = bucket.get_blob(filename)
            data = blob.download_as_text()
            raw_df = pd.read_csv(StringIO(data))

            transformed_df = transform(raw_df)

            # save transformed df to GCS
            blob = bucket.blob(f'{transformed_folder_name}/{transformed_df_name}')
            blob.upload_from_string(transformed_df.to_csv(index=False), 'text/csv')
            print(f'[INFO] Done saving {transformed_df_name} to GCS bucket.')      


if args.step == "load":

    credentials = service_account.Credentials.from_service_account_file(service_account_creds)
    pandas_gbq.context.credentials = credentials

    # Create a storage client
    client = storage.Client.from_service_account_json(service_account_creds)

    bucket = client.lookup_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f'{transformed_folder_name}/')
    blobs_list = [blob.name for blob in blobs if blob.name != f'{transformed_folder_name}/']

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

    table_ref = gbq_client.dataset(dataset).table(table)
    if gbq_client.get_table(table_ref).num_rows > 0:
        query = f"""
            SELECT DISTINCT
                DATE(created_at) AS day
            FROM {dataset}.{table}
        """
        query_job = gbq_client.query(query)
        ingested_days = [row.day.strftime('%Y-%m-%d') for row in query_job]
    else:
        ingested_days = []

    for filename in blobs_list:
        blob = bucket.get_blob(filename)
        data = blob.download_as_text()
        transformed_df = pd.read_csv(StringIO(data))
        day = filename.split('/')[-1][:10]
        if day in ingested_days:
            print(f'[INFO] {day} data already ingested to GBQ. Skipping.')
        else:
            print(f'[INFO] Ingesting {day} data to GBQ...')
            transformed_df.to_gbq(
                destination_table=f'{dataset}.{table}',
                project_id=project_id,
                if_exists='append'
            )  


def transform(df:pd.DataFrame) -> pd.DataFrame:

    drop_columns = [
        'type',
        'properties.ids', 
        'properties.sources', 
        'properties.types'
    ]
    df.drop(columns=drop_columns, inplace=True)

    columns_mapping = {
        'properties.mag': 'magnitude',
        'properties.place': 'place',
        'properties.time': 'created_at',
        'properties.updated': 'updated_at',
        'properties.tz': 'timezone',
        'properties.url': 'url',
        'properties.detail': 'detail',
        'properties.felt': 'felt',
        'properties.cdi': 'cdi',
        'properties.mmi': 'mmi',
        'properties.alert': 'alert',
        'properties.status': 'status',
        'properties.tsunami': 'tsunami',
        'properties.sig': 'sig',
        'properties.net': 'net',
        'properties.code': 'code',
        'properties.nst': 'nst',
        'properties.dmin': 'dmin',
        'properties.rms': 'rms',
        'properties.gap': 'gap',
        'properties.magType': 'magnitude_type',
        'properties.type': 'type',
        'properties.title': 'title',
        'geometry.type': 'geometry_type'
    }
    df.rename(columns=columns_mapping, inplace=True)

    df[['longitude', 'latitude', 'depth']] = df['geometry.coordinates'].apply(
        lambda x: pd.Series(eval(x))
    )
    
    df.drop(columns=['geometry.coordinates'], inplace=True)

    df['created_at'] = pd.to_datetime(df['created_at'], unit='ms', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['updated_at'] = pd.to_datetime(df['updated_at'], unit='ms', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

    return df
'''