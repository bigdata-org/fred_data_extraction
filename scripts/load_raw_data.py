import time 
import requests 
import os
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage
from io import BytesIO, StringIO

load_dotenv()

fred_api=os.getenv('FRED_API')
fred_api_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=T10Y2Y&api_key={fred_api}&file_type=json&observation_start=2020-02-23&observation_end=2025-02-23"
bucket_name = os.getenv('GCLOUD_BUCKET_NAME')

def upload_data_to_gcloud(url,bucket_name):
    try: 
        response  = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data['observations'])
        df.drop(columns=['realtime_start', 'realtime_end'], inplace=True)

        csv_data = StringIO()
        df.to_csv(csv_data, index=False)
        storage_client = storage.Client()
        bucket =  storage_client.bucket(bucket_name)
        blob = bucket.blob(f"fred_data/data.csv")
        blob.upload_from_string(csv_data.getvalue(), content_type='text/csv')   
    except Exception as e:
        return f"Error uploading data to GCP: {e}"
    return "Data uploaded to GCP"








# if __name__ == "__main__":  
    # upload_data_to_gcloud(fred_api_url,bucket_name)