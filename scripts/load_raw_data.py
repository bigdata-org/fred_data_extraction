import time 
import requests 
import os
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage
from io import BytesIO, StringIO
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType


load_dotenv()

fred_api=os.getenv('FRED_API')
fred_api_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=T10Y2Y&api_key={fred_api}&file_type=json&observation_start=2020-02-23&observation_end=2025-02-23"
bucket_name = os.getenv('GCLOUD_BUCKET_NAME')
FRED_Table = ['FredData']
TABLE_DICT = {
    "fred_data": {"schema": "RAW_FRED","tables":FRED_Table}
}



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



def load_raw_data(session, storagedir=None,schema=None,tname=None):
    session.use_role("FRED_ROLE")
    session.use_database("FRED_DB")
    session.use_schema(schema)
    location = "@external_data.FRED_RAW_STAGE/{}".format(storagedir)
    try:
        copy_query = f"""
            COPY INTO {tname}
            FROM {location}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE';
        """
        result = session.sql(copy_query).collect()
        print(f"Data successfully loaded into table: {tname}")
        print(f"Copy result: {result}")
        # # df = session.read.parquet(location)
                # df = session.read.parquet(location)
        # df.copy_into_table(table_name=tname,force=True,validation_mode="RETURN_ERRORS")
        # print(f"data stored in {tname}")

    except Exception as e:
        print(e)
        raise


def load_all_tables(session):
    # _ = session.sql("ALTER WAREHOUSE FRED_WH SET WAREHOUSE_SIZE= XSMALL WAIT_FOR_COMPLETION= TRUE").collect()

    for storagedir, data in TABLE_DICT.items():
        tnames = data['tables']
        schema = data['schema']
        for tname in tnames:
            print("loading{}".format(tname))
            load_raw_data(session,storagedir=storagedir,schema=schema,tname=tname)

    # _ = session.sql("ALTER WAREHOUSE FRED_WH SET WAREHOUSE_SIZE= XSMALL").collect()


if __name__ == "__main__":  
     with Session.builder.getOrCreate() as session:
        #  print("ok")
         load_all_tables(session)
        # upload_data_to_gcloud(fred_api_url,bucket_name)