import requests 
import os
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage
from io import BytesIO, StringIO
from snowflake.snowpark import Session
from datetime import date;


load_dotenv()

bucket_name = os.getenv('GCLOUD_BUCKET_NAME')
FRED_Table = ['FredData']
TABLE_DICT = {
    "fred_data": {"schema": "FRED_RAW","tables":FRED_Table}
}


connection_parameters = {
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "role": os.getenv('SNOWFLAKE_ROLE'),
    "warehouse": os.getenv('SNOWFLAKE_WH'),
    "database": os.getenv('SNOWFLAKE_DB'),
}


def set_timeline(session):
    check_for_data_sf = f"""
        SELECT COUNT(*) FROM FRED_DB.FRED_RAW.FREDDATA
    """
    count = session.sql(check_for_data_sf).collect()

    if count and count[0][0] > 0: 
        start_date= date.today()  
        end_date= date.today() 
    else  :
        start_date= '2020-01-01' 
        end_date= date.today() 
    
    return {
        "start_date": start_date, 
        "end_date" : end_date}
    

def upload_data_to_gcloud(bucket_name, start_date, end_date):
    end_date=date.today()
    fred_api=os.getenv('FRED_API')
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id=T10Y2Y&api_key={fred_api}&file_type=json&observation_start={start_date}&observation_end={end_date}"
    storage_client = storage.Client()
    try: 
        response  = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data['observations'])
        df.drop(columns=['realtime_start', 'realtime_end'], inplace=True)
        csv_data = StringIO()
        df.to_csv(csv_data, index=False)
        bucket =  storage_client.bucket(bucket_name)
        blob = bucket.blob(f"fred_data/FredData.csv")
        blob.upload_from_string(csv_data.getvalue(), content_type='text/csv')   
    except Exception as e:
        return f"Error uploading data to GCP: {e}"
    return "Data uploaded to GCP"


def load_raw_data(session, storagedir=None,schema=None,tname=None):
    session.use_role("FRED_ROLE")
    session.use_database("FRED_DB")
    session.use_warehouse("FRED_WH")
    session.use_schema(schema)
    location = "@objects.FRED_RAW_STAGE/{}/{}.csv".format(storagedir, tname)
    try:
        copy_query = f"""
            COPY INTO {tname}
            FROM {location}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 Null_IF= ('.'))
            ON_ERROR = 'CONTINUE';

        """
        result = session.sql(copy_query).collect()
        # print(f"Data successfully loaded into table: {tname}")
    except Exception as e:
        print(e)
        raise
    return "Data loaded successfully!!"


def load_all_tables(session):
    # _ = session.sql("ALTER WAREHOUSE FRED_WH SET WAREHOUSE_SIZE= XLARGE WAIT_FOR_COMPLETION= TRUE").collect()

    for storagedir, data in TABLE_DICT.items():
        tnames = data['tables']
        schema = data['schema']
        for tname in tnames:
            # print("loading{}".format(tname))
            result = load_raw_data(session,storagedir=storagedir,schema=schema,tname=tname)
    
    return result 

    # _ = session.sql("ALTER WAREHOUSE FRED_WH SET WAREHOUSE_SIZE= XSMALL").collect()


if __name__ == "__main__":  
     with Session.builder.configs(connection_parameters).create() as session :
        timeline = set_timeline(session)
        upload_result = upload_data_to_gcloud(bucket_name, start_date=timeline["start_date"], end_date=timeline["end_date"])
        data_loading_result = load_all_tables(session)
        print({
            "GCP":upload_result,
            "SF_table":data_loading_result
            })
        