import boto3
import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector as sf
from datetime import date
import numpy as np
import time as t
from datetime import time, timedelta
load_dotenv()

def sf_connector():
    # Create connection
    try:
        conn = sf.connect(
            user = os.getenv('SF_USER'),
            password = os.getenv('SF_PASSWORD'),
            account = os.getenv('SF_ACCOUNT'),
            warehouse = os.getenv('SF_WAREHOUSE'),
            database = os.getenv('SF_DB'),
            role = os.getenv('SF_ROLE'),
            private_key_file = 'rsa_key.p8'
        )
    except Exception as e:
        return None
    return conn
def init_hook():
    conn = sf_connector()
    if conn is None:
        return
    
    cur = conn.cursor()
    curr_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    py_fred_api_base_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=T10Y2Y&api_key=cf4bf1054e57bf267cb2fc89aa58b117&file_type=json&observation_start={curr_date}&observation_end={curr_date}"
    
    # FIX: Wrap URL in single quotes
    query = f"SELECT fred_db.prod_integrations.FREDAPI_RESPONSE_TO_DF('{py_fred_api_base_url}');"
    
    try:
        cur.execute(query)
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        cur.close()
        conn.close()
    
    t.sleep(20)

def test_method():
    init_hook()
    
    # Create an S3 client using credentials from environment variables
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    bucket_name = 'sfopenaccessbucket'
    prefix = 'fred/'  # The "folder" or prefix inside the bucket
    file_key = 'fred/data.csv'

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Check if file exists in S3
    file_exists = any(obj['Key'] == f"{prefix}data.csv" for obj in response.get('Contents', []))
    
    if file_exists:
        csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = csv_obj['Body']
        df = pd.read_csv(csv_data)
        line_count = len(df)
        
        assert line_count == 1, f"Expected 1 line, but found {line_count} lines."
    else:
        raise AssertionError("File does not exist in the S3 bucket.")