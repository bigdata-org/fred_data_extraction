import snowflake.connector as sf
import os 
from datetime import date
from dotenv import load_dotenv
import numpy as np
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

def count_weekdays(start_date, end_date=date.today()):
    return np.busday_count(start_date, end_date)

def test_method():
    conn = sf_connector()
    cur = conn.cursor()
    result = cur.execute(f"select count(*) from fred_db.prod_fred_analytics.freddata;").fetchall()[0][0]
    assert result == count_weekdays('2020-02-23')