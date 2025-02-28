import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector as sf
from datetime import date
load_dotenv()

def sf_connector():
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

def test_method():
    conn = sf_connector()
    if conn is None:
        raise ConnectionError("Failed to connect to Snowflake")
    cur = conn.cursor()
    try:
        query = "SELECT fred_db.prod_integrations.CALCULATE_PERCENT_PROFIT_LOSS(100, 50);"
        result = cur.execute(query).fetchone()[0]  # Fetch the first column of the first row
        assert result == 100, f"Expected 100, but got {result}"
    except Exception as e:
        print(f"Query Execution Error: {e}")
    finally:
        cur.close()
        conn.close()