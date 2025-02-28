import json
import os
from connector import get_secret,  sf_connector
import pandas as pd

def lambda_handler(event, context):
    # TODO implement
    try:
        secret = get_secret(secret_name=os.getenv("SECRET_PATH"))
        DB_NAME, DB_SCHEMA, DB_TABLE = secret['SF_DB'], secret['SF_SCHEMA'], "FREDDATA"
        conn = sf_connector(secret)
        cur = conn.cursor()
        master_result = []

        result = cur.execute(f"select * from {DB_NAME}.{DB_SCHEMA}.month_wise_analytics;").fetchall()
        df= pd.DataFrame(result, columns=['year', 'month', 'average_value'])
        q2 = df.to_json(orient='records') if not df.empty else "[]"

        result = cur.execute(f"select * from {DB_NAME}.{DB_SCHEMA}.year_wise_analytics;").fetchall()
        df= pd.DataFrame(result, columns=['year', 'average_value'])
        q3 = df.to_json(orient='records') if not df.empty else "[]"

        result = cur.execute(f"select * from {DB_NAME}.{DB_SCHEMA}.curr_vs_prev_analytics;").fetchall()
        df= pd.DataFrame(result, columns=['date', 'current_value', 'previous_day_value', 'percent_difference'])
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Coerce invalid dates to NaT
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        q4 = df.to_json(orient='records') if not df.empty else "[]"

        master_result.append(q2)
        master_result.append(q3)
        master_result.append(q4)

        return {
            'statusCode': 200,
            'body':master_result
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(e)
        }

