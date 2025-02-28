import os
from dotenv import load_dotenv

load_dotenv()

connection_parameters = {
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "role": os.getenv('SNOWFLAKE_ROLE'),
    "warehouse": os.getenv('SNOWFLAKE_WH'),
    "database": os.getenv('SNOWFLAKE_DB'),
}
