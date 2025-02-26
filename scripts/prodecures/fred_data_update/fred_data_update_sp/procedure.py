
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def create_fred_raw_stream(session):
    session.sql('''
        CREATE OR REPLACE STREAM FRED_DB.FRED_RAW.FRED_RAW_STAGE_STREAM 
        ON TABLE FRED_DB.FRED_RAW.FREDDATA
        SHOW_INITIAL_ROWS = TRUE
    ''').collect()
    print("Stream FRED_RAW_STAGE_STREAM created successfully.")

def create_fred_harmonized_table(session):
    session.sql('''
        CREATE TABLE IF NOT EXISTS FRED_DB.FRED_HARMONIZED.FRED_HARMONIZED_TABLE (
            DATA_DATE DATE PRIMARY KEY,
            VALUE FLOAT,
            CREATED_DATE TIMESTAMP
        )
    ''').collect()
    print("Table FRED_HARMONIZED_TABLE checked/created successfully.")

def process_fred_harmonized_data(session):
  stream_data = session.table("FRED_DB.FRED_RAW.FRED_RAW_STAGE_STREAM")
  try:
    if stream_data.count() > 0:
        print("Stream data detected, processing...")

        harmonized_data = stream_data.select(
            F.col("DATA_DATE").alias("DATA_DATE"), 
            F.col("VALUE"),
            F.current_timestamp().alias("CREATED_DATE")
        )

        # Save transformed data temporarily
        harmonized_data.write.mode("overwrite").save_as_table("FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED")

        # Ensure target table exists
        create_fred_harmonized_table(session)

        # Merge transformed data into harmonized table
        merge_query = '''
           MERGE INTO FRED_DB.FRED_HARMONIZED.FRED_HARMONIZED_TABLE AS target
            USING (
                SELECT DATA_DATE, VALUE, CURRENT_TIMESTAMP AS CREATED_DATE
                FROM FRED_DB.FRED_RAW.FRED_RAW_STAGE_STREAM
                WHERE METADATA$ACTION = 'INSERT'
            ) AS source
            ON target.DATA_DATE = source.DATA_DATE
            WHEN MATCHED THEN 
                UPDATE SET target.VALUE = source.VALUE 
                        -- target.CREATED_DATE = CURRENT_TIMESTAMP  -- Use current timestamp
            WHEN NOT MATCHED THEN 
                INSERT (DATA_DATE, VALUE, CREATED_DATE) 
                VALUES (source.DATA_DATE, source.VALUE, CURRENT_TIMESTAMP);
        '''
        session.sql(merge_query).collect()
        
        # Drop temp table after merging
        copy_result =  session.sql("DROP TABLE IF EXISTS FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED").collect()
        return copy_result    
    
    else:
        print("No new data in stream, skipping merge.")
  except Exception as e:
    return str(e)

def test_fred_harmonized_data(session):
    print("Showing data in FRED_HARMONIZED_TABLE:")
    session.table("FRED_DB.FRED_HARMONIZED.FRED_HARMONIZED_TABLE").limit(5).show()

# Entry point for the stored procedure
def main(session):
    create_fred_raw_stream(session)
    process_fred_harmonized_data(session)
    return "Procedure executed successfully."