# from snowflake.snowpark import Session
# import snowflake.snowpark.functions as F

# def main(session: Session) -> str:
#     try:
#         create_fred_raw_stream(session)  # Step 1: Create stream on raw data
#         process_fred_harmonized_data(session)  # Step 2: Process & merge data
#         test_fred_harmonized_data(session)  # Step 3: Test output
#         return "Procedure executed successfully."
#     except Exception as e:
#         return f"Error: {str(e)}"

# def create_fred_raw_stream(session):
#     session.sql("USE WAREHOUSE FRED_WH").collect()
#     session.use_database('FRED_DB')
#     session.use_schema('FRED_RAW')

#     session.sql('''
#         CREATE OR REPLACE STREAM FRED_RAW_STAGE_STREAM 
#         ON TABLE FREDDATA
#         SHOW_INITIAL_ROWS = TRUE
#     ''').collect()
#     print("Stream FRED_RAW_STAGE_STREAM created successfully.")

# def create_fred_harmonized_table(session):
#     session.sql("USE WAREHOUSE FRED_WH").collect()
#     session.use_database('FRED_DB')
#     session.use_schema('FRED_HARMONIZED')

#     session.sql('''
#         CREATE TABLE IF NOT EXISTS FRED_HARMONIZED_TABLE (
#             DATA_DATE DATE PRIMARY KEY,
#             VALUE FLOAT,
#             CREATED_DATE TIMESTAMP
#         )
#     ''').collect()
#     print("Table FRED_HARMONIZED_TABLE checked/created successfully.")

# def process_fred_harmonized_data(session):
#     session.sql("USE WAREHOUSE FRED_WH").collect()
#     session.use_database('FRED_DB')
#     session.use_schema('FRED_RAW')

#     stream_data = session.table("FRED_RAW_STAGE_STREAM")

#     if stream_data.count() > 0:
#         print("Stream data detected, processing...")

#         harmonized_data = stream_data.select(
#             F.col("DATA_DATE").alias("DATA_DATE"), 
#             F.col("VALUE"),
#             F.current_timestamp().alias("CREATED_DATE")
#         )

#         # Save transformed data temporarily
#         harmonized_data.write.mode("overwrite").save_as_table("FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED")

#         # Ensure target table exists
#         create_fred_harmonized_table(session)

#         # Merge transformed data into harmonized table
#         merge_query = '''
#             MERGE INTO FRED_DB.FRED_HARMONIZED.FRED_HARMONIZED_TABLE AS target
#             USING FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED AS source
#             ON target.DATA_DATE = source.DATA_DATE
#             WHEN MATCHED THEN 
#                 UPDATE SET target.VALUE = source.VALUE, 
#                            target.CREATED_DATE = source.CREATED_DATE
#             WHEN NOT MATCHED THEN 
#                 INSERT (DATA_DATE, VALUE, CREATED_DATE) 
#                 VALUES (source.DATA_DATE, source.VALUE, source.CREATED_DATE);
#         '''
#         session.sql(merge_query).collect()
        
#         # Drop temp table after merging
#         session.sql("DROP TABLE IF EXISTS FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED").collect()
        
#         print("Merge operation completed successfully.")
#     else:
#         print("No new data in stream, skipping merge.")

# def test_fred_harmonized_data(session):
#     session.sql("USE WAREHOUSE FRED_WH").collect()
#     session.use_database('FRED_DB')
#     session.use_schema('FRED_HARMONIZED')

#     print("Showing data in FRED_HARMONIZED_TABLE:")
#     session.table("FRED_HARMONIZED_TABLE").limit(5).show()



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
            USING FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED AS source
            ON target.DATA_DATE = source.DATA_DATE
            WHEN MATCHED THEN 
                UPDATE SET target.VALUE = source.VALUE, 
                           target.CREATED_DATE = source.CREATED_DATE
            WHEN NOT MATCHED THEN 
                INSERT (DATA_DATE, VALUE, CREATED_DATE) 
                VALUES (source.DATA_DATE, source.VALUE, source.CREATED_DATE);
        '''
        session.sql(merge_query).collect()
        
        # Drop temp table after merging
        session.sql("DROP TABLE IF EXISTS FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED").collect()
        
        print("Merge operation completed successfully.")
    else:
        print("No new data in stream, skipping merge.")

def test_fred_harmonized_data(session):
    print("Showing data in FRED_HARMONIZED_TABLE:")
    session.table("FRED_DB.FRED_HARMONIZED.FRED_HARMONIZED_TABLE").limit(5).show()

# Entry point for the stored procedure
def main(session):
    create_fred_raw_stream(session)
    process_fred_harmonized_data(session)
    return "Procedure executed successfully."
