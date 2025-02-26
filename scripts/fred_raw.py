
# from snowflake.snowpark import Session
# import snowflake.snowpark.functions as F

# def create_fred_raw_stream(session):
#     session.sql("USE WAREHOUSE FRED_WH").collect()  # Set the warehouse
#     session.use_database('FRED_DB')
#     session.use_schema('FRED_RAW')

#     session.sql('''
#         CREATE OR REPLACE STREAM FRED_RAW_STAGE_STREAM 
#         ON TABLE FRED_RAW.FREDDATA
#         SHOW_INITIAL_ROWS = TRUE
#     ''').collect()
#     print("Stream FRED_RAW_STAGE_STREAM created successfully.")

# def process_fred_harmonized_data(session):
#     session.sql("USE WAREHOUSE FRED_WH").collect()  # Set the warehouse
#     session.use_database('FRED_DB')
#     session.use_schema('FRED_RAW')

#     # Read new changes from the stream
#     stream_data = session.table("FRED_RAW_STAGE_STREAM")

#     # If there are new records, process them
#     if stream_data.count() > 0:
#         print("Stream data detected, processing...")

#         # Prepare data for the MERGE operation
#         harmonized_data = stream_data.select(
#             F.col("DATA_DATE").alias('"DATE"'),  
#             F.col("VALUE"),
#             F.current_timestamp().alias("CREATED_DATE")
#         )

#         # Directly insert the data using Snowpark DataFrame operations
#         # This is a simpler approach than building a complex MERGE statement
#         harmonized_data.write.mode("append").save_as_table("FRED_DB.FRED_HARMONIZED.FRED_HARMONIZED_TABLE")
        
#         print("Data transfer completed successfully.")
#     else:
#         print("No new data in stream, skipping data transfer.")

# def test_fred_harmonized_data(session):
#     session.sql("USE WAREHOUSE FRED_WH").collect()  # Set the warehouse
#     session.use_database('FRED_DB')
#     session.use_schema('FRED_HARMONIZED')

#     # Test query to verify data was loaded correctly
#     print("Showing data in FRED_HARMONIZED_TABLE:")
#     session.table("FRED_HARMONIZED_TABLE").limit(5).show()

# # For local debugging
# if __name__ == "__main__":
#     with Session.builder.getOrCreate() as session:
#         create_fred_raw_stream(session)  # Step 1: Create stream on raw data
#         process_fred_harmonized_data(session)  # Step 2: Process & transfer data
#         test_fred_harmonized_data(session)  # Step 3: Test output



from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def create_fred_raw_stream(session):
    session.sql("USE WAREHOUSE FRED_WH").collect()
    session.use_database('FRED_DB')
    session.use_schema('FRED_RAW')

    session.sql('''
        CREATE OR REPLACE STREAM FRED_RAW_STAGE_STREAM 
        ON TABLE FRED_RAW.FREDDATA
        SHOW_INITIAL_ROWS = TRUE
    ''').collect()
    print("Stream FRED_RAW_STAGE_STREAM created successfully.")

def process_fred_harmonized_data(session):
    session.sql("USE WAREHOUSE FRED_WH").collect()
    session.use_database('FRED_DB')
    session.use_schema('FRED_RAW')

    # Read new changes from the stream
    stream_data = session.table("FRED_RAW_STAGE_STREAM")

    if stream_data.count() > 0:
        print("Stream data detected, processing...")

        
        harmonized_data = stream_data.select(
            F.col("DATA_DATE").alias("DATA_DATE"), 
            F.col("VALUE"),
            F.current_timestamp().alias("CREATED_DATE")
        )

        # Use a temporary table to store the transformed data
        harmonized_data.write.mode("overwrite").save_as_table("FRED_DB.FRED_RAW.TEMP_FRED_HARMONIZED")

       
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
        
        print("Merge operation completed successfully.")
    else:
        print("No new data in stream, skipping merge.")

def test_fred_harmonized_data(session):
    session.sql("USE WAREHOUSE FRED_WH").collect()
    session.use_database('FRED_DB')
    session.use_schema('FRED_HARMONIZED')

    print("Showing data in FRED_HARMONIZED_TABLE:")
    session.table("FRED_HARMONIZED_TABLE").limit(5).show()

# For local debugging
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        create_fred_raw_stream(session)  # Step 1: Create stream on raw data
        process_fred_harmonized_data(session)  # Step 2: Process & merge data
        test_fred_harmonized_data(session)  # Step 3: Test output
