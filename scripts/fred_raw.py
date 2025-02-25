from snowflake.snowpark import Session
import snowflake.snowpark.functions as F


def create_fred_raw_stream(session):
    
    session.sql("USE WAREHOUSE COMPUTE_WH").collect()  # Set the warehouse
    session.use_database('FRED_DB')
    session.use_schema('FRED_RAW')

    session.sql('''
        CREATE OR REPLACE STREAM FRED_RAW_STAGE_STREAM 
        ON TABLE FRED_RAW.FREDDATA
        SHOW_INITIAL_ROWS = TRUE
    ''').collect()
    print("Stream FRED_RAW_STAGE_STREAM created successfully.")


def process_fred_harmonized_data(session):
    
    session.sql("USE WAREHOUSE COMPUTE_WH").collect()  # Set the warehouse
    session.use_database('FRED_DB')
    session.use_schema('FRED_RAW')

    # Read new changes from the stream
    stream_data = session.table("FRED_RAW_STAGE_STREAM")

    # If there are new records, process them
    if stream_data.count() > 0:
        print("Stream data detected, processing...")

        # Prepare data for the MERGE operation
        harmonized_data = stream_data.select(
            F.col("DATA_DATE").alias("DATE"),  
            F.col("VALUE"),
            F.current_timestamp().alias("CREATED_DATE")
        )

        # Collect data to pass into the MERGE query
        values_list = ', '.join([
            f"('{row['DATE']}', '{row['VALUE']}', '{row['CREATED_DATE']}')"
            for row in harmonized_data.collect()
        ])

        # Perform the MERGE operation
        merge_query = f'''
            MERGE INTO FRED_HARMONIZED.FRED_HARMONIZED_TABLE AS target
            USING (
                SELECT DATE, VALUE, CREATED_DATE
                FROM VALUES {values_list}
            ) AS source
            ON target.DATE = source.DATE
            WHEN MATCHED THEN 
                UPDATE SET target.VALUE = source.VALUE, 
                           target.CREATED_DATE = source.CREATED_DATE
            WHEN NOT MATCHED THEN 
                INSERT (DATE, VALUE, CREATED_DATE) 
                VALUES (source.DATE, source.VALUE, source.CREATED_DATE)
        '''
        session.sql(merge_query).collect()
        
        print("Merge operation completed successfully.")
    else:
        print("No new data in stream, skipping merge.")


def test_fred_harmonized_data(session):
    
    session.sql("USE WAREHOUSE COMPUTE_WH").collect()  # Set the warehouse
    session.use_database('FRED_DB')
    session.use_schema('FRED_HARMONIZED')

    
    session.table("FRED_HARMONIZED_TABLE").limit(5).show()


# For local debugging
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        create_fred_raw_stream(session)  # Step 1: Create stream on raw data
        process_fred_harmonized_data(session)  # Step 2: Process & MERGE data
        test_fred_harmonized_data(session)  # Step 3: Test output
