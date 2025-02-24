-- Project Initialization for Production Environment Setup
-- 
-- This script establishes the foundational environment necessary for deploying the project
-- into the production environment. The following steps will be executed:
-- Note: This script can call a notebook with Snowpark integration, but most operations can be performed using SQL 
-- with CREATE OR REPLACE commands, except for step (3)
-- 1. Create a user role and grant appropriate permissions.
-- 2. Use the created role to set up essential database objects, including:
--    2.1. Configure GIT API Integration.
--    2.2. Create Database, Schemas, and Warehouse.
--    2.3. Set up External/Internal Stages if necessary.
--    2.4. Create Permanent Tables under appropriate schemas (RAW/HARMONIZED/ANALYTICS).
--    2.5. Define User Defined Functions (UDFs).
--    2.6. Develop Stored Procedures.
--    2.7. Implement Streams on appropriate schemas.
--    2.8. Create Materialized Views in the ANALYTICS schema.
--    2.9. Define and schedule Tasks for automation.
-- 3. Load data upto current date


USE ROLE ACCOUNTADMIN;
--ROLE
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE  FRED_ROLE;
GRANT ROLE FRED_ROLE TO ROLE ACCOUNTADMIN;
GRANT ROLE FRED_ROLE TO USER IDENTIFIER($MY_USER);

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE FRED_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE FRED_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE FRED_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE FRED_ROLE;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE FRED_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE FRED_ROLE;

USE ROLE FRED_ROLE;
--DATABASE
CREATE OR REPLACE DATABASE  FRED_DB;


--WAREHOUSE
CREATE OR REPLACE WAREHOUSE FRED_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;

USE WAREHOUSE FRED_WH;
USE DATABASE FRED_DB;

--SCHEMA
CREATE OR REPLACE SCHEMA FRED_DB.OBJECTS; -- UDF/SPS/GIT API
CREATE OR REPLACE SCHEMA FRED_DB.FRED_RAW;
CREATE OR REPLACE SCHEMA FRED_DB.FRED_HARMONIZED;
CREATE OR REPLACE SCHEMA FRED_DB.FRED_ANALYTICS;


-- file format (schema level)
CREATE OR REPLACE FILE FORMAT FRED_DB.OBJECTS.CSV_FILE_FORMAT
    TYPE = CSV;
 
 -- STORAGE INTEGRATION (account level)
CREATE OR REPLACE STORAGE INTEGRATION my_gcs_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('*');
 
 
--STAGE (schema level)
CREATE OR REPLACE STAGE FRED_DB.OBJECTS.FRED_RAW_STAGE
    URL='gcs://dow30-datapipeline/'
    STORAGE_INTEGRATION = my_gcs_integration;

SET GITHUB_SECRET_USERNAME = {{GITHUB_SECRET_USERNAME}};
SET GITHUB_SECRET_PASSWORD = {{GITHUB_SECRET_PASSWORD}};
SET GITHUB_URL_PREFIX = 'https://github.com/bigdata-org'; 
SET GITHUB_REPO_ORIGIN = 'https://github.com/bigdata-org/fred_data_extraction.git';

-- Secrets (schema level)
CREATE OR REPLACE SECRET FRED_DB.OBJECTS.GITHUB_SECRET
  TYPE = password
  USERNAME = $GITHUB_SECRET_USERNAME
  PASSWORD = $GITHUB_SECRET_PASSWORD;


-- API Integration (account level)
-- This depends on the schema level secret!
CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = ($GITHUB_URL_PREFIX)
  ALLOWED_AUTHENTICATION_SECRETS = (FRED_DB.OBJECTS.GITHUB_SECRET)
  ENABLED = TRUE;
 
-- Git Repository
CREATE OR REPLACE GIT REPOSITORY FRED_DB.OBJECTS.GIT_REPO
  API_INTEGRATION = GITHUB_API_INTEGRATION
  GIT_CREDENTIALS = FRED_DB.OBJECTS.GITHUB_SECRET
  ORIGIN = $GITHUB_REPO_ORIGIN;



