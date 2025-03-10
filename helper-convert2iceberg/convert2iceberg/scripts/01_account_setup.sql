SET C2I_ROLE = '<MY_ROLE>';
SET C2I_DB = 'CONVERT2ICEBERG_<MY_ROLE>';
SET C2I_WH = 'CONVERT2ICEBERG_<MY_ROLE>_WH';

USE ROLE IDENTIFIER($C2I_ROLE);
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($C2I_WH) WITH WAREHOUSE_SIZE = 'XSMALL';

--create CONVERT2ICEBERG DB
USE WAREHOUSE IDENTIFIER($C2I_WH);
CREATE OR REPLACE DATABASE IDENTIFIER($C2I_DB);

USE DATABASE IDENTIFIER($C2I_DB);
CREATE OR REPLACE SCHEMA LOGGING;
CREATE OR REPLACE SCHEMA STREAMLIT;

--create CONVERSION_LOG table
USE SCHEMA LOGGING;
CREATE TABLE CONVERSION_LOG(
    id VARCHAR
    ,conversion_type VARCHAR
    ,original_tables ARRAY --one table if conversion_type is table, one or more if conversion_type is query
    ,original_query VARCHAR --null if conversion_type is table
    ,original_query_id VARCHAR --null if conversion_type is table
    ,converted_tables ARRAY --one table if conversion_type is table, one or more if conversion_type is query
    ,converted_query VARCHAR --null if conversion_type is table
    ,converted_query_id VARCHAR --null if conversion_type is table
    ,timestamp TIMESTAMP_NTZ(6)
);

--create CONVERT2ICEBERG streamlit
USE SCHEMA STREAMLIT;
CREATE STAGE IF NOT EXISTS CONVERT2ICEBERG;
CREATE STREAMLIT IF NOT EXISTS IDENTIFIER($C2I_DB)
  ROOT_LOCATION = '@CONVERT2ICEBERG_<MY_ROLE>.STREAMLIT.CONVERT2ICEBERG'
  MAIN_FILE = 'main.py'
  QUERY_WAREHOUSE = 'CONVERT2ICEBERG_<MY_ROLE>_WH'
  COMMENT = '{"origin":"sf_sit","name":"convert2iceberg","version":{"major":1, "minor":0},"attributes":{"env":"convert2iceberg","component":"convert2iceberg","type":"streamlit"}}';

--unset vars
UNSET(C2I_ROLE, C2I_DB, C2I_WH);