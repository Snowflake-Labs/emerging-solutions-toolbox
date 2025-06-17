/*************************************************************************************************************
Script:             Cohort Builder for Snowflake App Setup
Create Date:        2024-07-29
Author:             Adithya Nanduri
Description:        Cohort Builder for Snowflake
Copyright © 2024 Snowflake Inc. All rights reserved
**************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-07-29          Adithya Nanduri                           Initial Creation
2025-02-24          Brandon Barker                      Warehouse size default size lowered
*************************************************************************************************************/

/* set up roles */
USE ROLE ACCOUNTADMIN;
CALL SYSTEM$WAIT(10);

/* create warehouse */
CREATE OR REPLACE WAREHOUSE cohort_builder_load_wh
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}';

/* create role and add permissions required by role for installation of framework */
CREATE ROLE IF NOT EXISTS cohort_builder_role;

/* perform grants */
GRANT CREATE SHARE ON ACCOUNT TO ROLE cohort_builder_role;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE cohort_builder_role;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE cohort_builder_role WITH GRANT OPTION;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE cohort_builder_role;
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE cohort_builder_role;
GRANT CREATE APPLICATION ON ACCOUNT TO ROLE cohort_builder_role;
GRANT CREATE DATA EXCHANGE LISTING ON ACCOUNT TO ROLE cohort_builder_role;
/* add cortex_user database role to use Cortex */
GRANT DATABASE ROLE snowflake.cortex_user TO ROLE cohort_builder_role;
GRANT ROLE cohort_builder_role TO ROLE sysadmin;
GRANT USAGE, OPERATE ON WAREHOUSE cohort_builder_load_wh TO ROLE cohort_builder_role;

/* set up provider side objects */
USE ROLE cohort_builder_role;
CALL SYSTEM$WAIT(10);
USE WAREHOUSE cohort_builder_load_wh;

/* create database */
CREATE OR REPLACE DATABASE cohort_builder_for_snowflake
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}';

/* create schema */
CREATE OR REPLACE SCHEMA cohort_builder_for_snowflake.app
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}';

/* create table */
CREATE OR REPLACE TABLE cohort_builder_for_snowflake.app.cohorts
(
    CREATEDDATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATEDDATE TIMESTAMP_NTZ(9),
    COHORT_NAME STRING,
    TABLE_NAME STRING,
    COLUMN_LIST ARRAY,
    COHORT_SELECTIONS_JSON VARIANT
)
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}';

/* create procedure for upserting cohorts */
CREATE OR REPLACE PROCEDURE cohort_builder_for_snowflake.app.upsert_cohorts(
    COHORT_NAME STRING,
    TABLE_NAME STRING,
    COLUMN_LIST ARRAY,
    COHORT_SELECTIONS_JSON STRING
)
RETURNS STRING
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}'
AS
$$
BEGIN
    -- Merge statement to update or insert data
    MERGE INTO cohort_builder_for_snowflake.app.cohorts AS target
    USING (
        SELECT 
            :COHORT_NAME AS COHORT_NAME,
            :TABLE_NAME AS TABLE_NAME,
            :COLUMN_LIST AS COLUMN_LIST,
            :COHORT_SELECTIONS_JSON::VARIANT AS COHORT_SELECTIONS_JSON,
            CURRENT_TIMESTAMP() AS current_timestamp
    ) AS source
    ON target.TABLE_NAME = source.TABLE_NAME
        AND target.COHORT_NAME = source.COHORT_NAME
    WHEN MATCHED THEN
        UPDATE SET 
            target.COHORT_SELECTIONS_JSON = TO_VARIANT(source.COHORT_SELECTIONS_JSON),
            target.UPDATEDDATE = source.current_timestamp,
            target.COLUMN_LIST = source.COLUMN_LIST
    WHEN NOT MATCHED THEN
        INSERT (COHORT_NAME, TABLE_NAME, COHORT_SELECTIONS_JSON, CREATEDDATE, COLUMN_LIST)
        VALUES (source.COHORT_NAME, source.TABLE_NAME, TO_VARIANT(source.COHORT_SELECTIONS_JSON), source.current_timestamp, source.COLUMN_LIST);

    RETURN 'Merge operation completed successfully';
END;
$$;

/* create stage */
CREATE OR REPLACE STAGE cohort_builder_for_snowflake.app.stage_cohort_builder
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}';

/* create streamlit app */
CREATE OR REPLACE STREAMLIT cohort_builder
ROOT_LOCATION = '@cohort_builder_for_snowflake.app.stage_cohort_builder'
MAIN_FILE = '/Home.py'
QUERY_WAREHOUSE = cohort_builder_load_wh
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}';

/* create procedure for getting column stats */
CREATE OR REPLACE PROCEDURE cohort_builder_for_snowflake.app.get_column_stats(
    column_name STRING,
    data_type STRING,
    filter_type STRING,
    table_name STRING,
    where_clause STRING
)
RETURNS TABLE (
    COLUMN_NAME STRING,
    DATA_TYPE STRING,
    FILTER_TYPE STRING,
    MIN_VALUE STRING,
    MAX_VALUE STRING,
    DISTINCT_COUNT STRING,
    DISTINCT_VALUES ARRAY
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = '{"origin":"sf_sit","name":"cohort-builder","version":{"major":1, "minor":1}}'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, min, max, countDistinct, array_agg, lit

def run(session, column_name: str, data_type: str, filter_type: str, table_name: str, where_clause: str):

    where_clause = 'TRUE ' + where_clause
    
    # Create the base DataFrame
    df = session.table(table_name).filter(where_clause)
    
    if data_type in ['NUMBER', 'DATE', 'DATETIME', 'TIMESTAMP_NTZ', 'BOOLEAN']:
        result_df = df.select(
            lit(column_name).alias("COLUMN_NAME"),
            lit(data_type).alias("DATA_TYPE"),
            lit(filter_type).alias("FILTER_TYPE"),
            min(col(column_name)).cast('STRING').alias("MIN_VALUE"),
            max(col(column_name)).cast('STRING').alias("MAX_VALUE"),
            lit(None).cast('STRING').alias("DISTINCT_COUNT"),
            lit(None).cast('ARRAY').alias("DISTINCT_VALUES")
        )
    elif data_type == 'TEXT':
        distinct_count_df = df.select(countDistinct(col(column_name)).cast('STRING').alias("DISTINCT_COUNT"))
        distinct_count = int(distinct_count_df.collect()[0]['DISTINCT_COUNT'])
        
        if filter_type == 'text filter':
            result_df = distinct_count_df.select(
                lit(column_name).alias("COLUMN_NAME"),
                lit(data_type).alias("DATA_TYPE"),
                lit(filter_type).alias("FILTER_TYPE"),
                lit(None).cast('STRING').alias("MIN_VALUE"),
                lit(None).cast('STRING').alias("MAX_VALUE"),
                col("DISTINCT_COUNT"),
                lit(None).cast('ARRAY').alias("DISTINCT_VALUES")
            )
        else:
            if distinct_count <= 5000:
                distinct_values_df = df.select(array_agg(col(column_name), is_distinct=True).alias("DISTINCT_VALUES"))
                result_df = distinct_count_df.crossJoin(distinct_values_df).select(
                    lit(column_name).alias("COLUMN_NAME"),
                    lit(data_type).alias("DATA_TYPE"),
                    lit(filter_type).alias("FILTER_TYPE"),
                    lit(None).cast('STRING').alias("MIN_VALUE"),
                    lit(None).cast('STRING').alias("MAX_VALUE"),
                    col("DISTINCT_COUNT"),
                    col("DISTINCT_VALUES").cast('ARRAY').alias("DISTINCT_VALUES")
                )
            else:
                result_df = distinct_count_df.select(
                    lit(column_name).alias("COLUMN_NAME"),
                    lit(data_type).alias("DATA_TYPE"),
                    lit(filter_type).alias("FILTER_TYPE"),
                    lit(None).cast('STRING').alias("MIN_VALUE"),
                    lit(None).cast('STRING').alias("MAX_VALUE"),
                    col("DISTINCT_COUNT"),
                    lit(None).cast('ARRAY').alias("DISTINCT_VALUES")
                )
    else:
        result_df = df.select(
            lit(column_name).alias("COLUMN_NAME"),
            lit(data_type).alias("DATA_TYPE"),
            lit(filter_type).alias("FILTER_TYPE"),
            lit(None).cast('STRING').alias("MIN_VALUE"),
            lit(None).cast('STRING').alias("MAX_VALUE"),
            lit(None).cast('STRING').alias("DISTINCT_COUNT"),
            lit(None).cast('ARRAY').alias("DISTINCT_VALUES")
        )
    
    return result_df
$$;
