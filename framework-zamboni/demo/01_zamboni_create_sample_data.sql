/*SAMPLE DATA FROM DMM */

USE ROLE ZAMBONI_ROLE;
USE WAREHOUSE XS_WH;
USE SCHEMA ZAMBONI_DB.ZAMBONI_SRC;

--create sample input data
create or replace file format zamboni_db.zamboni_src.csv_infer
  TYPE = CSV
  PARSE_HEADER = true;

create or replace file format zamboni_db.zamboni_src.csv_load
  TYPE = CSV
  SKIP_HEADER = 1;

/* create stage for demo data */
create or replace stage zamboni_db.zamboni_src.sample_csv
  FILE_FORMAT = zamboni_db.zamboni_src.csv_load;

--NOTE: BEFORE executing the next steps, add the csv files located in the sample_data folder onto stage:  zamboni_db.zamboni_src.sample_csv

create or replace table zamboni_db.zamboni_src.inventory_on_hands
    using template (
        select array_agg(object_construct(*))
            from table(
                infer_schema(
                LOCATION=>'@zamboni_db.zamboni_src.sample_csv/inventoryOnHands.csv'
                , FILE_FORMAT=>'zamboni_db.zamboni_src.csv_infer'
                , IGNORE_CASE=>TRUE
                )
            )
    ) 
CHANGE_TRACKING = TRUE;

create or replace table zamboni_db.zamboni_src.inventory_transactions
    using template (
        select array_agg(object_construct(*))
            from table(
                infer_schema(
                LOCATION=>'@zamboni_db.zamboni_src.sample_csv/inventoryTransactions.csv'
                , FILE_FORMAT=>'zamboni_db.zamboni_src.csv_infer'
                , IGNORE_CASE=>TRUE
                )
            )
    ) 
CHANGE_TRACKING = TRUE;

create or replace table zamboni_db.zamboni_src.item_locations
    using template (
        select array_agg(object_construct(*))
            from table(
                infer_schema(
                LOCATION=>'@zamboni_db.zamboni_src.sample_csv/itemLocations.csv'
                , FILE_FORMAT=>'zamboni_db.zamboni_src.csv_infer'
                , IGNORE_CASE=>TRUE
                )
            )
    ) 
CHANGE_TRACKING = TRUE;

create or replace table zamboni_db.zamboni_src.items
    using template (
        select array_agg(object_construct(*))
            from table(
                infer_schema(
                LOCATION=>'@zamboni_db.zamboni_src.sample_csv/items.csv'
                , FILE_FORMAT=>'zamboni_db.zamboni_src.csv_infer'
                , IGNORE_CASE=>TRUE
                )
            )
    ) 
CHANGE_TRACKING = TRUE;

create or replace table zamboni_db.zamboni_src.locations
    using template (
        select array_agg(object_construct(*))
            from table(
                infer_schema(
                LOCATION=>'@zamboni_db.zamboni_src.sample_csv/locations.csv'
                , FILE_FORMAT=>'zamboni_db.zamboni_src.csv_infer'
                , IGNORE_CASE=>TRUE
                )
            )
    ) 
CHANGE_TRACKING = TRUE;


/* load data from files */
copy into zamboni_db.zamboni_src.inventory_on_hands from @zamboni_db.zamboni_src.sample_csv/inventoryOnHands.csv;
copy into zamboni_db.zamboni_src.inventory_transactions from @zamboni_db.zamboni_src.sample_csv/inventoryTransactions.csv;
copy into zamboni_db.zamboni_src.item_locations from @zamboni_db.zamboni_src.sample_csv/itemLocations.csv;
copy into zamboni_db.zamboni_src.items from @zamboni_db.zamboni_src.sample_csv/items.csv;
copy into zamboni_db.zamboni_src.locations from @zamboni_db.zamboni_src.sample_csv/locations.csv;


--create target table to incrementally update
CREATE OR REPLACE TABLE ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_INCREMENTAL (
RECORD_ID VARCHAR
,ITEM_ID VARCHAR
,LOCATION_ID VARCHAR
,PROJECT_NAME VARCHAR
,TYPE VARCHAR
,SUPPLY_DATE DATE
,BATCH_ID VARCHAR
,QUANTITY_SUM NUMBER(38,0)
,STORE_NAME VARCHAR
,START_DATE DATE
,TRANSACTION_CODE NUMBER(38,0)
);
