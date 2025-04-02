/*************************************************************************************************************
Script:             Consumer initialization
Create Date:        2023-10-24
Author:             B. Klein
Description:        Initializes consumer-side for Doctor Bernard Data Mapper
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-24          B. Klein                            Initial Creation
*************************************************************************************************************/

/* cleanup */
/*
use role accountadmin;
alter listing if exists drb_data_mapper_app_share set state = unpublished;
drop listing if exists drb_data_mapper_app_share;
drop share if exists drb_data_mapper_share;
drop application if exists drb_data_mapper_app;
drop database if exists drb_customer_sample_db;
*/

/* set up roles */
use role accountadmin;
call system$wait(5);


/* create warehouse */
create warehouse if not exists drb_data_mapper_wh 
comment='{"origin":"sf_sit","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';


/* create role and add permissions required by role for installation of framework */
create role if not exists drb_data_mapper_role;


/* perform grants */
set myusername = current_user();
grant role drb_data_mapper_role to user identifier($myusername);
grant role drb_data_mapper_role to role accountadmin;
grant create database on account to role drb_data_mapper_role;
grant execute task on account to role drb_data_mapper_role;
grant role drb_data_mapper_role to role sysadmin;
grant usage, operate on warehouse drb_data_mapper_wh to role drb_data_mapper_role;
grant imported privileges on database snowflake to role drb_data_mapper_role;

/* needed for shareback, not needed by app */
grant create share on account to role drb_data_mapper_role;


/* set role and warehouse */
use role drb_data_mapper_role;
call system$wait(5);
use warehouse drb_data_mapper_wh;


/* create sample customer data - this will be the raw data we map to our targets */
create or replace database drb_customer_sample_db;
create or replace schema drb_customer_sample_db.sample_data;
drop schema if exists drb_customer_sample_db.public;
create or replace table drb_customer_sample_db.sample_data.sample_base_table (
    base_table_record_col varchar,
    base_table_varchar_col varchar,
    base_table_number_col number,
    base_table_bool_col boolean,
    base_table_date_col date,
    base_table_timestamp_col timestamp,
    constraint pkey primary key (base_table_record_col) not enforced
);
create or replace table drb_customer_sample_db.sample_data.sample_agg_table (
    base_table_record_col varchar,
    agg_table_record_col varchar,
    agg_table_varchar_col varchar,
    agg_table_number_col number,
    agg_table_bool_col boolean,
    agg_table_date_col date,
    agg_table_timestamp_col timestamp,
    constraint pkey primary key (base_table_record_col, agg_table_record_col) not enforced,
    constraint fkey_1 foreign key (base_table_record_col) references drb_customer_sample_db.sample_data.sample_base_table (base_table_record_col) not enforced
);
insert into drb_customer_sample_db.sample_data.sample_base_table values (
    'my_record',
    'some_varchar',
    7,
    true,
    '2023-01-01',
    current_timestamp()
);
insert into drb_customer_sample_db.sample_data.sample_agg_table values (
    'my_record',
    'my_agg_record',
    'agg_me',
    99.9,
    false,
    '2022-06-01',
    current_timestamp()
);
insert into drb_customer_sample_db.sample_data.sample_agg_table values (
    'my_record',
    'my_agg_record_2',
    'agg_me',
    0.1,
    false,
    '2022-06-01',
    current_timestamp()
);


/* we now have a customer database with some customer source data available.. no app yet */

/* INSTALL APP */

/* GRANT APP ACCESS TO SOURCE DATABASE */

/* GRANT CREATE DATABASE TO APP */


/* database that will be created by the app - must be separate from app to be included in a share */
/* dropping listing and share if exists, as database 'replace' will fail if the db is included in the share*/
alter listing if exists drb_data_mapper_app_share set state = unpublished;
drop listing if exists drb_data_mapper_app_share;
drop share if exists drb_data_mapper_share;

/* sharing testing - will be performed by the consumer through Streamlit */
/* Note - the Listing API is in private preview and only for non-production use cases */
/* The native app can help users walk through using the GUI, which is GA, and the Listing API added later */
create or replace share drb_data_mapper_share;
grant usage on database drb_data_mapper_share_db to share drb_data_mapper_share;
grant reference_usage on database drb_data_mapper_db to share drb_data_mapper_share;
grant usage on schema drb_data_mapper_share_db.configuration to share drb_data_mapper_share;
grant usage on schema drb_data_mapper_share_db.shared to share drb_data_mapper_share;

grant select on view drb_data_mapper_share_db.configuration.source_collection_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_collection_filter_condition_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_entity_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_entity_join_condition_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_entity_attribute_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_to_target_mapping_vw to share drb_data_mapper_share;

call drb_data_mapper_share_db.utility.share_views();



/* FOR NON-PRODUCTION - feel free to use the Listing API to automate during development, but the Listing API is still in preview */
/* 
create listing drb_data_mapper_app_share in data exchange SNOWFLAKE_DATA_MARKETPLACE
for share drb_data_mapper_share as
$$
 title: "Data Mapping App Share"
 description: "The shareback from the Data Mapper App"
 terms_of_service:
   type: "OFFLINE"
 auto_fulfillment:
   refresh_schedule: "10 MINUTE"
   refresh_type: "FULL_DATABASE"
 targets:
   accounts: ["ORG_NAME.ACCOUNT_NAME"]
$$;

alter listing  drb_data_mapper_app_share set state = published;

show listings like 'drb_data_mapper_app_share' in data exchange snowflake_data_marketplace;

select 'Find LISTING_GLOBAL_NAME in consumer_init.sql and replace with ' || "global_name" || ' and then run code in script file consumer_init.sql on the consumer account' as DO_THIS_NEXT from table(result_scan(last_query_id()));
*/