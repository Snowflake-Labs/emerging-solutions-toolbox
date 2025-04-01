/*************************************************************************************************************
Objects :           Application Control Framework Source Objects
Create Date:        2023-07-20
Author:             Marc Henderson
Description:        This script creates the source database that stores the application data, functions and
                    stored procedurs.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Objects(s):         DATABASE:           P_<APP_CODE>_SOURCE_DB_DEV
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.DATA
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.ARTIFACTS
                    STAGE:              P_<APP_CODE>_SOURCE_DB_DEV.ARTIFACTS.ARTIFACTS
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.APP
                    TABLE:              P_<APP_CODE>_SOURCE_DB_DEV.APP.APP_KEY
                    TABLE:              P_<APP_CODE>_SOURCE_DB_DEV.APP.APP_MODE
                    TABLE:              P_<APP_CODE>_SOURCE_DB_DEV.APP.LIMIT_TRACKER
                    TABLE:              P_<APP_CODE>_SOURCE_DB_DEV.APP.RUN_TRACKER
                    TABLE:              P_<APP_CODE>_SOURCE_DB_DEV.APP.METRICS
                    TABLE:              P_<APP_CODE>_SOURCE_DB_DEV.APP.TRUST_CENTER_CHECK
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.METADATA
                    VIEW (SECURE):      P_<APP_CODE>_SOURCE_DB_DEV.METADATA.METADATA_V
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.TRUST_CENTER
                    VIEW (SECURE):      P_<APP_CODE>_SOURCE_DB_DEV.TRUST_CENTER.SCANNERS_V
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.UTIL_APP
                    VIEW (SECURE):      P_<APP_CODE>_SOURCE_DB_DEV.UTIL_APP.METADATA_C_V
                    TABLE:              P_<APP_CODE>_SOURCE_DB_DEV.UTIL_APP.REQUEST_ID_TEMP
                    PROCEDURE (SECURE): P_<APP_CODE>_SOURCE_DB_DEV.UTIL_APP.APP_LOGGER
                    PROCEDURE (SECURE): P_<APP_CODE>_SOURCE_DB_DEV.UTIL_APP.METRICS_LOGGER
                    PROCEDURE (SECURE): P_<APP_CODE>_SOURCE_DB_DEV.UTIL_APP.CUSTOM_BILLING_EVENTS        
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.FUNCS_APP
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.PROCS_APP
                    SCHEMA:             P_<APP_CODE>_SOURCE_DB_DEV.RESULTS_APP
Copyright © 2025 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-07-20          Marc Henderson                      Initial build
2023-08-15          Marc Henderson                      Renamed P_<APP_CODE>APP_DB to P_<APP_CODE>ACF_DB
2024-02-29          Marc Henderson                      Created ARTIFACTS schema to store app-related files
                                                        (moved from the ACF_STREAMLIT schema in the ACF_DB)
2024-12-19          Marc Henderson                      Moved this script to be included as part of ACF Setup
*************************************************************************************************************/

!print **********
!print Begin 07_create_dev_env_objects.sql
!print **********

!set exit_on_error=True;
!set variable_substitution=true;

--set parameter for admin role
SET ACF_ADMIN_ROLE = 'P_&{APP_CODE}_ACF_ADMIN';

--set parameter for source db
SET SOURCE_DB = 'P_&{APP_CODE}_SOURCE_DB_DEV';

--set parameter for app warehouse
SET ACF_WH = 'P_&{APP_CODE}_ACF_WH';

USE ROLE IDENTIFIER($ACF_ADMIN_ROLE);
USE WAREHOUSE IDENTIFIER($ACF_WH);

--source data db
CREATE DATABASE IF NOT EXISTS IDENTIFIER($SOURCE_DB);

USE DATABASE IDENTIFIER($SOURCE_DB);
CREATE SCHEMA IF NOT EXISTS ARTIFACTS;
CREATE SCHEMA IF NOT EXISTS DATA;
CREATE SCHEMA IF NOT EXISTS APP;
CREATE SCHEMA IF NOT EXISTS METADATA;
CREATE SCHEMA IF NOT EXISTS TRUST_CENTER;
CREATE SCHEMA IF NOT EXISTS UTIL_APP; 
CREATE SCHEMA IF NOT EXISTS FUNCS_APP;
CREATE SCHEMA IF NOT EXISTS PROCS_APP;
CREATE SCHEMA IF NOT EXISTS RESULTS_APP;

USE SCHEMA ARTIFACTS;

--create ARTIFACTS stage
CREATE STAGE IF NOT EXISTS ARTIFACTS;

--put template files on stage
PUT 'file://&{DIR}/application_control_framework/acf_acct/dev_env/templates/*' @ARTIFACTS/templates auto_compress=false overwrite=true;

--create APP_KEY table
CREATE TABLE IF NOT EXISTS APP.APP_KEY(app_key VARCHAR) AS
SELECT 'TEST_' || ENCRYPT(HASH(CURRENT_ACCOUNT()||UPPER(CURRENT_DATABASE())||DATEADD(YEAR, 1119, SYSDATE())), UUID_STRING())::string;
ALTER TABLE APP.APP_KEY SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"app_key","type":"table"}';

--create APP_MODE table
CREATE TABLE IF NOT EXISTS APP.APP_MODE(key VARCHAR, value VARCHAR);
ALTER TABLE APP.APP_MODE SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"app_mode","type":"table"}';

INSERT INTO APP.APP_MODE VALUES 
('app_mode', 'free')
,('events_shared', 'n');

--create METRICS table
CREATE TABLE IF NOT EXISTS APP.METRICS(msg VARIANT);
ALTER TABLE APP.METRICS SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"metrics","type":"table"}';

--create LIMIT_TRACKER table
CREATE TABLE IF NOT EXISTS APP.LIMIT_TRACKER(key VARCHAR, value VARCHAR);
ALTER TABLE APP.LIMIT_TRACKER SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"limit_tracker","type":"table"}';

INSERT INTO APP.LIMIT_TRACKER VALUES 
('total_requests', '0')
,('requests_processed_this_interval', '0')
,('input_records', '0')
,('input_records_this_interval', '0')
,('total_records_processed', '0')
,('records_processed_this_interval', '0')
,('total_matches', '0')
,('matches_this_interval', '0')
,('last_request_timestamp', '9998-01-01');

CREATE TABLE IF NOT EXISTS APP.RUN_TRACKER(timestamp TIMESTAMP_NTZ, request_id VARCHAR, request_type VARCHAR, input_table VARCHAR, output_table VARCHAR);
ALTER TABLE APP.RUN_TRACKER SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"run_tracker","type":"table"}';

--create TRUST_CENTER_CHECK table
CREATE TABLE IF NOT EXISTS APP.TRUST_CENTER_CHECK(key VARCHAR, value VARCHAR) AS
WITH trust_center_check_cte AS (
select $1 AS key
        ,$2 as value
    FROM 
    (VALUES  
      ('last_check_timestamp', '9999-01-01')
     ,('risk_count', '0')
     ,('affected_scanners', '')
    )
)
SELECT * FROM trust_center_check_cte;
ALTER TABLE APP.TRUST_CENTER_CHECK SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"trust_center_check","type":"table"}';

--create metadata_v view
CREATE OR REPLACE SECURE VIEW METADATA.METADATA_V AS 
SELECT * FROM IDENTIFIER('P_&{APP_CODE}_ACF_DB.METADATA.METADATA');
ALTER VIEW METADATA.METADATA_V SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"metadata_v","type":"view"}';

--create scanners_v view
CREATE OR REPLACE SECURE VIEW TRUST_CENTER.SCANNERS_V AS 
SELECT * FROM IDENTIFIER('P_&{APP_CODE}_ACF_DB.TRUST_CENTER.SCANNERS');
ALTER VIEW TRUST_CENTER.SCANNERS_V SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"scanners_v","type":"view"}';

--create METADATA_C_V view for consumer
CREATE OR REPLACE SECURE VIEW UTIL_APP.METADATA_C_V AS 
SELECT * FROM IDENTIFIER('P_&{APP_CODE}_ACF_DB.METADATA.METADATA') 
WHERE UPPER(account_locator) = UPPER(CURRENT_ACCOUNT()) AND UPPER(key) NOT IN (SELECT UPPER(control_name) FROM IDENTIFIER('P_&{APP_CODE}_ACF_DB.METADATA.METADATA_DICTIONARY') WHERE consumer_visible = FALSE);
ALTER VIEW UTIL_APP.METADATA_C_V SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"metadata_c_v","type":"view"}';

--create REQUEST_ID_TEMP table
CREATE OR REPLACE TABLE UTIL_APP.REQUEST_ID_TEMP (request_id VARCHAR) AS SELECT UUID_STRING();
ALTER TABLE UTIL_APP.REQUEST_ID_TEMP SET COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"request_id_temp","type":"table"}';

--create app_logger procedure to log messages
CREATE OR REPLACE PROCEDURE UTIL_APP.APP_LOGGER(account_locator VARCHAR, consumer_name VARCHAR, app_key VARCHAR, app_mode VARCHAR, entry_type VARCHAR, event_type VARCHAR, event_attributes VARCHAR, timestamp TIMESTAMP_NTZ, status VARCHAR, message VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"app_logger","type":"procedure"}}'
  EXECUTE AS OWNER
  AS 
  $$
  
  try {
      //construct log msg
      var rset = snowflake.execute({sqlText: `SELECT PARSE_JSON(
                                        '{
                                            "account":"${ACCOUNT_LOCATOR}",
                                            "app_code":"&{APP_CODE}",
                                            "consumer_name":"${CONSUMER_NAME}",
                                            "app_key":"${APP_KEY}",
                                            "app_mode":"${APP_MODE}",
                                            "entry_type":"${ENTRY_TYPE}",
                                            "event_type":"${EVENT_TYPE}",
                                            "event_attributes":${EVENT_ATTRIBUTES.replace(/\'/g, "\\'")},
                                            "timestamp":"'||SYSDATE()||'",
                                            "status":"${STATUS}",
                                            "message":'||TRIM(\$\$ ${MESSAGE}\$\$)||'
                                        }');`});
      rset.next()
      var msg = rset.getColumnValue(1);
      
      //add new msg to event table
      snowflake.log('info', msg);

      return `Logged: ${MESSAGE}`;
  } catch (err) {
    var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");

    //log error
    snowflake.log('error', `${result}`);
 
    return `Error: ${result}`;
  }
  $$
;

--create metrics_logger procedure to log stats to the metrics table
CREATE OR REPLACE PROCEDURE UTIL_APP.METRICS_LOGGER(account_locator VARCHAR, consumer_name VARCHAR, app_key VARCHAR, app_mode VARCHAR, entry_type VARCHAR, event_type VARCHAR, event_attributes VARCHAR, timestamp TIMESTAMP_NTZ, status VARCHAR, message VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"metrics_logger","type":"procedure"}}'
  EXECUTE AS OWNER
  AS 
  $$
  
  try {
      //add new msg to metrics table
      snowflake.execute({sqlText: `INSERT INTO APP.METRICS(msg) SELECT PARSE_JSON(
                                        '{
                                            "account":"${ACCOUNT_LOCATOR}",
                                            "app_code":"&{APP_CODE}",
                                            "consumer_name":"${CONSUMER_NAME}",
                                            "app_key":"${APP_KEY}",
                                            "app_mode":"${APP_MODE}",
                                            "entry_type":"${ENTRY_TYPE}",
                                            "event_type":"${EVENT_TYPE}",
                                            "event_attributes":${EVENT_ATTRIBUTES.replace(/\'/g, "\\'")},
                                            "timestamp":"'||SYSDATE()||'",
                                            "status":"${STATUS}",
                                            "message":'||TRIM(\$\$ ${MESSAGE}\$\$)||'
                                        }')`});

      return `Logged: ${MESSAGE}`;
  } catch (err) {
    var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");

    //log error
    snowflake.log('error', `${result}`);
 
    return `Error: ${result}`;
  }
  $$
;

CREATE OR REPLACE PROCEDURE UTIL_APP.CUSTOM_EVENT_BILLING(class_name VARCHAR, subclass_name VARCHAR, start_timestamp FLOAT, timestamp FLOAT, base_charge FLOAT, objects VARCHAR, additional_info VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"dev","component":"consumer_event_billing","type":"procedure"}}'
  EXECUTE AS OWNER
  AS
  $$
    /**
      * Format timestamps as Unix timestamps in milliseconds
    */

    try {
      var rset = snowflake.execute({sqlText: `SELECT SYSTEM$CREATE_BILLING_EVENT('${CLASS_NAME}',
                                                    '${SUBCLASS_NAME}',
                                                    ${START_TIMESTAMP},
                                                    ${TIMESTAMP},
                                                    ${BASE_CHARGE},
                                                    '${OBJECTS}',
                                                    '${ADDITIONAL_INFO}');`});
      rset.next()
      var msg = rset.getColumnValue(1);

      return msg;

      } catch(err) {
          var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");

          //log error
          snowflake.log('error', `${result}`);
      
          return `Error: ${result}`;
      }
  $$
  ;

--unset vars
UNSET (ACF_ADMIN_ROLE, SOURCE_DB, ACF_WH);

!print **********
!print End 07_create_dev_env_objects.sql
!print **********