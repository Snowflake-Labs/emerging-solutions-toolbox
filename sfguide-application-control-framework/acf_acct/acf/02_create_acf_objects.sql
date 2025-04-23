/*************************************************************************************************************
Objects :           Application Control Framework Objects
Create Date:        2023-03-14
Author:             Marc Henderson
Description:        This script creates the Application Control Framework objects
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Objects(s):         DATABASE:             P_<APP_CODE>_ACF_DB
                    SCHEMA:               EVENTS
                    PROCEDURE:            EVENTS.CREATE_EVENTS_DB_FROM_LISTING
                    PROCEDURE:            EVENTS.STREAM_TO_EVENT_MASTER
                    TABLE:                EVENTS.EVENTS_MASTER
                    STREAM:               EVENTA.EVENTS_MASTER_STREAM
                    TABLE:                EVENTS.CONTROL_EVENTS
                    STREAM:               EVENTS.<events_db>_EVENTS_STREAM
                    TASKS:                EVENTS.<events_db>_EVENTS_TASKS_i (1-2)
                    SCHEMA:               METRICS
                    VIEW:                 METRICS.REQUEST_SUMMARY_MASTER_V
                    SCHEMA:               METADATA
                    TABLE:                METADATA.METADATA_DICTIONARY
                    TABLE:                METADATA.RULES_DICTIONARY
                    TABLE:                METADATA.METADATA
                    SCHEMA:               UTIL
                    PROCEDURE:            UTIL.SOURCE_TABLE_PRIVILEGES
                    PROCEDURE:            UTIL.APP_PKG_SOURCE_VIEWS
                    SCHEMA:               CONSUMER_MGMT
                    SCHEMA:               ACF_STREAMLIT
                    STREAMLIT:            ACF_STREAMLIT.APP_CONTROL_MANAGER  
                    TABLE:                ACF_STREAMLIT.COMMANDS
                    PROCEDURE:            ACF_STREAMLIT.EXECUTE_CMD
                    STREAM:               ACF_STREAMLIT.COMMANDS_STREAM  
                    TASKS:                ACF_STREAMLIT.PROCESS_COMMANDS_i (0-12)
                    TABLE:                ACF_STREAMLIT.VERSION_HISTORY 
Used By:            Provider

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-03-14          Marc Henderson                      Initial build
2023-05-01          Marc Henderson                      New control_event_logs table, updated the
                                                        objects based on new logs/metrics variant format.
2023-05-11          Marc Henderson                      Streamlined object creation using APP_CODE parameter
2023-06-28          Marc Henderson                      Added the creation of the Streamlit UI
2023-08-15          Marc Henderson                      Renamed P_<APP_CODE>APP_DB to P_<APP_CODE>ACF_DB
2023-10-27          Marc Henderson                      Refactored for integrating event tables.
2023-11-29          Marc Henderson                      Renamed APP_WH TO ACF_WH and STREAMLIT schema to
                                                        ACF_STREAMLIT.  Update streamlit URIs
2023-12-05          Marc Henderson                      Added MOUNT_EVENT_SHARES proc to create a database
                                                        from each event share and create a stream and tasks to
                                                        stream events from the share to the EVENTS_MASTER 
                                                        table.
2024-01-26         Marc Henderson                       Created VERSION_HISTORY to track version patches
                                                        created by the ACF. MOUNT_EVENT_SHARES is removed.
2024-11-22         Marc Henderson                       Created TRUST_CENTER schema and SCANNERS table to 
                                                        store the Trust Center scanners the native app will
                                                        check and verify before granting consumers access to 
                                                        use the app. 
2025-01-07        Marc Henderson                        Added CREATE_EVENTS_DB_FROM_LISTING to
                                                        programmatically create region events dbs from 
                                                        listings.
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 02_create_acf_objects.sql
!print **********

--set parameter for admin role
SET ACF_ADMIN_ROLE = 'P_&{APP_CODE}_ACF_ADMIN';

--set parameter for warehouse
SET ACF_WH = 'P_&{APP_CODE}_ACF_WH';

--set parameter for app db
SET ACF_DB = 'P_&{APP_CODE}_ACF_DB';

--set parameter for app control manager streamlit 
SET ACF_STREAMLIT = 'P_&{APP_CODE}_APP_CONTROL_MANAGER';

USE ROLE IDENTIFIER($ACF_ADMIN_ROLE);
USE WAREHOUSE IDENTIFIER($ACF_WH);

-----APP DB
CREATE DATABASE IF NOT EXISTS IDENTIFIER($ACF_DB);

USE DATABASE IDENTIFIER($ACF_DB);

-----APP SCHEMAS
CREATE SCHEMA IF NOT EXISTS EVENTS;
CREATE SCHEMA IF NOT EXISTS METRICS;
CREATE SCHEMA IF NOT EXISTS METADATA;
CREATE SCHEMA IF NOT EXISTS TRUST_CENTER;
CREATE SCHEMA IF NOT EXISTS CONSUMER_MGMT;
CREATE SCHEMA IF NOT EXISTS UTIL;
CREATE SCHEMA IF NOT EXISTS ACF_STREAMLIT;


--CREATE EVENTS_MASTER TABLE
CREATE OR REPLACE TABLE EVENTS.EVENTS_MASTER(
  msg VARIANT
) CHANGE_TRACKING=TRUE
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"events_master","type":"table"}}';

--CREATE EVENTS_MASTER_STREAM
CREATE OR REPLACE STREAM EVENTS.EVENTS_MASTER_STREAM
  ON TABLE EVENTS.EVENTS_MASTER
  APPEND_ONLY = TRUE
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"events_master_stream","type":"stream"}}';

--CREATE CONTROL EVENTS TABLE
CREATE OR REPLACE TABLE EVENTS.CONTROL_EVENTS(
  msg VARIANT
)
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"control_event_logs","type":"table"}}';

--CREATE TABULAR VIEW OF CONSUMER_METRICS_MASTER TABLE
CREATE OR REPLACE VIEW METRICS.REQUEST_SUMMARY_MASTER_V COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"verification_metrics","type":"view"}}'
AS
SELECT
  MSG:account::VARCHAR account
  ,MSG:consumer_name::VARCHAR consumer_name
  ,MAX(MSG:entry_type) entry_type
  ,MSG:event_attributes[0]:request_id::VARCHAR request_id
  ,MAX(MSG:event_attributes[1]:proc_name::VARCHAR) proc_name
  ,MAX(MSG:event_attributes[2]:proc_parameters::VARCHAR) proc_parameters
  ,MAX(MSG:message:metrics:input_table_name::VARCHAR) input_table_name
  ,MAX(MSG:message:metrics:input_record_count::NUMBER(38,0)) input_record_count
  ,MAX(MSG:message:metrics:results_table_name::VARCHAR) results_table_name
  ,MAX(MSG:message:metrics:results_record_count::NUMBER(38,0)) results_record_count
  ,MAX(MSG:message:metrics:results_record_count_distinct::NUMBER(38,0)) results_record_count_distinct
  ,MIN(MSG:status::VARCHAR) status
  ,MAX(MSG:message:metrics:comments::VARCHAR) comments
  ,MAX(MSG:message:metrics:submitted_ts::VARCHAR) submitted_ts
  ,MAX(MSG:message:metrics:completed_ts::VARCHAR) completed_ts
FROM EVENTS.EVENTS_MASTER
WHERE UPPER(MSG:message:metric_type) = 'REQUEST_SUMMARY'
AND UPPER(MSG:entry_type) = 'METRIC'
GROUP BY 1,2,4
ORDER BY 1,2,4;

--CREATE METADATA_DICTIONARY TABLE
CREATE OR REPLACE TABLE METADATA.METADATA_DICTIONARY(
  control_name VARCHAR
  ,control_type VARCHAR
  ,condition VARCHAR
  ,default_value VARCHAR
  ,consumer_control BOOLEAN
  ,set_via_onboard BOOLEAN
  ,consumer_visible BOOLEAN
  ,description VARCHAR
)COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"metadata_dictionary","type":"procedure"}}';

--INSERT DEFAULT VALUES
INSERT INTO METADATA.METADATA_DICTIONARY VALUES
('app_mode','detective','=','enterprise',True,True,True,'the version of the app the Consumer has installed.')
,('allowed_procs','preventive','=','',True,True,True,'the comma-separated list of application stored procedures the Provider allows the Consumer to access (ideal for Providers that offer multiple types of functionality in their application)')
,('allowed_funcs','preventive','=','',True,True,True,'the comma-separated list of application user-defined functions the Provider allows the Consumer to access (ideal for Providers that offer multiple types of functionality in their application)')
,('record_cost','detective','=','0.05',True,True,True,'the per-record cost for using this solution.')
,('custom_billing','detective','=','N',True,True,True,'Y/N flag that determines whether custom billing is enabled for the Consumer.')
,('limit','preventive','<=','5000000',True,True,True,'the number of either records or requests the Consumer is allowed')
,('limit_type','preventive','=','records',True,True,True,'the type of limit (either records or requests)')
,('limit_interval','preventive','=','30 day',True,True,True,'the interval at which the limit is enforced (i.e. 1 day)')
,('limit_enforced','preventive','=','Y',True,True,True,'flag indicating whether to enforce request limits for the Consumer (ideal for Providers that create test Consumer accounts and do not want the limit to interfere with testing)')
,('custom_rules','preventive','=','',True,True,True,'flag indicating which custom rules (if any) should be enforced when making a request)')
,('managed','detective','=','N',True,True,True,'flag indicating whether the Consumer is managed via a Provider proxy Snowflake account (for those Consumers that are not on Snowflake)')
,('auto_enable','deterrent','=','Y',True,False,False,'flag indicating whether to auto-enable the Consumer once their log share is available to the Provider')
,('comments','detective','=','',True,False,True,'field to store applicable comments, i.e. why the Consumer has been disabled')
,('enabled','preventive','=','Y',True,False,True,'flag indicating whether the Consumer has been enabled to use the app')
,('app_key','preventive','=','',True,False,False,'unique key assigned to each install the Consumer makes')
,('install_count','detective','=','0',True,False,True,'number of times the app has been installed by the Consumer')
,('first_install_timestamp','detective','=','9998-01-01',True,False,True,'timestamp of first install by the Consumer')
,('last_install_timestamp','detective','=','9998-01-01',True,False,True,'timestamp of the most recent install by the Consumer')
,('input_records','detective','=','0',True,False,True,'the number of input records the Consumer has submitted')
,('input_records_this_interval','detective','=','0',True,False,True,'the number of input records submitted during the allotted period.  This gets reset to 0 at the allotted period')
,('total_requests','detective','=','0',True,False,True,'the number of requests made by the Consumer')
,('requests_processed_this_interval','detective','=','0',True,False,True,'the number of Consumer requests processed during the allotted period.  This gets reset to 0 at the allotted period')
,('last_request_timestamp','detective','=','9998-01-01',True,False,True,'timestamp of last request made by the Consumer')
,('total_records_processed','detective','=','0',True,False,True,'the number of Consumer records processed since installation')
,('records_processed_this_interval','detective','=','0',True,False,True,'the number of Consumer records processed during the allotted period.  This gets reset to 0 at the allotted period')
,('total_matches','detective','=','0',True,False,True,'the total number of matched records (if applicable) the Consumer has received since installing the app')
,('matches_this_interval','detective','=','0',True,False,True,'the number of matched records (if applicable) the Consumer has received during the allotted period.  This gets reset to 0 at the allotted period')
,('limit_reset_timestamp','detective','=','9998-01-01',True,False,True,'timestamp of when the counters will be reset');

--CREATE RULES_DICTIONARY TABLE
CREATE OR REPLACE TABLE METADATA.RULES_DICTIONARY(
  rule_name VARCHAR
  ,rule_type VARCHAR
  ,rule VARCHAR
  ,controls_used VARCHAR
  ,description VARCHAR
)COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"rules_dictionary","type":"table"}}';

--CREATE METADATA TABLE
CREATE OR REPLACE TABLE METADATA.METADATA(
  account_locator VARCHAR
  ,consumer_name VARCHAR
  ,key VARCHAR
  ,value VARCHAR
)
CHANGE_TRACKING = TRUE
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"consumer_metadata","type":"table"}}';

--INSERT GLOBAL VALUES
INSERT INTO METADATA.METADATA(account_locator, consumer_name, key, value) VALUES
('global', '', 'app_code', '&{APP_CODE}')
,('global', '', 'provider_account','&{ACF_ACCOUNT_LOCATOR}')
,('global', '', 'trust_center_enforcement','N')
,('global', '', 'trust_center_lookback_in_days','1')
--add any custom global values here 
;

--CREATE SCANNERS TABLE
CREATE OR REPLACE TABLE TRUST_CENTER.SCANNERS(
  scanner_package_id VARCHAR
  ,scanner_id VARCHAR
  ,scanner_name VARCHAR
  ,scanner_description VARCHAR
)
CHANGE_TRACKING = TRUE
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"scanners","type":"table"}}';


--CREATE PROCEDURE CREATE_EVENTS_DB_FROM_LISTING
CREATE OR REPLACE PROCEDURE EVENTS.CREATE_EVENTS_DB_FROM_LISTING()
  RETURNS ARRAY
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"create_events_db_from_listing","type":"procedure"}}'
  EXECUTE AS CALLER
  AS
  $$

    try{
      //create array to append each events db created
      let events_db_arr = [];

      //show listings shared with the ACF account
      snowflake.execute({sqlText: `SHOW AVAILABLE LISTINGS IS_SHARED_WITH_ME=TRUE;`});
      let rset = snowflake.execute({sqlText: `SELECT "global_name", "title", "is_ready_for_import", "is_imported" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE CONTAINS("title",'&{APP_CODE}_EVENTS_FROM_');`});

      while(rset.next()){
        let listing_id = rset.getColumnValue(1);
        let listing_title = rset.getColumnValue(2);
        let is_ready_for_import = rset.getColumnValue(3);
        let is_imported = rset.getColumnValue(4);

        if (is_ready_for_import == 'true' && is_imported == 'false') {
            //set events db name
            let events_db = `${listing_title}_SHARE`;
    
            //create region events db from listing
            snowflake.execute({sqlText: `CREATE DATABASE IF NOT EXISTS ${events_db} FROM LISTING '${listing_id}';`});
    
            //append events_db to array
            events_db_arr.push(events_db); 
        }  
      }

      return events_db_arr;

    } catch (err) {

      var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");
      return [`Error: ${result}`];
    }


  $$
  ;

--CREATE STREAM_TO_EVENT_MASTER PROC
CREATE OR REPLACE PROCEDURE EVENTS.STREAM_TO_EVENT_MASTER(events_dbs ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"stream_to_event_master","type":"procedure"}}'
EXECUTE AS CALLER
AS
$$
  function sleep(milliseconds) {
    const date = Date.now();
    let currentDate = null;
    do {
      currentDate = Date.now();
    } while (currentDate - date < milliseconds);
  }

  try {

    //create a stream on each listing event table in the array
    for(i = 0; i <= EVENTS_DBS.length; i++) {
      //get listing db
      let events_db = EVENTS_DBS[i];

      //set database
      snowflake.execute({sqlText:`USE DATABASE P_&{APP_CODE}_ACF_DB;`});

      //create a stream on the event table
      snowflake.execute({sqlText:`CREATE OR REPLACE STREAM EVENTS.${events_db}_EVENTS_STREAM
                                    ON DYNAMIC TABLE ${events_db}.EVENTS.EVENTS
                                    SHOW_INITIAL_ROWS = TRUE
                                    COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"events_db_stream","type":"stream"}}';`});

      //create tasks to insert event msg into master event table (one of the two tasks checks every 30 secs)
      for(i = 1; i <= 2; i++) {
        snowflake.execute({sqlText:`CREATE OR REPLACE TASK EVENTS.${events_db}_EVENTS_TASK_${i}
                                      SCHEDULE = '1 minute'
                                      COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"events_db_task_${i}","type":"task"}}'
                                      WHEN
                                        SYSTEM$STREAM_HAS_DATA('EVENTS.${events_db}_EVENTS_STREAM')
                                      AS
                                        EXECUTE IMMEDIATE
                                        \$\$
                                          BEGIN
                                            LET create_temp_table_stmt VARCHAR := 'CREATE OR REPLACE TABLE EVENTS.EVENTS_TEMP AS SELECT VALUE FROM ${events_db}_EVENTS_STREAM';
                                            
                                            EXECUTE IMMEDIATE :create_temp_table_stmt;

                                            LET insert_stmt VARCHAR := 'INSERT INTO EVENTS.EVENTS_MASTER(msg) SELECT VALUE FROM EVENTS.EVENTS_TEMP';
                                            EXECUTE IMMEDIATE :insert_stmt;

                                            LET drop_temp_table_stmt VARCHAR := 'DROP TABLE EVENTS.EVENTS_TEMP';
                                            EXECUTE IMMEDIATE :drop_temp_table_stmt;

                                            RETURN 'done';
                                          END;
                                        \$\$;
        `});

        //if this is not the first task, sleep 30 secs before starting the next task
        if (i > 1) {
          sleep(30000); 
        }
        snowflake.execute({sqlText:`ALTER TASK EVENTS.${events_db}_EVENTS_TASK_${i} RESUME;`});
      }
    }

    return `Success`;
  
  } catch (err) {
    var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");
    return `Error: ${result}`;
  }
    
$$
;

--CREATE PROCEDURE TO GRANT PRIVILEGES TO OBJECTS THE APP NEEDS, TO THE APP ADMIN ROLE
--THIS IS TO BE EXECUTED BY THE OWNER OF THE OBJECT(S)
CREATE OR REPLACE PROCEDURE UTIL.GRANTS_TO_DATA_OWNER(pkg_list ARRAY, role VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"grants_to_data_owner","type":"procedure"}}'
EXECUTE AS CALLER
AS
$$

  try{
    //grant privileges to APP_PKG_SOURCE_VIEWS to data owner role
    snowflake.execute({sqlText:`GRANT USAGE ON DATABASE P_&{APP_CODE}_ACF_DB TO ROLE ${ROLE};`});
    snowflake.execute({sqlText:`GRANT USAGE ON SCHEMA P_&{APP_CODE}_ACF_DB.UTIL TO ROLE ${ROLE};`});
    snowflake.execute({sqlText:`GRANT USAGE ON PROCEDURE P_&{APP_CODE}_ACF_DB.UTIL.APP_PKG_SOURCE_VIEWS(ARRAY,ARRAY,VARCHAR) TO ROLE ${ROLE};`});  

    for(i = 0; i < PKG_LIST.length; i++){
      var pkg = PKG_LIST[i];

      //grant usage on application package and DATA schema
      snowflake.execute({sqlText:`GRANT USAGE ON APPLICATION PACKAGE ${pkg} TO ROLE "${ROLE}";`});
      snowflake.execute({sqlText:`GRANT USAGE, CREATE VIEW ON SCHEMA ${pkg}.DATA TO ROLE "${ROLE}";`});
    }

    return `${priv} privileges granted to object(s): `+OBJ_LIST+`.`;

    
  } catch (err) {

    var result = `
    Failed: Code: `+err.code + `
    State: `+err.state+`
    Message: `+err.message+`
    Stack Trace:`+ err.stack;

    return `Error: ${result}`;
  }
$$
;

--CREATE PROCEDURE TO CREATE SECURE VIEWS AND ADD TO APPLICATION PACKAGE, WHEN CREATED.
CREATE OR REPLACE PROCEDURE UTIL.APP_PKG_SOURCE_VIEWS(table_list ARRAY, pkg_list ARRAY, action VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"app_pkg_source_views","type":"procedure"}}'
  EXECUTE AS CALLER
  AS
  $$

    try{
      //exit if action isn't GRANT or REVOKE
      if (!ACTION.match(/^(grant|revoke)$/i)) {
        return `Invalid action: ${ACTION}.  Please set this parameter to either GRANT or REVOKE, then resubmit`;  
      }

      //for each app_pkg create/grant or revoke/remove secure views depending on action
      for(i = 0; i < PKG_LIST.length; i++){
        var pkg = PKG_LIST[i];

        //create secure view for each source table in list
        for(i = 0; i < TABLE_LIST.length; i++){
          var tbl_fqn = TABLE_LIST[i];

          //get database, schema, and table
          var db = tbl_fqn.split(".")[0];
          var sch = tbl_fqn.split(".")[1];
          var tbl = tbl_fqn.split(".")[2];

          if(ACTION.toLocaleLowerCase() == 'grant') {
            //grant REFERENCE_USAGE to source database(s)
            snowflake.execute({sqlText:`GRANT REFERENCE_USAGE ON DATABASE ${db} TO SHARE IN APPLICATION PACKAGE ${pkg};`});

            //create secure view for application package
            snowflake.execute({sqlText:`CREATE OR REPLACE SECURE VIEW ${pkg}.DATA.${tbl} COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"native_app","component":"${tbl}","type":"secure_view"}}'
                                          AS SELECT * FROM ${db}.${sch}.${tbl};`});

            //grant view to application package
            snowflake.execute({sqlText:`GRANT SELECT ON VIEW ${pkg}.DATA.${tbl} TO SHARE IN APPLICATION PACKAGE ${pkg};`});
          }
            
          if (ACTION.toLocaleLowerCase() == 'revoke') {
            //revoke REFERENCE_USAGE from source database(s)
            snowflake.execute({sqlText:`REVOKE REFERENCE_USAGE ON DATABASE ${db} FROM SHARE IN APPLICATION PACKAGE ${pkg};`});

            //revoke view to from app package
            snowflake.execute({sqlText:`REVOKE SELECT ON VIEW ${pkg}.DATA.${tbl} FROM SHARE IN APPLICATION PACKAGE ${pkg};`});
            
            //create secure view for application package
            snowflake.execute({sqlText:`DROP VIEW ${pkg}.DATA.${tbl};`});
          }
        }
      }

      if(ACTION.toLocaleLowerCase() == 'grant') {
        return `Secure views created from table(s): ${TABLE_LIST.toString()} and granted to application package(s): ${PKG_LIST.toString()}.`;
      } else if (ACTION.toLocaleLowerCase() == 'revoke') {
        return `Views: ${TABLE_LIST} revoked from application package: ${PKG_LIST.toString()} and dropped.`;
      }
    } catch (err) {

      var result = `
      Failed: Code: `+err.code + `
      State: `+err.state+`
      Message: `+err.message+`
      Stack Trace:`+ err.stack;

      return `Error: ${result}`;
    }


  $$
  ;


USE SCHEMA ACF_STREAMLIT;

--CREATE STREAMLIT FOR APP
CREATE STAGE IF NOT EXISTS ACF_STREAMLIT;
CREATE STREAMLIT IF NOT EXISTS IDENTIFIER($ACF_STREAMLIT)
  ROOT_LOCATION = '@P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.ACF_STREAMLIT'
  MAIN_FILE = 'acf_streamlit.py'
  QUERY_WAREHOUSE = 'P_&{APP_CODE}_ACF_WH'
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"acf_streamlit_ui","type":"streamlit"}}';

--PUT ACF_STREAMLIT ITEMS ON STAGE
PUT 'file://&{DIR}/application_control_framework/acf_acct/acf_streamlit/acf_streamlit.py' @ACF_STREAMLIT auto_compress=false overwrite=true;
PUT 'file://&{DIR}/application_control_framework/acf_acct/acf_streamlit/acf/*' @ACF_STREAMLIT/acf auto_compress=false overwrite=true;
PUT 'file://&{DIR}/application_control_framework/acf_acct/acf_streamlit/img/*' @ACF_STREAMLIT/img auto_compress=false overwrite=true;


--CREATE TABLE TO LOAD CMDS THAT CANNOT BE EXECUTED BY SiS
CREATE OR REPLACE TABLE COMMANDS(cmd VARCHAR, timestamp TIMESTAMP_NTZ(9))
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"commands_table","type":"table"}}';

--CREATE PROCEDURE TO EXECUTE COMMANDS
CREATE OR REPLACE PROCEDURE EXECUTE_CMD()
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7}, "attributes":{"env":"acf","component":"commands_procedure","type":"procedure"}}'
  EXECUTE AS CALLER
  AS
  $$

    try{   
      //create temp table from stream
      snowflake.execute({sqlText:`CREATE OR REPLACE TEMPORARY TABLE ACF_STREAMLIT.COMMANDS_TEMP(cmd VARCHAR, timestamp TIMESTAMP_NTZ(9));`});

      //advance the stream by inserting into temp table from stream
      snowflake.execute({sqlText:`INSERT INTO ACF_STREAMLIT.COMMANDS_TEMP SELECT cmd, timestamp FROM ACF_STREAMLIT.COMMANDS_STREAM WHERE METADATA$ACTION = 'INSERT' ORDER BY timestamp ASC;`});

      //select commands from temp table
      var rset = snowflake.execute({sqlText:`SELECT cmd, timestamp FROM ACF_STREAMLIT.COMMANDS_TEMP;`});

      while (rset.next()){
        var cmd = rset.getColumnValue(1);
        var timestamp = rset.getColumnValue(2);

        //execute cmd
        snowflake.execute({sqlText:`${cmd}`});
      }

      return `Command(s) executed.`;

      
    } catch (err) {

      var result = `
      Failed: Code: `+err.code + `
      State: `+err.state+`
      Message: `+err.message+`
      Stack Trace:`+ err.stack;

      return `Error: ${result}`;
    }


  $$
  ;

--CREATE STREAM ON COMMANDS TABLE
CREATE OR REPLACE STREAM COMMANDS_STREAM
    ON TABLE COMMANDS
    APPEND_ONLY = TRUE
    COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"commands_stream","type":"stream"}}';

--CREATE TASKS TO PROCESS COMMANDS
CREATE OR REPLACE TASK PROCESS_COMMANDS_01 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_02 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_03 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_04 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_05 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_06 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_07 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_08 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_09 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_10 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_11 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();
CREATE OR REPLACE TASK PROCESS_COMMANDS_12 SCHEDULE = '1 minute'  WAREHOUSE = 'P_&{APP_CODE}_ACF_WH' WHEN  SYSTEM$STREAM_HAS_DATA('P_&{APP_CODE}_ACF_DB.ACF_STREAMLIT.COMMANDS_STREAM') AS call ACF_STREAMLIT.EXECUTE_CMD();

//ENABLE TASKS 5 SECONDS APART TO REDUCE WAIT TIME FOR COMMAND EXECUTION

ALTER TASK PROCESS_COMMANDS_01 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_02 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_03 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_04 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_05 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_06 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_07 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_08 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_09 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_10 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_11 RESUME;
call system$wait(5);
ALTER TASK PROCESS_COMMANDS_12 RESUME;

--CREATE VERSION HISTORY TABLE TO TRACK VERSIONS/PATCHES CREATED BY THE ACF
CREATE OR REPLACE TABLE VERSION_HISTORY(
    application_package VARCHAR
    ,version VARCHAR
    ,patch VARCHAR
    ,app_mode VARCHAR
    ,limit_enforced VARCHAR
    ,app_funcs_env VARCHAR
    ,app_funcs_list ARRAY
    ,app_procs_env VARCHAR
    ,app_procs_list ARRAY
    ,templates_env VARCHAR
    ,streamlit_env VARCHAR
    ,created_on TIMESTAMP_LTZ(9)
);


--UNSET VARS
UNSET (ACF_ADMIN_ROLE, ACF_WH, ACF_DB);
  
!print **********
!print End 02_create_acf_objects.sql
!print **********