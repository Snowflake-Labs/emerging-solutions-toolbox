/*************************************************************************************************************
Procedure:          P_<APP_CODE>_ACF_DB.EVENTS.PROCESS_CONSUMER_EVENTS
Create Date:        2023-03-14
Author:             Marc Henderson
Description:        This procedure processes new events entered into the EVENTS_MASTER table.  This procedure
                    checks for installs and metrics from requests
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): TABLE:  P_<APP_CODE>_ACF_DB.METADATA.METADATA (installs and metric updates)
Used By:            Provider
Parameter(s):       N/A
Usage:              CALL P_<APP_CODE>_ACF_DB.EVENTS.PROCESS_CONSUMER_EVENTS();

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-27          Marc Henderson                      Initial build (replaces VERIFY_LOGS & VERIFY_METRICS)
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 03_create_process_consumer_events_procedure.sql
!print **********

--set parameter for admin role
SET ACF_ADMIN_ROLE = 'P_&{APP_CODE}_ACF_ADMIN';

--set parameter for warehouse
SET ACF_WH = 'P_&{APP_CODE}_ACF_WH';

--set parameter for app db
SET ACF_DB = 'P_&{APP_CODE}_ACF_DB';

USE ROLE IDENTIFIER($ACF_ADMIN_ROLE);
USE WAREHOUSE IDENTIFIER($ACF_WH);
USE DATABASE IDENTIFIER($ACF_DB);

CREATE OR REPLACE PROCEDURE EVENTS.PROCESS_CONSUMER_EVENTS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"process_consumer_events","type":"procedure"}}'
EXECUTE AS OWNER
AS
$$
  function update_install_metadata(consumer_account, consumer_name, app_key, app_mode, install_count, install_tmstmp) {
    //update app key
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = '${app_key}'
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                        AND LOWER(key) = 'app_key';`});
    
                                        
    //update install count
    install_count++;
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = '${install_count}'
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                        AND LOWER(key) = 'install_count';`});

    //update first install and limit reset timestamp
    if(install_count == 1) {
      snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                          SET value = '${install_tmstmp}'
                                          WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                          AND LOWER(consumer_name) = LOWER('${consumer_name}')  
                                          AND LOWER(key) = 'first_install_timestamp';`});

      //set limit reset timestamp
      var rset = snowflake.execute({sqlText: `SELECT value FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                              WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                              AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                              AND LOWER(key) = 'limit_interval';`});
      rset.next();
      let interval = rset.getColumnValue(1);

      if (interval.toLocaleLowerCase() != 'n/a') {
        snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                          SET value = '${install_tmstmp}'::timestamp_ntz + INTERVAL '${interval}'
                                          WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                          AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                          AND LOWER(key) = 'limit_reset_timestamp';`});
      }
    }


    //update last install timestamp
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = '${install_tmstmp}'
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}')  
                                        AND LOWER(key) = 'last_install_timestamp';`});
                                        
    snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON('{"account":"${consumer_account}", "consumer_name":"${consumer_name}", "event_type":"install_check", "timestamp":"'||SYSDATE()::string||'", "status":"INFO", "message":"New install total: ${install_count}"}');`});
  }

  function reset_counters_metadata(consumer_account, consumer_name, app_mode) {
    //reset counters
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = '0'
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                        AND LOWER(key) IN ('input_records_this_interval', 'requests_processed_this_interval', 'records_processed_this_interval', 'matches_this_interval');`});

    //set limit reset timestamp
    var rset = snowflake.execute({sqlText: `SELECT value FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                            WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                            AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                            AND LOWER(key) = 'limit_interval';`});
    rset.next();
    let interval = rset.getColumnValue(1);

    if (interval.toLocaleLowerCase() != 'n/a') {
      snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                          SET value = value::timestamp_ntz + INTERVAL '${interval}'
                                          WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                          AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                          AND LOWER(key) = 'limit_reset_timestamp';`});
    }
                                        
    snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON('{"account":"${consumer_account}", "consumer_name":"${consumer_name}", "event_type":"reset_counter_check", "timestamp":"'||SYSDATE()::string||'", "status":"INFO", "message":"Counters reset."}');`});
  }

  function update_request_metadata(consumer_account, consumer_name, request_tmstmp, app_key_metadata, app_mode) {
    //update requests_processed_this_interval and total_requests
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = TO_NUMBER(value) + 1
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                        AND LOWER(key) IN ('requests_processed_this_interval', 'total_requests');`});

    //update last request timestamp
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = '${request_tmstmp}'
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                        AND LOWER(key) = 'last_request_timestamp';`});
    
    //get new requests count
    var rset = snowflake.execute({sqlText: `SELECT value FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                            WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                            AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                            AND LOWER(key) = 'total_requests';`});
    rset.next();
    let total_requests = parseInt(rset.getColumnValue(1));
                                        
    snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON('{"account":"${consumer_account}", "consumer_name":"${consumer_name}", "event_type":"request_check", "timestamp":"'||SYSDATE()::string||'", "message":"New request total: ${total_requests}"}');`});
  }

  function update_record_count_metadata(consumer_account, consumer_name, updated_input_record_count, updated_record_counter, app_key_metadata, app_mode) {
    //update input_records and input_records_this_interval
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = TO_NUMBER(value) + ${updated_input_record_count}
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                        AND LOWER(key) IN ('input_records', 'input_records_this_interval');`});
    
    //update records_processed_this_interval and total_records_processed
    snowflake.execute({sqlText: `UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                        SET value = TO_NUMBER(value) + ${updated_record_counter}
                                        WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                        AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                        AND LOWER(key) IN ('records_processed_this_interval', 'total_records_processed');`});

    //get new record value
    var rset = snowflake.execute({sqlText: `SELECT value FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                            WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                            AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                            AND LOWER(key) = 'total_records_processed';`});
    rset.next();
    let total_records_processed = parseInt(rset.getColumnValue(1));
                                        
    snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON('{"account":"${consumer_account}", "consumer_name":"${consumer_name}", "event_type":"distinct_record_count_check", "timestamp":"'||SYSDATE()::string||'", "message":"New distinct record count total: ${total_records_processed}"}');`});
  }

  //TODO:  add match count logic, if applicable

  try {  
    var consumer_account = "";
    var consumer_name = "";

    //create temp log table from stream
    snowflake.execute({sqlText:`CREATE OR REPLACE TEMPORARY TABLE EVENTS.CONSUMER_EVENTS_TEMP(msg VARIANT);`});

    //advance the stream by inserting into temp table from stream
    snowflake.execute({sqlText:`INSERT INTO EVENTS.CONSUMER_EVENTS_TEMP SELECT msg FROM EVENTS.EVENTS_MASTER_STREAM;`});

    //install check
    var rset = snowflake.execute({sqlText: `SELECT 
                                              REPLACE(TO_JSON(msg:account)::string,'"','')
                                              ,REPLACE(TO_JSON(msg:consumer_name)::string,'"','')
                                              ,REPLACE(TO_JSON(msg:app_key)::string,'"','')
                                              ,REPLACE(TO_JSON(msg:app_mode)::string,'"','')
                                              ,REPLACE(TO_JSON(msg:timestamp),'"','')::timestamp_ntz::string
                                      FROM EVENTS.CONSUMER_EVENTS_TEMP
                                      WHERE TO_JSON(msg:event_type)::string ILIKE '%install%'
                                      AND TO_JSON(msg:message)::string ILIKE '%app key generated%'
                                      ORDER BY REPLACE(TO_JSON(msg:timestamp),'"','')::timestamp_ntz;`});
    while(rset.next()){
      //get consumer account
      consumer_account = rset.getColumnValue(1);

      //get consumer name
      consumer_name = rset.getColumnValue(2);

      //get app key
      let app_key = rset.getColumnValue(3);

      //get app mode
      let app_mode = rset.getColumnValue(4);

      //get last install timestamp
      let install_tmstmp = rset.getColumnValue(5);

      //check for installs (if paid, onboard consumer if not already), metrics and the reset counter msg
      //check if consumer is onboarded
      var rset = snowflake.execute({sqlText:`SELECT consumer_name FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA WHERE LOWER(account_locator) = LOWER('${consumer_account}') AND LOWER(consumer_name) = LOWER('${consumer_name}') LIMIT 1;`});

      if(rset.next()){
        if(install_tmstmp &&&& app_key){ 
          //check for existing app key (install)
          var rset2 = snowflake.execute({sqlText: `SELECT value FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA WHERE LOWER(key) = 'app_key' AND LOWER(account_locator) = LOWER('${consumer_account}') AND LOWER(consumer_name) = LOWER('${consumer_name}');`});

          if(rset2.next()){
            let app_key_metadata = rset2.getColumnValue(1);

            ///if the latest app_key from events is different from stored app_key (re-install) or there is no app key in metadata (new install)
            if(app_key_metadata.toLocaleLowerCase() != app_key.toLocaleLowerCase()) {
              //get install count
              var rset3 = snowflake.execute({sqlText: `SELECT MAX(value) FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                                      WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                                      AND LOWER(consumer_name) = LOWER('${consumer_name}')  
                                                      AND LOWER(key) = 'install_count';`});
              rset3.next();

              //update install metadata
              let install_count = parseInt(rset3.getColumnValue(1));
              update_install_metadata(consumer_account, consumer_name, app_key, app_mode, install_count, install_tmstmp);
            }
          }
        }
      } else {
        //onboard consumer if FREE
        if(app_mode.toLocaleLowerCase() == 'free') {
          snowflake.execute({sqlText:`CALL P_&{APP_CODE}_ACF_DB.CONSUMER_MGMT.ONBOARD_CONSUMER('${consumer_account}', '${consumer_name}', OBJECT_CONSTRUCT('app_mode', 'free'
                                                                                                                                    ,'allowed_procs', 'enrich'
                                                                                                                                    ,'limit','5'
                                                                                                                                    ,'limit_type','requests'
                                                                                                                                    ,'limit_interval', 'N/A'
                                                                                                                                    ,'limit_enforced','Y')::varchar)`});


          //get install count
          var rset3 = snowflake.execute({sqlText: `SELECT MAX(value) FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                                  WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                                  AND LOWER(consumer_name) = LOWER('${consumer_name}')  
                                                  AND LOWER(key) = 'install_count';`});
          rset3.next();

          //update install metadata
          let install_count = parseInt(rset3.getColumnValue(1));
          update_install_metadata(consumer_account, consumer_name, app_key, app_mode, install_count, install_tmstmp);
        }
        


        //onboard consumer if PAID
        if(app_mode.toLocaleLowerCase() == 'paid') {
          snowflake.execute({sqlText:`CALL P_&{APP_CODE}_ACF_DB.CONSUMER_MGMT.ONBOARD_CONSUMER('${consumer_account}', '${consumer_name}', OBJECT_CONSTRUCT('app_mode', 'paid'
                                                                                                                                    ,'allowed_procs', 'enrich'
                                                                                                                                    ,'limit','100000'
                                                                                                                                    ,'limit_type','records'
                                                                                                                                    ,'limit_interval', '30 day'
                                                                                                                                    ,'limit_enforced','Y')::varchar)`});

          //get install count
          var rset3 = snowflake.execute({sqlText: `SELECT MAX(value) FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                                  WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                                  AND LOWER(consumer_name) = LOWER('${consumer_name}')  
                                                  AND LOWER(key) = 'install_count';`});
          rset3.next();

          //update install metadata
          let install_count = parseInt(rset3.getColumnValue(1));
          update_install_metadata(consumer_account, consumer_name, app_key, app_mode, install_count, install_tmstmp);
        }
      }
    }



    //reset counter check
    var rset = snowflake.execute({sqlText: `SELECT 
                                              REPLACE(TO_JSON(msg:account)::string,'"','')
                                              ,REPLACE(TO_JSON(msg:consumer_name)::string,'"','')
                                              ,REPLACE(TO_JSON(msg:app_mode)::string,'"','')
                                            FROM EVENTS.CONSUMER_EVENTS_TEMP
                                            WHERE TO_JSON(msg:event_type)::string ILIKE '%reset_counters%';`});

    while (rset.next()){
      //get consumer account
      consumer_account = rset.getColumnValue(1);

      //get consumer name
      consumer_name = rset.getColumnValue(2);

      //get app mode
      let app_mode = rset.getColumnValue(3);

      //check if consumer is onboarded
      var rset2 = snowflake.execute({sqlText:`SELECT consumer_name FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA WHERE LOWER(account_locator) = LOWER('${consumer_account}') AND LOWER(consumer_name) = LOWER('${consumer_name}') LIMIT 1;`});

      if(rset2.next()){
        reset_counters_metadata(consumer_account, consumer_name, app_mode)
      }
    }



    //select metrics from temp table
    var rset = snowflake.execute({sqlText:`SELECT msg::varchar FROM EVENTS.CONSUMER_EVENTS_TEMP WHERE REPLACE(LOWER(TO_JSON(msg:entry_type)::string),'"','') = 'metric';`});

    while (rset.next()){
      let msg = rset.getColumnValue(1);

      //get msg object
      let msg_obj = JSON.parse(msg);

      //get consumer account
      consumer_account = msg_obj.account;

      //get consumer name
      consumer_name = msg_obj.consumer_name;

      //get app mode
      let app_mode = msg_obj.app_mode;

      //get table
      let metric_type = msg_obj.message.metric_type;

      //get status
      let status = msg_obj.status;

      //set input_record_count
      let input_record_count = 0;

      //set results_record_count
      let results_record_count = 0;

      //set match_count
      let match_count = 0;

      //set completed timestamp
      let completed_ts = '';

      //check if consumer is onboarded
      var rset2 = snowflake.execute({sqlText:`SELECT consumer_name FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA WHERE LOWER(account_locator) = LOWER('${consumer_account}') AND LOWER(consumer_name) = LOWER('${consumer_name}') LIMIT 1;`});

      if(rset2.next()){
        if(metric_type.toLocaleLowerCase().includes("request_summary")){
          if (status.toLocaleLowerCase() == 'complete') {
            //get stored app key
            var rset3 = snowflake.execute({sqlText: `SELECT value FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA 
                                                    WHERE LOWER(account_locator) = LOWER('${consumer_account}')
                                                    AND LOWER(consumer_name) = LOWER('${consumer_name}') 
                                                    AND LOWER(key) = 'app_key';`});
            rset3.next();
            let app_key_metadata = rset3.getColumnValue(1);

            //get input_record_count
            input_record_count = msg_obj.message.metrics.input_record_count;

            //get results_record_count
            results_record_count = msg_obj.message.metrics.results_record_count;

            //get completed_ts
            completed_ts = msg_obj.message.metrics.completed_ts;

            //update request metadata
            update_request_metadata(consumer_account, consumer_name, completed_ts, app_key_metadata, app_mode);
            update_record_count_metadata(consumer_account, consumer_name, input_record_count, results_record_count, app_key_metadata, app_mode);
          }
        }
        //TODO:  add match count logic, if applicable
      }
    }
    
    //drop temp table
    snowflake.execute({sqlText:`DROP TABLE EVENTS.CONSUMER_EVENTS_TEMP;`});

    return `Event Processing Complete`
    
  } catch (err) {
    let result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");

    snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON(\$\${"account":"${consumer_account}", "consumer_name":"${consumer_name}", "event_type":"logs_verification", "timestamp":"\$\$||SYSDATE()::string||\$\$", "message":"${result}"}\$\$);`});

    return `Error: ${result}`;
  }


$$
;


--create tasks to process events
CREATE OR REPLACE TASK EVENTS.PROCESS_CONSUMER_EVENTS_TASK_1 SCHEDULE = '1 minute' WHEN SYSTEM$STREAM_HAS_DATA('EVENTS.EVENTS_MASTER_STREAM') AS CALL EVENTS.PROCESS_CONSUMER_EVENTS();
CREATE OR REPLACE TASK EVENTS.PROCESS_CONSUMER_EVENTS_TASK_2 SCHEDULE = '1 minute' WHEN SYSTEM$STREAM_HAS_DATA('EVENTS.EVENTS_MASTER_STREAM') AS CALL EVENTS.PROCESS_CONSUMER_EVENTS();

--start tasks 30 seconds apart
ALTER TASK EVENTS.PROCESS_CONSUMER_EVENTS_TASK_1 RESUME;
call system$wait(30);
ALTER TASK EVENTS.PROCESS_CONSUMER_EVENTS_TASK_2 RESUME;

--unset vars
UNSET (ACF_ADMIN_ROLE, ACF_WH, ACF_DB);

!print **********
!print End 03_create_process_consumer_events_procedure.sql
!print **********
