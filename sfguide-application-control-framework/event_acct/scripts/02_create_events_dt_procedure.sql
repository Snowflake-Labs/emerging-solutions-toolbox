/*************************************************************************************************************
Procedure:          EVENTS.EVENTS.EVENTS_DT
Create Date:        2023-10-19
Author:             Marc Henderson
Description:        This procedure creates a stream on the event table and tasks to insert events into the 
                    replicated/shared table.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): TABLE (DYNAMIC): <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>.EVENTS.EVENTS
Used By:            Provider
Parameter(s):       app_codes ARRAY - array of app_codes to remove app events for
Usage:              CALL EVENTS.EVENTS.EVENTS_DT(TO_ARRAY('<app_codes>'));

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-09          Marc Henderson                      Initial build
2024-02-29          Marc Henderson                      Added support for creating stream/tasks for multiple 
                                                        apps per run   
2024-11-11          Marc Henderson                      Migrated stream/task to dynamic tables (renamed 
                                                        script to 02_create_events_dt_procedure.sql)                                                 
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 02_create_events_dt_procedure.sql
!print **********

--set session vars
SET EVENT_WH = '&{APP_CODE}_EVENTS_WH';

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($EVENT_WH);
USE DATABASE EVENTS;

CREATE OR REPLACE PROCEDURE EVENTS.EVENTS_DT(app_codes ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"event_acct","component":"events_dt_proc","type":"procedure"}}'
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
    //get current region
    var rset = snowflake.execute({sqlText: `SELECT CURRENT_REGION();`});
    rset.next();
    let current_region = rset.getColumnValue(1);

    //create stream and tasks for each app code
    for (i = 0; i <= APP_CODES.length; i++) {
      //get app code
      let app_code = APP_CODES[i];

      //create dynamic table from
      snowflake.execute({sqlText:`CREATE OR REPLACE DYNAMIC TABLE ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS
                                      TARGET_LAG = '1 minutes'
                                      WAREHOUSE = ${app_code}_EVENTS_WH
                                      REFRESH_MODE = AUTO
                                      COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"event_acct","component":"${app_code}_events_from_${current_region}_dt","type":"dynamic_table"}}'
                                      AS
                                        SELECT
                                          timestamp
                                          ,start_timestamp
                                          ,observed_timestamp
                                          ,trace
                                          ,resource
                                          ,resource_attributes
                                          ,scope
                                          ,scope_attributes
                                          ,record_type
                                          ,record
                                          ,record_attributes
                                          ,value
                                          ,exemplars
                                        FROM EVENTS.EVENTS.EVENTS
                                        WHERE value:app_code = '&{APP_CODE}';`});


      snowflake.execute({sqlText:`GRANT SELECT ON DYNAMIC TABLE ${app_code}_EVENTS_FROM_${current_region}.EVENTS.EVENTS TO SHARE ${app_code}_EVENTS_FROM_${current_region}_SHARE;`});
    }

    return `Success`;
  
  } catch (err) {
    var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");
    return `Error: ${result}`;
  }
    
$$
;

--call EVENTS_DT
CALL EVENTS.EVENTS_DT(TO_ARRAY('&APP_CODE'));

--unset vars
UNSET (EVENT_WH);

!print **********
!print End 02_create_events_dt_procedure.sql
!print **********