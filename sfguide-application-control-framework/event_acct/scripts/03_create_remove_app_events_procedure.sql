/*************************************************************************************************************
Procedure:          EVENTS.EVENTS.REMOVE_APP_EVENTS
Create Date:        2024-02-29
Author:             Marc Henderson
Description:        This procedure removes the events objects from this account.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): LISTING:    <APP_CODE>_EVENTS_FROM_<CURRENT_REGION> (removed)
                    SHARE:      <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>_SHARE (removed)
                    DATABASE:   <APP_CODE>_EVENTS_FROM_<CURRENT_REGION> (removed)
Used By:            Provider
Parameter(s):       app_codes ARRAY - array of app_codes to remove app events for
Usage:              CALL EVENTS.EVENTS.REMOVE_APP_EVENTS(TO_ARRAY('<app_codes>'));

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-02-29          Marc Henderson                      Initial build
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 03_create_remove_app_events_procedure.sql
!print **********

--set session vars
SET EVENT_WH = '&{APP_CODE}_EVENTS_WH';

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($EVENT_WH);
USE DATABASE EVENTS;

CREATE OR REPLACE PROCEDURE EVENTS.REMOVE_APP_EVENTS(app_codes ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"event_acct","component":"remove_events","type":"procedure"}}'
EXECUTE AS CALLER
AS
$$
  try {
    //get current region
    var rset = snowflake.execute({sqlText: `SELECT CURRENT_REGION();`});
    rset.next();
    let current_region = rset.getColumnValue(1);

    //remove objects for each app code in the array
    for(i = 0; i <= APP_CODES.length; i++) {
      //get app code
      let app_code = APP_CODES[i];

      //unpublish, then delete listing events listing
      snowflake.execute({sqlText: `ALTER LISTING ${app_code}_EVENTS_FROM_${current_region} UNPUBLISH;`});
      snowflake.execute({sqlText: `DROP LISTING IF EXISTS ${app_code}_EVENTS_FROM_${current_region};`});

      //drop share
      snowflake.execute({sqlText: `DROP SHARE IF EXISTS ${app_code}_EVENTS_FROM_${current_region}_SHARE;`});

      //drop events database shared to acf account
      snowflake.execute({sqlText: `DROP DATABASE IF EXISTS ${app_code}_EVENTS_FROM_${current_region};`}); 

      //drop events warehouse
      snowflake.execute({sqlText: `DROP WAREHOUSE IF EXISTS ${app_code}_EVENTS_WH;`}); 
    }

    //return to least privilged role
    snowflake.execute({sqlText: `USE ROLE PUBLIC;`});

    return `Events for app(s): ${APP_CODES.toString()} removed`;
    
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

--unset vars
UNSET (EVENT_WH);

!print **********
!print End 03_create_remove_app_events_procedure.sql
!print **********