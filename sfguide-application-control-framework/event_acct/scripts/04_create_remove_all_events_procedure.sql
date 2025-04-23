/*************************************************************************************************************
Procedure:          EVENTS.EVENTS.REMOVE_ALL_EVENTS
Create Date:        2024-02-29
Author:             Marc Henderson
Description:        This procedure removes the events table and warehouse from this account.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): DATABASE:   <APP_CODE>_EVENTS_FROM_<CURRENT_REGION> (all remaining removed)
                    SHARE:      <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>_SHARE (all remaining removed)
                    LISTING:    <APP_CODE>_EVENTS_FROM_<CURRENT_REGION> (all remaining removed)
                    DATABASE:   EVENTS (removed)
                    WAREHOUSE:  EVENTS_WH (removed)
Used By:            Provider
Parameter(s):       N/A
Usage:              CALL EVENTS.EVENTS.REMOVE_ALL_EVENTS();

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-02-29          Marc Henderson                      Initial build
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 04_create_remove_all_events_procedure.sql
!print **********

--set session vars
SET EVENT_WH = '&{APP_CODE}_EVENTS_WH';

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($EVENT_WH);
USE DATABASE EVENTS;

CREATE OR REPLACE PROCEDURE EVENTS.REMOVE_ALL_EVENTS()
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

    //unpublish, then drop remaining listing events listings
    snowflake.execute({sqlText: `SHOW LISTINGS LIKE '%_EVENTS_FROM_${current_region}';`});
    var rset = snowflake.execute({sqlText: `SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});

    while(rset.next()){
      var listing = rset.getColumnValue(1);

      //unpublish and drop listing
      snowflake.execute({sqlText: `ALTER LISTING ${listing} UNPUBLISH;`});
      snowflake.execute({sqlText: `DROP LISTING IF EXISTS ${listing};`});
    }

    //drop remaining shares
    snowflake.execute({sqlText: `SHOW SHARES LIKE '%_EVENTS_FROM_${current_region}_SHARE';`});
    var rset = snowflake.execute({sqlText: `SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});

    while(rset.next()){
      var share = rset.getColumnValue(1);

      //drop share
      snowflake.execute({sqlText: `DROP SHARE IF EXISTS ${share};`});
    }

    //drop remaining events databases shared to acf account
    snowflake.execute({sqlText: `SHOW DATABASES LIKE '%_EVENTS_FROM_${current_region}';`});
    var rset = snowflake.execute({sqlText: `SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});

    while(rset.next()){
      var database = rset.getColumnValue(1);

      //drop database
      snowflake.execute({sqlText: `DROP DATABASE IF EXISTS ${database};`});
    }


    //drop events database, including event table
    snowflake.execute({sqlText: `DROP DATABASE IF EXISTS EVENTS;`});

    //drop remaining events warehouses
    snowflake.execute({sqlText: `SHOW WAREHOUSES LIKE '%_EVENTS_WH';`});
    var rset = snowflake.execute({sqlText: `SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});

    while(rset.next()){
      var warehouse = rset.getColumnValue(1);

      //drop warehouse
      snowflake.execute({sqlText: `DROP WAREHOUSE IF EXISTS ${warehouse};`});
    }



    //return to least privilged role
    snowflake.execute({sqlText: `USE ROLE PUBLIC;`});

    return `Any remaining event listings and shares, events table, and warehouse have been removed.`;
    
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
!print End 04_create_remove_all_events_procedure.sql
!print **********