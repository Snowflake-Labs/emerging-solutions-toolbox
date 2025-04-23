/*************************************************************************************************************
Procedure:          P_<APP_CODE>_ACF_DB.UTIL.REMOVE_ACF
Create Date:        2023-03-14
Author:             Marc Henderson
Description:        This procedure removes a Provider's app and package from their Snowflake Account.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): DATABASE:   P_<APP_CODE>_ACF_DB
                    DATABASE:   P_<APP_CODE>_APP_PKG
                    WAREHOUSE:  (ACF_WH)
                    ROLE:       (ACF_ADMIN_ROLE)
Used By:            Provider
Parameter(s):       N/A
Usage:              CALL P_<APP_CODE>_ACF_DB.UTIL.REMOVE_ACF();

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-03-14          Marc Henderson                      Initial build
2023-05-01          Marc Henderson                      Now removes all active consumer objects.
2023-05-11          Marc Henderson                      Streamlined object creation using APP_CODE parameter 
2023-08-15          Marc Henderson                      Renamed P_<APP_CODE>APP_DB to P_<APP_CODE>ACF_DB 
2023-10-27          Marc Henderson                      Removed log share related objects
2024-03-06          Marc Henderson                      Renamed to 06_create_remove_acf_procedure.sql
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 06_create_remove_acf_procedure.sql
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

CREATE OR REPLACE PROCEDURE UTIL.REMOVE_ACF()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"app_remove","type":"procedure"}}'
EXECUTE AS CALLER
AS
$$
  try {
    //drop all remaining application packages
    snowflake.execute({sqlText: `SHOW DATABASES LIKE 'P_&{APP_CODE}_APP_PKG_%';`});
    var rset = snowflake.execute({sqlText: `SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});

    while(rset.next()){
      var app_pkg = rset.getColumnValue(1);

      //drop app pkg db
      snowflake.execute({sqlText: `DROP DATABASE IF EXISTS ${app_pkg};`});

    }

    //drop all event dbs
    snowflake.execute({sqlText: `SHOW DATABASES LIKE '&{APP_CODE}_EVENTS_FROM_%';`});
    var rset = snowflake.execute({sqlText: `SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});

    while(rset.next()){
      var events_db = rset.getColumnValue(1);

      //drop events_db
      snowflake.execute({sqlText: `DROP DATABASE IF EXISTS ${events_db};`});

    }

    //drop dev source db
    snowflake.execute({sqlText: `DROP DATABASE IF EXISTS  P_&{APP_CODE}_SOURCE_DB_DEV;`});

    //drop prod source db
    snowflake.execute({sqlText: `DROP DATABASE IF EXISTS  P_&{APP_CODE}_SOURCE_DB_PROD;`});

    //drop acf db
    snowflake.execute({sqlText: `DROP DATABASE IF EXISTS  P_&{APP_CODE}_ACF_DB;`});

    //drop acf warehouse
    snowflake.execute({sqlText: `DROP WAREHOUSE IF EXISTS P_&{APP_CODE}_ACF_WH;`});

    //drop native app admin role
    snowflake.execute({sqlText: `USE ROLE ACCOUNTADMIN;`});
    snowflake.execute({sqlText: `DROP ROLE IF EXISTS P_&{APP_CODE}_ACF_ADMIN;`});
    
    //return to least privilged role
    snowflake.execute({sqlText: `USE ROLE PUBLIC;`});

    return `App: P_&{APP_CODE}_ACF_DB removed`;
    
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
UNSET (ACF_ADMIN_ROLE, ACF_WH, ACF_DB);

!print **********
!print End 06_create_remove_acf_procedure.sql
!print **********