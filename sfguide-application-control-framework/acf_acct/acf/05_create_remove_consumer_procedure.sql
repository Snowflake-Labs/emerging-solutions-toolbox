/*************************************************************************************************************
Procedure:          P_<APP_CODE>_ACF_DB.CONSUMER_MGMT.REMOVE_CONSUMER
Create Date:        2023-03-14
Author:             Marc Henderson
Description:        This procedure removes a Consumer's app-related objects and details.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): TABLE:      P_<APP_CODE>_ACF_DB.METADATA.METADATA (Consumer entries removed)
Used By:            Provider
Parameter(s):       account_locator VARCHAR - The Consumer's Snowflake Account Locator
                    consumer_name VARCHAR - the Consumer's Company Name
Usage:              CALL P_<APP_CODE>_ACF_DB.CONSUMER_MGMT.REMOVE_CONSUMER(
                      account_locator
                      ,consumer_name
                    );

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-03-14          Marc Henderson                      Initial build
2023-05-01          Marc Henderson                      Insert log messages into control_event_logs table. 
2023-05-11          Marc Henderson                      Streamlined object creation using APP_CODE parameter
2023-08-15          Marc Henderson                      Renamed P_<APP_CODE>APP_DB to P_<APP_CODE>ACF_DB
2023-10-27          Marc Henderson                      Removed log share related objects
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 05_create_remove_consumer_procedure.sql
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

CREATE OR REPLACE PROCEDURE CONSUMER_MGMT.REMOVE_CONSUMER(account_locator VARCHAR, consumer_name VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"consumer_remove","type":"procedure"}}'
EXECUTE AS CALLER
AS
$$
  try {
    //remove Consumer metadata
    snowflake.execute({sqlText: `DELETE FROM  P_&{APP_CODE}_ACF_DB.METADATA.METADATA WHERE UPPER(account_locator) = UPPER('${ACCOUNT_LOCATOR}') AND UPPER(consumer_name) = UPPER('${CONSUMER_NAME}');`});

    snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON('{"account":"${ACCOUNT_LOCATOR}", "consumer_name":"${CONSUMER_NAME}", "event_type":"removal", "timestamp":"'||SYSDATE()::string||'", "message":"Consumer removed."}');`});

    return `Consumer: ${CONSUMER_NAME} removed.`;
  } catch (err) {
    var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");
 

    snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON(\$\${"account":"${ACCOUNT_LOCATOR}", "consumer_name":"${CONSUMER_NAME}", "event_type":"removal", "timestamp":"\$\$||SYSDATE()::string||\$\$", "message":"${result}"}\$\$);`});

    return `Error: ${result}`;
    }
$$
;

--unset vars
UNSET (ACF_ADMIN_ROLE, ACF_WH, ACF_DB);

!print **********
!print End 05_create_remove_consumer_procedure.sql
!print **********