/*************************************************************************************************************
Procedure:          P_<APP_CODE>_ACF_DB.CONSUMER_MGMT.ONBOARD_CONSUMER
Create Date:        2023-03-14
Author:             Marc Henderson
Description:        This procedure onboards a Consumer for app usage.  The Consumer's record limit and interval
                    are added to the Metadata table.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Affected object(s): TABLE:      P_<APP_CODE>_ACF_DB.METADATA.METADATA (Consumer-specific details inserted/updated)
Used By:            Provider
Parameter(s):       account_locator VARCHAR - The Consumer's Snowflake Account Locator
                    consumer_name VARCHAR - the Consumer's Company Name
                    controls VARCHAR - JSON payload, passed as VARCHAR, that contains the controls values to set
                                      Any values passed will overwrite defaults.  If no controls are passed,
                                      then defaults, as defined in the METADATA_DICTIONARY table, will be used.
Usage:              CALL P_<APP_CODE>_ACF_DB.CONSUMER_MGMT.ONBOARD_CONSUMER(
                      account_locator
                      ,consumer_name
                      ,controls
                    );

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-03-14          Marc Henderson                      Initial build
2023-05-01          Marc Henderson                      All metadata values to be added during onboarding.
2023-05-11          Marc Henderson                      Streamlined object creation using APP_CODE parameter
2023-05-15          Marc Henderson                      Now allows controls to be passed via JSON string,
                                                        allowing for custom controls to be passed, without
                                                        having to know what they are.
2023-08-08          Marc Henderson                      Added logic to account for the TEST_CONSUMER consumer
                                                        and the framework now creates a test consumer when
                                                        executed.
2023-08-15          Marc Henderson                      Renamed P_<APP_CODE>APP_DB to P_<APP_CODE>ACF_DB
2023-10-27          Marc Henderson                      Refactored for integrating event tables.  Removed
                                                        objects needed for log share check.
*************************************************************************************************************/

!set variable_substitution=true;

!print **********
!print Begin 04_create_onboard_consumer_procedure.sql
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

CREATE OR REPLACE PROCEDURE CONSUMER_MGMT.ONBOARD_CONSUMER(account_locator VARCHAR, consumer_name VARCHAR, controls VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"acf","component":"consumer_onboard","type":"procedure"}}'
EXECUTE AS CALLER
AS
$$
  try {

    //var to store onboard msg - if newly or already onboarded
    var msg = ``;  

    //check if controls are valid and replace with empty string if none are passed
    var controls = CONTROLS ? JSON.parse(CONTROLS) : '';

    var control_keys = Object.keys(controls).toString();

    var rset = snowflake.execute({sqlText:`SELECT value FROM TABLE(SPLIT_TO_TABLE(UPPER('${control_keys}'),',')) f 
                                            WHERE NOT EXISTS( 
                                              SELECT UPPER(d.control_name) FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA_DICTIONARY d WHERE UPPER(d.control_name) = UPPER(f.value) AND
                                                set_via_onboard = TRUE
                                            ) AND value <> '';`});

    if(rset.next() !== false){
      msg = `Invalid control(s):, please resubmit with valid control(s).`;
      return `ERROR:  ${msg}`;
    } else {
      //check if consumer has existing metadata
      var rset = snowflake.execute({sqlText:`SELECT consumer_name FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA WHERE UPPER(consumer_name) = UPPER('${CONSUMER_NAME}') LIMIT 1;`});

      msg = `Consumer: ${CONSUMER_NAME} already onboarded.  Metadata has been updated.`;  

      //create consumer objects and set default controls, if consumer is new
      if(!rset.next()){

        //set default controls
        snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.METADATA.METADATA
                                    SELECT '${ACCOUNT_LOCATOR}','${CONSUMER_NAME}', control_name, default_value FROM P_&{APP_CODE}_ACF_DB.METADATA.METADATA_DICTIONARY
                                    WHERE consumer_control = TRUE;`});

        msg = `Consumer: ${CONSUMER_NAME} successfully onboarded.`;
      }

      //update metadata
      Object.keys(controls).forEach(function(k){
        snowflake.execute({sqlText:`UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA SET value = '${controls[k]}' WHERE UPPER(key) = UPPER('${k}') AND UPPER(account_locator) = UPPER('${ACCOUNT_LOCATOR}') AND UPPER(consumer_name) = UPPER('${CONSUMER_NAME}');`});
      });

      snowflake.execute({sqlText:`UPDATE P_&{APP_CODE}_ACF_DB.METADATA.METADATA SET value = '${msg}' WHERE UPPER(key) = 'COMMENTS' AND UPPER(account_locator) = UPPER('${ACCOUNT_LOCATOR}') AND UPPER(consumer_name) = UPPER('${CONSUMER_NAME}');`});

      snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON('{"account":"${ACCOUNT_LOCATOR}", "consumer_name":"${CONSUMER_NAME}", "event_type":"onboard", "timestamp":"'||SYSDATE()::string||'", "message":"${msg}"}');`});
    
      return `${msg}`;
    }

  } catch (err) {
      var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");

      snowflake.execute({sqlText:`INSERT INTO P_&{APP_CODE}_ACF_DB.EVENTS.CONTROL_EVENTS(msg) SELECT PARSE_JSON(\$\${"account":"${ACCOUNT_LOCATOR}", "consumer_name":"${CONSUMER_NAME}", "event_type":"onboard", "timestamp":"\$\$||SYSDATE()::string||\$\$", "message":"${result}"}\$\$);`});

      return `Error: ${result}`;
  }
$$
;

--create a test consumer to be used to test the app locally in the provider account
CALL CONSUMER_MGMT.ONBOARD_CONSUMER('&{ACF_ACCOUNT_LOCATOR}', 'TEST_CONSUMER', OBJECT_CONSTRUCT('limit_enforced', 'N')::varchar);

--unset vars
UNSET (ACF_ADMIN_ROLE, ACF_WH, ACF_DB);

!print **********
!print End 04_create_onboard_consumer_procedure.sql
!print **********