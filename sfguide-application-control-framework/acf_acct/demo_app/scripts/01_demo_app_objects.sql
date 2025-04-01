/*************************************************************************************************************
Objects :           Application Control Framework Demo App Objects
Create Date:        2024-03-01
Author:             Marc Henderson
Description:        This script creates the source database that stores the application data, functions and
                    stored procedurs.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Objects(s):         TABLE:      P_<APP_CODE>_SOURCE_DB_DEV.DATA.CUSTOMER
                    FUNCTION:   P_<APP_CODE>_SOURCE_DB_DEV.FUNCS_APP.JS_FACTORIAL
                    PROCEDURE:  P_<APP_CODE>_SOURCE_DB_DEV.PROCS_APP.ENRICH
Copyright © 2025 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-03-01          Marc Henderson                      Initial build
*************************************************************************************************************/

!print **********
!print Begin 01_demo_app_objects.sql
!print **********

!set exit_on_error=True;
!set variable_substitution=true;

--set parameter for admin role
SET ACF_ADMIN_ROLE = 'P_&{APP_CODE}_ACF_ADMIN';

--set parameter for acf db
SET ACF_DB = 'P_&{APP_CODE}_ACF_DB';

--set parameter for source db
SET SOURCE_DB = 'P_&{APP_CODE}_SOURCE_DB_DEV';

--set parameter for app warehouse
SET ACF_WH = 'P_&{APP_CODE}_ACF_WH';

USE ROLE IDENTIFIER($ACF_ADMIN_ROLE);
USE WAREHOUSE IDENTIFIER($ACF_WH);

USE DATABASE IDENTIFIER($ACF_DB);

--set default allowed proc
UPDATE METADATA.METADATA_DICTIONARY SET default_value = 'enrich' WHERE LOWER(control_name) = 'allowed_procs';

USE DATABASE IDENTIFIER($SOURCE_DB);

--DEMO SOURCE DATA(GRAPH)
USE SCHEMA DATA;
CREATE OR REPLACE TABLE CUSTOMER COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"native_app","component":"customer","type":"table"}}'
AS 
SELECT 
    'user'||seq4()||'_'||UNIFORM(1, 3, RANDOM())||'@email.com' AS email,
    REPLACE(to_varchar(seq4() % 999, '000') ||'-'||to_varchar(seq4() % 888, '000')||'-'||to_varchar(seq4() % 777, '000')||UNIFORM(1, 10, RANDOM()),' ','') AS phone,
        CASE WHEN UNIFORM(1,10,RANDOM())>3 THEN 'MEMBER'
        WHEN UNIFORM(1,10,RANDOM())>5 THEN 'SILVER'
        WHEN UNIFORM(1,10,RANDOM())>7 THEN 'GOLD'
        ELSE 'PLATINUM' END AS status,
    ROUND(18+UNIFORM(0,10,RANDOM())+UNIFORM(0,50,RANDOM()),-1)+5*UNIFORM(0,1,RANDOM()) AS age_band,
    'REGION_'||UNIFORM(1,20,RANDOM()) AS region_code,
    UNIFORM(1,720,RANDOM()) AS days_active
FROM TABLE(GENERATOR(rowcount => 5000000));

--DEMO SOURCE FUNCTIONS
USE SCHEMA FUNCS_APP;
CREATE OR REPLACE FUNCTION JS_FACTORIAL(D DOUBLE)
  RETURNS DOUBLE
  LANGUAGE JAVASCRIPT
  STRICT
  COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"js_factorial","type":"function"}}}}'
  AS 
  $$
  if (D <= 0) {
    return 1;
  } else {
    var result = 1;
    for (var i = 2; i <= D; i++) {
      result = result * i;
    }
    return result;
  }
  $$
  ;

--DEMO SOURCE PROCEDURES
USE SCHEMA PROCS_APP;
CREATE OR REPLACE PROCEDURE ENRICH(INPUT_TABLE_NAME VARCHAR, MATCH_KEY VARCHAR, RESULTS_TABLE_NAME VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"enrich","type":"procedure"}}}}'
  EXECUTE AS OWNER
  AS
  $$
    try {
      //get request_id
      var rset = snowflake.execute({sqlText:`SELECT request_id FROM UTIL_APP.REQUEST_ID_TEMP;`});
      rset.next();
      let request_id = rset.getColumnValue(1);

      //set proc_parameters
      let proc_parameters_esc = `${INPUT_TABLE_NAME},${MATCH_KEY},${RESULTS_TABLE_NAME}`;

      //get consumer Snowflake Account Locator
      var rset = snowflake.execute({sqlText:`SELECT CURRENT_ACCOUNT();`});
      rset.next();
      let account_locator = rset.getColumnValue(1);

      //get local app key
      var rset = snowflake.execute({sqlText: `SELECT app_key FROM APP.APP_KEY;`});
      rset.next();
      let app_key_local = rset.getColumnValue(1);

      //check app_mode
      var rset = snowflake.execute({sqlText: `SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'app_mode';`});
      rset.next();
      let app_mode = rset.getColumnValue(1);

      //get consumer_name
      let consumer_name = ''

      if (app_mode.toLocaleLowerCase() == 'free') {
        var rset = snowflake.execute({sqlText:`SELECT CURRENT_ACCOUNT_NAME() as acct_name;`});
        rset.next();
        consumer_name = rset.getColumnValue(1);
      } 

      if (app_mode.toLocaleLowerCase() == 'paid') {
        var rset = snowflake.execute({sqlText:`SELECT 'PD_' || CURRENT_ACCOUNT_NAME() as acct_name;`});
        rset.next();
        consumer_name = rset.getColumnValue(1);
      }
      
      //check for either enterprise or an optional demo mode (if adding a demo app_mode type, it is recommended to copy the ENTERPRISE app mode)
      let entRegex = new RegExp('enterprise*', 'gi');
      if (entRegex.test(app_mode.toLocaleLowerCase())) {
        var rset = snowflake.execute({sqlText:`SELECT consumer_name FROM UTIL_APP.METADATA_C_V;`});
        rset.next();
        consumer_name = rset.getColumnValue(1);
      }

      //get list of cols from consumer input table
      var c_cols_list = [];

      //use SHOW command to get list of cols from input table
      snowflake.execute({sqlText: `SHOW COLUMNS IN TABLE ${INPUT_TABLE_NAME};`});

      var c_cols = snowflake.execute({sqlText: `SELECT "column_name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});
      var c_prefix = "c.";
      while (c_cols.next()) {
        c_col = c_cols.getColumnValue(1);
        c_cols_list.push(c_prefix.concat(c_col," AS C_",c_col));
      }

      //create results table, dynamically prepending the input columns
      var tbl_cmd = `CREATE OR REPLACE TABLE ${RESULTS_TABLE_NAME} AS SELECT
                        ${c_cols_list.toString()}, p.*
                    FROM DATA.CUSTOMER p RIGHT JOIN ${INPUT_TABLE_NAME} c ON p.${MATCH_KEY} = c.${MATCH_KEY};`;

      snowflake.execute({sqlText:tbl_cmd});

      //write results to run_tracker
      snowflake.execute({sqlText: `INSERT INTO APP.RUN_TRACKER(timestamp, request_id, request_type, input_table, output_table) VALUES(SYSDATE(), '${request_id}', 'enrich', '${INPUT_TABLE_NAME}', '${RESULTS_TABLE_NAME}');`});

      //log enrichment complete
      snowflake.execute({sqlText: `CALL UTIL_APP.APP_LOGGER('${account_locator}', '${consumer_name}', '${app_key_local}', '${app_mode}', 'log', 'request', '[{{"request_id":"${request_id}"}}, {{"proc_name":"enrich"}}, {{"proc_parameters":"${proc_parameters_esc}"}}]', SYSDATE(), 'COMPLETE', '"Results are located: ${RESULTS_TABLE_NAME}."');`});

      return `Results are located: ${RESULTS_TABLE_NAME}.`;             
      
    } catch (err) {
      var result = `Failed: Code: `+err.code + ` State: `+err.state+` Message: `+err.message.replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ")+` Stack Trace:`+ err.stack.toString().replace(/\'|\"/gm, "").replace(/\r|\n|\r\n|\n\r/gm, " ");

      return `Error: ${result}`;
    }
    
  $$
  ;

--replace default readme files with ones for the demo app and add demo app streamlit
USE SCHEMA ARTIFACTS;
PUT 'file://&{DIR}/application_control_framework/acf_acct/demo_app/templates/*' @ARTIFACTS/templates auto_compress=false overwrite=true;
PUT 'file://&{DIR}/application_control_framework/acf_acct/demo_app/streamlit/*' @ARTIFACTS/streamlit auto_compress=false overwrite=true;

--unset vars
UNSET (ACF_ADMIN_ROLE, SOURCE_DB, ACF_WH);

!print **********
!print End 01_demo_app_objects.sql
!print **********