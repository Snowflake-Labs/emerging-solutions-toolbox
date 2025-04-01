# ACME Data Enrichment App - ENTERPRISE

## Prerequisites
Prior to using the this app, the following one-time setup steps below **must be executed**.  Please make ONLY the changes mentioned in the **NOTES** below.  
### Any other changes may result in the setup process failing.

**NOTES:**  
- Replace ```<MY_ROLE>``` with either the ACCOUNTADMIN role or a role that has been granted ACCOUNTADMIN. 
  - ACCOUNTADMIN privileges are required for this step.
- Replace ```<MY_WAREHOUSE>``` with the desired warehouse.  
- For this step, an XSMALL warehouse can be used.
- Replace all ```<APP_NAME>``` references with the name of the native app, as installed in the consumer account.
  - The App Name can be found by executing (as ACCOUNTADMIN or the role that installed the app):  ```SHOW APPLICATIONS;``` (reference the **name** column)

```
SET APP_NAME = '<APP_NAME>';
SET MY_ROLE = '<MY_ROLE>';
SET MY_WAREHOUSE = '<MY_WAREHOUSE>';

USE ROLE IDENTIFIER($MY_ROLE);
USE WAREHOUSE IDENTIFIER($MY_WAREHOUSE);

--create the event table, if the account does not have one
CREATE DATABASE IF NOT EXISTS C_[[APP_CODE]]_HELPER_DB;
CREATE SCHEMA IF NOT EXISTS C_[[APP_CODE]]_HELPER_DB.EVENTS;
CREATE OR REPLACE PROCEDURE C_[[APP_CODE]]_HELPER_DB.EVENTS.DETECT_EVENT_TABLE()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"helper_db","component":"detect_event_table","type":"procedure"}}'
  EXECUTE AS CALLER
  AS
  $$
      snowflake.execute({sqlText:"SHOW PARAMETERS LIKE '%%event_table%%' IN ACCOUNT"});
      var table_name = snowflake.execute({sqlText:'SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));'});
      table_name.next();
      table_name = table_name.getColumnValue(1);
      if(table_name == '')
      {
          snowflake.execute({sqlText:"CREATE DATABASE IF NOT EXISTS EVENTS"});
          snowflake.execute({sqlText:"CREATE SCHEMA IF NOT EXISTS EVENTS"});
          snowflake.execute({sqlText:"CREATE EVENT TABLE IF NOT EXISTS EVENTS"});
          snowflake.execute({sqlText:"ALTER ACCOUNT SET EVENT_TABLE = EVENTS.EVENTS.EVENTS"});
          return 'ADDED EVENT TABLE'
      }
      return table_name
  $$;

CALL C_[[APP_CODE]]_HELPER_DB.EVENTS.DETECT_EVENT_TABLE();
[[TC_ACCESS]]
--grant account privileges to application
GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_NAME);
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION IDENTIFIER($APP_NAME);

-- use app database
USE DATABASE IDENTIFIER($APP_NAME);
[[TC_FLAG]]
--insert initial logs --REQUIRED TO ENABLE APP
CALL PROCS_APP.LOG_SHARE_INSERT();

--call configure_tracker  --REQUIRED TO ENABLE APP
CALL UTIL_APP.CONFIGURE_TRACKER();

--create sample data to use with this demo app
CREATE SCHEMA IF NOT EXISTS C_[[APP_CODE]]_HELPER_DB.SOURCE;

USE SCHEMA C_[[APP_CODE]]_HELPER_DB.SOURCE;
CREATE OR REPLACE TABLE CUSTOMER AS 
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

--unset session variables
UNSET (APP_NAME, MY_ROLE, MY_WAREHOUSE);

SELECT 'Done' AS STATUS;
```

## App Usage
Please refer to the Consumer Guide for details on how to use the app.