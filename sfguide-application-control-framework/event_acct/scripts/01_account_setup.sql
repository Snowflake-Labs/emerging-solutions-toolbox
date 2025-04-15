/*************************************************************************************************************
Objects :           Provider Event Account Setup
Create Date:        2023-10-19
Author:             Marc Henderson
Description:        This script creates and sets the event table in the Provider's event account
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Objects(s):         WAREHOUSE:        <APP_CODE>_EVENTS_WH
                    DATABASE:         EVENTS
                    SCHEMA:           EVENTS.EVENTS
                    TABLE (EVENT):    EVENTS.EVENTS.EVENTS
                    DATABASE          <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>
                    SCHEMA:           <APP_CODE>_EVENTS_FROM_<CURRENT_REGION>.EVENTS
                    SHARE:          
                    
Used By:            Provider

Copyright © 2025 Snowflake Inc. All rights reserved

*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-09          Marc Henderson                      Initial build
2024-01-07          Marc Henderson                      Converted CREATE EXTERNAL LISTING to an EXECUTE 
                                                        IMMEDIATE statemewnt to pass parameters into listing
                                                        yaml string.
*************************************************************************************************************/

!print **********
!print Begin 01_account_setup.sql
!print **********

--set session vars
SET EVENT_WH = '&{APP_CODE}_EVENTS_WH';
SET SHARE_DB = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION();
SET SHARE_SCH = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION() || '.EVENTS';
SET SHARE_TBL = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION() || '.EVENTS.EVENTS';
SET SHARE_NAME = '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION() || '_SHARE';

USE ROLE ACCOUNTADMIN;

--create warehouse for processing events
CREATE OR REPLACE WAREHOUSE IDENTIFIER($EVENT_WH) WITH WAREHOUSE_SIZE = 'XSMALL' 
  COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"event_acct","component":"&{APP_CODE}_events_warehouse","type":"warehouse"}}';

USE WAREHOUSE IDENTIFIER($EVENT_WH);

--create event table
CREATE DATABASE IF NOT EXISTS EVENTS;
CREATE SCHEMA IF NOT EXISTS EVENTS.EVENTS;
CREATE EVENT TABLE IF NOT EXISTS EVENTS.EVENTS.EVENTS;

--set account's event table
ALTER ACCOUNT SET EVENT_TABLE = 'EVENTS.EVENTS.EVENTS';

--create database to share this app's events, using streams/tasks, to provider's main account.
CREATE DATABASE IF NOT EXISTS IDENTIFIER($SHARE_DB);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SHARE_SCH);

--create share
CREATE SHARE IF NOT EXISTS IDENTIFIER($SHARE_NAME);
GRANT USAGE ON DATABASE IDENTIFIER($SHARE_DB) TO SHARE IDENTIFIER($SHARE_NAME);
GRANT USAGE ON SCHEMA IDENTIFIER($SHARE_SCH) TO SHARE IDENTIFIER($SHARE_NAME);

--create private listing to share events with main account
EXECUTE IMMEDIATE
$$
    DECLARE
      share_db VARCHAR;
      share_name VARCHAR;
      
      c_share_db CURSOR FOR SELECT '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION();
      c_share_name CURSOR FOR SELECT '&{APP_CODE}_EVENTS_FROM_' || CURRENT_REGION() || '_SHARE';
    BEGIN
      OPEN c_share_db;
      FETCH c_share_db INTO share_db;

      OPEN c_share_name;
      FETCH c_share_name INTO share_name;
    
      LET create_listing_stmt VARCHAR := 'CREATE EXTERNAL LISTING IF NOT EXISTS ' || share_db || '
                                            SHARE ' || share_name || ' AS
                                            \$\$
                                             title: "'|| share_db || '"
                                             description: "The share that contains &{APP_CODE} App Events from account: &{EVENT_ACCOUNT_LOCATOR}."
                                             listing_terms:
                                               type: "OFFLINE"
                                             auto_fulfillment:
                                               refresh_schedule: "5 MINUTE"
                                               refresh_type: "FULL_DATABASE"
                                             targets:
                                               accounts: ["&{ORG_NAME}.&{ACF_ACCOUNT_NAME}"]
                                            \$\$
                                            PUBLISH=TRUE REVIEW=TRUE;';
      EXECUTE IMMEDIATE :create_listing_stmt;
    
      RETURN 'External Listing: '|| share_db || ' successfully created.';
    END;
$$;

--unset vars
UNSET (EVENT_WH, SHARE_DB, SHARE_SCH, SHARE_TBL, SHARE_NAME);

!print **********
!print End 01_account_setup.sql
!print **********