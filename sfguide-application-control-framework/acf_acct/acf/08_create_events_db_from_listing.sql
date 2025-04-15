/*************************************************************************************************************
Objects :           Application Control Framework Source Objects
Create Date:        2024-01-07
Author:             Marc Henderson
Description:        This script creates events DBs for each region shared to the ACF account. A stream and 
                    tasks are created for each events db created.
Called by:          SCRIPT(S):
                      setup/sql/setup.sql
Objects(s):         N/A
Copyright © 2025 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-01-07          Marc Henderson                      Initial build
*************************************************************************************************************/

!print **********
!print Begin 08_create_events_db_from_listing.sql
!print **********

!set exit_on_error=True;
!set variable_substitution=true;

--set parameter for admin role
SET ACF_ADMIN_ROLE = 'P_&{APP_CODE}_ACF_ADMIN';

--set parameter for app warehouse
SET ACF_WH = 'P_&{APP_CODE}_ACF_WH';

--set parameter for app db
SET ACF_DB = 'P_&{APP_CODE}_ACF_DB';


USE ROLE IDENTIFIER($ACF_ADMIN_ROLE);
USE WAREHOUSE IDENTIFIER($ACF_WH);

--create events DB from listing(s)
USE DATABASE IDENTIFIER($ACF_DB);
CALL EVENTS.CREATE_EVENTS_DB_FROM_LISTING();

--create a stream and tasks for each events db created from CREATE_EVENTS_DB_FROM_LISTING
--NOTE: CREATE_EVENTS_DB_FROM_LISTING returns an array of event DB names that will be used by STREAM_TO_EVENT_MASTER
USE DATABASE IDENTIFIER($ACF_DB);
CALL EVENTS.STREAM_TO_EVENT_MASTER((SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2))))); --skip the USE DATABASE command

--unset vars
UNSET (ACF_ADMIN_ROLE, ACF_DB, ACF_WH);

!print **********
!print End 08_create_events_db_from_listing.sql
!print **********