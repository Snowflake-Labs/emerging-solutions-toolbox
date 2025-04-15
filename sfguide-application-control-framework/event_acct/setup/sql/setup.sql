/*************************************************************************************************************
Script :            setup.sql
Create Date:        2023-03-14
Author:             Marc Henderson
Description:        This script calls the sql scripts that sets the Provider's account for event logging.
Called by:          SCRIPT(S):
                        framework/setup/setup.sh
Calls:              SCRIPTS:
                        scripts/01_account_setup.sql
                        scripts/02_create_events_dt_procedure.sql
                        scripts/03_create_remove_app_events_procedure.sql
                        scripts/04_create_remove_all_events_procedure.sql

Used By:            Provider
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-03-14          Marc Henderson                      Initial build
2024-02-29          Marc Henderson                      Added REMOVE_ALL_EVENTS proc (renamed REMOVE_EVENTS 
                                                        proc to REMOVE_APP_EVENTS)
2024-11-11          Marc Henderson                      Renamed 02_create_stream_events_procedure.sql to 
                                                        02_create_events_dt_procedure.sql
*************************************************************************************************************/

!set exit_on_error=True;
!set variable_substitution=true;

--account setup
!source  ../scripts/01_account_setup.sql

--create/call EVENTS_DT procedure
!source  ../scripts/02_create_events_dt_procedure.sql

--create REMOVE_APP_EVENTS procedure
!source  ../scripts/03_create_remove_app_events_procedure.sql

--create REMOVE_ALL_EVENTS procedure
!source  ../scripts/04_create_remove_all_events_procedure.sql