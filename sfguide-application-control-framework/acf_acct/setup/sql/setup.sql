/*************************************************************************************************************
Script :            setup.sql
Create Date:        2023-03-14
Author:             Marc Henderson
Description:        This script calls the sql framework that create the Provider's ACF Objects.
Called by:          SCRIPT(S):
                        acf_acct/setup/setup.sh
Calls:              framework:
                        acf/01_account_setup.sql
                        acf/02_create_acf_objects.sql
                        acf/03_create_process_consumer_events_procedure.sql
                        acf/04_create_onboard_consumer_procedure.sql
                        acf/05_create_remove_consumer_procedure.sql
                        acf/06_create_remove_acf_procedure.sql
                        acf/07_create_dev_env_objects.sql
                        acf/08_create_events_db_from_listing.sql
Used By:            Provider
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-03-14          Marc Henderson                      Initial build
2023-04-13          Marc Henderson                      Moved sample data to separate folder
2023-05-11          Marc Henderson                      Streamlined object creation using APP_CODE parameter
2023-06-28          Marc Henderson                      Removed app/10_create_application_package.sql
2023-07-18          Marc Henderson                      Updated location of framework from /app to /framework
2023-08-15          Marc Henderson                      Rename P_<APP_CODE>APP_DB to P_<APP_CODE>ACF_DB
2023-10-27          Marc Henderson                      Removed log/metrics verification scripts
2024-03-06          Marc Henderson                      Replaced 06_create_remove_app_procedure.sql with 
                                                        06_create_remove_acf_procedure.sql
2024-12-19          Marc Henderson                      Added 07_create_dev_env_objects.sql, removing it as a
                                                        separate setup script/step.
2025-01-07          Marc Henddrson                      Added 08_create_events_db_from_listing.sql
*************************************************************************************************************/

!set exit_on_error=True;
!set variable_substitution=true;

--account setup
!source  ../acf/01_account_setup.sql

--create app objects
!source  ../acf/02_create_acf_objects.sql

--create PROCESS_CONSUMER_EVENTS procedure and TASKS
!source  ../acf/03_create_process_consumer_events_procedure.sql

--create ONBOARD_CONSUMER procedure
!source  ../acf/04_create_onboard_consumer_procedure.sql

--create REMOVE_CONSUMER procedure
!source  ../acf/05_create_remove_consumer_procedure.sql

--create REMOVE_ACF procedure
!source  ../acf/06_create_remove_acf_procedure.sql

--create DEV environment
!source ../acf/07_create_dev_env_objects.sql

--create events DBs
!source ../acf/08_create_events_db_from_listing.sql
