/*************************************************************************************************************
Script :            setup.sql
Create Date:        2024-03-01
Author:             Marc Henderson
Description:        This script calls the sql scripts that create the demo app objects.  NOTE:  this is 
                    optional, if the provider wants to create a sample app with the ACF.
Called by:          SCRIPT(S):
                        demo_app/setup/setup.sh
Calls:              SCRIPTS:
                        scripts/01_demo_app_objects.sql

Used By:            Provider
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-03-01          Marc Henderson                      Initial build
*************************************************************************************************************/

!set exit_on_error=True;
!set variable_substitution=true;

--source objects:
!source  ../scripts/01_demo_app_objects.sql