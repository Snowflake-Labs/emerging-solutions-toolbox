/*************************************************************************************************************
Script:             Solution Installation Wizard Provider Setup
Create Date:        2024-01-25
Author:             B. Klein
Description:        Provider-setup for Solution Installation Wizard
Copyright © 2024 Snowflake Inc. All rights reserved
**************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-01-25          B. Klein                            Initial Creation
*************************************************************************************************************/

/* Setup roles */
use role accountadmin;
call system$wait(10);
create warehouse if not exists sol_inst_wzd_wh comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';

/* create role and add permissions required by role for installation of framework */
create role if not exists sol_inst_wzd_role;

/* perform grants */
grant create share on account to role sol_inst_wzd_role;
grant import share on account to role sol_inst_wzd_role;
grant create database on account to role sol_inst_wzd_role with grant option;
grant execute task on account to role sol_inst_wzd_role;
grant create application package on account to role sol_inst_wzd_role;
grant create application on account to role sol_inst_wzd_role;
grant create data exchange listing on account to role sol_inst_wzd_role;
grant role sol_inst_wzd_role to role sysadmin;
grant usage, operate on warehouse sol_inst_wzd_wh to role sol_inst_wzd_role;

/* Setup provider side objects */
use role sol_inst_wzd_role;
call system$wait(10);
use warehouse sol_inst_wzd_wh;

/* create provider application package (database) and schemas */
drop application package if exists sol_inst_wzd_package;
create application package sol_inst_wzd_package comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';
create or replace schema sol_inst_wzd_package.admin comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';
drop schema sol_inst_wzd_package.public;

/* setup application supporting tables */
create or replace table sol_inst_wzd_package.admin.provider_account_identifier(provider_organization_name string, provider_account_name string, provider_account_locator string) comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';

/* insert current account for referencing in app */
insert into sol_inst_wzd_package.admin.provider_account_identifier(provider_organization_name, provider_account_name, provider_account_locator)
select
        current_organization_name()
    ,   current_account_name()
    ,   current_account()
;

/* the following is used to replace placeholder text in the scripts with consumer-specific values, useful for per-consumer object names/abbreviations */
/*
    consumer_organization_name - the consumer's organization name
    consumer_account_name - the consumer's account name
    placholder_text - the placholder text in the script to replace
    replacement_value - the text to replace the placeholder text
 */
create or replace table sol_inst_wzd_package.admin.placeholder_definition (consumer_organization_name string, consumer_account_name string, placeholder_text string, replacement_value string) comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';

/* create a row access policy to ensure consumer details are not exposed to other consumers */
create row access policy if not exists sol_inst_wzd_package.admin.consumer_record_filter
as (consumer_account_name varchar) returns boolean ->
    consumer_account_name = current_account_name()
    or
    current_account_name() = (select provider_account_name from sol_inst_wzd_package.admin.provider_account_identifier limit 1)
;

alter table sol_inst_wzd_package.admin.placeholder_definition add row access policy sol_inst_wzd_package.admin.consumer_record_filter on (consumer_account_name);

/* insert the scripts that will be used, placholder_text will be replaced at runtime in the consumer environment */
/*
    workflow_name - a common name for a set of scripts, e.g. name of the app
    workflow_description - a human readable description of the workflow
    script_name - a name for the current script
    script_order - the run order of the script within the workflow
    is_autorun - if true, the app will run this script as the app, otherwise present the script the consumer to have the consumer run - be wary of required permissions
    is_autorun_code_visible - if true, the code that will run is displayed on the page
    script_description - a human readable description of what this script does - this will be made available to the consumer before running
    script_text - the actual text of the script - if your script has $$ terminators, you may need to regex replace them during the insert (the admin streamlit UI will handle this)

    example regex - REGEXP_REPLACE($$ $$,':::','$$')
 */
create or replace table sol_inst_wzd_package.admin.script(workflow_name string, workflow_description string, script_name string, script_order string, is_autorun boolean, is_autorun_code_visible boolean, script_description string, script_text varchar(16777216)) comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';

/* add schemas/tables to application package's share - this makes them available to the app, but not necessarily to the consumer */
grant usage on schema sol_inst_wzd_package.admin to share in application package sol_inst_wzd_package;
grant select on table sol_inst_wzd_package.admin.placeholder_definition to share in application package sol_inst_wzd_package;
grant select on table sol_inst_wzd_package.admin.provider_account_identifier to share in application package sol_inst_wzd_package;

select 'Load script and placeholder data (or run the demo script to add sample info)' as DO_THIS_NEXT;
