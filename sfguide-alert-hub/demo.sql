/*************************************************************************************************************
Script:             Alert Hub - Demo
Create Date:        2024-03-20
Author:             B. Klein
Description:        Demo for Alert Hub
Copyright © 2024 Snowflake Inc. All rights reserved
**************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-03-20          B. Klein                            Initial Creation
*************************************************************************************************************/

-------------------------------- DEMO --------------------------------
use role alert_hub_role;
use warehouse alerts_wh;
use database alert_hub;
use schema alert_hub.admin;


call system$wait(10);

insert into alert_hub.admin.condition
select
        'new_records_to_test_records'
    ,   'new_records'
    ,   object_construct(
            'database','alert_hub',
            'schema','example',
            'table','records_to_test',
            'timestamp_column','row_timestamp'
        )
    ,   current_timestamp()
;

insert into alert_hub.admin.condition
select
        'new_records_to_test_records_2'
    ,   'new_records'
    ,   object_construct(
            'database','alert_hub',
            'schema','example',
            'table','records_to_test',
            'timestamp_column','row_timestamp'
        )
    ,   current_timestamp()
;

-- test condition query
select alert_hub.admin.get_sql_jinja(
        (
            select ct.template_configuration
            from alert_hub.admin.condition_template ct
            inner join alert_hub.admin.condition c    on ct.template_name = c.template_name
            where c.condition_name = 'new_records_to_test_records' limit 1
        )
    ,   (
            select parameters
            from alert_hub.admin.condition c
            where c.condition_name = 'new_records_to_test_records' limit 1
        )
);

-- test condition
select alert_hub.admin.get_condition('new_records_to_test_records');

-- test condition result - will not run if references snowflake.alert functions


-- insert example integration into table
insert into alert_hub.admin.notification_integration select
        'alert_email_int'
    ,   true
    ,   'EMAIL'
    ,   (
            select object_construct(
                'allowed_recipients',['<<YOUR_EMAIL_HERE>>'],
                'comment','cool'
            )
        )
    ,    current_timestamp()
;

select * from alert_hub.admin.notification_integration;

-- get integration sql statement
select
    alert_hub.admin.construct_notification_integration(name, type, enabled, parameters)
from alert_hub.admin.notification_integration;

-- paste and run result to create integration

            create or replace notification integration alert_email_int
                type=EMAIL
                enabled=True
                allowed_recipients=('<<YOUR_EMAIL_HERE>>')
comment='cool'

        ;

-- deploy notification integration
call alert_hub.admin.deploy_notification_integration('alert_email_int');

insert into alert_hub.admin.action
select
        'email_with_results'
    ,   'email_users'
    ,   object_construct(
            'email_integration','alert_email_int',
            'emails',array_construct( '<<YOUR_EMAIL_HERE>>'),
            'email_subject','Heyyy new records!',
            'email_body','{condition_results}'
        )
    ,    current_timestamp()
;

-- get action statement
select alert_hub.admin.get_sql_jinja(
        (
            select at.template_configuration
            from alert_hub.admin.action_template at
            inner join alert_hub.admin.action a    on at.template_name = a.template_name
            where a.action_name = 'email_with_results' limit 1
        )
    ,   (
            select parameters
            from alert_hub.admin.action
            where action_name = 'email_with_results' limit 1
        )
);

-- get action
select alert_hub.admin.get_action('email_with_results');

-- paste to test action


-- example notification
/*
call alert_hub.admin.notify(
        'email_with_results',
        (
            select
                array_agg(condition_result)
            from (
                select
                    object_construct(*) as condition_result
                from  (
                    select
                    *
                    from identifier('alert_hub.example.records_to_test')
                )
            )
        ));
*/

select 'alerts_wh', '1 MINUTE', 'cool_alert', 'new_records', 'email_with_results';

-- insert alert configuration - schedule also support cron
insert into alert_hub.admin.alert values ('alerts_wh', '1 MINUTE', 'cool_alert', 'new_records_to_test_records', 'email_with_results', current_timestamp());

-- see alert configurations
select * from alert_hub.admin.alert;

-- get alerts
select alert_hub.admin.construct_alert(
        warehouse_name
    ,   alert_schedule
    ,   alert_name
    ,   (select alert_hub.admin.get_condition(ac.condition_name))
    ,   action_name
)
from alert_hub.admin.alert ac;

-- deploy alert
call alert_hub.admin.deploy_alert('cool_alert');

-- test alert
truncate table alert_hub.example.records_to_test;
show alerts;
alter alert alert_hub.admin.cool_alert resume;
call system$wait(10);

insert into alert_hub.example.records_to_test values ('cool_record_1', current_timestamp);
insert into alert_hub.example.records_to_test values ('cool_record_2', current_timestamp);
insert into alert_hub.example.records_to_test values ('cool_record_3', current_timestamp);
insert into alert_hub.example.records_to_test values ('cool_record_4', current_timestamp);
select * from alert_hub.example.records_to_test;

-- wait for alert, observe alert history
select
    *
from table(information_schema.alert_history(
    SCHEDULED_TIME_RANGE_START
      =>dateadd('hour',-1,current_timestamp())))
where name = upper('cool_alert') and database_name = upper('alert_hub')
order by scheduled_time desc;

-- check condition and action results by pasting in query ID
--select * from table(result_scan('01ab6791-0504-68dd-0054-db87000e358e'));

call system$wait(90);
-- don't forget to turn off your alert if no longer needed
alter alert cool_alert suspend;
