Condition:
new records to test
{
    "database": "alert_hub",
    "schema": "example",
    "table": "records_to_test",
    "timestamp_column": "row_timestamp"
}

Notification Integration:
alert email int
{
    "allowed_recipients": ["<<YOUR_EMAIL_HERE>>"],
    "comment": "cool"
}

Action:
email with results
{
    "email_integration": "alert_email_int",
    "emails": ["<<YOUR_EMAIL_HERE>>"],
    "email_subject": "Heyyy new records!",
    "email_body": "{condition_results}"
}

Alert:
cool alert

Test:
insert into alert_hub.example.records_to_test values ('cool_record_1', current_timestamp);
insert into alert_hub.example.records_to_test values ('cool_record_2', current_timestamp);
insert into alert_hub.example.records_to_test values ('cool_record_3', current_timestamp);
insert into alert_hub.example.records_to_test values ('cool_record_4', current_timestamp);

Suspend (important):
alter alert cool_alert suspend;
