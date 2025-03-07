create or replace database ml_forecasting;
create or replace schema foot_traffic;
create or replace table traffic
(store_id varchar, date timestamp, traffic int);

select * from traffic order by store_id, date;
