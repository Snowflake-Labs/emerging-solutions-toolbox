/*************************************************************************************************************
Script:             Supply Chain Network Optimization (SCNO) App Setup
Create Date:        2024-03-26
Author:             B. Klein
Description:        Supply Chain Network Optimization
Copyright © 2024 Snowflake Inc. All rights reserved
**************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-03-26          B. Klein                            Initial Creation
*************************************************************************************************************/

/* set up roles */
use role accountadmin;
call system$wait(10);
create warehouse if not exists scno_wh WAREHOUSE_SIZE=SMALL comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';

/* create role and add permissions required by role for installation of framework */
create role if not exists scno_role;

/* perform grants */
grant create share on account to role scno_role;
grant import share on account to role scno_role;
grant create database on account to role scno_role with grant option;
grant execute task on account to role scno_role;
grant create application package on account to role scno_role;
grant create application on account to role scno_role;
grant create data exchange listing on account to role scno_role;
/* add cortex_user database role to use Cortex */
grant database role snowflake.cortex_user to role scno_role;
grant role scno_role to role sysadmin;
grant usage, operate on warehouse scno_wh to role scno_role;

/* set up provider side objects */
use role scno_role;
call system$wait(10);
use warehouse scno_wh;

/* create database */
create or replace database supply_chain_network_optimization_db comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';
create or replace schema supply_chain_network_optimization_db.entities comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';
create or replace schema supply_chain_network_optimization_db.relationships comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';
create or replace schema supply_chain_network_optimization_db.results comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';
create or replace schema supply_chain_network_optimization_db.code comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';
create or replace schema supply_chain_network_optimization_db.streamlit comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';
create or replace stage supply_chain_network_optimization_db.streamlit.streamlit_stage comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}';
drop schema if exists supply_chain_network_optimization_db.public;

/* entity tables */
create or replace table supply_chain_network_optimization_db.entities.factory comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' (
	ID NUMBER(4,0),
	NAME VARCHAR(12),
	LATITUDE NUMBER(8,6),
	LONGITUDE NUMBER(9,6),
	LONG_LAT GEOGRAPHY,
	CITY VARCHAR(11),
	STATE VARCHAR(14),
	COUNTRY VARCHAR(13),
	PRODUCTION_CAPACITY NUMBER(5,0),
	PRODUCTION_COST NUMBER(9,5)
);

create or replace table supply_chain_network_optimization_db.entities.distributor comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' (
	ID NUMBER(4,0),
	NAME VARCHAR(16),
	LATITUDE NUMBER(8,6),
	LONGITUDE NUMBER(9,6),
	LONG_LAT GEOGRAPHY,
	CITY VARCHAR(13),
	STATE VARCHAR(14),
	COUNTRY VARCHAR(13),
	THROUGHPUT_CAPACITY NUMBER(4,0),
	THROUGHPUT_COST NUMBER(7,5)
);

create or replace table supply_chain_network_optimization_db.entities.customer comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' (
	ID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(16777216) NOT NULL,
	LATITUDE VARCHAR(16777216) NOT NULL,
	LONGITUDE VARCHAR(16777216) NOT NULL,
	LONG_LAT VARCHAR(16777216) NOT NULL,
	DEMAND NUMBER(38,0) NOT NULL
);

/* relationship tables/views */
create or replace table supply_chain_network_optimization_db.relationships.factory_to_distributor_rates comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' (
	FACTORY VARCHAR(12),
	DISTRIBUTOR VARCHAR(16),
	MILEAGE FLOAT,
	COST_FACTOR NUMBER(9,6),
	FREIGHT_COST FLOAT
);

create or replace view supply_chain_network_optimization_db.relationships.factory_to_distributor_rates_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select
        factory
    ,   distributor
    ,   freight_cost
from supply_chain_network_optimization_db.relationships.factory_to_distributor_rates
order by
        factory asc
    ,   distributor asc
;

create or replace view supply_chain_network_optimization_db.relationships.factory_to_distributor_mileage_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select
        factory
    ,   distributor
    ,   mileage
from supply_chain_network_optimization_db.relationships.factory_to_distributor_rates
order by
        factory asc
    ,   distributor asc
;

create or replace table supply_chain_network_optimization_db.relationships.distributor_to_customer_rates comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' (
	DISTRIBUTOR VARCHAR(16),
	CUSTOMER VARCHAR(16777216),
	MILEAGE FLOAT,
	COST_FACTOR NUMBER(9,6),
	FREIGHT_COST FLOAT
);

create or replace view supply_chain_network_optimization_db.relationships.distributor_to_customer_rates_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select
        distributor
    ,   customer
    ,   freight_cost
from supply_chain_network_optimization_db.relationships.distributor_to_customer_rates
order by
        distributor asc
    ,   customer asc
;

create or replace view supply_chain_network_optimization_db.relationships.distributor_to_customer_mileage_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select
        distributor
    ,   customer
    ,   mileage
from supply_chain_network_optimization_db.relationships.distributor_to_customer_rates
order by
        distributor asc
    ,   customer asc
;


/* results tables/views */
create or replace table supply_chain_network_optimization_db.results.model_results comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' (
    PROBLEM_NAME VARCHAR(16777216),
    RUN_DATE DATETIME,
    LP_STATUS VARCHAR(16777216),
    OBJECTIVE_VALUE FLOAT,
    DECISION_VARIABLES ARRAY,
    TOTAL_F2D_MILES FLOAT,
    TOTAL_D2C_MILES FLOAT,
    TOTAL_PRODUCTION_COSTS FLOAT,
    TOTAL_F2D_FREIGHT FLOAT,
    TOTAL_THROUGHPUT_COSTS FLOAT,
    TOTAL_D2C_FREIGHT FLOAT
);

create or replace view supply_chain_network_optimization_db.results.factory_to_distributor_shipments_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select distinct
        res.problem_name
    ,   res.run_date
    ,   '_' || split_part(replace(object_keys(val.value)[0],'"'),'__',2) || '_' as factory
    ,   split_part(replace(object_keys(val.value)[0],'"'),'__',3) as distributor
    ,   val.value[replace(object_keys(val.value)[0],'"')] as f2d_amount
from supply_chain_network_optimization_db.results.model_results res
, lateral flatten( input => decision_variables ) val
where split_part(replace(object_keys(val.value)[0],'"'),'__',1) = 'F2DRoute'
;

create or replace view supply_chain_network_optimization_db.results.distributor_to_customer_shipments_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select distinct
        res.problem_name
    ,   res.run_date
    ,   '_' || split_part(replace(object_keys(val.value)[0],'"'),'__',2) || '_' as distributor
    ,   '_' || split_part(replace(object_keys(val.value)[0],'"'),'__',3) || '_' as customer
    ,   val.value[replace(object_keys(val.value)[0],'"')] as d2c_amount
from supply_chain_network_optimization_db.results.model_results res
, lateral flatten( input => decision_variables ) val
where split_part(replace(object_keys(val.value)[0],'"'),'__',1) = 'D2CRoute'
;

create or replace view supply_chain_network_optimization_db.results.factory_to_distributor_shipment_details_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select distinct
        ship.problem_name
    ,   ship.run_date
    ,   ship.f2d_amount as factory_to_distributor_shipped_amount
    ,   to_geography(fact.long_lat) as factory_geocodes
    ,   st_asgeojson(to_geography(fact.long_lat)):coordinates[0] as factory_longitude
    ,   st_asgeojson(to_geography(fact.long_lat)):coordinates[1] as factory_latitude
    ,   fact.name as factory_name
    ,   fact.city as factory_city
    ,   fact.state as factory_state
    ,   fact.country as factory_country
    ,   to_geography(dist.long_lat) as distributor_geocodes
    ,   st_asgeojson(to_geography(dist.long_lat)):coordinates[0] as distributor_longitude
    ,   st_asgeojson(to_geography(dist.long_lat)):coordinates[1] as distributor_latitude
    ,   dist.name as distributor_name
    ,   dist.city as distributor_city
    ,   dist.state as distributor_state
    ,   dist.country as distributor_country
    ,   f2dr.mileage as mileage
    ,   f2dr.freight_cost as freight_cost
from supply_chain_network_optimization_db.results.factory_to_distributor_shipments_vw ship
inner join supply_chain_network_optimization_db.entities.factory fact on ship.factory = replace(fact.name,' ','_')
inner join supply_chain_network_optimization_db.entities.distributor dist on ship.distributor = replace(dist.name,' ','_')
inner join supply_chain_network_optimization_db.relationships.factory_to_distributor_rates f2dr on 
    f2dr.factory = fact.name and
    f2dr.distributor = dist.name
;

create or replace view supply_chain_network_optimization_db.results.distributor_to_customer_shipment_details_vw comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' as
select distinct
        ship.problem_name
    ,   ship.run_date
    ,   ship.d2c_amount as distributor_to_customer_shipped_amount
    ,   to_geography(dist.long_lat) as distributor_geocodes
    ,   st_asgeojson(to_geography(dist.long_lat)):coordinates[0] as distributor_longitude
    ,   st_asgeojson(to_geography(dist.long_lat)):coordinates[1] as distributor_latitude
    ,   dist.name as distributor_name
    ,   dist.city as distributor_city
    ,   dist.state as distributor_state
    ,   dist.country as distributor_country
    ,   to_geography(cust.long_lat) as customer_geocodes
    ,   st_asgeojson(to_geography(cust.long_lat)):coordinates[0] as customer_longitude
    ,   st_asgeojson(to_geography(cust.long_lat)):coordinates[1] as customer_latitude
    ,   cust.name as customer_name
    ,   d2cr.mileage as mileage
    ,   d2cr.freight_cost as freight_cost
from supply_chain_network_optimization_db.results.distributor_to_customer_shipments_vw ship
inner join supply_chain_network_optimization_db.entities.distributor dist on ship.distributor = replace(dist.name,' ','_')
inner join supply_chain_network_optimization_db.entities.customer cust on rtrim(ltrim(customer,'_'),'_') = replace(cust.name,' ','_')
inner join supply_chain_network_optimization_db.relationships.distributor_to_customer_rates d2cr on 
    d2cr.distributor = dist.name and
    d2cr.customer = cust.name
;

/* streamlit deployment objects */

/* helper stored procedure to help create the objects */
create or replace procedure supply_chain_network_optimization_db.code.put_to_stage(stage varchar,filename varchar, content varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='put_to_stage'
comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' 
AS $$
import io
import os

def put_to_stage(session, stage, filename, content):
    local_path = '/tmp'
    local_file = os.path.join(local_path, filename)
    f = open(local_file, "w", encoding='utf-8')
    f.write(content)
    f.close()
    session.file.put(local_file, '@'+stage, auto_compress=False, overwrite=True)
    return "saved file "+filename+" in stage "+stage
$$;

create or replace table supply_chain_network_optimization_db.code.script comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}' (
	name varchar,
	script varchar(16777216)
);

/* streamlit */
insert into supply_chain_network_optimization_db.code.script (name , script) 
values ('STREAMLIT_V1',$$

from snowflake.snowpark.context import get_active_session
import streamlit as st
from abc import ABC, abstractmethod
import datetime
from snowflake.snowpark.functions import col
import operator
import pandas as pd
from faker import Faker
from random import randint
from pulp import *
from snowflake.cortex import Complete,ExtractAnswer
import json


# Check snowflake connection type
def set_session():
    try:
        import snowflake.permissions as permissions

        session = get_active_session()

        # will fail for a non native app
        privilege_check = permissions.get_held_account_privileges(["EXECUTE TASK"])

        st.session_state["streamlit_mode"] = "NativeApp"
    except:
        try:
            session = get_active_session()

            st.session_state["streamlit_mode"] = "SiS"
        except:
            import snowflake_conn as sfc

            session = sfc.init_snowpark_session("account_1")

            st.session_state["streamlit_mode"] = "OSS"

    return session


# Initiate session
session = set_session()

fake = Faker()

# demo data
factories_sql = '''insert overwrite into supply_chain_network_optimization_db.entities.factory
    select
            1000 as id
        ,   '_Factory_1_' as name
        ,   39.798363 as latitude
        ,   -89.654961 as longitude
        ,   to_geography('POINT (-89.654961 39.798363)') as long_lat
        ,   'Springfield' as city
        ,   'Illinois' as state
        ,   'United States' as country
        ,   7000 as production_capacity
        ,   600 as production_cost
union
    select
            1005 as id     
        ,   '_Factory_2_' as name     
        ,   38.576668 as latitude     
        ,   -121.493629 as longitude     
        ,   to_geography('POINT (-121.493629 38.576668)') as long_lat     
        ,   'Sacramento' as city     
        ,   'California' as state     
        ,   'United States' as country     
        ,   11000 as production_capacity
        ,   1100 as production_cost
union
    select         
            1010 as id     
        ,   '_Factory_3_' as name
        ,   39.739227 as latitude
        ,   -104.984856 as longitude
        ,   to_geography('POINT (-104.984856 39.739227)') as long_la
        ,   'Denver' as city
        ,   'Colorado' as state
        ,   'United States' as country
        ,   8500 as production_capacity
        ,   750.5 as production_cost
union
    select
            1015 as id
        ,   '_Factory_4_' as name     
        ,   30.438118 as latitude     
        ,   -84.281296 as longitude     
        ,   to_geography('POINT (-84.281296 30.438118)') as long_lat     
        ,   'Tallahassee' as city     
        ,   'Florida' as state     
        ,   'United States' as country     
        ,   7500 as production_capacity
        ,   760 as production_cost
union
    select
            1020 as id
        ,   '_Factory_5_' as name
        ,   43.617775 as latitude
        ,   -116.199722 as longitude
        ,   to_geography('POINT (-116.199722 43.617775)') as long_lat
        ,   'Boise' as city
        ,   'Idaho' as state
        ,   'United States' as country
        ,   11000 as production_capacity
        ,   690 as production_cost
union     
    select         
            1025 as id     
        ,   '_Factory_6_' as name
        ,   44.307167 as latitude
        ,   -69.781693 as longitude
        ,   to_geography('POINT (-69.781693 44.307167)') as long_lat
        ,   'Augusta' as city
        ,   'Maine' as state
        ,   'United States' as country
        ,   4100 as production_capacity
        ,   680 as production_cost
union     
    select         
            1030 as id     
        ,   '_Factory_7_' as name
        ,   46.585709 as latitude
        ,   -112.018417 as longitude
        ,   to_geography('POINT (-112.018417 46.585709)') as long_lat
        ,   'Helena' as city
        ,   'Montana' as state
        ,   'United States' as country
        ,   6600 as production_capacity
        ,   800 as production_cost
union 
    select         
            1035 as id     
        ,   '_Factory_8_' as name     
        ,   42.652843 as latitude     
        ,   -73.757874 as longitude     
        ,   to_geography('POINT (-73.757874 42.652843)') as long_lat     
        ,   'Albany' as city     
        ,   'New York' as state     
        ,   'United States' as country     
        ,   6000 as production_capacity
        ,   1200 as production_cost
union     
    select         
            1050 as id     
        ,   '_Factory_9_' as name
        ,   30.27467 as latitude
        ,   -97.740349 as longitude
        ,   to_geography('POINT (-97.740349 30.27467)') as long_lat
        ,   'Austin' as city
        ,   'Texas' as state
        ,   'United States' as country
        ,   5200 as production_capacity
        ,   750 as production_cost
        union     
    select         
            1045 as id     
        ,   '_Factory_10_' as name
        ,   34.000343 as latitude
        ,   -81.033211 as longitude
        ,   to_geography('POINT (-81.033211 34.000343)') as long_lat
        ,   'Columbia' as city
        ,   'South Carolina' as state
        ,   'United States' as country
        ,   4500 as production_capacity
        ,   500 as production_cost
;'''
distributor_sql = '''insert overwrite into supply_chain_network_optimization_db.entities.distributor
    select
            2000 as id     
        ,   '_Distributor_1_' as name
        ,   33.448143 as latitude
        ,   -112.096962 as longitude
        ,   to_geography('POINT (-112.096962 33.448143)') as long_lat
        ,   'Phoenix' as city
        ,   'Arizona' as state
        ,   'United States' as country
        ,   1500 as throughput_capacity
        ,   7 as throughput_cost
union     
    select         
            2002 as id     
        ,   '_Distributor_2_' as name
        ,   33.749027 as latitude
        ,   -84.388229 as longitude
        ,   to_geography('POINT (-84.388229 33.749027)') as long_lat
        ,   'Atlanta' as city
        ,   'Georgia' as state
        ,   'United States' as country
        ,   1900 as throughput_capacity
        ,   6 as throughput_cost
union     
    select         
            2004 as id     
        ,   '_Distributor_3_' as name
        ,   39.768623 as latitude
        ,   -86.162643 as longitude
        ,   to_geography('POINT (-86.162643 39.768623)') as long_lat
        ,   'Indianapolis' as city
        ,   'Indiana' as state
        ,   'United States' as country
        ,   2200 as throughput_capacity
        ,   5 as throughput_cost
union     
    select         
            2006 as id     
        ,   '_Distributor_4_' as name
        ,   38.186722 as latitude
        ,   -84.875374 as longitude
        ,   to_geography('POINT (-84.875374 38.186722)') as long_lat
        ,   'Frankfort' as city
        ,   'Kentucky' as state
        ,   'United States' as country
        ,   800 as throughput_capacity
        ,   6 as throughput_cost
union     
    select         
            2008 as id
        ,   '_Distributor_5_' as name
        ,   42.358162 as latitude
        ,   -71.063698 as longitude
        ,   to_geography('POINT (-71.063698 42.358162)') as long_lat
        ,   'Boston' as city
        ,   'Massachusetts' as state
        ,   'United States' as country
        ,   2700 as throughput_capacity
        ,   25 as throughput_cost
union     
    select         
            2010 as id
        ,   '_Distributor_6_' as name
        ,   32.303848 as latitude
        ,   -90.182106 as longitude
        ,   to_geography('POINT (-90.182106 32.303848)') as long_lat
        ,   'Jackson' as city
        ,   'Mississippi' as state
        ,   'United States' as country
        ,   1800 as throughput_capacity
        ,   4 as throughput_cost
union     
    select         
            2012 as id     
        ,   '_Distributor_7_' as name
        ,   40.808075 as latitude
        ,   -96.699654 as longitude
        ,   to_geography('POINT (-96.699654 40.808075)') as long_lat
        ,   'Lincoln' as city
        ,   'Nebraska' as state
        ,   'United States' as country
        ,   1200 as throughput_capacity
        ,   8 as throughput_cost
union     
    select         
            2014 as id     
        ,   '_Distributor_8_' as name
        ,   43.206898 as latitude
        ,   -71.537994 as longitude
        ,   to_geography('POINT (-71.537994 43.206898)') as long_lat
        ,   'Concord' as city
        ,   'New Hampshire' as state
        ,   'United States' as country
        ,   1900 as throughput_capacity
        ,   15 as throughput_cost
union     
    select         
            2016 as id     
        ,   '_Distributor_9_' as name     
        ,   40.220596 as latitude     
        ,   -74.769913 as longitude     
        ,   to_geography('POINT (-74.769913 40.220596)') as long_lat
        ,   'Trenton' as city
        ,   'New Jersey' as state
        ,   'United States' as country
        ,   2300 as throughput_capacity
        ,   12 as throughput_cost
union     
    select         
            2018 as id
        ,   '_Distributor_10_' as name
        ,   35.68224 as latitude
        ,   -105.939728 as longitude
        ,   to_geography('POINT (-105.939728 35.68224)') as long_lat
        ,   'Santa Fe' as city
        ,   'New Mexico' as state
        ,   'United States' as country
        ,   2450 as throughput_capacity
        ,   6 as throughput_cost
union     
    select         
            2020 as id     
        ,   '_Distributor_11_' as name
        ,   46.82085 as latitude
        ,   -100.783318 as longitude
        ,   to_geography('POINT (-100.783318 46.82085)') as long_lat
        ,   'Bismarck' as city
        ,   'North Dakota' as state
        ,   'United States' as country
        ,   900 as throughput_capacity
        ,   5 as throughput_cost
union     
    select         
            2022 as id
        ,   '_Distributor_12_' as name
        ,   39.961346 as latitude
        ,   -82.999069 as longitude
        ,   to_geography('POINT (-82.999069 39.961346)') as long_lat
        ,   'Columbus' as city
        ,   'Ohio' as state
        ,   'United States' as country
        ,   1500 as throughput_capacity
        ,   4.5 as throughput_cost
union     
    select         
            2024 as id
        ,   '_Distributor_13_' as name
        ,   35.492207 as latitude
        ,   -97.503342 as longitude
        ,   to_geography('POINT (-97.503342 35.492207)') as long_lat
        ,   'Oklahoma City' as city
        ,   'Oklahoma' as state
        ,   'United States' as country
        ,   700 as throughput_capacity
        ,   4 as throughput_cost
union     
    select         
            2026 as id
        ,   '_Distributor_14_' as name
        ,   35.78043 as latitude
        ,   -78.639099 as longitude
        ,   to_geography('POINT (-78.639099 35.78043)') as long_lat
        ,   'Raleigh' as city
        ,   'North Carolina' as state
        ,   'United States' as country
        ,   4900 as throughput_capacity
        ,   7 as throughput_cost
union     
    select         
            2028 as id
        ,   '_Distributor_15_' as name
        ,   44.367031 as latitude
        ,   -100.346405 as longitude
        ,   to_geography('POINT (-100.346405 44.367031)') as long_lat
        ,   'Pierre' as city
        ,   'South Dakota' as state
        ,   'United States' as country
        ,   4000 as throughput_capacity
        ,   6 as throughput_cost
union     
    select         
            2030 as id     
        ,   '_Distributor_16_' as name
        ,   44.262436 as latitude
        ,   -72.580536 as longitude
        ,   to_geography('POINT (-72.580536 44.262436)') as long_lat
        ,   'Montpelier' as city
        ,   'Vermont' as state
        ,   'United States' as country
        ,   3400 as throughput_capacity
        ,   11 as throughput_cost
union     
    select         
            2032 as id     
        ,   '_Distributor_17_' as name
        ,   37.538857 as latitude
        ,   -77.43364 as longitude
        ,   to_geography('POINT (-77.43364 37.538857)') as long_lat
        ,   'Richmond' as city
        ,   'Virginia' as state
        ,   'United States' as country
        ,   1650 as throughput_capacity
        ,   9 as throughput_cost
union     
    select         
            2034 as id
        ,   '_Distributor_18_' as name
        ,   47.035805 as latitude
        ,   -122.905014 as longitude
        ,   to_geography('POINT (-122.905014 47.035805)') as long_lat
        ,   'Olympia' as city
        ,   'Washington' as state
        ,   'United States' as country
        ,   400 as throughput_capacity
        ,   18 as throughput_cost
union     
    select         
            2036 as id
        ,   '_Distributor_19_' as name
        ,   38.336246 as latitude
        ,   -81.612328 as longitude
        ,   to_geography('POINT (-81.612328 38.336246)') as long_lat
        ,   'Charleston' as city
        ,   'West Virginia' as state
        ,   'United States' as country
        ,   2450 as throughput_capacity
        ,   16 as throughput_cost
union     
    select         
            2038 as id
        ,   '_Distributor_20_' as name
        ,   43.074684 as latitude
        ,   -89.384445 as longitude
        ,   to_geography('POINT (-89.384445 43.074684)') as long_lat
        ,   'Madison' as city
        ,   'Wisconsin' as state
        ,   'United States' as country
        ,   1900 as throughput_capacity
        ,   11 as throughput_cost
union     
    select         
            2040 as id
        ,   '_Distributor_21_' as name
        ,   41.140259 as latitude
        ,   -104.820236 as longitude
        ,   to_geography('POINT (-104.820236 41.140259)') as long_lat
        ,   'Cheyenne' as city
        ,   'Wyoming' as state
        ,   'United States' as country
        ,   2250 as throughput_capacity
        ,   7 as throughput_cost
union     
    select         
            2042 as id
        ,   '_Distributor_22_' as name
        ,   34.746613 as latitude
        ,   -92.288986 as longitude
        ,   to_geography('POINT (-92.288986 34.746613)') as long_lat
        ,   'Little Rock' as city
        ,   'Arkansas' as state
        ,   'United States' as country
        ,   3600 as throughput_capacity
        ,   5 as throughput_cost
union     
    select         
            2044 as id
        ,   '_Distributor_23_' as name
        ,   39.157307 as latitude
        ,   -75.519722 as longitude
        ,   to_geography('POINT (-75.519722 39.157307)') as long_lat
        ,   'Dover' as city
        ,   'Delaware' as state
        ,   'United States' as country
        ,   4200 as throughput_capacity
        ,   16 as throughput_cost
union     
    select         
            2046 as id
        ,   '_Distributor_24_' as name
        ,   21.307442 as latitude
        ,   -157.857376 as longitude
        ,   to_geography('POINT (-157.857376 21.307442)') as long_lat
        ,   'Honolulu' as city
        ,   'Hawaii' as state
        ,   'United States' as country
        ,   3500 as throughput_capacity
        ,   25 as throughput_cost
union     
    select         
            2048 as id
        ,   '_Distributor_25_' as name
        ,   41.591087 as latitude
        ,   -93.603729 as longitude
        ,   to_geography('POINT (-93.603729 41.591087)') as long_lat
        ,   'Des Moines' as city
        ,   'Iowa' as state
        ,   'United States' as country
        ,   4700 as throughput_capacity
        ,   8 as throughput_cost
;'''
fact_to_dist_sql = '''insert overwrite into supply_chain_network_optimization_db.relationships.factory_to_distributor_rates
with mileage as
    ((st_distance(fact.long_lat, dist.long_lat))/1609.344),
cost_factor as
    ((uniform(1, 20, random())+89)/100)
select
        fact.name as factory
    ,   dist.name as distributor
    ,   mileage as mileage
    ,   cost_factor as cost_factor
    ,   mileage*cost_factor as freight_cost
from supply_chain_network_optimization_db.entities.factory fact
cross join supply_chain_network_optimization_db.entities.distributor dist
order by
        fact.name asc
    ,   dist.name asc
;'''
dist_to_cust_sql = '''insert overwrite into supply_chain_network_optimization_db.relationships.distributor_to_customer_rates
with mileage as
    ((st_distance(dist.long_lat, to_geography(cust.long_lat)))/1609.344),
cost_factor as
    ((uniform(1, 20, random())+89)/100)
select
        dist.name as distributor
    ,   cust.name as customer
    ,   mileage as mileage
    ,   cost_factor as cost_factor
    ,   mileage*cost_factor as freight_cost
from supply_chain_network_optimization_db.entities.distributor dist
cross join supply_chain_network_optimization_db.entities.customer cust
order by
        dist.name asc
    ,   cust.name asc
;'''

# needed after any factory changes due to pivot
fact_to_dist_rate_matrix_sql = """select 'create or replace table 
supply_chain_network_optimization_db.relationships.factory_to_distributor_rates_matrix as select * from 
supply_chain_network_optimization_db.relationships.factory_to_distributor_rates_vw pivot (avg(freight_cost) for factory in (
'||listagg (distinct ''''||factory||'''', ',') ||')) order by distributor;' as query from 
supply_chain_network_optimization_db.relationships.factory_to_distributor_rates_vw;"""
fact_to_dist_mileage_matrix_sql = """select 'create or replace table 
supply_chain_network_optimization_db.relationships.factory_to_distributor_mileage_matrix as select * from 
supply_chain_network_optimization_db.relationships.factory_to_distributor_mileage_vw pivot (avg(mileage) for factory in (
'||listagg (distinct ''''||factory||'''', ',') ||')) order by distributor;' as query from 
supply_chain_network_optimization_db.relationships.factory_to_distributor_mileage_vw;"""

# needed after any distributor changes due to pivot
dist_to_cust_rate_matrix_sql = """select 'create or replace table 
supply_chain_network_optimization_db.relationships.distributor_to_customer_rates_matrix as select * from 
supply_chain_network_optimization_db.relationships.distributor_to_customer_rates_vw pivot (avg(freight_cost) for distributor in (
'||listagg (distinct ''''||distributor||'''', ',') ||')) order by customer;' as query from 
supply_chain_network_optimization_db.relationships.distributor_to_customer_rates_vw;"""
dist_to_cust_mileage_matrix_sql = """select 'create or replace table 
supply_chain_network_optimization_db.relationships.distributor_to_customer_mileage_matrix as select * from 
supply_chain_network_optimization_db.relationships.distributor_to_customer_mileage_vw pivot (avg(mileage) for distributor in (
'||listagg (distinct ''''||distributor||'''', ',') ||')) order by customer;' as query from 
supply_chain_network_optimization_db.relationships.distributor_to_customer_mileage_vw;"""

# Page settings
st.set_page_config(
    page_title="Supply Chain Network Optimization",
    page_icon="❄️️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/snowflakecorp/supply-chain-optimization',
        'Report a bug': "https://github.com/snowflakecorp/supply-chain-optimization",
        'About': "This app performs network optimization model for a 3-tier supply chain"
    }
)

# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "Welcome"


# Sets the page based on page name
def set_page(page: str):
    st.session_state.page = page


class OptimizationModel:
    """
    A class used to represent a supply chain network optimization model.  An optimization model mathematically
    maximizes/minimizes an objective function based on decision variables and a given set of constraints.

    To use:
        - Instantiate the class
        - Run prepare_date method to set up model data
        - Run the solve method, passing in one of the supported problem names
        - There is a solve_sample method to run for a self-contained model

    Any model is specific to a particular business's entities, relationships, constraints, and objective.  This is a
    demo model that outlines a typical supply chain scenario.
    """

    def __init__(self):
        self.factories = []
        self.production_capacities = {}
        self.production_costs = {}
        self.distributors = []
        self.throughput_capacities = {}
        self.throughput_costs = {}
        self.customers = []
        self.demand = {}
        self.factory_to_distributor_rates = {}
        self.distributor_to_customer_rates = {}
        self.factory_to_distributor_mileage = {}
        self.distributor_to_customer_mileage = {}
        self.problem_name = None
        self.lp_status = None
        self.decision_variables = None
        self.objective_value = None
        self.total_f2d_miles = None
        self.total_d2c_miles = None
        self.total_production_costs = None
        self.total_f2d_freight = None
        self.total_throughput_costs = None
        self.total_d2c_freight = None

    def solve_sample(self):
        """
        The Beer Distribution Problem with Extension for A Competitor Supply Node for the PuLP Modeller
        """

        # Creates a list of all the supply nodes
        warehouses = ["A", "B", "C"]

        # Creates a dictionary for the number of units of supply for each supply node
        supply = {"A": 1000, "B": 4000, "C": 100}

        # Creates a list of all demand nodes
        bars = ["1", "2", "3", "4", "5"]

        # Creates a dictionary for the number of units of demand for each demand node
        demand = {
            "1": 500,
            "2": 900,
            "3": 1800,
            "4": 200,
            "5": 700,
        }

        # Creates a list of costs of each transportation path
        costs = [  # bars
            # 1 2 3 4 5
            [2, 4, 5, 2, 1],  # A   warehouses
            [3, 1, 3, 2, 3],  # B
            [0, 0, 0, 0, 0],
        ]

        # The cost data is made into a dictionary
        costs = makeDict([warehouses, bars], costs, 0)

        # Creates the 'prob' variable to contain the problem data
        problem_name = "Beer_Distribution_Problem"
        self.problem_name = problem_name
        prob = LpProblem(problem_name, LpMinimize)

        # Creates a list of tuples containing all the possible routes for transport
        routes = [(w, b) for w in warehouses for b in bars]

        # A dictionary called 'Vars' is created to contain the referenced variables(the routes)
        decision_vars = LpVariable.dicts("Route", (warehouses, bars), 0, None, LpInteger)

        # The objective function is added to 'prob' first
        prob += (
            lpSum([decision_vars[w][b] * costs[w][b] for (w, b) in routes]),
            "Sum_of_Transporting_Costs",
        )

        # The supply maximum constraints are added to prob for each supply node (warehouse)
        for w in warehouses:
            prob += (
                lpSum([decision_vars[w][b] for b in bars]) <= supply[w],
                f"Sum_of_Products_out_of_Warehouse_{w}",
            )

        # The demand minimum constraints are added to prob for each demand node (bar)
        for b in bars:
            prob += (
                lpSum([decision_vars[w][b] for w in warehouses]) >= demand[b],
                f"Sum_of_Products_into_Bar{b}",
            )

        # The problem data is written to an .lp file
        prob.writeLP("BeerDistributionProblem.lp")

        # The problem is solved using PuLP's choice of Solver
        prob.solve()

        self.lp_status = LpStatus[prob.status]
        self.decision_variables = prob.variables()
        self.objective_value = value(prob.objective)

    def prepare_data(self, session):
        # Entities
        # Factory values
        factories_query = session.table("supply_chain_network_optimization_db.entities.factory").select(
            col("name")).collect()
        factories = []
        for i in range(len(factories_query)):
            factories.append(factories_query[i][0])
        self.factories = factories

        production_capacity_values_query = session.table(
            "supply_chain_network_optimization_db.entities.factory").select(
            col("production_capacity")).collect()
        production_capacity_values = []
        for i in range(len(production_capacity_values_query)):
            production_capacity_values.append(production_capacity_values_query[i][0])
        production_capacities = dict(zip(factories, production_capacity_values))
        self.production_capacities = production_capacities

        production_cost_values_query = session.table("supply_chain_network_optimization_db.entities.factory").select(
            col("production_cost")).collect()
        production_cost_values = []
        for i in range(len(production_cost_values_query)):
            production_cost_values.append(float(production_cost_values_query[i][0]))
        production_costs = dict(zip(factories, production_cost_values))
        self.production_costs = production_costs

        # Distributor values
        distributors_query = session.table("supply_chain_network_optimization_db.entities.distributor").select(
            col("name")).collect()
        distributors = []
        for i in range(len(distributors_query)):
            distributors.append(distributors_query[i][0])
        self.distributors = distributors

        throughput_capacity_values_query = session.table(
            "supply_chain_network_optimization_db.entities.distributor").select(
            col("throughput_capacity")).collect()
        throughput_capacity_values = []
        for i in range(len(throughput_capacity_values_query)):
            throughput_capacity_values.append(throughput_capacity_values_query[i][0])
        throughput_capacities = dict(zip(distributors, throughput_capacity_values))
        self.throughput_capacities = throughput_capacities

        throughput_cost_values_query = session.table(
            "supply_chain_network_optimization_db.entities.distributor").select(
            col("throughput_cost")).collect()
        throughput_cost_values = []
        for i in range(len(throughput_cost_values_query)):
            throughput_cost_values.append(float(throughput_cost_values_query[i][0]))
        throughput_costs = dict(zip(distributors, throughput_cost_values))
        self.throughput_costs = throughput_costs

        # Customer Values
        customers_query = session.table("supply_chain_network_optimization_db.entities.customer").select(
            col("name")).collect()
        customers = []
        for i in range(len(customers_query)):
            customers.append(customers_query[i][0])
        self.customers = customers

        demand_values_query = session.table("supply_chain_network_optimization_db.entities.customer").select(
            col("demand")).collect()
        demand_values = []
        for i in range(len(demand_values_query)):
            demand_values.append(demand_values_query[i][0])
        demand = dict(zip(customers, demand_values))
        self.demand = demand

        # Relationships
        # Factory to Distributor freight values
        f2d_rate_values = session.sql('''select *
        from supply_chain_network_optimization_db.relationships.factory_to_distributor_rates_matrix
        order by distributor asc;''').collect()
        f2d_rates = []
        for i in range(len(factories)):
            f2d_rates.append(f2d_rate_values[i][1:len(factories)])

        f2d_costs = makeDict([factories, distributors], f2d_rates, 0)
        self.factory_to_distributor_rates = f2d_costs

        # Factory to Distributor mileage values
        f2d_mileage_values = session.sql('''select *
                from supply_chain_network_optimization_db.relationships.factory_to_distributor_mileage_matrix
                order by distributor asc;''').collect()
        f2d_mileage = []
        for i in range(len(factories)):
            f2d_mileage.append(f2d_mileage_values[i][1:len(factories)])

        f2d_distance = makeDict([factories, distributors], f2d_mileage, 0)
        self.factory_to_distributor_mileage = f2d_distance

        # Distributor to Customer freight values
        d2c_rate_values = session.sql('''select *
                from supply_chain_network_optimization_db.relationships.distributor_to_customer_rates_matrix
                order by customer asc;''').collect()
        d2c_rates = []
        for i in range(len(distributors)):
            d2c_rates.append(f2d_rate_values[i][1:len(distributors)])
        d2c_costs = makeDict([distributors, customers], d2c_rates, 0)
        self.distributor_to_customer_rates = d2c_costs

        # Distributor to Customer freight values
        d2c_mileage_values = session.sql('''select *
                        from supply_chain_network_optimization_db.relationships.distributor_to_customer_mileage_matrix
                        order by customer asc;''').collect()
        d2c_mileage = []
        for i in range(len(distributors)):
            d2c_mileage.append(d2c_mileage_values[i][1:len(distributors)])
        d2c_distance = makeDict([distributors, customers], d2c_mileage, 0)
        self.distributor_to_customer_mileage = d2c_distance

    def solve(self, problem_name):
        factories = self.factories
        production_capacities = self.production_capacities
        production_costs = self.production_costs
        distributors = self.distributors
        throughput_capacities = self.throughput_capacities
        throughput_costs = self.throughput_costs
        customers = self.customers
        demand = self.demand
        factory_to_distributor_mileage = self.factory_to_distributor_mileage
        distributor_to_customer_mileage = self.distributor_to_customer_mileage
        factory_to_distributor_rates = self.factory_to_distributor_rates
        distributor_to_customer_rates = self.distributor_to_customer_rates

        # Creates the 'prob' variable to contain the problem data
        self.problem_name = problem_name
        prob = LpProblem(problem_name, LpMinimize)

        # Creates a list of tuples containing all the possible routes for transport
        f2d_routes = [(f, d) for f in factories for d in distributors]
        d2c_routes = [(d, c) for d in distributors for c in customers]

        # The decision variables - The decisions the solver changes to decide an optimal outcome
        # A dictionary called 'Vars' is created to contain the referenced variables(the routes)
        f2d_decision_vars = LpVariable.dicts("F2DRoute", (factories, distributors), 0, None, LpInteger)
        d2c_decision_vars = LpVariable.dicts("D2CRoute", (distributors, customers), 0, None, LpInteger)

        # The Objective Function - What the real goal is and how it is calculated
        # The objective function is added to 'prob' first
        f2d_miles = lpSum([f2d_decision_vars[f][d] * factory_to_distributor_mileage[f][d] for (f, d) in f2d_routes])
        d2c_miles = lpSum([d2c_decision_vars[d][c] * distributor_to_customer_mileage[d][c] for (d, c) in d2c_routes])
        f_production = lpSum([f2d_decision_vars[f][d] * production_costs[f] for (f, d) in f2d_routes])
        f2d_freight = lpSum([f2d_decision_vars[f][d] * factory_to_distributor_rates[f][d]
                             for (f, d) in f2d_routes])
        d_throughput = lpSum([d2c_decision_vars[d][c] * throughput_costs[d] for (d, c) in d2c_routes])
        d2c_freight = lpSum([d2c_decision_vars[d][c] * distributor_to_customer_rates[d][c]
                             for (d, c) in d2c_routes])

        # Determine which objective based on the problem
        if problem_name == "Minimize_Distance":
            prob += (
                f2d_miles + d2c_miles,
                "Sum_of_Transporting_Distance",
            )
        elif problem_name == "Minimize_Freight":
            prob += (
                f2d_freight + d2c_freight,
                "Sum_of_Freight",
            )
        elif problem_name == "Minimize_Total_Fulfillment":
            prob += (
                f_production + f2d_freight + d_throughput + d2c_freight,
                "Sum_of_Fulfillment_Costs",
            )

        # Constraint Section - The limits that define the business realities
        # Factories can only ship as much as they can produce
        for f in factories:
            prob += (
                lpSum([f2d_decision_vars[f][d] for d in distributors]) <= production_capacities[f],
                f"Sum_of_Products_out_of_Factory_{f}",
            )

        # Factories can only ship to distributors up until their throughput capacity
        for d in distributors:
            prob += (
                lpSum([f2d_decision_vars[f][d] for f in factories]) <= throughput_capacities[d],
                f"Sum_of_Products_into_Distributor_{d}",
            )

        # Distributors need to have inventory replenished
        for d in distributors:
            prob += (
                    lpSum([f2d_decision_vars[f][d] for f in factories]) >= lpSum([d2c_decision_vars[d][c] for c in
                                                                                  customers])
            )

        # Distributors can only ship their throughput capacity
        for d in distributors:
            prob += (
                lpSum([d2c_decision_vars[d][c] for c in customers]) <= throughput_capacities[d],
                f"Sum_of_Products_out_of_Distributor_{d}",
            )

        # Customer demand must be covered
        for c in customers:
            prob += (
                lpSum([d2c_decision_vars[d][c] for d in distributors]) >= demand[c],
                f"Sum_of_Products_to_Customer_{c}",
            )

        # The problem data is written to an .lp file
        prob.writeLP(problem_name + ".lp")

        # The problem is solved using PuLP's choice of Solver
        prob.solve()

        self.total_f2d_miles = value(f2d_miles)
        self.total_d2c_miles = value(d2c_miles)
        self.total_production_costs = value(f_production)
        self.total_f2d_freight = value(f2d_freight)
        self.total_throughput_costs = value(d_throughput)
        self.total_d2c_freight = value(d2c_freight)

        self.lp_status = LpStatus[prob.status]
        self.decision_variables = prob.variables()
        try:
            self.objective_value = value(prob.objective)
        except:
            self.objective_value = None


class Page(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def print_page(self):
        pass

    @abstractmethod
    def print_sidebar(self):
        pass


def set_default_sidebar():
    # Sidebar for navigating pages
    with st.sidebar:
        st.title("Supply Chain Network Optimization 🐻‍❄")
        st.markdown("")
        st.markdown("This application sets up and runs 3-tier network optimization models.")
        st.markdown("")
        if st.button(label="Data Preparation 🗃️", help="Warning: Unsaved changes will be lost!"):
            set_page('Data Preparation')
            st.experimental_rerun()
        if st.button(label="Model Parameters 🧮", help="Warning: Unsaved changes will be lost!"):
            set_page('Model Parameters')
            st.experimental_rerun()
        if st.button(label="Model Execution 🚀", help="Warning: Unsaved changes will be lost!"):
            set_page('Model Execution')
            st.experimental_rerun()
        if st.button(label="Model Results 📊", help="Warning: Unsaved changes will be lost!"):
            set_page('Model Results')
            st.experimental_rerun()
        if st.button(label="Cortex Enrichment 🤖", help="Warning: Unsaved changes will be lost!"):
            set_page('Cortex Enrichment')
            st.experimental_rerun()
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        if st.button(label="Return Home", help="Warning: Unsaved changes will be lost!"):
            set_page('Welcome')
            st.experimental_rerun()


class WelcomePage(Page):
    def __init__(self):
        self.name = "Welcome"

    def print_page(self):
        # Set up main page
        col1, col2 = st.columns((6, 1))
        col1.title("Supply Chain Network Optimization 🐻‍❄️")

        # Welcome page
        st.subheader("Welcome ❄️")

        st.write('''The Supply Chain Network Optimization Model utilizes [Linear Programming](
        https://en.wikipedia.org/wiki/Linear_programming), also called linear optimization or constraint programming.''')

        st.write('''Linear programming uses a system of inequalities to define a feasible regional mathematical space, and a 
        'solver' that traverses that space by adjusting a number of decision variables, efficiently finding the most optimal 
        set of decisions given constraints to meet a stated objective function.''')

        st.write('''It is possible to also introduce integer-based decision 
        variables, which transforms the problem from a linear program into a mixed integer program, but the mechanics are 
        fundamentally the same.''')

        st.write("The objective function defines the goal - maximizing or minimizing a value, such as profit or costs.")
        st.write("Decision variables are a set of decisions - the values that the solver can change to impact the "
                 "objective value.")
        st.write(
            "The constraints define the realities of the business - such as only shipping up to a stated capacity.")

        st.write("We are utilizing the PuLP library, which allows us to define a linear program and use virtually any "
                 "solver we want.  We are using PuLP's default free [CBC solver](https://github.com/coin-or/Cbc) for our "
                 "models.")

        with st.expander("Additional Resources", expanded=True):
            st.write("Interested in [Optimization Programming using PuLP](https://coin-or.github.io/pulp/)?")

    def print_sidebar(self):
        set_default_sidebar()


class DataPreparationPage(Page):
    def __init__(self):
        self.name = "Data Preparation"

    def print_page(self):
        def deploy_demo_data(customer_count, session):
            customer_list = []
            for i in range(0, customer_count):
                coordinates_list = fake.local_latlng()
                cust_id = operator.add(i, 3001)
                lat = coordinates_list[0]
                lon = coordinates_list[1]
                long_lat = "POINT (" + coordinates_list[1] + " " + coordinates_list[0] + ")"
                name = str(fake.company()) + "_" + str(i)
                demand = randint(1, 100)
                customer_list.append([cust_id, name, lon, lat, long_lat, demand])

            # Set up database
            data = session.create_dataframe(customer_list, schema=["ID", "NAME", "LATITUDE", "LONGITUDE",
                                                                   "LONG_LAT", "DEMAND"])
            data.write.mode("overwrite").saveAsTable("entities.customer")
            session.sql(factories_sql).collect()
            session.sql(distributor_sql).collect()

            session.sql(fact_to_dist_sql).collect()
            session.sql(dist_to_cust_sql).collect()

            sync_factory_data(session)
            sync_distributor_data(session)

            # Store customers to show in UI
            st.session_state.customer_data = customer_list

        def sync_factory_data(session):
            fact_to_dist_rate_pivot_sql = session.sql(fact_to_dist_rate_matrix_sql).collect()[0][0]
            session.sql(fact_to_dist_rate_pivot_sql).collect()

            fact_to_dist_mileage_pivot_sql = session.sql(fact_to_dist_mileage_matrix_sql).collect()[0][0]
            session.sql(fact_to_dist_mileage_pivot_sql).collect()

        def sync_distributor_data(session):
            dist_to_cust_rate_pivot_sql = session.sql(dist_to_cust_rate_matrix_sql).collect()[0][0]
            session.sql(dist_to_cust_rate_pivot_sql).collect()

            dist_to_cust_mileage_pivot_sql = session.sql(dist_to_cust_mileage_matrix_sql).collect()[0][0]
            session.sql(dist_to_cust_mileage_pivot_sql).collect()

        col1, col2 = st.columns((6, 1))
        col1.title("Supply Chain Network Optimization 🐻‍❄️")

        st.subheader("Let's Mock Some Data ❄️")

        st.write("The following will set up all of the data objects in Snowflake for the model, including generating a "
                 "synthetic set of customers.")

        st.write("There is some randomness associated with the data generation, so it is possible to get infeasible "
                 "results if too many customers are created.  The number and capacities of factories and distributors "
                 "are fixed.  We recommend going with the 1000 for a feasible model, but feel free to increase in "
                 "order to see what infeasible results look like.  Also, using the Model Parameters page, "
                 "you can increase capacities to make more customers feasible.")

        count_column, submit_column = st.columns(2)

        customer_count = count_column.number_input(label="How many test customers would you like to include?",
                                                   min_value=1,
                                                   max_value=1000000, value=1000)
        submit_column.write(" #")

        if submit_column.button("Generate Data"):
            with st.spinner("Generating Synthetic Customer Data..."):
                deploy_demo_data(customer_count, session)
                df = pd.DataFrame(st.session_state.customer_data,
                                  columns=["ID", "LONGITUDE", "LATITUDE", "LAT_LONG", "NAME", "DEMAND"])
                st.dataframe(df)

    def print_sidebar(self):
        set_default_sidebar()


class ModelParametersPage(Page):
    def __init__(self):
        self.name = "Model Parameters"

    def print_page(self):
        col1, col2 = st.columns((6, 1))
        col1.title("Supply Chain Network Optimization 🐻‍❄️")

        # Used to view and modify model values within Snowflake
        st.subheader("Model Parameters ❄️")

        factory_tab, distributor_tab, customer_tab, f2d_rates_tab, d2c_rates_tab = st.tabs(["Factories", "Distributors",
                                                                                            "Customers", "F2D Rates",
                                                                                            "D2C Rates"])
        with st.spinner("Retrieving Filter Values..."):
            factories_query = session.table("supply_chain_network_optimization_db.entities.factory").select(
                col("name")).collect()
            factories = []
            for i in range(len(factories_query)):
                factories.append(factories_query[i][0])

            distributors_query = session.table("supply_chain_network_optimization_db.entities.distributor").select(
                col("name")).collect()
            distributors = []
            for i in range(len(distributors_query)):
                distributors.append(distributors_query[i][0])

            customers_query = session.table("supply_chain_network_optimization_db.entities.customer").select(
                col("name")).collect()
            customers = []
            for i in range(len(customers_query)):
                customers.append(customers_query[i][0])

        with factory_tab:
            results = session.table("supply_chain_network_optimization_db.entities.factory")
            st.write(results)

        with distributor_tab:
            results = session.table("supply_chain_network_optimization_db.entities.distributor")
            st.write(results)

        with customer_tab:
            results = session.table("supply_chain_network_optimization_db.entities.customer")
            st.write(results)

        with f2d_rates_tab:
            factory_name = st.selectbox("Choose a factory name", factories)

            results = session.table(
                "supply_chain_network_optimization_db.relationships.factory_to_distributor_rates").filter(
                col("factory") == factory_name)
            st.write(results)

        with d2c_rates_tab:
            distributor_name = st.selectbox("Choose a distributor name", distributors)

            results = session.table(
                "supply_chain_network_optimization_db.relationships.distributor_to_customer_rates").filter(
                col("distributor") == distributor_name)
            st.write(results)

        st.write("After updating the parameters, please move on to model execution!")

    def print_sidebar(self):
        set_default_sidebar()


class ModelExecutionPage(Page):
    def __init__(self):
        self.name = "Model Execution"

    def print_page(self):
        col1, col2 = st.columns((6, 1))
        col1.title("Supply Chain Network Optimization 🐻‍❄️")

        # Connects to Snowflake, prepares the model data, and runs the model
        st.subheader("Model Execution ❄️")

        with st.form("model_parameters_form"):
            st.write("The model runner runs three different scenarios:")
            st.write("-- Minimize Distance (Crawl) - Emulating a fairly naive present-day")
            st.write("-- Minimize Freight (Walk) - Emulating an organization with freight analytics capability")
            st.write("-- Minimize Total Fulfillment (Run) - Emulating a fully realized network analytics capability")

            submitted = st.form_submit_button("Run")

            col_dict = {
                "Minimize_Distance": "distance_col",
                "Minimize_Freight": "freight_col",
                "Minimize_Total_Fulfillment": "fulfillment_col",
            }

            if submitted:
                if session.table("supply_chain_network_optimization_db.entities.customer").count() > 0:
                    with st.spinner("Solving Models..."):
                        model = OptimizationModel()
                        model.prepare_data(session)

                        model_count = 1
                        for current_model in ["Minimize_Distance", "Minimize_Freight", "Minimize_Total_Fulfillment"]:
                            model.solve(current_model)

                            decision_variables = []

                            for v in model.decision_variables:
                                variant = {v.name: v.varValue}
                                decision_variables.append(variant)

                            run_date = datetime.datetime.now()

                            model_list = [[model.problem_name, run_date, model.lp_status, model.objective_value,
                                           decision_variables, model.total_f2d_miles, model.total_d2c_miles,
                                           model.total_production_costs, model.total_f2d_freight,
                                           model.total_throughput_costs,
                                           model.total_d2c_freight]]

                            model_df = session.create_dataframe(model_list, schema=["problem_name",
                                                                                    "run_date",
                                                                                    "lp_status",
                                                                                    "objective_value",
                                                                                    "decision_variables",
                                                                                    "total_f2d_miles",
                                                                                    "total_d2c_miles",
                                                                                    "total_production_costs",
                                                                                    "total_f2d_freight",
                                                                                    "total_throughput_costs",
                                                                                    "total_d2c_freight"])
                            model_df.write.mode("append").saveAsTable("results.model_results")

                            st.subheader("Model Name: " + model.problem_name)

                            # The status of the solution is printed to the screen
                            st.write("Status: ", model.lp_status)

                            # Each of the variables is printed with it's resolved optimum value
                            st.write("Decisions Made: ", len(model.decision_variables))

                            with st.expander("Sample of Decisions: "):
                                # The optimised objective function value is printed to the screen
                                st.write("Objective Value (based on goal): ", model.objective_value)

                                variable_count = 0
                                for v in model.decision_variables:
                                    while variable_count <= 1000:
                                        st.write(v.name, "=", v.varValue)
                                        variable_count += 1

                            total_fulfillment_costs = model.total_production_costs + model.total_f2d_freight + \
                                                      model.total_throughput_costs + model.total_d2c_freight

                            st.write("Total Fulfillment Costs: ", total_fulfillment_costs)

                            with st.expander("Result Details: "):
                                st.write("Factory-to-Distributor Miles: ", model.total_f2d_miles)
                                st.write("Distributor-to-Customer Miles: ", model.total_d2c_miles)
                                st.write("Total Production Costs: ", model.total_production_costs)
                                st.write("Total Factory-to-Distributor Freight: ", model.total_f2d_freight)
                                st.write("Total Distributor Throughput Costs: ", model.total_throughput_costs)
                                st.write("Total Distributor-to-Customer Freight: ", model.total_d2c_freight)

                    st.success("Models Solved!")
                    st.snow()
                else:
                    st.warning("Data is not loaded, please go back to Data Preparation and generate customer data "
                               "first.")

        st.write("After running the models, please move on to the results!")

    def print_sidebar(self):
        set_default_sidebar()


class ModelResultsPage(Page):
    def __init__(self):
        self.name = "Model Results"

    def print_page(self):
        col1, col2 = st.columns((6, 1))
        col1.title("Supply Chain Network Optimization 🐻‍❄️")

        # Used for viewing and analyzing the results
        st.subheader("Model Results ❄️")

        problem_name_col, run_date_col = st.columns(2)

        problem_name_query = session.table(
            "supply_chain_network_optimization_db.results.factory_to_distributor_shipment_details_vw").select(
            col("problem_name")).distinct().collect()
        problem_names = []
        for i in range(len(problem_name_query)):
            problem_names.append(problem_name_query[i][0])

        with problem_name_col:
            problem_name = st.selectbox("Select a Problem Name", problem_names)

        run_date_query = session.table(
            "supply_chain_network_optimization_db.results.factory_to_distributor_shipment_details_vw").filter(
            col("problem_name") == problem_name).select(col("run_date")).distinct().sort(col("run_date").desc()) \
            .collect()
        run_dates = []
        for i in range(len(run_date_query)):
            run_dates.append(run_date_query[i][0])

        with run_date_col:
            run_date = st.selectbox("Select a Run Date", run_dates)

        f2d_results_df = session.table(
            "supply_chain_network_optimization_db.results.factory_to_distributor_shipment_details_vw").filter(
            col("problem_name") == problem_name).filter(col("run_date") == run_date)
        c2d_results_df = session.table(
            "supply_chain_network_optimization_db.results.distributor_to_customer_shipment_details_vw").filter(
            col("problem_name") == problem_name).filter(col("run_date") == run_date)

        factories_query = session.table(
            "supply_chain_network_optimization_db.results.factory_to_distributor_shipment_details_vw").select(
            col("factory_name")).distinct().sort(col("factory_name").asc()).collect()
        factories = [""]
        for i in range(len(factories_query)):
            factories.append(factories_query[i][0])

        distributors_query = session.table(
            "supply_chain_network_optimization_db.results.factory_to_distributor_shipment_details_vw").select(
            col("distributor_name")).distinct().sort(col("distributor_name").asc()).collect()
        distributors = [""]
        for i in range(len(distributors_query)):
            distributors.append(distributors_query[i][0])

        customers_query = session.table(
            "supply_chain_network_optimization_db.results.distributor_to_customer_shipment_details_vw").select(
            col("customer_name")).distinct().sort(col("customer_name").asc()).collect()
        customers = [""]
        for i in range(len(customers_query)):
            customers.append(customers_query[i][0])

        factory_filter_col, distributor_filter_col, customer_filter_col = st.columns(3)

        with factory_filter_col:
            factory_name = st.selectbox("Choose a factory name", factories)
        with distributor_filter_col:
            distributor_name = st.selectbox("Choose a distributor name", distributors)
        with customer_filter_col:
            customer_name = st.selectbox("Choose a customer name", customers)

        # Tables
        st.subheader("Shipment Tables")
        fact_to_dist_col, dist_to_cust_col = st.columns(2)

        with fact_to_dist_col:
            st.write("Amount shipped from Factories to Distributors:")
            st.write(f2d_results_df.select(col("factory_name"), col("distributor_name"),
                                           col("factory_to_distributor_shipped_amount"))
                     .filter(col("factory_to_distributor_shipped_amount") > 0)
                     .filter((col("factory_name") == factory_name) | (factory_name == ""))
                     .filter((col("distributor_name") == distributor_name) | (distributor_name == ""))
                     .collect())

        with dist_to_cust_col:
            st.write("Amount shipped from Distributors to Customers:")
            st.write(c2d_results_df.select(col("distributor_name"), col("customer_name"),
                                           col("distributor_to_customer_shipped_amount"))
                     .filter(col("distributor_to_customer_shipped_amount") > 0)
                     .filter((col("distributor_name") == distributor_name) | (distributor_name == ""))
                     .filter((col("customer_name") == customer_name) | (customer_name == ""))
                     .collect())

        # Maps
        st.subheader("Shipment Maps")
        factory_map_col, distributor_map_col, customer_map_col = st.columns(3)

        if f2d_results_df.count() > 0:
            with factory_map_col:
                st.map(f2d_results_df
                       .filter(col("factory_to_distributor_shipped_amount") > 0)
                       .filter((col("factory_name") == factory_name) | (factory_name == ""))
                       .filter((col("distributor_name") == distributor_name) | (distributor_name == ""))
                       .select(col("factory_latitude").cast("float").alias("latitude"),
                               col("factory_longitude").cast("float").alias("longitude")).collect())

        if c2d_results_df.count() > 0:
            with distributor_map_col:
                st.map(c2d_results_df
                       .filter(col("distributor_to_customer_shipped_amount") > 0)
                       .filter((col("distributor_name") == distributor_name) | (distributor_name == ""))
                       .select(col("distributor_latitude").cast("float").alias("latitude"),
                               col("distributor_longitude").cast("float").alias("longitude")).collect())

        if c2d_results_df.count() > 0:
            with customer_map_col:
                st.map(c2d_results_df
                       .filter(col("distributor_to_customer_shipped_amount") > 0)
                       .filter((col("distributor_name") == distributor_name) | (distributor_name == ""))
                       .filter((col("customer_name") == customer_name) | (customer_name == ""))
                       .select(col("customer_latitude").cast("float").alias("latitude"),
                               col("customer_longitude").cast("float").alias("longitude")).collect())

    def print_sidebar(self):
        set_default_sidebar()


class CortexPage(Page):
    def __init__(self):
        self.name = "Cortex Enrichment"

    def print_page(self):

        closest_airport_sql = '''select
        id
    ,   snowflake.cortex.complete(
        'llama2-70b-chat', 
        concat('A factory is located in ', city, ', ', state, ', ', country, ' at latitude ', latitude, ' and longitude ', longitude, '.  Given that latitude and longitude represent geographic coordinates, what is the closest airport?  You are only allowed to respond with the name of the airport, nothing more.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.')) as closest_airport
from supply_chain_network_optimization_db.entities.factory'''
        closest_interstate_sql = '''select
        id
    ,   snowflake.cortex.complete(
        'llama2-70b-chat', 
        concat('A factory is located in ', city, ', ', state, ', ', country, ' at latitude ', latitude, ' and longitude ', longitude, '.  Given that latitude and longitude represent geographic coordinates, what is the closest interstate?  You are only allowed to respond with the name of the interstate, nothing more.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.')) as closest_interstate
from supply_chain_network_optimization_db.entities.factory'''
        closest_river_sql = '''select
        id
    ,   snowflake.cortex.complete(
        'llama2-70b-chat', 
        concat('A factory is located in ', city, ', ', state, ', ', country, ' at latitude ', latitude, ' and longitude ', longitude, '.  Given that latitude and longitude represent geographic coordinates, what is the closest river?  You are only allowed to respond with the name of the river, nothing more.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.  Simply respond with the name of the river.')) as closest_river
from supply_chain_network_optimization_db.entities.factory'''
        average_temperature_sql = '''select
        id
    ,   regexp_substr(snowflake.cortex.complete(
        'llama2-70b-chat', 
        concat('A factory is located in ', city, ', ', state, ', ', country, ' at latitude ', latitude, ' and longitude ', longitude, '.  Given that latitude and longitude represent geographic coordinates, what is the average annual temperature in fahrenheit?  Format your answer as a number without text.  Do not include celsius.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.  Only reply with a number.')),'[0-9]{1,3}\\.?[0-9]?') as average_temperature
from supply_chain_network_optimization_db.entities.factory'''
        average_rainfall_sql = '''select
        id
    ,   regexp_substr(snowflake.cortex.complete(
        'llama2-70b-chat', 
        concat('A factory is located in ', city, ', ', state, ', ', country, ' at latitude ', latitude, ' and longitude ', longitude, '.  Given that latitude and longitude represent geographic coordinates, what is the average annual rainfall in inches?  Format your answer as a number without text.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.  Only reply with a number.')),'[0-9]{1,4}\\.?[0-9]?') as average_rainfall
from supply_chain_network_optimization_db.entities.factory'''

        sql_statements = {
                "Closest Airport": closest_airport_sql
            ,   "Closest Interstate": closest_interstate_sql
            ,   "Closest River": closest_river_sql
            ,   "Average Temperature": average_temperature_sql
            ,   "Average Rainfall": average_rainfall_sql
        }

        prompt_examples = {
                "Closest Airport": """A factory is located in Springfield, Illinois, United States at latitude 39.798363 and longitude -89.654961.  Given that latitude and longitude represent geographic coordinates, what is the closest airport?  You are only allowed to respond with the name of the airport, nothing more.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence."""
            ,   "Closest Interstate": """A factory is located in Springfield, Illinois, United States at latitude 39.798363 and longitude -89.654961.  Given that latitude and longitude represent geographic coordinates, what is the closest interstate?  You are only allowed to respond with the name of the interstate, nothing more.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence."""
            ,   "Closest River": """A factory is located in Springfield, Illinois, United States at latitude 39.798363 and longitude -89.654961.  Given that latitude and longitude represent geographic coordinates, what is the closest river?  You are only allowed to respond with the name of the river, nothing more.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.  Simply respond with the name of the river."""
            ,   "Average Temperature": """A factory is located in Springfield, Illinois, United States at latitude 39.798363 and longitude -89.654961.  Given that latitude and longitude represent geographic coordinates, what is the average annual temperature in fahrenheit?  Format your answer as a number without text.  Do not include celsius.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.  Only reply with a number."""
            ,   "Average Rainfall": """A factory is located in Springfield, Illinois, United States at latitude 39.798363 and longitude -89.654961.  Given that latitude and longitude represent geographic coordinates, what is the average annual rainfall in inches?  Format your answer as a number without text.  Do not explain the answer.  Do not add comments.  Do not include a note.  Do not reply in a sentence.  Only reply with a number."""
        }

        col1, col2 = st.columns((6, 1))
        col1.title("Supply Chain Network Optimization 🐻‍❄️")

        # Used for analyzing results with Cortex
        st.subheader("Cortex Enrichment ❄️")

        st.info("The following page includes Cortex's Large Language Model (LLM) functions, which are in *Preview*")

        st.write("Let's enrich our information on our factories to see if we can get greater context for the supply "
                 "chain.")

        if "factory_df" not in st.session_state:
            st.session_state.factory_df = session.table("supply_chain_network_optimization_db.entities.factory")\
                .select(col("id"), col("name"), col("latitude"), col("longitude"), col("city"), col("state"), col("country"))\
                .distinct()

        with st.form("EnrichmentForm"):
            st.subheader("Enrichment Selection")

            st.write("Choose which fields you'd like to add to the factory data.")
            st.write("The data will be provided via Cortex, using the llama2-70b-chat model.  Check out the expander below to see the "
                     "prompts/queries directly.")

            add_closest_airport = st.checkbox("Closest Airport", value=False)
            add_closest_interstate = st.checkbox("Closest Interstate", value=False)
            add_closest_river = st.checkbox("Closest River", value=False)
            add_average_temperature = st.checkbox("Average Temperature", value=False)
            add_average_rainfall = st.checkbox("Average Rainfall", value=False)

            submitted = st.form_submit_button("Enrich")

            if submitted:
                factory_df = st.session_state.factory_df

                if add_closest_airport:
                    closest_airport_df = session.sql(closest_airport_sql)
                    factory_df = factory_df.join(closest_airport_df, factory_df.id == closest_airport_df.id, rsuffix="_airport")

                if add_closest_interstate:
                    closest_interstate_df = session.sql(closest_interstate_sql)
                    factory_df = factory_df.join(closest_interstate_df, factory_df.id == closest_interstate_df.id, rsuffix="_interstate")

                if add_closest_river:
                    closest_river_df = session.sql(closest_river_sql)
                    factory_df = factory_df.join(closest_river_df, factory_df.id == closest_river_df.id, rsuffix="_river")

                if add_average_temperature:
                    average_temperature_df = session.sql(average_temperature_sql)
                    factory_df = factory_df.join(average_temperature_df, factory_df.id == average_temperature_df.id, rsuffix="_temperature")

                if add_average_rainfall:
                    average_rainfall_df = session.sql(average_rainfall_sql)
                    factory_df = factory_df.join(average_rainfall_df, factory_df.id == average_rainfall_df.id, rsuffix="_rainfall")

                st.dataframe(factory_df)

        with st.expander("See the queries themselves"):
            selected_query = st.selectbox("Which query would you like to view?", sql_statements.keys())
            st.code(sql_statements[selected_query])

            st.write("Example Prompt")
            st.caption(prompt_examples[selected_query])

    def print_sidebar(self):
        set_default_sidebar()


pages = [WelcomePage(), DataPreparationPage(), ModelParametersPage(), ModelExecutionPage(), ModelResultsPage(), CortexPage()]


def main():
    for page in pages:
        if page.name == st.session_state.page:
            page.print_page()
            page.print_sidebar()


main()

$$);

/* add environment file for streamlit - includes all references to libraries that the Streamlit needs */
insert into supply_chain_network_optimization_db.code.script (name , script) 
values ( 'ENVIRONMENT_V1',$$
name: sf_env
channels:
- snowflake
dependencies:
- pulp
- faker
- snowflake-ml-python

$$);

/* put files into stage */
call supply_chain_network_optimization_db.code.put_to_stage('supply_chain_network_optimization_db.streamlit.streamlit_stage','streamlit_ui.py', (select script from supply_chain_network_optimization_db.code.script where name = 'STREAMLIT_V1'));
call supply_chain_network_optimization_db.code.put_to_stage('supply_chain_network_optimization_db.streamlit.streamlit_stage','environment.yml', (select script from supply_chain_network_optimization_db.code.script where name = 'ENVIRONMENT_V1'));

/* create streamlit */
create or replace streamlit supply_chain_network_optimization_db.streamlit.supply_chain_network_optimization
  root_location = '@supply_chain_network_optimization_db.streamlit.streamlit_stage'
  main_file = '/streamlit_ui.py'
  query_warehouse = scno_wh
  comment='{"origin":"sf_sit","name":"scno","version":{"major":1, "minor":0},"attributes":{"component":"scno"}}'; 