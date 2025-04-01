/*************************************************************************************************************
Script:             Solution Installation Wizard Demo Configuration
Create Date:        2024-03-25
Author:             B. Klein
Description:        Demo Configuration
Copyright © 2023 Snowflake Inc. All rights reserved
**************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-03-25          B. Klein                            Initial Creation
*************************************************************************************************************/

/* example placeholders */
/* current_* is just for local testing, you would typically use consumer account details in this table */
insert into sol_inst_wzd_package.admin.placeholder_definition (consumer_organization_name, consumer_account_name, placeholder_text, replacement_value)
values
        (current_organization_name(), current_account_name(), 'PARTNER_ABBREV', 'SF_TEST_1')
    ,   (current_organization_name(), current_account_name(), 'PARTNER_ID','0123456789')
    ,   ('<<MY CONSUMERS ORG>>','<<MY CONSUMERS ACCOUNT NAME>>','PARTNER_ABBREV','SF_TEST_2')
;

/* insert an example scripts */
insert into sol_inst_wzd_package.admin.script
values
(
        'Hello World App'
    ,   'An app to say hello to the world'
    ,   'App Setup'
    ,   1
    ,   false
    ,   true
    ,   'Initial select'
    ,   $$select 'hello_world' as PARTNER_ABBREV;$$
)
;

insert into sol_inst_wzd_package.admin.script
values
(
        'Hello World App'
    ,   'An app to say hello to the world'
    ,   'App Setup 2'
    ,   2
    ,   true
    ,   true
    ,   'Second select'
    ,   $$select 'hello_world_2' as PARTNER_ABBREV;$$
)
;

insert into sol_inst_wzd_package.admin.script
values
(
        'Hello World App'
    ,   'An app to say hello to the world'
    ,   'App Setup 3'
    ,   3
    ,   true
    ,   false
    ,   'Third select'
    ,   $$select 'hello_world_3' as PARTNER_ABBREV;$$
)
;

insert into sol_inst_wzd_package.admin.script
values
(
        'Extra App'
    ,   $$An extra app you didn't know you needed$$
    ,   'App Setup'
    ,   1
    ,   false
    ,   true
    ,   'Initial select'
    ,   $$select 'hello_world' as PARTNER_ABBREV;$$
)
;

/* streamlit deployment example */
insert into sol_inst_wzd_package.admin.script
select
        'Streamlit Deployment' as workflow_name
    ,   $$Demonstrates deploying a consumer-owned Streamlit$$ as workflow_description
    ,   'Full Depoyment' as script_name
    ,   1 as script_order
    ,   false as is_autorun
    ,   true as is_autorun_code_visible
    ,   'Deploys a consumer-owned Streamlit' as script_description
    ,
(select REGEXP_REPLACE($$
use role accountadmin;

create warehouse if not exists xs_wh;

create or replace database streamlit_deploy_demo_db;
create or replace schema streamlit_deploy_demo_db.streamlit;
drop schema streamlit_deploy_demo_db.public;
create or replace stage streamlit_deploy_demo_db.streamlit.streamlit_stage;

create or replace schema streamlit_deploy_demo_db.code;

/* helper stored procedure to help create the objects */
create or replace procedure streamlit_deploy_demo_db.code.put_to_stage(stage varchar,filename varchar, content varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='put_to_stage'
AS :::
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
:::;

create or replace table streamlit_deploy_demo_db.code.script (
	name varchar,
	script varchar(16777216)
);

/* streamlit */
insert into streamlit_deploy_demo_db.code.script (name , script)
values ( 'STREAMLIT_V1',:::
# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Example Streamlit App :balloon:")
st.write(
    """Replace this example with your own code!
    **And if you're new to Streamlit,** check
    out our easy-to-follow guides at
    [docs.streamlit.io](https://docs.streamlit.io).
    """
)

# Get the current credentials
session = get_active_session()

# Use an interactive slider to get user input
hifives_val = st.slider(
    "Number of high-fives in Q3",
    min_value=0,
    max_value=90,
    value=60,
    help="Use this to enter the number of high-fives you gave in Q3",
)

#  Create an example dataframe
#  Note: this is just some dummy data, but you can easily connect to your Snowflake data
#  It is also possible to query data using raw SQL using session.sql() e.g. session.sql("select * from table")
created_dataframe = session.create_dataframe(
    [[50, 25, "Q1"], [20, 35, "Q2"], [hifives_val, 30, "Q3"]],
    schema=["HIGH_FIVES", "FIST_BUMPS", "QUARTER"],
)

# Execute the query and convert it into a Pandas dataframe
queried_data = created_dataframe.to_pandas()

# Create a simple bar chart
# See docs.streamlit.io for more types of charts
st.subheader("Number of high-fives")
st.bar_chart(data=queried_data, x="QUARTER", y="HIGH_FIVES")

st.subheader("Underlying data")
st.dataframe(queried_data, use_container_width=True)

:::);

/* put files into stage */
call streamlit_deploy_demo_db.code.put_to_stage('streamlit_deploy_demo_db.streamlit.streamlit_stage','streamlit_ui.py', (select script from streamlit_deploy_demo_db.code.script where name = 'STREAMLIT_V1'));

/* create streamlit */
create or replace streamlit streamlit_deploy_demo_db.streamlit.streamlit_demo
  root_location = '@streamlit_deploy_demo_db.streamlit.streamlit_stage'
  main_file = '/streamlit_ui.py'
  query_warehouse = xs_wh;
$$,':::','$$')) as script_text
;
