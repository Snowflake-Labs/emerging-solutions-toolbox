import streamlit as st
from appPages.page import BasePage, col, set_page

class InitialSetupPage(BasePage):
    def __init__(self):
        self.name = "initial_setup"

    def print_page(self):
        session = st.session_state.session

        collection_names_pd = (
            session.table("ADMIN.SUBSCRIPTION")
            .select(col("CUSTOMER_NAME"), col("TARGET_COLLECTION_NAME"))
            .distinct()
            .to_pandas()
        )

        customer_name = collection_names_pd.loc[0, "CUSTOMER_NAME"]

        st.header("Initial Setup")
        st.markdown("")
        st.markdown("**Please run through the following steps to setup the Data Sharing App:**")
        st.markdown("")
        with st.expander("**Step 1**", expanded=True):
            st.write("Run the following in a worksheet to set up your listing to share back your mapped data.")
            st.code(f'''use role accountadmin;
create or replace share data_model_mapper_share;
grant usage on database data_model_mapper_share_db to share data_model_mapper_share;
grant usage on schema data_model_mapper_share_db.configuration to share data_model_mapper_share;
grant usage on schema data_model_mapper_share_db.mapped to share data_model_mapper_share;
grant usage on schema data_model_mapper_share_db.modeled to share data_model_mapper_share;
grant usage on schema data_model_mapper_share_db.validated to share data_model_mapper_share;

grant select on table data_model_mapper_share_db.configuration.source_collection to share data_model_mapper_share;
grant select on table data_model_mapper_share_db.configuration.source_collection_filter_condition to share data_model_mapper_share;
grant select on table data_model_mapper_share_db.configuration.source_entity to share data_model_mapper_share;
grant select on table data_model_mapper_share_db.configuration.source_entity_join_condition to share data_model_mapper_share;
grant select on table data_model_mapper_share_db.configuration.source_entity_attribute to share data_model_mapper_share;
grant select on table data_model_mapper_share_db.configuration.source_to_target_mapping to share data_model_mapper_share;
grant select on table data_model_mapper_share_db.validated.validation_log to share data_model_mapper_share;

create listing {customer_name}_data_model_mapper_app_share in data exchange SNOWFLAKE_DATA_MARKETPLACE
for share data_model_mapper_share as
$$
title: "{customer_name} Data Mapping App Share"
description: "The shareback from the Data Mapper App"
terms_of_service:
type: "OFFLINE"
auto_fulfillment:
refresh_schedule: "10 MINUTE"
refresh_type: "FULL_DATABASE"
targets:
accounts: ["ORG_NAME.ACCOUNT_NAME"]
$$;

alter listing  {customer_name}_data_model_mapper_app_share set state = published;

show listings like '{customer_name}_data_model_mapper_app_share' in data exchange snowflake_data_marketplace;''', language="sql")

        with st.expander("**Step 2**", expanded=True):
            st.write("Run the following in a worksheet to transfer ownership of the share_views stored procedure to a local role in your environment.")
            st.code(f'''use role accountadmin;
grant ownership on procedure data_model_mapper_share_db.utility.share_views to role <role that will be using the app>;''', language="sql")
            
        with st.expander("**Step 3**", expanded=True):
            st.write("Run the following in a worksheet to create a task that runs the share_views stored procedure once a day. This will ensure any new views are added to your data share. ")
            st.code(f'''use role <role that will be using the app>; 
create or replace task data_model_mapper_share_db.utility.share_views_daily_task
WAREHOUSE = <your warehouse>
SCHEDULE = 'USING CRON 0 4 * * * UTC' -- This cron expression means it runs at 4 AM UTC (midnight EDT)
AS
call data_model_mapper_share_db.utility.share_views();
alter task data_model_mapper_share_db.utility.share_views resume;''', language="sql")
        
        with st.expander("**Step 4**", expanded=True):
            st.write("Please run the following script in a worksheet to grant the Native App access to your source table in order to map it.")
            st.code(f'''use role accountadmin;
grant usage on database <DATABASE_NAME> to application <APPLICATION_NAME>;
grant usage on schema <DATABASE_NAME>.<SCHEMA_NAME> to application <APPLICATION_NAME>;
grant select on table <DATABASE_NAME>.<SCHEMA_NAME>.<TABLE_NAME> to application <APPLICATION_NAME>;
--run the following grant if you prefer to grant access to all tables under your schema rather than just one table
--grant select on all tables in schema <DATABASE_NAME>.<SCHEMA_NAME> to application <APPLICATION_NAME>; ''', language="sql")
            
    def print_sidebar(self):
        super().print_sidebar()