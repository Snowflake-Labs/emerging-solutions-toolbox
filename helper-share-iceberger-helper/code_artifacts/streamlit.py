# Import python packages
import json
from abc import ABC, abstractmethod

import pandas as pd
import snowflake.permissions as permissions
import streamlit as st
from snowflake.snowpark.context import get_active_session


# Sets the page based on page name
def set_page(page: str):

    st.session_state.current_page = page

QUERY_TAG = {"origin": "sf_sit",
             "name": "sit_share_iceberger_helper",
             "version": '{major: 1, minor: 0}'
            }


def sql_to_dataframe(sql: str) -> pd.DataFrame:
    session = st.session_state.session
    return session.sql(sql).collect(
        statement_params={
            "QUERY_TAG": json.dumps(QUERY_TAG)
        }
    )


def sql_to_pandas(sql: str) -> pd.DataFrame:
    session = st.session_state.session
    return session.sql(sql).to_pandas(
        statement_params={
            "QUERY_TAG": json.dumps(QUERY_TAG)
        }
    )

dates_chron_dict = {
            "Hourly": "0 * * * *",
            "Daily":"0 1 * * *", 
            "Weekly": "0 1 * * 1", 
            "Monthly":"0 1 1 * *",
            "Annually":"0 1 1 1 *"
        }

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

st.set_page_config(layout="wide")
# Write directly to the app
st.title("Share (Ice)Berger Helper")


if 'session' not in st.session_state:
    st.session_state.session = get_active_session()

session = st.session_state.session

if not permissions.get_held_account_privileges(["EXECUTE TASK"]):
    permissions.request_account_privileges(["EXECUTE TASK","EXECUTE MANAGED TASK", "CREATE DATABASE"])


class select_view_page(Page):

    def __init__(self):
        self.name = "svp"

    def print_page(self):
        st.write("Select the object you would like to export")
        views = sql_to_dataframe("SHOW VIEWS")
        views = pd.DataFrame(views)
        view_names = views['name'].to_list()
        
        # st.write(views)

        # tables = sql_to_dataframe("SHOW TABLES")
        # tables = pd.DataFrame(tables)
        # table_names = tables['name'].to_list()

        all_names = view_names
        all_names.sort()

        app_name = sql_to_pandas("SELECT CURRENT_ROLE() AS APP_NAME;")
        app_name = app_name["APP_NAME"].values[0]
        st.session_state.app_name = app_name
        st.session_state.chosen_obj = st.selectbox("Object Name",all_names)

        columns = sql_to_dataframe(f"SHOW COLUMNS IN VIEW {app_name}.SRC.{st.session_state.chosen_obj}")
        columns = pd.DataFrame(columns)
        columns = columns["column_name"]

        chosen_columns = st.multiselect("Choose Columns",columns)

        if chosen_columns:
            column_select = ', '.join(chosen_columns)
        else:
            column_select = '*'

        view_sql = f"SELECT {column_select} FROM SRC.{st.session_state.chosen_obj}"
        st.write(sql_to_dataframe(view_sql))

        st.session_state.columns = column_select
        st.button("Next", on_click=set_page, args=("gva",))

    def print_sidebar(self):
        with st.sidebar:
            st.button("Manage Iceberg Tables", on_click=set_page, args=("mit",))

class get_volume_access(Page):

    def __init__(self):
        self.name = "gva"

    def print_page(self):
        st.write("Please provide the name of the External Volume where the tables will be made and run the code provided")

        volume = st.text_input("Volume Name")
        st.session_state.volume = volume
        app_name = sql_to_dataframe("SELECT CURRENT_ROLE()")[0][0]

        code = f'''GRANT ALL ON EXTERNAL VOLUME {volume} TO APPLICATION {app_name};'''

        st.code(code,language="sql")
        st.button("Next", on_click=set_page, args=("cit",))
        
    def print_sidebar(self):
        with st.sidebar:
            st.button("Start Over", on_click=set_page, args=("svp",))


class create_iceberg_table(Page):

    def create_iceberg_table(self,table_name,location, schedule):
        sql_to_dataframe(f"""
        CREATE OR REPLACE ICEBERG TABLE src.{table_name}
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = '{st.session_state.volume}'
        BASE_LOCATION = '{location}'
        COMMENT = 'sit_share_iceberger_helper'
        AS SELECT {st.session_state.columns} FROM SRC.{st.session_state.chosen_obj};""")

        sql_to_dataframe(f"GRANT ALL ON TABLE SRC.{table_name} TO APPLICATION ROLE PUBLIC_DB_ROLE")

        sql_to_dataframe(f"CREATE OR REPLACE STREAM SRC.{table_name}_STREAM ON VIEW SRC.{st.session_state.chosen_obj};")

        sql_to_dataframe(f"""CREATE OR REPLACE TASK TASKS.{table_name}_task
                          SCHEDULE = 'USING CRON {str(dates_chron_dict[schedule])} UTC'
                          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
                          COMMENT = 'sit_share_iceberger_helper'
                          AS
                            INSERT INTO src.{table_name}
                            SELECT {st.session_state.columns} FROM src.{table_name}_STREAM;""")
        sql_to_dataframe(f"ALTER TASK TASKS.{table_name}_task RESUME;")

        st.success("Table Made!")
    def __init__(self):
        self.name = "cit"

    def print_page(self):
        st.write("Provide the details for your iceberg table and click create!")

        if st.checkbox("Create App Managed Task and Stream"):
            name = st.text_input("Table Name")
            location = st.text_input("External Volume Directory")
            wh_size = st.selectbox("Schedule",["Hourly","Daily","Weekly","Monthly"])

            st.button("Create", on_click=self.create_iceberg_table, args=(name,location,wh_size))
        else:
            sql_to_dataframe("CREATE DATABASE IF NOT EXISTS ICEBERG_TABLES")
            sql_to_dataframe("CREATE SCHEMA IF NOT EXISTS ICEBERG_TABLES.DYNAMIC_TABLES")
            sql_to_dataframe("GRANT ALL ON DATABASE ICEBERG_TABLES TO APPLICATION ROLE PUBLIC_DB_ROLE")
            sql_to_dataframe("GRANT ALL ON SCHEMA ICEBERG_TABLES.DYNAMIC_TABLES TO APPLICATION ROLE PUBLIC_DB_ROLE")
            name = st.text_input("Table Name")
            location = st.text_input("External Volume Directory")
            wh_size = st.text_input("Target Lag (minutes)")
            wh_name = st.text_input("Warehouse Name")
            dit_code = f"""
            CREATE OR REPLACE DYNAMIC ICEBERG TABLE ICEBERG_TABLES.DYNAMIC_TABLES.{name}
                CATALOG = 'SNOWFLAKE'
                TARGET_LAG = '{wh_size} minutes' 
                WAREHOUSE = {wh_name}
                EXTERNAL_VOLUME = '{st.session_state.volume}'
                BASE_LOCATION = '{location}'
                AS SELECT {st.session_state.columns} FROM {st.session_state.app_name}.SRC.{st.session_state.chosen_obj};
            """
            st.code(dit_code,language="sql")
    def print_sidebar(self):
        with st.sidebar:
            st.button("Start Over", on_click=set_page, args=("svp",))

class manage_iceberg_tables(Page):

    def __init__(self):
        self.name = "mit"

    def delete(self,table_name):
        sql_to_dataframe(f"DROP TABLE SRC.{table_name}")
        sql_to_dataframe(f"DROP STREAM SRC.{table_name}_stream")
        sql_to_dataframe(f"DROP TASK TASKS.{table_name}_task")
        st.success(f"TABLE {table_name} Dropped")

    def save(self,table_name,schedule):
        sql_to_dataframe(f"ALTER TASK TASKS.{table_name}_task SUSPEND;")
        sql_to_dataframe(f"alter task TASKS.{table_name}_task SET SCHEDULE = 'USING CRON {str(dates_chron_dict[schedule])} UTC'")
        sql_to_dataframe(f"ALTER TASK TASKS.{table_name}_task RESUME;")
        st.success(f"TABLE {table_name} Saved")

    def delete_dt(self,table_name):
        sql_to_dataframe(f"DROP TABLE ICEBERG_TABLES.DYNAMIC_TABLES.{table_name}")
        st.success(f"TABLE {table_name} Dropped")

    def print_page(self):
        sql_to_dataframe("CREATE DATABASE IF NOT EXISTS ICEBERG_TABLES")
        sql_to_dataframe("CREATE SCHEMA IF NOT EXISTS ICEBERG_TABLES.DYNAMIC_TABLES")
        sql_to_dataframe("GRANT ALL ON DATABASE ICEBERG_TABLES TO APPLICATION ROLE PUBLIC_DB_ROLE")
        sql_to_dataframe("GRANT ALL ON SCHEMA ICEBERG_TABLES.DYNAMIC_TABLES TO APPLICATION ROLE PUBLIC_DB_ROLE")
        tables = sql_to_dataframe("SHOW ICEBERG TABLES IN SRC")
        tables = pd.DataFrame(tables)
        st.subheader("Manage Iceberg Tables")
        for index,table in tables.iterrows():
            with st.expander(table['name']):
                col1,col2,col3,col4 = st.columns(4)
                col1.write("Created On: "+str(table["created_on"]))
                schedule = col2.selectbox("Schedule",["Hourly","Daily","Weekly","Monthly"],key=f"schedule_{table['name']}")
                col3.button("Save",key=f"save_{table['name']}", on_click=self.save,args=(table['name'],schedule))
                col4.button("Delete",key=f"delete_{table['name']}",on_click=self.delete,args=(table['name'],))
        dyn_tables = sql_to_dataframe("SHOW ICEBERG TABLES IN ICEBERG_TABLES.DYNAMIC_TABLES;")
        dyn_tables = pd.DataFrame(dyn_tables)
        
        for index,table in dyn_tables.iterrows():
            with st.expander(table['name']):
                col1,col2,col3,col4 = st.columns(4)
                col1.write("Created On: "+str(table["created_on"]))
                col4.button("Delete",key=f"delete_{table['name']}",on_click=self.delete_dt,args=(table['name'],))

    def print_sidebar(self):
        with st.sidebar:
            st.button("Return to main", on_click=set_page, args=("svp",))

pages = [select_view_page(),get_volume_access(),create_iceberg_table(),manage_iceberg_tables()]

if "current_page" not in st.session_state:
    st.session_state["current_page"] = 'svp'

for page in pages:
    if page.name == st.session_state["current_page"]:
        page.print_page()
        page.print_sidebar()