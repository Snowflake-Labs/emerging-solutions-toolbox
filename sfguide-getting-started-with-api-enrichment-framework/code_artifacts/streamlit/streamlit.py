# Import python packages
import streamlit as st
import uuid
from snowflake.snowpark.context import get_active_session
from abc import ABC, abstractmethod
from task_class import Scheduled_Task
import snowflake.permissions as permissions
import pandas as pd
import json



if "layout" not in st.session_state:
    st.session_state.layout = "wide"
    st.set_page_config(layout=st.session_state.layout)


session = get_active_session()

QUERY_TAG = {"origin": "sf_sit",
             "name": "api_enrichment_framework",
             "version": '{major: 1, minor: 0}'
            }


def sql_to_dataframe(sql: str) -> pd.DataFrame:
    return session.sql(sql).collect(
        statement_params={
            "QUERY_TAG": json.dumps(QUERY_TAG)
        }
    )


def sql_to_pandas(sql: str) -> pd.DataFrame:
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


def set_page(page: str):
    st.session_state.page = page

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


class Base_Page(Page):
    def __init__(self):
        pass
    
    def print_page(self):
        st.title('Data Enrichment Framework')
        pass

    def print_sidebar(self):
        with st.sidebar:
            app_name = sql_to_dataframe("SELECT CURRENT_ROLE()")[0][0]
            st.write(app_name)
            pass

class main_page(Base_Page):
    def __init__(self):
        self.name="main"

    def print_page(self):
        super().print_page()

        st.write("Welcome to the Data Enrichment Framework!")

        scheduled_jobs = sql_to_pandas("SELECT * FROM DATA.SCHEDULED_TASKS ORDER BY CREATED_DATETIME")

        secret_associations = permissions.get_reference_associations("CONSUMER_SECRET")

        external_access_associations = permissions.get_reference_associations("MY_EXTERNAL_ACCESS")

        if len(secret_associations) == 0 or len(external_access_associations) == 0:
            st.warning("The app has not been setup yet, redirecting to settings page")
            set_page('settings')
            st.experimental_rerun()

        if len(secret_associations) != 0 and len(external_access_associations) != 0:
            sql_to_dataframe("CALL SRC.INIT()")

        if len(scheduled_jobs) == 0:
            st.warning("No jobs scheduled yet!")

        else:
            for index, task in scheduled_jobs.iterrows():
                Scheduled_Task(task)

        st.button("Enrich Data", on_click=set_page, args=('pick_table',))


    def print_sidebar(self):
        with st.sidebar:
            super().print_sidebar()
            st.button("Settings", on_click=set_page, args=("settings",))

class settings_page(Base_Page):
    def __init__(self):
        self.name="settings"
    def print_page(self):
        super().print_page()

        st.write("In order for this application to function, we'll need a few things!")

        secret_associations = permissions.get_reference_associations("CONSUMER_SECRET")

        external_access_associations = permissions.get_reference_associations("MY_EXTERNAL_ACCESS")

        st.write("First, please add an api key, and approve the api call endpoint")

        col1,col2,__ = st.columns((2,2,10))

        if col1.button("Set API Key"):
            permissions.request_reference("CONSUMER_SECRET")

        if col2.button("Approve API"):
            permissions.request_reference("MY_EXTERNAL_ACCESS")

        if len(secret_associations) != 0 and len(external_access_associations) != 0:
            sql_to_dataframe("CALL SRC.INIT()")

        st.divider()

        st.write("Second, this app needs to write out your data!")
        st.write("Please give the permissions to create a database, and tasks so we can schedule your jobs!")
        if st.button("Grant Permissions"):
            permissions.request_account_privileges(["CREATE DATABASE", "EXECUTE MANAGED TASK"])

        if len(secret_associations) != 0 and len(external_access_associations) != 0 and permissions.get_held_account_privileges(["CREATE DATABASE", "EXECUTE MANAGED TASK"]):
            st.success("The application is configured!")
            st.button("Go To Main Page", on_click = set_page, args=("main",))

    def print_sidebar(self):
        with st.sidebar:
            super().print_sidebar()


class table_mapping_page(Base_Page):
    
    def __init__(self):
        self.name="pick_table"
    
    def print_page(self):
        super().print_page()


        clicked = st.button("Pick Table")
        if clicked:
            permissions.request_reference("enrichment_table")


        reference_associations = permissions.get_reference_associations("enrichment_table")
        tables = {}
        if len(reference_associations) == 0:    
            st.write("No tables chosen yet! please pick a table")
        else:

            for table in reference_associations:

                sqlstring = f"""show columns in table reference('enrichment_table','{table}')"""

                describe_data = pd.DataFrame(sql_to_dataframe(sqlstring))

                columns = describe_data['column_name']

                table_name = describe_data['table_name'][0]

                tables[table_name] = {}
                tables[table_name]["columns"] = list(columns)
                tables[table_name]["reference"] = table
                tables[table_name]["name"] = table_name

        if len(tables) > 0:
            pick_table = st.selectbox("Pick a Table to Enrich", list(tables.keys()))
            st.session_state.pick_table = tables[pick_table]
            # st.session_state.pick_reference = tables[pick_table]["reference"]
            # st.session_state.pick_columns = tables[pick_table]["columns"]
            st.button("Next",key = 'page button', on_click=set_page,args=('two',))

    def print_sidebar(self):
        with st.sidebar:
            super().print_sidebar()
            st.button("Go to main",key='sidebar button', on_click=set_page,args=('main',))

class page_two(Base_Page):
    def __init__(self):
        self.name="two"

    def save_and_schedule(self):
        pick_table = st.session_state.pick_table
        sched = dates_chron_dict[st.session_state["task_schedule"]]
        wh_size = st.session_state["task_wh_size"]
        task_name = st.session_state["task_name"]

        pick_table["schedule"] = st.session_state["task_schedule"]
        pick_table["wh_size"] = wh_size
        pick_table["task_name"] = task_name


        task_id = sql_to_dataframe("SELECT DATA.TASK_ID_SEQUENCE.NEXTVAL")[0][0]        
        # st.write(task_id)

        pick_table = str(pick_table).replace("'",'"')

        insert_sql = f"""INSERT INTO DATA.SCHEDULED_TASKS(task_id, job_specs, CREATED_DATETIME) SELECT '{task_id}',PARSE_JSON('{pick_table}'),current_timestamp"""

        # st.write(insert_sql)

        sql_to_dataframe(str(insert_sql))

        sql_to_dataframe(f"""
        CREATE OR REPLACE TASK DATA.{task_name}_ENRICHMENT_TASK
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{wh_size}'
        SCHEDULE = 'USING CRON {sched} America/Los_Angeles'
        COMMENT = 'sit_api_enrichment_framework'
        AS
            CALL SRC.ENRICH_DATA_WRAPPER({task_id})
            """)
        st.success("Task Saved!")
        pass

    def enrich(self,pick_table):
        enriched_data = sql_to_dataframe(f"CALL SRC.ENRICH_DATA({pick_table})")
        st.success('Enrichment Successfull!')
        # st.write(enriched_data)

    def print_page(self):
        # super().print_page()
        # fields = ["street_address", "city", "region", "postal_code", "iso_country_code"]
        fields = sql_to_dataframe("SELECT VALUE FROM DATA.METADATA WHERE KEY = 'FIELDS'")
        # st.write(st.session_state.pick_table)

        pick_table = st.session_state.pick_table

        columns = pick_table["columns"]
        table = pick_table["reference"]
        
        st.subheader("Map Data")

        map_dict = {}

        if len(fields) == 0:
            st.warning("There are no fields, please have the admin configure the fields")
        else:
            fields = json.loads(fields[0][0])["fields"]     
            for field in fields:
                map_dict[field] = st.selectbox(f"Pick a column to map ***{field}*** to", columns, key="pick_"+field)

        sqlstring = f"""select top 10 * from reference('enrichment_table','{table}')"""
        address_data = sql_to_pandas(sqlstring)
        st.divider()

        st.subheader("Data Preview")

        st.write(address_data)

        pick_table["mapping"] = map_dict

        st.divider()
        st.write("What would you like to name the output table?")

        output_table = st.text_input("Output Table")


        if output_table:
            pick_table["output_table"] = output_table            
            col1,col2,__ = st.columns((2,2,20))
            col1.button("Save", key = 'enrich', on_click=self.enrich,args=(pick_table,))     

            if col2.button("Schedule"):
                st.session_state.pick_table = pick_table
                with st.form("Schedule Task"):
                    st.text_input("Task Name", key="task_name")
                    st.selectbox("Warehouse Size", ["SMALL","MEDIUM","LARGE","XLARGE","XXLARGE"], key="task_wh_size")
                    st.selectbox("Schedule", ["Hourly", "Daily", "Weekly", "Monthly"], key="task_schedule")
                    st.form_submit_button("Save Task", on_click = self.save_and_schedule)

        st.warning("All saved data will appear under ENRICHMENT.DATA")

        # st.write(pick_table)

    def print_sidebar(self):
        with st.sidebar:
            super().print_sidebar()
            st.button("Go to main",key='sidebar button 2', on_click=set_page,args=('main',))

class page_three(Base_Page):
    def __init__(self):
        self.name="three"
    def print_page(self):
        super().print_page()
        enriched_data = st.session_state.enriched_data
        st.write(enriched_data)

    def print_sidebar(self):
        with st.sidebar:
            super().print_sidebar()
            st.button("Go to main",key='sidebar button 2', on_click=set_page,args=('main',))

if "session" not in st.session_state:
    st.session_state.session = get_active_session()

if "page" not in st.session_state:
    st.session_state.page="main"

pages = [main_page(),settings_page(),table_mapping_page(),page_two(),page_three()]

def main():
    for page in pages:
        if page.name == st.session_state.page:
            page.print_page();
            page.print_sidebar();

main()
