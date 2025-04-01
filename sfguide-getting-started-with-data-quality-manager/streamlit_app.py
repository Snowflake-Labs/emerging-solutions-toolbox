import streamlit as st
import json
import sys
import pandas as pd

from src.Main_Page import Main_Page
from src.Schedule_Check_Page import Schedule_Check_Page
from src.Job_Edit_Page import Job_Edit_Page
from src.start import StartPage
from src.column_select import ColumnPage
from src.report_package import ReportPage
from src.notification import NotificationPage
from src.DQ_Check import DQCheckPage
from src.Scheduled_Checks import ScheduledChecksPage
from src.Metrics_Page import MetricsPage
from src.metrics_alert_monitoring import table_metrics
from src.Page import set_page
from dotenv import load_dotenv

# Add all databases you wish the access to this array
st.session_state.databases = ["DATA_QUALITY"]

load_dotenv(override=True)
st.session_state["streamlit_mode"] = "SiS"
def set_session():
    try:
        import snowflake.permissions as permissions

        session = get_active_session()

        st.session_state["streamlit_mode"] = "NativeApp"
    except:
        try:
            session = get_active_session()

            st.session_state["streamlit_mode"] = "SiS"
        except:
            import snowflake_conn as sfc

            session = sfc.init_snowpark_session("account")

            st.session_state["streamlit_mode"] = "OSS"

    return session

st.set_page_config(layout="wide")
APP_OPP_DB = "DATA_QUALITY"
APP_CONFIG_SCHEMA = "CONFIG"
APP_RESULTS_SCHEMA = "RESULTS"
APP_DATA_SCHEMA = "DATA"
APP_TEMP_DATA_SCHEMA = "TEMPORARY_DQ_OBJECTS"
st.session_state.pag_interval = 10
st.session_state.warehouses = ["XSMALL","SMALL","MEDIUM","LARGE","XLARGE"]
st.session_state.non_stat_proc='dq_non_stat_sproc'
st.session_state.anomoly_proc='dq_anomaly_detection_sproc'

dates_chron_dict = {
            "Hourly": "0 * * * *",
            "Daily":"0 1 * * *",
            "Weekly": "0 1 * * 1",
            "Monthly":"0 1 1 * *",
            "Annually":"0 1 1 1 *"
        }
reverse_chron_dict = inv_map = {v: k for k, v in dates_chron_dict.items()}

if 'snowflake_import_directory' in sys._xoptions:

    print("We are in Snowflake.")

    from snowflake.snowpark.context import get_active_session

    st.session_state.session = get_active_session()

else:

    print("We are not in Snowflake.")

    st.session_state.session = st.connection("snowflake").session()

session = st.session_state.session


st.title("Data Quality Manager")


if "catalog_info" not in st.session_state:
    with st.spinner("Loading Catalog"):
        databases = ("', '").join(st.session_state.databases)
        schemas = (f"SELECT CATALOG_NAME, SCHEMA_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE CATALOG_NAME in ('{databases}') AND DELETED IS NULL ORDER BY SCHEMA_NAME")
        schema_df = session.sql(schemas).to_pandas()
        schemas = ("', '").join(list(schema_df["SCHEMA_NAME"]))
        tables_df = session.sql(f"SELECT TABLE_NAME, TABLE_SCHEMA FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE TABLE_CATALOG in ('{databases}') and TABLE_SCHEMA in ('{schemas}') AND DELETED IS NULL ORDER BY TABLE_NAME").to_pandas()
        database_dict = {}
        for database in st.session_state.databases:
            database_dict[database] = []
            schemas = schema_df[schema_df["CATALOG_NAME"] == database]
            for index,schema in schemas.iterrows():
                schema = schema["SCHEMA_NAME"]
                tables = list(tables_df[tables_df["TABLE_SCHEMA"] == schema]["TABLE_NAME"])
                new_obj = {
                    "schema" : schema,
                    "tables" :tables
                }
                database_dict[database].append(new_obj)
        st.session_state.catalog_info = database_dict


if 'session' not in st.session_state:
    st.session_state.session = set_session()

pages = [Main_Page(), Schedule_Check_Page(),MetricsPage(), table_metrics(), Job_Edit_Page(), StartPage(), ColumnPage(), ReportPage(), NotificationPage(), DQCheckPage(), ScheduledChecksPage()]

with st.sidebar:
    
    st.button("Notification Page", key='notification_page', use_container_width=True, on_click = set_page, args=('not_page',))

    st.button("Data Quality Checks", key='dataquality_check', use_container_width=True, on_click = set_page, args=('dataquality_check',))

    st.button("Scheduled Checks", key='sched_checks', use_container_width=True, on_click = set_page, args=('sched_checks',))
 
    st.button("Manual DMF Metrics", key='metrics_page', use_container_width=True, on_click = set_page, args=('metrics_page',))

    st.button("Table DMF Metrics", key='table_metrics', use_container_width=True, on_click = set_page, args=('table_metrics',))



if "current_page" not in st.session_state:
    st.session_state["current_page"] = 'not_page'

for page in pages:
    if page.name == st.session_state["current_page"]:
        page.print_page()
        page.print_sidebar()
