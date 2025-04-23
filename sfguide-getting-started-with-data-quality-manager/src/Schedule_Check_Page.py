import streamlit as st
import time
import random
import json
import sys
import os
import math
import pandas as pd
import matplotlib.pyplot as plt
import snowflake.snowpark.functions as F

from abc import ABC, abstractmethod
from dotenv import load_dotenv
from snowflake.snowpark import Row, Session
from snowflake.snowpark import types as T
from datetime import datetime, timedelta
from ast import literal_eval

from src.Page import Page
from src.globals import APP_OPP_DB, APP_CONFIG_SCHEMA, APP_TEMP_DATA_SCHEMA, APP_RESULTS_SCHEMA, APP_DATA_SCHEMA, dates_chron_dict, reverse_chron_dict
from src.tools import toggle_button, get_tables, get_schemas, get_anomaly_chart, change_page, print_nsc_results, pag_up, pag_down, sql_to_dataframe, sql_to_pandas
from src.Paginator import paginator

class Schedule_Check_Page:
    def __init__(self):
        self.name = "schedule_check"

    def save_and_create_table_checks(self,freq):
        session = st.session_state.session
        specs = st.session_state.snowflake_dmf_specs

        # table = specs["TABLE"]
        user = st.experimental_user

        for table in specs:
            table_name = table["TABLE"]
            sql_to_dataframe(f"""
            ALTER TABLE {table_name} SET
            DATA_METRIC_SCHEDULE = '{freq}';""")
            for column in table["COLUMNS"]:
                for check in column["CHECKS"]:
                    sql_to_dataframe(f"""
                    ALTER TABLE {table_name}
                      ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{check}
                      ON ({column["COLUMN"]});""")
            table_specs = str(table).replace("'","\\'")   
            insert_query = f"insert into {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS (JOB_NAME,CREATE_DTTM,CREATE_BY,LAST_RUN,SCHEDULE,JOB_SPECS,LAST_UPDATED,LABEL,IS_ACTIVE,SPROC_NAME,CHECK_CATEGORY) SELECT 'SNOWFLAKE TABLE CHECK - {table_name}',CURRENT_TIMESTAMP(),'{user['user_name']}',CURRENT_TIMESTAMP(),'{freq}',PARSE_JSON('{table_specs}'),CURRENT_TIMESTAMP(),'NONE','True','NONE','UNMANAGED'"
            sql_to_dataframe(insert_query)


    def save_and_create_table_check(self,freq):
        session = st.session_state.session
        specs = st.session_state.snowflake_dmf_specs

        table = specs["TABLE"]
        user = st.experimental_user
        sql_to_dataframe(f"""
            ALTER TABLE {table} SET
            DATA_METRIC_SCHEDULE = '{freq}';""")

        for column in specs["COLUMNS"]:
            for check in column["CHECKS"]:
                sql_to_dataframe(f"""
                ALTER TABLE {table}
                  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{check}
                  ON ({column["COLUMN"]});""")
        specs = str(specs).replace("'","\\'")   
        insert_query = f"insert into {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS (JOB_NAME,CREATE_DTTM,CREATE_BY,LAST_RUN,SCHEDULE,JOB_SPECS,LAST_UPDATED,LABEL,IS_ACTIVE,SPROC_NAME,CHECK_CATEGORY) SELECT 'SNOWFLAKE TABLE CHECK - {table}',CURRENT_TIMESTAMP(),'{user['user_name']}',CURRENT_TIMESTAMP(),'{freq}',PARSE_JSON('{specs}'),CURRENT_TIMESTAMP(),'NONE','True','NONE','UNMANAGED'"
        sql_to_dataframe(insert_query)

    def save_and_create_check(self,name,freq,label,warehouse):

        session = st.session_state.session

        check_type = st.session_state.session_check_type
        if check_type == 'Metadata Check':
            type = 'METADATA'
            table_data = st.session_state.metadata_table.split(".")
            # st.write(table_data)
            db = table_data[0]
            schema = table_data[1]
            table = table_data[2]
            column_jsons = st.session_state.metadata_spec
            specs = column_jsons
            proc = "DATA_QUALITY.CONFIG.METADATA_QUALITY"
            for json in column_jsons:
                sql = f"insert into DATA_QUALITY.CONFIG.control_report (object_var,active_flg) select parse_json('{json}'),true"
                sql_to_dataframe(sql)

            user = st.experimental_user
            task = f"""CREATE OR REPLACE TASK {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{name}_DQ_TASK
                SCHEDULE = 'USING CRON {freq} UTC'
                USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{warehouse}'
                COMMENT = 'sit_data_quality_framework'
                AS
                CALL DATA_QUALITY.CONFIG.METADATA_QUALITY('{db}', '{schema}', '{table}')
                """
            insert_query = f"""insert into {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS (JOB_NAME,CREATE_DTTM,CREATE_BY,LAST_RUN,SCHEDULE,JOB_SPECS,LAST_UPDATED,LABEL,IS_ACTIVE,SPROC_NAME,CHECK_CATEGORY) SELECT '{name}',CURRENT_TIMESTAMP(),'{user['user_name']}',CURRENT_TIMESTAMP(),'{freq}',PARSE_JSON('{{"LOCATION":"CONTROL_REPORT"}}'),CURRENT_TIMESTAMP(),'{label}','True','{proc}','{type}'"""

            # st.write(insert_query)
            sql_to_dataframe(insert_query)
            sql_to_dataframe(task)
            if st.session_state.schedule_job:
                sql_to_dataframe(f"alter task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{name}_DQ_TASK RESUME")
        
        elif check_type == "Native Snowflake Checks":
            specs = st.session_state.snowflake_dmf_specs
            type = "SNOWFLAKE_DMF"
            proc = "DATA_QUALITY.CONFIG.DMF_WRAPPER"

            user = st.experimental_user
            specs = str(specs).replace("'","\\'")
            insert_query = f"insert into {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS (JOB_NAME,CREATE_DTTM,CREATE_BY,LAST_RUN,SCHEDULE,JOB_SPECS,LAST_UPDATED,LABEL,IS_ACTIVE,SPROC_NAME,CHECK_CATEGORY) SELECT '{name}',CURRENT_TIMESTAMP(),'{user['user_name']}',CURRENT_TIMESTAMP(),'{freq}',PARSE_JSON('{specs}'),CURRENT_TIMESTAMP(),'{label}','True','{proc}','{type}'"

            # st.write(insert_query)

            sql_to_dataframe(insert_query)
            id = sql_to_dataframe(f"SELECT JOB_ID from {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS WHERE JOB_NAME = '{name}'")[0][0]
            task = f"""CREATE OR REPLACE TASK {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{name}_DQ_TASK
                SCHEDULE = 'USING CRON {freq} UTC'
                USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{warehouse}'
                COMMENT = 'sit_data_quality_framework'
                AS
                CALL {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DMF_WRAPPER('{id}')
                """
            st.write(task)
            sql_to_dataframe(task)
            if st.session_state.schedule_job:
                sql_to_dataframe(f"alter task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{name}_DQ_TASK RESUME")

        else:
            if 'custom_json' in st.session_state:
                if check_type == 'Non-statistical data quality check':
        
                    specs = st.session_state.custom_json
                    type = 'NON_STATISTICAL'
                    proc = st.session_state.non_stat_proc
                elif check_type == "Anomaly detection":
        
                    specs = st.session_state.custom_json
                    type = 'ANOMOLY_DETECTION'
                    proc = st.session_state.anomoly_proc
            else:
                if check_type == 'Non-statistical data quality check':
        
                    specs = st.session_state.dq_nonstat_specs
                    type = 'NON_STATISTICAL'
                    proc = st.session_state.non_stat_proc
                elif check_type == "Anomaly detection":
        
                    specs = st.session_state.dq_anomaly_specs
                    type = 'ANOMOLY_DETECTION'
                    proc = st.session_state.anomoly_proc

            

            user = st.experimental_user
            specs = str(specs).replace("'","\\'")
            insert_query = f"insert into {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS (JOB_NAME,CREATE_DTTM,CREATE_BY,LAST_RUN,SCHEDULE,JOB_SPECS,LAST_UPDATED,LABEL,IS_ACTIVE,SPROC_NAME,CHECK_CATEGORY) SELECT '{name}',CURRENT_TIMESTAMP(),'{user['user_name']}',CURRENT_TIMESTAMP(),'{freq}',PARSE_JSON('{specs}'),CURRENT_TIMESTAMP(),'{label}','True','{proc}','{type}'"

            sql_to_dataframe(insert_query)
            id = sql_to_dataframe(f"SELECT JOB_ID from {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS WHERE JOB_NAME = '{name}'")[0][0]
            task = f"""CREATE OR REPLACE TASK {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{name}_DQ_TASK
                SCHEDULE = 'USING CRON {freq} America/Vancouver'
                USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{warehouse}'
                COMMENT = 'sit_data_quality_framework'
                AS
                CALL {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{proc}('{name}',{{}})
                """
            # st.write(task)
            sql_to_dataframe(task)
            if st.session_state.schedule_job:
                sql_to_dataframe(f"alter task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{name}_DQ_TASK RESUME")
        
    def print_page(self):

        session = st.session_state.session


        if 'custom_json' in st.session_state:
            del st.session_state.custom_json
        check_type = st.session_state.session_check_type
        # st.button("Back to main page", on_click=change_page, args=('main',))
        st.subheader(f"Schedule {check_type}")

        if "scan_schema" not in st.session_state:
            st.session_state.scan_schema = False

        if check_type == "Native Snowflake Checks" or st.session_state.scan_schema == True:
            unmanaged_check = st.checkbox("Schedule check directly on table")
        else:
            unmanaged_check = False
        if unmanaged_check or st.session_state.scan_schema:
            check_freq = st.selectbox("Frequency",["Hourly","Daily", "Weekly", "Monthly","Annually","TRIGGER_ON_CHANGES"])
            if check_freq == "TRIGGER_ON_CHANGES":
                frequency = "TRIGGER_ON_CHANGES"
            else:
                frequency = 'USING CRON '+str(dates_chron_dict[check_freq])+' UTC'
            space, b1 = st.columns((20,2))
            if st.session_state.scan_schema:
                if b1.button("Save",type="primary", on_click=self.save_and_create_table_checks, args = (frequency,)):
                    st.success(f":white_check_mark: Check saved successfully")
                    time.sleep(5)
                    # change_page("not_page")
                    change_page("table_metrics")
                    st.experimental_rerun()
            else:
                if b1.button("Save",type="primary", on_click=self.save_and_create_table_check, args = (frequency,)):
                    st.success(f":white_check_mark: Check saved successfully")
                    time.sleep(5)
                    # change_page("not_page")
                    change_page("table_metrics")
                    st.experimental_rerun()
        else:
            check_name = st.text_input("Name")
            warehouse = st.selectbox("Warehouse", st.session_state.warehouses)
            one,two = st.columns(2)
                
            check_freq = one.selectbox("Frequency",["Hourly","Daily", "Weekly", "Monthly","Annually"])
            check_label = two.text_input("Add label")
            st.session_state.schedule_job = st.checkbox("Schedule job", value=True)
            with st.expander("Advanced"):
                custom_cron = st.text_input("Custom Cron", placeholder="Write your custom cron expression here!")
                custom_json = st.text_area("Custom JSON")
            
            if custom_cron:
                frequency = custom_cron
            else:
                frequency = dates_chron_dict[check_freq]
            
            if custom_json:
                st.json(custom_json)
                st.session_state.custom_json = custom_json
            else:
                if check_type == 'Non-statistical data quality check':
                    st.write(st.session_state.dq_nonstat_specs)
                elif check_type == "Anomaly detection":
                    st.write(st.session_state.dq_anomaly_specs)
                elif check_type == "Metadata Check":
                    st.write(st.session_state.metadata_spec)
                elif check_type == "Native Snowflake Checks":
                    st.write(st.session_state.snowflake_dmf_specs)
            space, b1 = st.columns((20,2))
            

            if b1.button("Save",type="primary", on_click=self.save_and_create_check, args = (check_name,frequency,check_label,warehouse), disabled=(False if check_name else True)):
                st.success(f":white_check_mark: Check {check_name} saved successfully")
                time.sleep(5)
                change_page("not_page")
                # change_page("table_metrics")
                st.experimental_rerun()
    
    def print_sidebar(self):
        pass

