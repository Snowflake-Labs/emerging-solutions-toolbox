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

class Job_Edit_Page(Page):
    
    def __init__(self):
        self.name = "job_edit"

    def save_and_create_table_check(self,freq,specs):
        session = st.session_state.session
        specs = json.loads(specs)

        table = specs["TABLE"]
        user = st.experimental_user
        session.sql(f"""
            ALTER TABLE {table} SET
            DATA_METRIC_SCHEDULE = '{freq}';""").collect()

        for column in specs["COLUMNS"]:
            for check in column["CHECKS"]:
                session.sql(f"""
                ALTER TABLE {table}
                  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{check}
                  ON ({column["COLUMN"]});""").collect()
        # specs = str(specs).replace("'","\\'")   
        # insert_query = f"insert into {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS (JOB_NAME,CREATE_DTTM,CREATE_BY,LAST_RUN,SCHEDULE,JOB_SPECS,LAST_UPDATED,LABEL,IS_ACTIVE,SPROC_NAME,CHECK_CATEGORY) SELECT 'SNOWFLAKE TABLE CHECK - {table}',CURRENT_TIMESTAMP(),'{user['user_name']}',CURRENT_TIMESTAMP(),'{freq}',PARSE_JSON('{specs}'),CURRENT_TIMESTAMP(),'NONE','True','NONE','UNMANAGED'"
        # session.sql(insert_query).collect()

    def edit_check(self,edit_name,check_name,frequency,check_label,warehouse,job_proc,job_type,job_id):
        
        session = st.session_state.session

        if edit_name == check_name:
            session.sql(f"alter task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK SUSPEND;").collect()
            session.sql(f"""
            ALTER TASK {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK SET SCHEDULE = 'USING CRON {frequency} America/Vancouver', USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{warehouse}', comment = "sit_data_quality_framework";
            """).collect()
            if st.session_state.start_edited_task:
                session.sql(f"alter task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK RESUME;").collect()
            session.sql(f"""
            UPDATE {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS
            SET SCHEDULE = '{frequency}', LABEL = '{check_label}', JOB_SPECS = PARSE_JSON('{st.session_state.edit_json}')
            WHERE JOB_NAME = '{check_name}'
            """).collect()
        else:
            session.sql(f"alter TASK IF EXISTS {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK suspend").collect()
            session.sql(f"DROP TASK IF EXISTS {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK").collect()
            task = f"""CREATE OR REPLACE TASK {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{edit_name}_DQ_TASK
            SCHEDULE = 'USING CRON {frequency} America/Vancouver'
            USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{warehouse}'
            comment = 'sit_data_quality_framework'
            AS
            CALL {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{job_proc}('{edit_name}',{{}})
            """
            st.write(task)
            session.sql(task).collect()
            session.sql(f"""
            UPDATE {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS
            SET SCHEDULE = '{frequency}', LABEL = '{check_label}', JOB_SPECS = PARSE_JSON('{st.session_state.edit_json}'), JOB_NAME = '{edit_name}'
            WHERE JOB_ID = '{job_id}'
            """).collect()
            st.session_state.edit_job = session.sql(f"SELECT * FROM {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS WHERE JOB_ID = '{job_id}'").collect()[0]
    
    def toggle_task(self,check_name,action):
        if action == "START":
            sql_to_dataframe(f"alter task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK RESUME;")
            st.success("Job started")
        elif action == "STOP":
            sql_to_dataframe(f"alter task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK SUSPEND;")
            st.success("Job stopped")
    
    def print_page(self):

        session = st.session_state.session

        if "start_edited_task" in st.session_state:
            del st.session_state.start_edited_task
        job = st.session_state.edit_job
        # st.write(job)
        # st.dataframe(job)
        # st.button("Back to main page", on_click=change_page, args=('main',))
        st.subheader("Editing Job: "+job[1])



        check_name = job[1]
        job_specs = job[6]
        job_proc=job[9]
        job_type=job[10]
        job_id = job[0]

        if job_type == "UNMANAGED":
            check_freq = st.selectbox("Frequency",["Hourly","Daily", "Weekly", "Monthly","Annually","TRIGGER_ON_CHANGES"])
            if check_freq == "TRIGGER_ON_CHANGES":
                frequency = "TRIGGER_ON_CHANGES"
            else:
                frequency = 'USING CRON '+str(dates_chron_dict[check_freq])+' UTC'
            space, b1 = st.columns((20,2))
            if b1.button("Save",type="primary", on_click=self.save_and_create_table_check, args = (frequency,job_specs)):
                st.success(f":white_check_mark: Check saved successfully")
                time.sleep(5)
                # change_page("not_page")
                change_page("table_metrics")
                st.experimental_rerun()

        else:
            task_data = session.sql(f'describe task {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{check_name}_DQ_TASK').collect()
            task_state = task_data[0].state
            if task_state == 'suspended':
                st.write("**Job is not scheduled**")
                st.button("Start",type="primary", on_click = self.toggle_task, args = (check_name,"START"))
                st.session_state.start_edited_task = False
            
            elif task_state == 'started':
                st.write("**Job is scheduled**")
                st.button("Stop",type="primary", on_click = self.toggle_task, args = (check_name,"STOP"))
                st.session_state.start_edited_task = True

            edit_name = st.text_input("Job Name", value = job[1], disabled=True)
            warehouse = st.selectbox("Warehouse", st.session_state.warehouses)
            one,two = st.columns(2)
            freq_list = list(dates_chron_dict.keys())
            freq_val_list = list(dates_chron_dict.values())
            if job[5] in freq_val_list:
                freq_index = freq_val_list.index(job[5])
            else:
                freq_index = 0
            check_freq = one.selectbox("Frequency",freq_list,index=freq_index)
            # st.write(check_freq)
            check_label = two.text_input("Add label",job[11])
            with st.expander("Advanced"):
                custom_cron = st.text_input("Custom Cron", placeholder="Write your custom cron expression here!", value = (job[5] if freq_index == 0 else ''))
                custom_json = st.text_area("Custom JSON")
            if custom_cron:
                frequency = custom_cron
            else:
                frequency = dates_chron_dict[check_freq]

            if custom_json:
                st.json(custom_json)
                st.session_state.edit_json = custom_json
            else:
                st.json(job_specs)
                st.session_state.edit_json = job_specs
            space, b1 = st.columns((20,2))
            
            
            if b1.button("Save",type="primary", on_click=self.edit_check, args = (edit_name,check_name,frequency,check_label,warehouse,job_proc,job_type,job_id)):
                st.success(f":white_check_mark: Check {check_name} saved successfully")
                time.sleep(5)
                # change_page("not_page")
                change_page("table_metrics")
                st.experimental_rerun()
        
    def print_sidebar(self):
        pass
