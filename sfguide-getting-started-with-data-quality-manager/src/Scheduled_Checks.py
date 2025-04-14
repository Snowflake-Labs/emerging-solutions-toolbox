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

class ScheduledChecksPage(Page):
    def __init__(self):
        self.name = 'sched_checks'
        pass

        
    def go_to_edit(self,job,category):
        st.session_state.edit_job = job
        st.session_state.edit_job_category = category
        change_page("job_edit")
    def execute_job(self,job):
        session = st.session_state.session

        job_name = job[1]
        sql_to_dataframe(f"EXECUTE TASK {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{job_name}_DQ_TASK")

    def print_page(self):
        session = st.session_state.session

        if "job_paginator" not in st.session_state or st.session_state.job_paginator["start"] == \
                st.session_state.job_paginator["end"]:
            st.session_state.job_paginator = {"start": 0, "end": st.session_state.pag_interval}
        # search, fbf, fbl, fbd, fbdqc, button = st.columns((6, 2, 2, 2, 2, 2))
        job_search = st.text_input("Search", key="scttsearch")
        # fbf.selectbox("Filter by frequency", ["All"])
        # fbl.selectbox("Filter by label", ["All"])
        # fbd.selectbox("Filter by date", ["All"])
        # fbdqc.selectbox("Filter by data quality check", ["All"], key="dqcfilterdqc")
        # button.write("20 checks scheduled")

        jobs = sql_to_dataframe(
            f"SELECT * FROM {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS WHERE JOB_NAME ILIKE '%{job_search}%' or LABEL ILIKE '%{job_search}%' ORDER BY CREATE_DTTM DESC")
        st.write("-------")

        # total_pages = int(len(jobs)/st.session_state.pag_interval)
        # current_page = int(st.session_state.job_paginator["end"]/st.session_state.pag_interval)
        def jobs_callback(job):
            colOne, colTwo, colThree = st.columns((30, 4, 4))
            job_id = job[0]
            job_name = job[1]
            runtime = job[4]
            schedule = job[5]
            category = job[10]
            label = job[11]
            if schedule in reverse_chron_dict:
                colOne.write(reverse_chron_dict[schedule])
            else:
                colOne.write(schedule)
            colOne.markdown(f'***{job_name}***')
            colOne.write("Label: " + label)
            colOne.write("Check Type: "+category)
            colTwo.button("edit", type="primary", key=f"{job_id}_edit", on_click=self.go_to_edit, args=(job,category))
            if colThree.button("run", key=f"{job_id}_run", on_click=self.execute_job, args=(job,), disabled =(True if category == "UNMANAGED" else False)):
                st.success("Job run successfully")
            if category =="UNMANAGED":
                st.warning("Unmanaged Quality Checks cannot be run manually")
            st.write("-----")

        paginator("jobsinator", jobs, 10, jobs_callback)
        # for job in jobs[st.session_state.job_paginator["start"]:st.session_state.job_paginator["end"]]:
        #     job_id = job[0]
        #     job_name = job[1]
        #     runtime = job[4]
        #     schedule = job[5]
        #     label = job[11]
        #     st.write(runtime)
        #     st.markdown(f'***{job_name}***')
        #     st.write("Label: "+label)
        #     st.write("-----")
        # if len(jobs) > st.session_state.pag_interval:
        #     space,btn1,text,btn2,space = st.columns([20,2,2,2,20])
        #     text.write(f"Page ***{current_page}/{total_pages}***")
        #     if st.session_state.job_paginator["start"] > 0:
        #         btn1.button("Prev",on_click=pag_down, key="job_next", args=('job',))
        #     if st.session_state.job_paginator["end"] < len(jobs):
        #         btn2.button("Next",on_click=pag_up, key="job_back", args=('job',))

    def print_sidebar(self):
        pass