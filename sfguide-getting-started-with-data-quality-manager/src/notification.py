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

from src.job_class import Job
from src.Page import Page
from src.globals import APP_OPP_DB, APP_CONFIG_SCHEMA, APP_TEMP_DATA_SCHEMA, APP_RESULTS_SCHEMA, APP_DATA_SCHEMA, dates_chron_dict, reverse_chron_dict
from src.tools import toggle_button, get_tables, get_schemas, get_anomaly_chart, change_page, print_nsc_results, pag_up, pag_down, sql_to_dataframe, sql_to_pandas
from src.Paginator import paginator

class NotificationPage(Page):
    def __init__(self):
        self.name = 'not_page'
        pass

    def read_all_notes(self,notes):
        for note in notes:
            id = note[0]
            read = note[7]
            type = note[6]
            if read is 'pending review':
                self.read_note(id,type)
    
    def read_note(self,note_id, note_type):
        session = st.session_state.session
        if note_type == 'ANOMOLY':
            sql_str = f"UPDATE {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS SET ALERT_STATUS = 'READ' WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'"
        elif note_type == 'NON-STAT':
            sql_str = f"UPDATE {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS SET ALERT_STATUS = 'READ' WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'"
        elif note_type == 'SNOWFLAKE_DMF':
            sql_str = f"UPDATE {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_SNOWFLAKE_DMF_RESULTS SET ALERT_STATUS = 'READ' WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'"
        sql_to_dataframe(sql_str)

    def save_edits(self, df, table):

        session = st.session_state.session

        t = session.table(table).filter(F.col("ALERT_FLAG") == 1)

        df = df[df["COMMENTS"].notnull()]

        if "RECORD_IDS" in df.columns:

            t_new = session.createDataFrame(df).with_column("RECORD_IDS",
                                                            F.parse_json(F.col("RECORD_IDS")).cast(T.VariantType()))
            session.table(table).merge(
                t_new,
                ((t['JOB_ID'] == t_new['JOB_ID']) &
                 (t['RUN_DATETIME'] == t_new['RUN_DATETIME']) &
                 (t['CHECK_TYPE_ID'] == t_new['CHECK_TYPE_ID']) &
                 (t['RECORD_IDS'] == t_new['RECORD_IDS'])),
                [
                    F.when_matched().update(
                        {"COMMENTS": t_new["COMMENTS"]}
                    )
                ]
            )

        else:
            t_new = session.createDataFrame(df).with_column("PARTITION_VALUES",
                                                            F.parse_json(F.col("PARTITION_VALUES")).cast(T.ArrayType()))
            session.table(table).merge(
                t_new,
                ((t['JOB_ID'] == t_new['JOB_ID']) &
                 (t['RUN_DATETIME'] == t_new['RUN_DATETIME']) &
                 (t['CHECK_TYPE_ID'] == t_new['CHECK_TYPE_ID']) &
                 (t['PARTITION_VALUES'] == t_new['PARTITION_VALUES'])),
                [
                    F.when_matched().update(
                        {"COMMENTS": t_new["COMMENTS"]}
                    )
                ]
            )
    def print_page(self):
        session = st.session_state.session

        if "note_paginator" not in st.session_state or st.session_state.note_paginator["start"] == \
                st.session_state.note_paginator["end"]:
            st.session_state.note_paginator = {"start": 0, "end": st.session_state.pag_interval}
        search, show, date, DQC, jc = st.columns((5, 2, 2, 2, 2))
        search = search.text_input("Search")
        show = show.selectbox("Show", ["All", "Read", "Unread"])
        show = "= 'READ'" if show == "Read" else "= 'pending review'" if show == 'Unread' else " ilike '%%' OR ALERT_STATUS IS NULL"
        start_date = date.date_input("Start Date", value=(datetime.today() - timedelta(days=30)))
        end_date = DQC.date_input("End Date")
        job_info = sql_to_pandas(
            f"""SELECT DISTINCT IFNULL(IFF(JOB_NAME = '',NULL,JOB_NAME), 'No Job Name') as JOB_NAME,JOB_ID FROM {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS ORDER BY JOB_NAME""")
        job_names = list(job_info["JOB_NAME"])
        job_names.insert(0, '')
        job_name = jc.selectbox("Filter by job", job_names)
        if job_name != '':
            job_id_f = list(job_info[job_info["JOB_NAME"] == job_name]["JOB_ID"])[0]
            alert_flag = ''
            alert_field = ', IFF(SUM(ALERT_FLAG)>0, 1, 0) AS ALERT_FLAG'
            dmf_alert_field = ', IFF(SUM(IFF(ALERT_FLAG, 1, 0))>0, 1, 0) AS ALERT_FLAG'
            alert_grouper = ''
            emoji_flag = True
        else:
            job_id_f = '%%'
            alert_flag = 'and ALERT_FLAG = 1'
            alert_field = ', ALERT_FLAG'
            dmf_alert_field = ', IFF(ALERT_FLAG, 1, 0) as ALERT_FLAG'
            alert_grouper = alert_field
            emoji_flag = False
        notifications = (
            f"""SELECT (JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY ,JOB_ID, RUN_DATETIME, COUNT(*), CHECK_TBL_NM {alert_field}, 'ANOMOLY' as CHECK_TYPE, ALERT_STATUS 
                FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS M 
                WHERE CHECK_TBL_NM ILIKE '%{search}%'
                and JOB_ID ilike '{job_id_f}'
                {alert_flag}
                and (ALERT_STATUS {show})
                AND RUN_DATETIME >= CONCAT('{start_date}', ' 00:00:00')
                AND RUN_DATETIME <= CONCAT('{end_date}', ' 23:59:59') 
                GROUP BY JOB_ID,CHECK_TBL_NM,RUN_DATETIME {alert_grouper}, ALERT_STATUS
                UNION
                SELECT (JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY ,JOB_ID, RUN_DATETIME, COUNT(*), CONTROL_TBL_NM {alert_field}, 'NON-STAT' as CHECK_TYPE, ALERT_STATUS 
                FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS M 
                WHERE CONTROL_TBL_NM ILIKE '%{search}%'
                and JOB_ID ilike '{job_id_f}'
                {alert_flag}
                and (ALERT_STATUS {show})
                AND RUN_DATETIME >= CONCAT('{start_date}', ' 00:00:00')
                AND RUN_DATETIME <= CONCAT('{end_date}', ' 23:59:59')
                GROUP BY JOB_ID,CONTROL_TBL_NM,RUN_DATETIME {alert_grouper}, ALERT_STATUS
                UNION
                SELECT (JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY ,JOB_ID, RUN_DATETIME, COUNT(*), '' as CONTROL_TBL_NM {dmf_alert_field}, 'SNOWFLAKE_DMF' as CHECK_TYPE, ALERT_STATUS 
                FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_SNOWFLAKE_DMF_RESULTS M 
                WHERE CONTROL_TBL_NM ILIKE '%{search}%'
                and JOB_ID ilike '{job_id_f}'
                {alert_flag}
                and (ALERT_STATUS {show})
                AND RUN_DATETIME >= CONCAT('{start_date}', ' 00:00:00')
                AND RUN_DATETIME <= CONCAT('{end_date}', ' 23:59:59')
                GROUP BY JOB_ID,CONTROL_TBL_NM,RUN_DATETIME {alert_grouper}, ALERT_STATUS order by ALERT_STATUS DESC, RUN_DATETIME DESC 
                """)
        # st.write(notifications)
        notifications = sql_to_dataframe(notifications)
        st.button("Mark all as read", type="primary", on_click=self.read_all_notes, args=(notifications,))
        st.write("-----")
        # st.write(notifications)
        total_pages = int(len(notifications) / st.session_state.pag_interval)
        current_page = int(st.session_state.note_paginator["end"] / st.session_state.pag_interval)
        i = 0
        for note in notifications[st.session_state.note_paginator["start"]:st.session_state.note_paginator["end"]]:
            Job(note,job_info, i, emoji_flag)
            i = i + 1
            st.write("-----")
        if len(notifications) > st.session_state.pag_interval:
            space, btn1, text, btn2, space = st.columns([20, 2, 2, 2, 20])
            text.write(f"Page ***{current_page}/{total_pages}***")
            if st.session_state.note_paginator["start"] > 0:
                btn1.button("Prev", on_click=pag_down, key="note_next", args=('note',))
            if st.session_state.note_paginator["end"] < len(notifications):
                btn2.button("Next", on_click=pag_up, key="note_back", args=('note',))


    # Repeatable element: sidebar buttons that navigate to linked pages
    def print_sidebar(self):
        pass
