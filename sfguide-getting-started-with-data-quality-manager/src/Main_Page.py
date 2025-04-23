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
from src.tools import toggle_button, get_tables, get_schemas, get_anomaly_chart, change_page, print_nsc_results, pag_up, pag_down
from src.Paginator import paginator

class Main_Page(Page):

    def __init__(self):
        self.name = "main"
    
    def read_all_notes(self,notes):
        for note in notes:
            id = note[0]
            read = note[7]
            type = note[6]
            if read is 'pending review':
                self.read_note(id,type)

    def execute_job(self,job):
        session = st.session_state.session

        job_name = job[1]
        session.sql(f"EXECUTE TASK {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{job_name}_DQ_TASK").collect()

    
    def read_note(self,note_id, note_type):
        session = st.session_state.session
        if note_type == 'ANOMOLY':
            sql_str = f"UPDATE {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS SET ALERT_STATUS = 'READ' WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'"
        elif note_type == 'NON-STAT':
            sql_str = f"UPDATE {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS SET ALERT_STATUS = 'READ' WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'"
        session.sql(sql_str).collect()
    
    def make_non_stat_json(self,dq_checks):
        
        
        
        dict = {
            "TABLE_A_DB_NAME": st.session_state.a_database,
            "TABLE_A_SCHEMA_NAME": st.session_state.a_schema,
            "TABLE_A_NAME": st.session_state.a_table,
            "TABLE_A_FILTER": f"{st.session_state.a_filter}",
            "TABLE_A_RECORD_ID_COLUMNS": st.session_state.a_r_id_columns,
            "TABLE_A_PARTITION_COLUMNS": st.session_state.a_part_columns,
        
            "TABLE_B_DB_NAME": st.session_state.b_database,
            "TABLE_B_SCHEMA_NAME": st.session_state.b_schema,
            "TABLE_B_NAME": st.session_state.b_table,
            "TABLE_B_FILTER": f"{st.session_state.b_filter}",
            "TABLE_B_RECORD_ID_COLUMNS": st.session_state.b_r_id_columns,
            "TABLE_B_PARTITION_COLUMNS": st.session_state.b_part_columns,
        
            "RESULTS_DB": APP_OPP_DB,
            "RESULTS_SCHEMA": APP_RESULTS_SCHEMA,
            "RESULTS_TBL_NM": "DQ_NON_STAT_CHECK_RESULTS",
        
            "TEMPORARY_DQ_OBJECTS_SCHEMA": APP_TEMP_DATA_SCHEMA,
        
            "CHECKS": dq_checks
        }
        return dict

    def go_to_edit(self,job):
        st.session_state.edit_job = job
        change_page("job_edit")

    def save_edits(self,df,table):

        session = st.session_state.session

        t = session.table(table).filter(F.col("ALERT_FLAG") == 1)


        df = df[df["COMMENTS"].notnull()]

        

        if "RECORD_IDS" in df.columns:

            t_new = session.createDataFrame(df).with_column("RECORD_IDS", F.parse_json(F.col("RECORD_IDS")).cast(T.VariantType()))
            session.table(table).merge(
                t_new,
                ((t['JOB_ID'] == t_new['JOB_ID']) & 
                 (t['RUN_DATETIME'] == t_new['RUN_DATETIME']) & 
                 (t['CHECK_TYPE_ID'] == t_new['CHECK_TYPE_ID']) & 
                 (t['RECORD_IDS'] == t_new['RECORD_IDS'])),
                 [
                     F.when_matched().update(
                         {"COMMENTS" :   t_new["COMMENTS"]}
                     )
                 ]
            )

        else:
            t_new = session.createDataFrame(df).with_column("PARTITION_VALUES", F.parse_json(F.col("PARTITION_VALUES")).cast(T.ArrayType()))
            session.table(table).merge(
                t_new,
                ((t['JOB_ID'] == t_new['JOB_ID']) & 
                 (t['RUN_DATETIME'] == t_new['RUN_DATETIME']) & 
                 (t['CHECK_TYPE_ID'] == t_new['CHECK_TYPE_ID']) & 
                 (t['PARTITION_VALUES'] == t_new['PARTITION_VALUES'])),
                 [
                     F.when_matched().update(
                         {"COMMENTS" :   t_new["COMMENTS"]}
                     )
                 ]
            )

            
    def run_notTab(self):

        session = st.session_state.session

        if "note_paginator" not in st.session_state or st.session_state.note_paginator["start"] == st.session_state.note_paginator["end"]:
            st.session_state.note_paginator={"start":0,"end":st.session_state.pag_interval}
        search, show, date, DQC, jc = st.columns((5,2,2,2,2))
        search = search.text_input("Search")
        show = show.selectbox("Show",["All","Read","Unread"])
        show = "= 'READ'"  if show == "Read" else "= 'pending review'" if show == 'Unread' else " ilike '%%' OR ALERT_STATUS IS NULL"
        start_date = date.date_input("Start Date", value = (datetime.today() - timedelta(days=30)))
        end_date = DQC.date_input("End Date")
        job_info = session.sql(f"""SELECT DISTINCT IFNULL(IFF(JOB_NAME = '',NULL,JOB_NAME), 'No Job Name') as JOB_NAME,JOB_ID FROM {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS ORDER BY JOB_NAME""").to_pandas()
        job_names = list(job_info["JOB_NAME"])
        job_names.insert(0,'')
        job_name = jc.selectbox("Filter by job", job_names)
        if job_name != '':
            job_id_f = list(job_info[job_info["JOB_NAME"] == job_name]["JOB_ID"])[0]
            alert_flag = ''
            alert_field = ', IFF(SUM(ALERT_FLAG)>0, 1, 0) AS ALERT_FLAG'
            alert_grouper = ''
            emoji_flag = True
        else:
            job_id_f = '%%'
            alert_flag = 'and ALERT_FLAG = 1'
            alert_field = ', ALERT_FLAG'
            alert_grouper = alert_field
            emoji_flag = False
        notifications = session.sql(f"""SELECT (JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY ,JOB_ID, RUN_DATETIME, COUNT(*), CHECK_TBL_NM {alert_field}, 'ANOMOLY' as CHECK_TYPE, ALERT_STATUS 
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
        GROUP BY JOB_ID,CONTROL_TBL_NM,RUN_DATETIME {alert_grouper}, ALERT_STATUS order by ALERT_STATUS DESC, RUN_DATETIME DESC 
        """).collect()
        # st.write("#")
        st.button("Mark all as read",type="primary", on_click=self.read_all_notes, args=(notifications,))
        st.write("-----")
        # st.write(notifications)
        total_pages = int(len(notifications)/st.session_state.pag_interval)
        current_page = int(st.session_state.note_paginator["end"]/st.session_state.pag_interval)
        i=0
        for note in notifications[st.session_state.note_paginator["start"]:st.session_state.note_paginator["end"]]:
            note_id = note[0]
            Job_id = note[1]
            run_time = note[2]
            count = note[3]
            table = note[4]
            alert_flag = note[5]
            # st.write(alert_flag)
            read = note[7]
            # st.write(read)
            type = note[6]
            job_name = list(job_info[job_info["JOB_ID"] == Job_id]["JOB_NAME"])[0] if len(list(job_info[job_info["JOB_ID"] == Job_id]["JOB_NAME"])) > 0 else "No Job Name"
            description, button1,button2 = st.columns((19,2,2))
            description.subheader(f"**{job_name}**")
            description.write(run_time)
            
            button1.write("#")
            if(read == 'pending review'):
                button1.button("Mark as read", type="primary", key="mark read"+str(note_id), on_click=self.read_note, args=(note_id,type))
            else:
                button1.write(" ")
            button2.write("#")
            if "show_flag"+str(note_id) not in st.session_state:
                st.session_state["show_flag"+str(note_id)] = False
            button2.button("Show more", key="show"+str(note_id)+str(i), on_click=toggle_button, args = ("show_flag"+str(note_id),))
            
            i = i+1
            # st.write(st.session_state["show_flag"+str(note_id)])
            if type == "ANOMOLY":
                if emoji_flag:
                    if alert_flag == 0:
                        message = ':white_check_mark: ***No Alerts for run***'
                    elif alert_flag == 1:
                        message = f':x: ***{count} anomilies found in {table}***'
                else:
                    message = f'***{count} anomilies found in {table}***'
                description.write(f"{message}")
                if (st.session_state["show_flag"+str(note_id)]):
                    df = session.sql(f"SELECT * FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}' AND ALERT_FLAG=1").collect()
                    # df['PARTITION_VALUES'] = df["PARTITION_VALUES"].astype('string').replace('\\[','')
                    # df['PARTITION_STR'] = [row["PARTITION_VALUES"].split(',') for index,row in df.iterrows()]
                    edited_df = st.experimental_data_editor(df, key="editdf"+str(note_id)+str(i))
                    st.button("Save", type='primary', on_click = self.save_edits, args=(edited_df,f'{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS'), key="save_btn"+str(note_id)+str(i))
                    with st.expander("Results"):
                        get_anomaly_chart(f"{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS",note_id,1)
            elif type == 'NON-STAT':
                if emoji_flag:
                    if alert_flag == 0:
                        message = ':white_check_mark: ***No Alerts for run***'
                    elif alert_flag == 1:
                        message = f':x: ***{count} alerts found in {table}***'
                else:
                    message = f'***{count} alerts found in {table}***'
                description.write(f"{message}")
                if (st.session_state["show_flag"+str(note_id)]):
                    df = session.sql(f"SELECT * FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}' AND ALERT_FLAG=1").collect()
                    # df['PARTITION_STR'] = [row["PARTITION_VALUES"].split(',') for index,row in df.iterrows()]
                    edited_df = st.experimental_data_editor(df,key="editdf"+str(note_id)+str(i))
                    description.write(f'Table impacted: {table}')
                    st.button("Save", type='primary', on_click = self.save_edits, args=(edited_df,f'{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS'), key="save_btn"+str(note_id)+str(i))
                    with st.expander("Results"):
                        print_nsc_results(f"{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS",note_id,1)
            
            st.write("-----")
        if len(notifications) > st.session_state.pag_interval:
            space,btn1,text,btn2,space = st.columns([20,2,2,2,20])
            text.write(f"Page ***{current_page}/{total_pages}***")
            if st.session_state.note_paginator["start"] > 0:
                btn1.button("Prev",on_click=pag_down, key="note_next", args=('note',))
            if st.session_state.note_paginator["end"] < len(notifications):
                btn2.button("Next",on_click=pag_up, key="note_back", args=('note',))
    
    def run_DQCTab(self):
            
        session = st.session_state.session

        st.session_state.a_r_id_columns = ""
        st.session_state.b_r_id_columns = ""
        databases = st.session_state.databases
        st.subheader("Select your data check")
        check_type = st.selectbox('', ["Anomaly detection","Non-statistical data quality check", "Distribution check"])
        st.session_state.session_check_type = check_type
        st.write("-----")
        if check_type == "Non-statistical data quality check":
            dq_checks = []
            
            st.subheader("Job specifications")
            control,comparison = st.columns(2)
        
            with control:
                with st.expander("**Control**", expanded=True):
                    one, two, three = st.columns(3)
                    chosen_db = one.selectbox("Select **control** database", databases)
                    
                    schemas = []
                    if chosen_db:
                        schemas = get_schemas(chosen_db)
                    chosen_schema = two.selectbox("Select **control** schema", schemas, disabled= (False if chosen_db else True))

                    tables = []
                    if chosen_schema:
                        tables = get_tables(chosen_db,chosen_schema)
                    chosen_table = three.selectbox("Select **control** table", tables, disabled= (False if chosen_schema else True))
                    
                    sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder")
                    
                    t_a_available_columns = []
                    if chosen_table:
                        try:
                            t_a_available_columns = session.sql(f"describe table {chosen_db}.{chosen_schema}.{chosen_table}").collect()
                        except:
                            st.warning("You may not have access to the objects above")
                    # record_id_columns = st.multiselect("Record ID columns", t_a_available_columns,  key="control record id")
                    part_columns = st.multiselect("Select partition columns", t_a_available_columns,  key="control partitions")

                    st.session_state.a_database = chosen_db
                    st.session_state.a_schema = chosen_schema
                    st.session_state.a_table = chosen_table
                    st.session_state.a_filter = sql_filter
                    # st.session_state.a_r_id_columns = record_id_columns
                    st.session_state.a_part_columns = part_columns
                    
            with comparison:
                with st.expander("**Comparison**", expanded=True):
                    one, two, three = st.columns(3)
                    chosen_b_db = one.selectbox("Select **comparison** database", databases)
                    
                    schemas = []
                    if chosen_b_db:
                        schemas = get_schemas(chosen_b_db)
                    chosen_b_schema = two.selectbox("Select **comparison** schema", schemas, disabled= (False if chosen_b_db else True))

                    tables = []
                    if chosen_b_schema:
                        tables = get_tables(chosen_b_db,chosen_b_schema)
                    chosen_b_table = three.selectbox("Select **comparison** table", tables, disabled= (False if chosen_b_schema else True))
                    
                    sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder", key="sql comparison filter")
                    
                    
                    t_b_available_columns = []
                    if chosen_b_table:
                        try:
                            t_b_available_columns = session.sql(f"describe table {chosen_b_db}.{chosen_b_schema}.{chosen_b_table}").collect()
                        except:
                            st.warning("You may not have access to the objects above")
                    # record_id_columns = st.multiselect("Record ID columns", t_b_available_columns,  key="comparison record id")
                    part_columns = st.multiselect("Select partition columns", t_b_available_columns,  key="comparison partitions")

                    st.session_state.b_database = chosen_b_db
                    st.session_state.b_schema = chosen_b_schema
                    st.session_state.b_table = chosen_b_table
                    st.session_state.b_filter = sql_filter
                    # st.session_state.b_r_id_columns = record_id_columns
                    st.session_state.b_part_columns = part_columns
            if( tptrc := st.checkbox("tables/partitions total row count") ):
                with st.expander("**tables/partitions total row count settings**"):
                    cols = t_b_available_columns

                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Please input the desired threshold")
                    thresh = st.text_input("Threshold", key="tptrc_thres")
                    dq_checks.append(
                        {
                            "CHECK_TYPE_ID": 1,
                            "CHECK_DESCRIPTION": "table/partition total row count",
                            "ALERT_THRESHOLD": [(thresh if thresh else 100)] #NOTE: Always a single value
                        })
            
            if (vv := st.checkbox("record-level value change")):
                with st.expander("**record-level value change settings**"):   
                        cols = t_b_available_columns
                        rlvc_dict = {}
                        rlvc_rid_dict = {}
                        # column_df = {
                        #     "Control table columns":cols,
                        #     "Comparison table columns":cols
                        # }
                        # st.header("Record level value change")
                        st.header("Record ID columns")
                        st.write("1. Select Record ID columns from **control** table")
                        pick_cols = st.multiselect("",t_a_available_columns)
                        if pick_cols:
                            st.write("2. Select columns from the **comparison** table to match them with the **control** table")
                            st.write("###")
                            idcol1, idcol2,space = st.columns((4,4,6))
                            idcol1.subheader("Control Column")
                            idcol2.subheader("Comparison Column")
                            for column in pick_cols:
                                idcol1, idcol2,space = st.columns((4,4,6))
                                idcol1.write("###")
                                idcol1.write(f'***{column}***')
                                rlvc_rid_dict[column] = idcol2.selectbox("",cols,key=column+"rlvc")
                        st.write("----")
    
                        cols = t_b_available_columns
    
                        # column_df = {
                        #     "Control table columns":cols,
                        #     "Comparison table columns":cols
                        # }
                        # st.header("Record level value change")
                        st.subheader("Columns to Analyze")
                        st.write("1. Select comparison columns from **control** table")
                        pick_cols = st.multiselect("",t_a_available_columns, key="CTA_SELECT_COLUMNS")
                        if pick_cols:    
                            st.write("2. Select columns from the **comparison** table to match them with the **control** table")
                            st.write("###")
                            idcol1, idcol2,space = st.columns((4,4,6))
                            idcol1.subheader("Control Column")
                            idcol2.subheader("Comparison Column")
                            for column in pick_cols:
                                idcol1, idcol2,space = st.columns((4,4,6))
                                idcol1.write("###")
                                idcol1.write(f'***{column}***')
                                rlvc_dict[column] = idcol2.selectbox("",cols,key=column+"CTA_rlvc")
                        st.write("----")
                        st.subheader("Threshold")
                        st.write("1. Please specify a threshold for all columns (this applies to scheduled jobs only)")
                        thresh = st.text_input("Threshold", key="rlvc_thresh")
                        
                T_A_COLUMNS = list(rlvc_dict.keys())
                T_B_COLUMNS = list(rlvc_dict.values())
                st.session_state.a_r_id_columns = list(rlvc_rid_dict.keys())
                st.session_state.b_r_id_columns = list(rlvc_rid_dict.values())
                dq_checks.append(
                    {
                        "CHECK_TYPE_ID": 2,
                        "CHECK_DESCRIPTION": "record-level value change",
                        "TABLE_A_COLUMNS": T_A_COLUMNS,
                        "TABLE_B_COLUMNS": T_B_COLUMNS,
                        "ALERT_THRESHOLD": [float(thresh if thresh else 100)] #NOTE: Always a single value
                    })
                    
            if( cnc := st.checkbox("column null count")):
                with st.expander("**column null count settings**"):
                    cols = t_b_available_columns
                    cnc_dict={}
                    cnc_thresh = []
    
                        # column_df = {
                        #     "Control table columns":cols,
                        #     "Comparison table columns":cols
                        # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select columns to analyze from **control** table")
                    pick_cols = st.multiselect("",t_a_available_columns, key="1_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write("2. Match columns from the **comparison** table to the **control** table and apply threshold")
                        st.warning("Thresholds only apply for scheduled checks")
                        st.write("###")
                        idcol1, idcol2,idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            col_dict = {}
                            idcol1, idcol2,idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cnc_dict[column]=idcol2.selectbox("",cols,key=column+"CTA_cnc")
                            thresh = idcol3.text_input("",key=column+"CTA_cnc_thresh")
                            thresh = thresh if (thresh or thresh == 0) else 100
                            cnc_thresh.append(float(thresh))
                        
                T_A_COLUMNS = list(cnc_dict.keys())
                T_B_COLUMNS = list(cnc_dict.values())
                dq_checks.append(
                        {
                            "CHECK_TYPE_ID": 4,
                            "CHECK_DESCRIPTION": "column null count",
                            "TABLE_A_COLUMNS": T_A_COLUMNS,
                            "TABLE_B_COLUMNS": T_B_COLUMNS,
                            "ALERT_THRESHOLD": cnc_thresh
                        })
            if( cdvc := st.checkbox("column distinct value count")):
                with st.expander("**column distinct value count settings**"):
                    cols = t_b_available_columns
                    cdvc_dict={}
                    cdvc_thresh = []
                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select columns to analyze from **control** table")
                    pick_cols = st.multiselect("",t_a_available_columns, key="2_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write("2. Match columns from the **comparison** table to the **control** table and apply threshold")
                        st.warning("Thresholds only apply for scheduled checks")
                        st.write("###")
                        idcol1, idcol2,idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            idcol1, idcol2,idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cdvc_dict[column] = idcol2.selectbox("",cols,key=column+"CTA_cdvc")
                            thresh = idcol3.text_input("",key=column+"CTA_cdvc_thresh")
                            thresh = thresh if (thresh or thresh == 0) else 100
                            cdvc_thresh.append(float(thresh))
                T_A_COLUMNS = list(cdvc_dict.keys())
                T_B_COLUMNS = list(cdvc_dict.values())
                dq_checks.append(
                        {
                            "CHECK_TYPE_ID": 5,
                            "CHECK_DESCRIPTION": "column distinct value count",
                            "TABLE_A_COLUMNS": T_A_COLUMNS,
                            "TABLE_B_COLUMNS": T_B_COLUMNS,
                            "ALERT_THRESHOLD": cdvc_thresh
                        })

            if( cdvda := st.checkbox("column distinct values dropped or added")):
                with st.expander("**column distinct values dropped or added settings**"):
                        cols = t_b_available_columns
                        cdvda_dict = {}
                        cdvda_thresh = []
                        # column_df = {
                        #     "Control table columns":cols,
                        #     "Comparison table columns":cols
                        # }
                        # st.header("Record level value change")
                        st.subheader("Columns to Analyze")
                        st.write("1. Select columns to analyze from **control** table")
                        pick_cols = st.multiselect("",t_a_available_columns, key="3_CTA_SELECT_COLUMNS")
                        if pick_cols:
                            st.write("2. Match columns from the **comparison** table to the **control** table and apply threshold")
                            st.warning("Thresholds only apply for scheduled checks")
                            st.write("###")
                            idcol1, idcol2,idcol3 = st.columns(3)
                            idcol1.subheader("Control Column")
                            idcol2.subheader("Comparison Column")
                            idcol3.subheader("Threshold")
                            for column in pick_cols:
                                idcol1, idcol2,idcol3 = st.columns(3)
                                idcol1.write("#")
                                idcol1.write(f'***{column}***')
                                cdvda_dict[column] = idcol2.selectbox("",cols,key=column+"CTA_cdvda")
                                thresh = idcol3.text_input("",key=column+"CTA_cdvda_thresh")
                                thresh = thresh if (thresh or thresh == 0) else 100
                                cdvda_thresh.append(float(thresh))
                T_A_COLUMNS = list(cdvda_dict.keys())
                T_B_COLUMNS = list(cdvda_dict.values())
                dq_checks.append(
                        {
                            "CHECK_TYPE_ID": 6,
                            "CHECK_DESCRIPTION": "column distinct values dropped or added",
                            "TABLE_A_COLUMNS": T_A_COLUMNS,
                            "TABLE_B_COLUMNS": T_B_COLUMNS,
                            "ALERT_THRESHOLD": cdvda_thresh
                        })
            if( cmmv := st.checkbox("column min/max values")):
                with st.expander("**column min/max values settings**"):
                    cols = t_b_available_columns
                    cmmv_dict={}
                    cmmv_thresh = []
                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select columns to analyze from **control** table")
                    pick_cols = st.multiselect("",t_a_available_columns, key="4_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write("2. Match columns from the **comparison** table to the **control** table and apply threshold")
                        st.warning("Thresholds only apply for scheduled checks")
                        st.write("###")
                        idcol1, idcol2,idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            idcol1, idcol2,idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cmmv_dict[column]=idcol2.selectbox("",cols,key=column+"CTA_cmmv")
                            thresh = idcol3.text_input("",key=column+"CTA_cmmv_thresh")
                            thresh = thresh if (thresh or thresh == 0) else 100
                            cmmv_thresh.append(float(thresh))
                
                T_A_COLUMNS = list(cmmv_dict.keys())
                T_B_COLUMNS = list(cmmv_dict.values())
                dq_checks.append(
                        {
                            "CHECK_TYPE_ID": 7,
                            "CHECK_DESCRIPTION": "column min/max values",
                            "TABLE_A_COLUMNS": T_A_COLUMNS,
                            "TABLE_B_COLUMNS": T_B_COLUMNS,
                            "ALERT_THRESHOLD": cmmv_thresh
                        })
                
            if( cs := st.checkbox("column(s) sum")):
                with st.expander("**column(s) sum settings**"):
                    cols = t_b_available_columns
                    cs_dict = {}
                    cs_thresh = []
                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select columns to analyze from **control** table")
                    pick_cols = st.multiselect("",t_a_available_columns, key="5_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write("2. Match columns from the **comparison** table to the **control** table")
                        st.write("###")
                        idcol1, idcol2, idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            idcol1, idcol2,idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cs_dict[column] = idcol2.selectbox("",cols,key=column+"CTA_cs")
                            thresh = idcol3.text_input("",key=column+"CTA_cs_thresh")
                            thresh = thresh if (thresh or thresh == 0) else 100
                            cs_thresh.append(float(thresh))
                
                T_A_COLUMNS = list(cs_dict.keys())
                T_B_COLUMNS = list(cs_dict.values())
                dq_checks.append(
                        {
                            "CHECK_TYPE_ID": 3,
                            "CHECK_DESCRIPTION": "column(s) sum",
                            "TABLE_A_COLUMNS": T_A_COLUMNS,
                            "TABLE_B_COLUMNS": T_B_COLUMNS,
                            "ALERT_THRESHOLD": cs_thresh
                        })
            st.session_state.dq_nonstat_specs = self.make_non_stat_json(dq_checks)
            
            space, b1,b2 = st.columns((20,3,2))
            # st.write(st.session_state.dq_nonstat_specs)
            b1.button("Schedule check", on_click=change_page, args=('schedule_check',))
            if b2.button("Run check", type= "primary"):
                with st.spinner("Running...."):
                    anom_detect_output = session.call(
                    f"{APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{st.session_state.non_stat_proc}",
                    "",
                    st.session_state.dq_nonstat_specs
                    )
                    # anom_detect_output = { "JOB_ID": -1, "JOB_NAME": "", "PARTITION_COLUMNS": [ "CAT01", "CAT02" ], "QUALIFIED_RESULT_TBL_NM": "DATA_QUALITY.TEMPORARY_DQ_OBJECTS.TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "RESULT_DB": "DATA_QUALITY", "RESULT_SCHEMA": "TEMPORARY_DQ_OBJECTS", "RESULT_TBL_NM": "TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "UDTF_NM": "TEMPORARY_DQ_OBJECTS.perform_anom_detection_2024_01_31_13_12_04_udtf_5587" }
                    st.success("Check Run Successfully!")
                    results_table = json.loads(anom_detect_output)["RESULT_TBL_NM"]
                    results = session.sql(f"SELECT *,(JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY FROM {APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}").to_pandas()
                    st.write(results)
                    r_key = results["RUN_KEY"][0]
                    print_nsc_results(f"{APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}",r_key,0)
            

            
        elif check_type == "Distribution check":
            st.subheader("Job specifications")
            control,comparison = st.columns(2)
        
            with control:
                with st.expander("**Control**", expanded=True):
                    one, two = st.columns(2)
                    one.selectbox("Select **control** database and schema", ["DB"])
                    two.selectbox("Select **control** table", ["Table"])
                    st.text_input("Enter SQL filter condition", placeholder="Placeholder")
                    st.multiselect("Select partition columns", ["col1","col2","col3"])
                    st.multiselect("Distribution checks", ["Check Two", "Check Two"])
        
            with comparison:
                with st.expander("**Comparison**", expanded=True):
                    one, two = st.columns(2)
                    one.selectbox("Select **comparison** database and schema", ["DB"])
                    two.selectbox("Select **comparison** table", ["Table"])
                    st.text_input("Enter SQL filter condition", placeholder="Placeholder", key="control filter")
                    st.multiselect("Select partition columns", ["col1","col2","col3"],  key="control partitions")
                    st.multiselect("Distribution checks", ["Check Two", "Check Two"],  key="control check")
        
            space, b1,b2 = st.columns((20,3,2))
            b1.button("Schedule check", on_click=change_page, args=('schedule_check',))
            if b2.button("Run check", type= "primary"):
                st.success("Check Run Successfully!")
        
        elif check_type == "Anomaly detection":
            
            st.subheader("Job specifications")
        
            with st.expander("**Settings**", expanded=True):
                one, two, three = st.columns(3)
                chosen_db = one.selectbox("Select database", databases)
                
                schemas = []
                if chosen_db:
                    schemas = get_schemas(chosen_db)
                chosen_schema = two.selectbox("Select schema", schemas, disabled= (False if chosen_db else True))

                tables = []
                if chosen_schema:
                    tables = get_tables(chosen_db,chosen_schema)
                chosen_table = three.selectbox("Select table", tables, disabled= (False if chosen_schema else True))
                
                sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder")
                
                
                table_columns = []
                if chosen_table:
                    try:
                        table_columns = session.sql(f"describe table {chosen_db}.{chosen_schema}.{chosen_table}").collect()
                    except:
                        st.warning("You may not have access to the objects above")
                p_c = st.multiselect("Select partition columns", table_columns)
                r_id_c = st.multiselect("Record ID columns", table_columns)
                c_c = st.multiselect("Select columns to check", table_columns)
                threshold = st.text_input("Threshold", placeholder="Enter Your Alert Threshold")
                if not threshold:
                    threshold = .5
                st.session_state.chosen_anomaly_table = f"{chosen_db}.{chosen_schema}.{chosen_table}"
                # st.session_state.dq_specs = {"chosen_table": f"{chosen_db}.{chosen_schema}.{chosen_table}","sql_filter" : sql_filter,"Partition_columns": p_c,"Record_id_columns": r_id_c,"Check Columns":c_c}
                st.session_state.dq_anomaly_specs = {"TABLE_B_DB_NAME": f"{chosen_db}","TABLE_B_SCHEMA_NAME": f"{chosen_schema}","TABLE_B_NAME": f"{chosen_table}","TABLE_B_FILTER": f"{sql_filter}","TABLE_B_RECORD_ID_COLUMNS": r_id_c,"TABLE_B_PARTITION_COLUMNS": p_c,"RESULTS_DB": f"{APP_OPP_DB}","RESULTS_SCHEMA": f"{APP_RESULTS_SCHEMA}","RESULTS_TBL_NM": "DQ_ANOMALY_DETECT_RESULTS","TEMPORARY_DQ_OBJECTS_SCHEMA": "TEMPORARY_DQ_OBJECTS","CHECKS": [{"CHECK_TYPE_ID": 8,"CHECK_DESCRIPTION": "isolation forest","TABLE_B_COLUMNS": c_c,"HYPERPARAMETERS_DICT": {"n_estimators": 1000,"max_samples": 0.75,"max_features": 0.75,"random_state": 42,},"ALERT_THRESHOLD": [threshold]}],}
                
            space, b1,b2 = st.columns((20,3,2))
            b1.button("Schedule check", on_click=change_page, args=('schedule_check',))
            if b2.button("Run check", type= "primary"):
                anom_detect_output = session.call(
                f"{APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{st.session_state.anomoly_proc}",
                "",
                st.session_state.dq_anomaly_specs
                )
                # anom_detect_output = { "JOB_ID": -1, "JOB_NAME": "", "PARTITION_COLUMNS": [ "CAT01", "CAT02" ], "QUALIFIED_RESULT_TBL_NM": "DATA_QUALITY.TEMPORARY_DQ_OBJECTS.TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "RESULT_DB": "DATA_QUALITY", "RESULT_SCHEMA": "TEMPORARY_DQ_OBJECTS", "RESULT_TBL_NM": "TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "UDTF_NM": "TEMPORARY_DQ_OBJECTS.perform_anom_detection_2024_01_31_13_12_04_udtf_5587" }
                st.success("Check Run Successfully!")
                results_table = json.loads(anom_detect_output)["RESULT_TBL_NM"]
                results = session.sql(f"SELECT *,(JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY FROM {APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}").to_pandas()
                # st.write(results)
                r_key = results["RUN_KEY"][0]
                get_anomaly_chart(f"{APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}",r_key, 0)

    def run_SCTab(self):
        
        session = st.session_state.session
        
        if "job_paginator" not in st.session_state or st.session_state.job_paginator["start"] == st.session_state.job_paginator["end"]:
                    st.session_state.job_paginator={"start":0,"end":st.session_state.pag_interval}
        search, fbf, fbl, fbd, fbdqc, button = st.columns((6,2,2,2,2,2))
        job_search = search.text_input("Search", key="scttsearch")
        fbf.selectbox("Filter by frequency", ["All"])
        fbl.selectbox("Filter by label", ["All"])
        fbd.selectbox("Filter by date", ["All"])
        fbdqc.selectbox("Filter by data quality check", ["All"], key="dqcfilterdqc")
        # button.write("20 checks scheduled")
    
        jobs = session.sql(f"SELECT * FROM {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS WHERE JOB_NAME ILIKE '%{job_search}%' or LABEL ILIKE '%{job_search}%' ORDER BY CREATE_DTTM DESC").collect()
        st.write("-------")
        # total_pages = int(len(jobs)/st.session_state.pag_interval)
        # current_page = int(st.session_state.job_paginator["end"]/st.session_state.pag_interval)
        def jobs_callback(job):
            colOne,colTwo, colThree = st.columns((30,4,4))
            job_id = job[0]
            job_name = job[1]
            runtime = job[4]
            schedule = job[5]
            label = job[11]
            if schedule in reverse_chron_dict:
                colOne.write(reverse_chron_dict[schedule])
            else:
                colOne.write(schedule)
            colOne.markdown(f'***{job_name}***')
            colOne.write("Label: "+label)

            colTwo.write("#")
            colTwo.button("edit", type="primary", key = f"{job_id}_edit", on_click=self.go_to_edit, args=(job,))
            colThree.write("#")
            if colThree.button("run", key = f"{job_id}_run", on_click=self.execute_job, args=(job,)):
                st.success("Job run successfully")

            
            st.write("-----")

        paginator("jobsinator",jobs,10,jobs_callback)
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
    
    
    def print_page(self):

        session = st.session_state.session
        notTab, DQCTab, SCTab = st.tabs(["Notifications","Data quality checks", "Scheduled Checks"])


        with notTab:
            self.run_notTab()


        with DQCTab:
            self.run_DQCTab()


        with SCTab:
            self.run_SCTab()

    
    def print_sidebar(self):
        pass
