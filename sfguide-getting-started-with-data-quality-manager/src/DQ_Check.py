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

class DQCheckPage(Page):
    def __init__(self):
        self.name = 'dataquality_check'
        pass

    def make_non_stat_json(self, dq_checks):

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

    def print_page(self):
        session = st.session_state.session

        st.session_state.a_r_id_columns = ""
        st.session_state.b_r_id_columns = ""
        databases = st.session_state.databases
        st.subheader("Select your data check")
        check_type = st.selectbox('', ["Column Value Check","Native Snowflake Checks","Anomaly detection", "Non-statistical data quality check"])
        # check_type = "Native Snowflake Checks"
        st.session_state.session_check_type = check_type
        st.write("-----")

        if check_type =="Column Value Check":
            check_type = "Metadata Check"
            st.session_state.session_check_type = check_type
            with st.expander("**Settings**", expanded=True):
                    one, two, three = st.columns(3)
                    chosen_db = one.selectbox("Select database", databases)

                    schemas = []
                    if chosen_db:
                        schemas = get_schemas(chosen_db)
                    chosen_schema = two.selectbox("Select schema", schemas, disabled=(False if chosen_db else True))

                    tables = []
                    if chosen_schema:
                        tables = get_tables(chosen_db, chosen_schema)
                    chosen_table = three.selectbox("Select table", tables, disabled=(False if chosen_schema else True))

                    # sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder")

                    table_columns = []
                    if chosen_table:
                        try:
                            table_columns = sql_to_dataframe(
                                f"describe table {chosen_db}.{chosen_schema}.{chosen_table}")
                            table_columns = pd.DataFrame(table_columns)["name"]
                        except:
                            st.warning("You may not have access to the objects above")

                    c_c = st.multiselect("Select columns to check", table_columns)
                    # threshold = st.text_input("Threshold", placeholder="Enter Your Alert Threshold")
                    # if not threshold:
                    #     threshold = .5
                    st.session_state.metadata_table = f"{chosen_db}.{chosen_schema}.{chosen_table}"
            column_jsons = []
            for column in c_c:
                column_jsons.append(f'{{"database_name":"{chosen_db}","schema_name":"{chosen_schema}","table_name":"{chosen_table}","c_name":"{column}","c_datatype":"string"}}')
            st.session_state.metadata_spec = column_jsons
            st.button("Schedule check", on_click=change_page, args=('schedule_check',), type="primary")

        elif check_type == "Native Snowflake Checks":
            if st.checkbox("Scan whole schema"):
                st.session_state.scan_schema = True
                with st.expander("**Settings**", expanded=True):
                    one, two= st.columns(2)
                    chosen_db = one.selectbox("Select database", databases)

                    schemas = []
                    if chosen_db:
                        schemas = get_schemas(chosen_db)
                    chosen_schema = two.selectbox("Select schema", schemas, disabled=(False if chosen_db else True))

                    tables = []
                    if chosen_schema:
                        tables = get_tables(chosen_db, chosen_schema)

                    # sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder")
                table_specs = []

                if chosen_schema:
                    st.success("Configure checks by column and table")
                for table in tables:
                    st.session_state.metadata_table = f"{chosen_db}.{chosen_schema}.{table}"
                    specs = {}
                    specs["COLUMNS"] = []
                    column_spec = {}
                    with st.expander(f"***{table}***"):
                        rc = st.checkbox("Row Count",key=f"{table}_row_check")
                        table_columns = []
                        try:
                            table_columns = sql_to_dataframe(
                                f"describe table {chosen_db}.{chosen_schema}.{table}")
                            sql_to_dataframe(f"SHOW COLUMNS IN TABLE {chosen_db}.{chosen_schema}.{table}")
                            column_data = sql_to_pandas('SELECT "column_name",LOWER(PARSE_JSON("data_type"):type::string) AS data_type FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))')
                            column_names = column_data["column_name"]
                        except:
                            st.warning("You may not have access to this object")
                        c_c = []
                        c_c = st.multiselect("Select columns to check", column_names, key=f"{table}_cc")
                        st.divider()
                        column = st.selectbox("Select checks for column", c_c, key=f"{table}_conjfig_column")
                        if column:
                            st.success("Select the data checks for column: "+column)
                            type = column_data[column_data["column_name"] == column]["DATA_TYPE"].values[0]
                            st.write(type)
                            st.write("*Accuracy*")
                            if type == "text":
                                bc = st.checkbox("Blank Count", key = f"{column}_bc")
                                bp = st.checkbox("Blank Percent", key = f"{column}_bp")
                            else:
                                bc = False
                                bp = False
                            nc = st.checkbox("Null Count", key = f"{column}_nc")
                            np = st.checkbox("Null Percent", key = f"{column}_np")

                            st.divider()
                            if type == "date" or "timestamp" in type:
                                st.write("*Freshness*")
                                fc = st.checkbox("Freshness", key = f"{column}_f")
                                st.divider()
                            else:
                                fc = False
                            if type == "fixed":
                                st.write("Statistics")
                                avg = st.checkbox("Average", key = f"{column}_avg")
                                max_c = st.checkbox("Maximum", key = f"{column}_max")
                                min_c = st.checkbox("Minimum", key = f"{column}_min")
                                sd = st.checkbox("Standard Deviation", key = f"{column}_sd")
                                st.divider()
                            else:
                                avg = False
                                max_c = False
                                min_c = False
                                sd = False
                            st.write("Uniqueness")
                            dc = st.checkbox("Duplicate Count", key = f"{column}_dc")
                            uc = st.checkbox("Unique Count", key = f"{column}_uc")

                            checks = []
                            thresholds = []
                            if bc:
                                checks.append("BLANK_COUNT")
                            if bp:
                                checks.append("BLANK_PERCENT")
                            if nc:
                                checks.append("NULL_COUNT")
                            if np:
                                checks.append("NULL_PERCENT")
                            if fc:
                                checks.append("FRESHNESS")
                            if avg:
                                checks.append("AVG")
                            if max_c:
                                checks.append("MAX")
                            if min_c:
                                checks.append("MIN")
                            if sd:
                                checks.append("STDDEV")
                            if dc:
                                checks.append("DUPLICATE_COUNT")
                            if uc:
                                checks.append("UNIQUE_COUNT")


                            specs["COLUMNS"].append({"COLUMN":column, "CHECKS":checks})
                
                    specs["TABLE"] = st.session_state.metadata_table
                    specs["COUNT_CHECK"] = rc
                    table_specs.append(specs)

                st.session_state.snowflake_dmf_specs = table_specs
            else:
                st.session_state.scan_schema = False
                with st.expander("**Settings**", expanded=True):
                    one, two, three = st.columns(3)
                    chosen_db = one.selectbox("Select database", databases)

                    schemas = []
                    if chosen_db:
                        schemas = get_schemas(chosen_db)
                    chosen_schema = two.selectbox("Select schema", schemas, disabled=(False if chosen_db else True))

                    tables = []
                    if chosen_schema:
                        tables = get_tables(chosen_db, chosen_schema)
                    chosen_table = three.selectbox("Select table", tables, disabled=(False if chosen_schema else True))

                    # sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder")
                    column_names = []
                    table_columns = []
                    if chosen_table:
                        try:
                            table_columns = sql_to_dataframe(
                                f"describe table {chosen_db}.{chosen_schema}.{chosen_table}")
                            sql_to_dataframe(f"SHOW COLUMNS IN TABLE {chosen_db}.{chosen_schema}.{chosen_table}")
                            column_data = sql_to_pandas('SELECT "column_name",LOWER(PARSE_JSON("data_type"):type::string) AS data_type FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))')
                            column_names = column_data["column_name"]
                        except:
                            st.warning("You may not have access to the objects above")

                    c_c = st.multiselect("Select columns to check", column_names)

                    
                with st.expander("Table Volume"):
                    st.success("Select to measure table row count over time")
                    rc = st.checkbox("Row Count")
                
                specs = {}
                specs["COLUMNS"] = []
                for column in c_c:
                    column_spec = {}
                    with st.expander(column +" Data Checks"):
                        st.success("Select the data checks for column: "+column)
                        type = column_data[column_data["column_name"] == column]["DATA_TYPE"].values[0]
                        st.write(type)
                        st.write("*Accuracy*")
                        if type == "text":
                            bc = st.checkbox("Blank Count", key = f"{column}_bc")
                            bc_t = st.text_input("","Threshold", key = f"{column}_bc_t")
                            bp = st.checkbox("Blank Percent", key = f"{column}_bp")
                            bp_t = st.text_input("","Threshold", key = f"{column}_bp_t")
                        else:
                            bc = False
                            bp = False
                        nc = st.checkbox("Null Count", key = f"{column}_nc")
                        nc_t = st.text_input("","Threshold", key = f"{column}_nc_t")
                        np = st.checkbox("Null Percent", key = f"{column}_np")
                        np_t = st.text_input("","Threshold", key = f"{column}_np_t")

                        st.divider()
                        if type == "date" or "timestamp" in type:
                            st.write("*Freshness*")
                            fc = st.checkbox("Freshness", key = f"{column}_f")
                            fc_t = st.text_input("","Threshold", key = f"{column}_fc_t")
                            st.divider()
                        else:
                            fc = False
                        if type == "fixed":
                            st.write("Statistics")
                            avg = st.checkbox("Average", key = f"{column}_avg")
                            avg_t = st.text_input("","Threshold", key = f"{column}_avg_t")
                            max_c = st.checkbox("Maximum", key = f"{column}_max")
                            max_t = st.text_input("","Threshold", key = f"{column}_max_t")
                            min_c = st.checkbox("Minimum", key = f"{column}_min")
                            min_t = st.text_input("","Threshold", key = f"{column}_min_t")
                            sd = st.checkbox("Standard Deviation", key = f"{column}_sd")
                            sd_t = st.text_input("","Threshold", key = f"{column}_sd_t")
                            st.divider()
                        else:
                            avg = False
                            max_c = False
                            min_c = False
                            sd = False
                        st.write("Uniqueness")
                        dc = st.checkbox("Duplicate Count", key = f"{column}_dc")
                        dc_t = st.text_input("","Threshold", key = f"{column}_dc_t")
                        uc = st.checkbox("Unique Count", key = f"{column}_uc")
                        uc_t = st.text_input("","Threshold", key = f"{column}_uc_t")

                        checks = []
                        thresholds = []
                    if bc:
                        checks.append("BLANK_COUNT")
                        thresholds.append(bc_t)
                    if bp:
                        checks.append("BLANK_PERCENT")
                        thresholds.append(bp_t)
                    if nc:
                        checks.append("NULL_COUNT")
                        thresholds.append(nc_t)
                    if np:
                        checks.append("NULL_PERCENT")
                        thresholds.append(np_t)
                    if fc:
                        checks.append("FRESHNESS")
                        thresholds.append(fc_t)
                    if avg:
                        checks.append("AVG")
                        thresholds.append(avg_t)
                    if max_c:
                        checks.append("MAX")
                        thresholds.append(max_t)
                    if min_c:
                        checks.append("MIN")
                        thresholds.append(min_t)
                    if sd:
                        checks.append("STDDEV")
                        thresholds.append(sd_t)
                    if dc:
                        checks.append("DUPLICATE_COUNT")
                        thresholds.append(dc_t)
                    if uc:
                        checks.append("UNIQUE_COUNT")
                        thresholds.append(uc_t)

                    thresholds = [0 for threshold in thresholds if threshold == "Threshold"]

                    specs["COLUMNS"].append({"COLUMN":column, "CHECKS":checks, "THRESHOLDS":thresholds})


                # threshold = st.text_input("Threshold", placeholder="Enter Your Alert Threshold")
                # if not threshold:
                #     threshold = .5
                st.session_state.metadata_table = f"{chosen_db}.{chosen_schema}.{chosen_table}"

                specs["TABLE"] = st.session_state.metadata_table
                specs["COUNT_CHECK"] = rc


                st.session_state.snowflake_dmf_specs = specs

            st.button("Schedule check", on_click=change_page, args=('schedule_check',), type="primary")
        elif check_type == "Non-statistical data quality check":
            dq_checks = []

            st.subheader("Job specifications")
            control, comparison = st.columns(2)

            with control:
                with st.expander("**Control**", expanded=True):
                    one, two, three = st.columns(3)
                    chosen_db = one.selectbox("Select **control** database", databases)

                    schemas = []
                    if chosen_db:
                        schemas = get_schemas(chosen_db)
                    chosen_schema = two.selectbox("Select **control** schema", schemas,
                                                  disabled=(False if chosen_db else True))

                    tables = []
                    if chosen_schema:
                        tables = get_tables(chosen_db, chosen_schema)
                    chosen_table = three.selectbox("Select **control** table", tables,
                                                   disabled=(False if chosen_schema else True))

                    sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder")

                    t_a_available_columns = []
                    if chosen_table:
                        try:
                            t_a_available_columns = sql_to_dataframe(
                                f"describe table {chosen_db}.{chosen_schema}.{chosen_table}")
                            t_a_available_columns = pd.DataFrame(t_a_available_columns)["name"]
                        except:
                            st.warning("You may not have access to the objects above")
                    # record_id_columns = st.multiselect("Record ID columns", t_a_available_columns,  key="control record id")
                    part_columns = st.multiselect("Select partition columns", t_a_available_columns,
                                                  key="control partitions")

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
                    chosen_b_schema = two.selectbox("Select **comparison** schema", schemas,
                                                    disabled=(False if chosen_b_db else True))

                    tables = []
                    if chosen_b_schema:
                        tables = get_tables(chosen_b_db, chosen_b_schema)
                    chosen_b_table = three.selectbox("Select **comparison** table", tables,
                                                     disabled=(False if chosen_b_schema else True))

                    sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder",
                                               key="sql comparison filter")

                    t_b_available_columns = []
                    if chosen_b_table:
                        try:
                            t_b_available_columns = sql_to_dataframe(
                                f"describe table {chosen_b_db}.{chosen_b_schema}.{chosen_b_table}")
                            t_b_available_columns = pd.DataFrame(t_b_available_columns)["name"]
                        except:
                            st.warning("You may not have access to the objects above")
                    # record_id_columns = st.multiselect("Record ID columns", t_b_available_columns,  key="comparison record id")
                    part_columns = st.multiselect("Select partition columns", t_b_available_columns,
                                                  key="comparison partitions")

                    st.session_state.b_database = chosen_b_db
                    st.session_state.b_schema = chosen_b_schema
                    st.session_state.b_table = chosen_b_table
                    st.session_state.b_filter = sql_filter
                    # st.session_state.b_r_id_columns = record_id_columns
                    st.session_state.b_part_columns = part_columns
            if (tptrc := st.checkbox("tables/partitions total row count")):
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
                            "ALERT_THRESHOLD": [(thresh if thresh else 100)]  # NOTE: Always a single value
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
                    pick_cols = st.multiselect("", t_a_available_columns)
                    if pick_cols:
                        st.write(
                            "2. Select columns from the **comparison** table to match them with the **control** table")
                        st.write("###")
                        idcol1, idcol2, space = st.columns((4, 4, 6))
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        for column in pick_cols:
                            idcol1, idcol2, space = st.columns((4, 4, 6))
                            idcol1.write("###")
                            idcol1.write(f'***{column}***')
                            rlvc_rid_dict[column] = idcol2.selectbox("", cols, key=column + "rlvc")
                    st.write("----")

                    cols = t_b_available_columns

                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select comparison columns from **control** table")
                    pick_cols = st.multiselect("", t_a_available_columns, key="CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write(
                            "2. Select columns from the **comparison** table to match them with the **control** table")
                        st.write("###")
                        idcol1, idcol2, space = st.columns((4, 4, 6))
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        for column in pick_cols:
                            idcol1, idcol2, space = st.columns((4, 4, 6))
                            idcol1.write("###")
                            idcol1.write(f'***{column}***')
                            rlvc_dict[column] = idcol2.selectbox("", cols, key=column + "CTA_rlvc")
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
                        "ALERT_THRESHOLD": [float(thresh if thresh else 100)]  # NOTE: Always a single value
                    })

            if (cnc := st.checkbox("column null count")):
                with st.expander("**column null count settings**"):
                    cols = t_b_available_columns
                    cnc_dict = {}
                    cnc_thresh = []

                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select columns to analyze from **control** table")
                    pick_cols = st.multiselect("", t_a_available_columns, key="1_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write(
                            "2. Match columns from the **comparison** table to the **control** table and apply threshold")
                        st.warning("Thresholds only apply for scheduled checks")
                        st.write("###")
                        idcol1, idcol2, idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            col_dict = {}
                            idcol1, idcol2, idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cnc_dict[column] = idcol2.selectbox("", cols, key=column + "CTA_cnc")
                            thresh = idcol3.text_input("", key=column + "CTA_cnc_thresh")
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
            if (cdvc := st.checkbox("column distinct value count")):
                with st.expander("**column distinct value count settings**"):
                    cols = t_b_available_columns
                    cdvc_dict = {}
                    cdvc_thresh = []
                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select columns to analyze from **control** table")
                    pick_cols = st.multiselect("", t_a_available_columns, key="2_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write(
                            "2. Match columns from the **comparison** table to the **control** table and apply threshold")
                        st.warning("Thresholds only apply for scheduled checks")
                        st.write("###")
                        idcol1, idcol2, idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            idcol1, idcol2, idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cdvc_dict[column] = idcol2.selectbox("", cols, key=column + "CTA_cdvc")
                            thresh = idcol3.text_input("", key=column + "CTA_cdvc_thresh")
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

            if (cdvda := st.checkbox("column distinct values dropped or added")):
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
                    pick_cols = st.multiselect("", t_a_available_columns, key="3_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write(
                            "2. Match columns from the **comparison** table to the **control** table and apply threshold")
                        st.warning("Thresholds only apply for scheduled checks")
                        st.write("###")
                        idcol1, idcol2, idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            idcol1, idcol2, idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cdvda_dict[column] = idcol2.selectbox("", cols, key=column + "CTA_cdvda")
                            thresh = idcol3.text_input("", key=column + "CTA_cdvda_thresh")
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
            if (cmmv := st.checkbox("column min/max values")):
                with st.expander("**column min/max values settings**"):
                    cols = t_b_available_columns
                    cmmv_dict = {}
                    cmmv_thresh = []
                    # column_df = {
                    #     "Control table columns":cols,
                    #     "Comparison table columns":cols
                    # }
                    # st.header("Record level value change")
                    st.subheader("Columns to Analyze")
                    st.write("1. Select columns to analyze from **control** table")
                    pick_cols = st.multiselect("", t_a_available_columns, key="4_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write(
                            "2. Match columns from the **comparison** table to the **control** table and apply threshold")
                        st.warning("Thresholds only apply for scheduled checks")
                        st.write("###")
                        idcol1, idcol2, idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            idcol1, idcol2, idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cmmv_dict[column] = idcol2.selectbox("", cols, key=column + "CTA_cmmv")
                            thresh = idcol3.text_input("", key=column + "CTA_cmmv_thresh")
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

            if (cs := st.checkbox("column(s) sum")):
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
                    pick_cols = st.multiselect("", t_a_available_columns, key="5_CTA_SELECT_COLUMNS")
                    if pick_cols:
                        st.write("2. Match columns from the **comparison** table to the **control** table")
                        st.write("###")
                        idcol1, idcol2, idcol3 = st.columns(3)
                        idcol1.subheader("Control Column")
                        idcol2.subheader("Comparison Column")
                        idcol3.subheader("Threshold")
                        for column in pick_cols:
                            idcol1, idcol2, idcol3 = st.columns(3)
                            idcol1.write("#")
                            idcol1.write(f'***{column}***')
                            cs_dict[column] = idcol2.selectbox("", cols, key=column + "CTA_cs")
                            thresh = idcol3.text_input("", key=column + "CTA_cs_thresh")
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

            space, b1, b2 = st.columns((20, 3, 2))
            # st.write(st.session_state.dq_nonstat_specs)
            b1.button("Schedule check", on_click=change_page, args=('schedule_check',))
            if b2.button("Run check", type="primary"):
                with st.spinner("Running...."):
                    anom_detect_output = session.call(
                        f"{APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{st.session_state.non_stat_proc}",
                        "",
                        st.session_state.dq_nonstat_specs
                    )
                    # anom_detect_output = { "JOB_ID": -1, "JOB_NAME": "", "PARTITION_COLUMNS": [ "CAT01", "CAT02" ], "QUALIFIED_RESULT_TBL_NM": "DATA_QUALITY.TEMPORARY_DQ_OBJECTS.TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "RESULT_DB": "DATA_QUALITY", "RESULT_SCHEMA": "TEMPORARY_DQ_OBJECTS", "RESULT_TBL_NM": "TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "UDTF_NM": "TEMPORARY_DQ_OBJECTS.perform_anom_detection_2024_01_31_13_12_04_udtf_5587" }
                    st.success("Check Run Successfully!")
                    results_table = json.loads(anom_detect_output)["RESULT_TBL_NM"]
                    results = sql_to_pandas(
                        f"SELECT *,(JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY FROM {APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}")
                    st.write(results)
                    r_key = results["RUN_KEY"][0]
                    print_nsc_results(f"{APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}", r_key, 0)



        elif check_type == "Distribution check":
            st.subheader("Job specifications")
            control, comparison = st.columns(2)

            with control:
                with st.expander("**Control**", expanded=True):
                    one, two = st.columns(2)
                    one.selectbox("Select **control** database and schema", ["DB"])
                    two.selectbox("Select **control** table", ["Table"])
                    st.text_input("Enter SQL filter condition", placeholder="Placeholder")
                    st.multiselect("Select partition columns", ["col1", "col2", "col3"])
                    st.multiselect("Distribution checks", ["Check Two", "Check Two"])

            with comparison:
                with st.expander("**Comparison**", expanded=True):
                    one, two = st.columns(2)
                    one.selectbox("Select **comparison** database and schema", ["DB"])
                    two.selectbox("Select **comparison** table", ["Table"])
                    st.text_input("Enter SQL filter condition", placeholder="Placeholder", key="control filter")
                    st.multiselect("Select partition columns", ["col1", "col2", "col3"], key="control partitions")
                    st.multiselect("Distribution checks", ["Check Two", "Check Two"], key="control check")

            space, b1, b2 = st.columns((20, 3, 2))
            b1.button("Schedule check", on_click=change_page, args=('schedule_check',))
            if b2.button("Run check", type="primary"):
                st.success("Check Run Successfully!")

        elif check_type == "Anomaly detection":

            st.subheader("Job specifications")

            with st.expander("**Settings**", expanded=True):
                one, two, three = st.columns(3)
                chosen_db = one.selectbox("Select database", databases)

                schemas = []
                if chosen_db:
                    schemas = get_schemas(chosen_db)
                chosen_schema = two.selectbox("Select schema", schemas, disabled=(False if chosen_db else True))

                tables = []
                if chosen_schema:
                    tables = get_tables(chosen_db, chosen_schema)
                chosen_table = three.selectbox("Select table", tables, disabled=(False if chosen_schema else True))

                sql_filter = st.text_input("Enter SQL filter condition", placeholder="Placeholder")

                table_columns = []
                if chosen_table:
                    try:
                        table_columns = sql_to_dataframe(
                            f"describe table {chosen_db}.{chosen_schema}.{chosen_table}")
                        table_columns = pd.DataFrame(table_columns)["name"]
                    except:
                        st.warning("You may not have access to the objects above")
                p_c = st.multiselect("Select partition columns", table_columns)
                r_id_c = st.multiselect("Record ID columns", table_columns)
                c_c = st.multiselect("Select columns to check", table_columns)
                threshold = st.text_input("Threshold", placeholder="Enter Your Alert Threshold")
                if not threshold:
                    threshold = .5
                st.session_state.metadata_table = f"{chosen_db}.{chosen_schema}.{chosen_table}"
                # st.session_state.dq_specs = {"chosen_table": f"{chosen_db}.{chosen_schema}.{chosen_table}","sql_filter" : sql_filter,"Partition_columns": p_c,"Record_id_columns": r_id_c,"Check Columns":c_c}
                st.session_state.dq_anomaly_specs = {"TABLE_B_DB_NAME": f"{chosen_db}",
                                                     "TABLE_B_SCHEMA_NAME": f"{chosen_schema}",
                                                     "TABLE_B_NAME": f"{chosen_table}",
                                                     "TABLE_B_FILTER": f"{sql_filter}",
                                                     "TABLE_B_RECORD_ID_COLUMNS": r_id_c,
                                                     "TABLE_B_PARTITION_COLUMNS": p_c, "RESULTS_DB": f"{APP_OPP_DB}",
                                                     "RESULTS_SCHEMA": f"{APP_RESULTS_SCHEMA}",
                                                     "RESULTS_TBL_NM": "DQ_ANOMALY_DETECT_RESULTS",
                                                     "TEMPORARY_DQ_OBJECTS_SCHEMA": "TEMPORARY_DQ_OBJECTS", "CHECKS": [
                        {"CHECK_TYPE_ID": 8, "CHECK_DESCRIPTION": "isolation forest", "TABLE_B_COLUMNS": c_c,
                         "HYPERPARAMETERS_DICT": {"n_estimators": 1000, "max_samples": 0.75, "max_features": 0.75,
                                                  "random_state": 42, }, "ALERT_THRESHOLD": [threshold]}], }

            space, b1, b2 = st.columns((20, 3, 2))
            b1.button("Schedule check", on_click=change_page, args=('schedule_check',))
            if b2.button("Run check", type="primary"):
                anom_detect_output = session.call(
                    f"{APP_OPP_DB}.{APP_CONFIG_SCHEMA}.{st.session_state.anomoly_proc}",
                    "",
                    st.session_state.dq_anomaly_specs
                )
                # anom_detect_output = { "JOB_ID": -1, "JOB_NAME": "", "PARTITION_COLUMNS": [ "CAT01", "CAT02" ], "QUALIFIED_RESULT_TBL_NM": "DATA_QUALITY.TEMPORARY_DQ_OBJECTS.TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "RESULT_DB": "DATA_QUALITY", "RESULT_SCHEMA": "TEMPORARY_DQ_OBJECTS", "RESULT_TBL_NM": "TEMP_2024_01_31_13_12_04_ANOM_DETECT_RESULTS_5587", "UDTF_NM": "TEMPORARY_DQ_OBJECTS.perform_anom_detection_2024_01_31_13_12_04_udtf_5587" }
                st.success("Check Run Successfully!")
                results_table = json.loads(anom_detect_output)["RESULT_TBL_NM"]
                results = sql_to_pandas(
                    f"SELECT *,(JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY FROM {APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}")
                # st.write(results)
                r_key = results["RUN_KEY"][0]
                get_anomaly_chart(f"{APP_OPP_DB}.{APP_TEMP_DATA_SCHEMA}.{results_table}", r_key, 0)

    # Repeatable element: sidebar buttons that navigate to linked pages
    def print_sidebar(self):
        pass