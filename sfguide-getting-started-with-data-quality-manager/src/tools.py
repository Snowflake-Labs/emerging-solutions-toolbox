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



QUERY_TAG = {"origin": "sf_sit",
             "name": "sit_data_quality_framework",
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

def get_anomaly_chart(table,note_id,flag):

    session = st.session_state.session

    _percentiles = [
        0.10,
        0.25,
        0.50,
        0.75,
        0.90,
        0.95,
        0.99,
        0.999,
        0.9999,
        0.99999,
        0.999999,
    ]
    _percentile_nms = [
        "10%",
        "25%",
        "50%",
        "75%",
        "90%",
        "95%",
        "99%",
        "99.9%",
        "99.99%",
        "99.999%",
        "99.9999%",
    ]

    
    partitions = session.sql(f"SELECT DISTINCT PARTITION_VALUES FROM {table}").to_pandas()
    
    chosen_part = st.selectbox("Choose your partition", partitions, key ='PPICKER'+note_id)

    chosen_part = literal_eval(chosen_part)

    pandas_percentiles = pd.DataFrame()

    part_sql = ''
    for i in range(len(chosen_part)):
        part_sql += f"AND ARRAY_CONTAINS('{chosen_part[i]}'::variant,PARTITION_VALUES) "
        
    part_meta = session.sql(f"""
    with A as (SELECT PARTITION_VALUES, ARRAY_AGG(ANOMALY_SCORE) AS SCORES, ARRAY_MIN(SCORES) as min, ARRAY_MAX(SCORES) as MAX 
    FROM {table}
    WHERE ALERT_FLAG = {flag} 
    and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
    {part_sql}
    GROUP BY PARTITION_VALUES
    )
    SELECT * EXCLUDE SCORES FROM A;
        """).to_pandas()
    
    pandas_percentiles["min"] = part_meta["MIN"]
    
    for i in range(len(_percentiles)):
        data = session.sql(f"""
        with A as (SELECT PARTITION_VALUES, ANOMALY_SCORE, PERCENTILE_CONT({_percentiles[i]}) WITHIN GROUP (ORDER BY ANOMALY_SCORE) OVER (  PARTITION BY PARTITION_VALUES  ) AS PERCENTILES FROM {table}
        WHERE ALERT_FLAG = {flag} 
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        {part_sql}
        ) SELECT PARTITION_VALUES,PERCENTILES FROM A GROUP BY PARTITION_VALUES, PERCENTILES;
        """).to_pandas()

        pandas_percentiles[_percentile_nms[i]] = data["PERCENTILES"]

    pandas_percentiles["max"] = part_meta["MAX"]
    st.dataframe(pandas_percentiles)

    
    melted_df = pd.melt(
    pandas_percentiles,
    var_name="PERCENTILE",
    value_name="VALUE",
    )
    
    # Map PERCENTILE values so the PERCENTILE column is numeric
    melted_df["PERCENTILE"] = melted_df["PERCENTILE"].map(
        dict(zip(_percentile_nms + ["MIN", "MAX"], _percentiles + [0.0, 1.0]))
    )
    
    # Pivot the DataFrame
    pivoted_df = melted_df.pivot_table(
        index=["PERCENTILE"], values="VALUE", aggfunc="first"
    )
    pivoted_df.reset_index(inplace=True)
    
    
    # Plot separate line chart for each partition
    # The chart shows the Anomaly Score cutoffs for each percentile
    y_min = pivoted_df.iloc[:, 1:].min().min()
    y_max = pivoted_df.iloc[:, 1:].max().max()

    figure = plt.figure()
    plt.plot(pivoted_df["PERCENTILE"], pivoted_df["VALUE"], marker="o")

    # Add labels to each data point
    # for i, txt in enumerate(pivoted_df["VALUE"]):
    #     plt.annotate(
    #         f"{txt:.2f}",
    #         (pivoted_df["PERCENTILE"][i], pivoted_df["VALUE"][i]),
    #         textcoords="offset points",
    #         xytext=(0, 5),
    #         ha="center",
    #     )

    plt.suptitle(f"Partition: {chosen_part}", y=1.02, fontsize=14)
    plt.title("Anomaly Score Percentiles")
    plt.xlabel("Percentile")
    plt.ylabel("Score Threshold Value")
    plt.xticks(
        pivoted_df["PERCENTILE"],
        [f"{x:,.0%}" for x in pivoted_df["PERCENTILE"]],
        rotation=90,
    )  # Set x-axis ticks as vertical percentages
    plt.ylim(y_min, y_max)

    st.plotly_chart(figure)

def print_nsc_results(table,note_id,flag):
    session = st.session_state.session
    
    c_one = session.sql(f"""
        SELECT 
        PV.value,
        RESULTS:"TABLE_A_ROW_COUNT" as "Control table count",
        RESULTS:"TABLE_B_ROW_COUNT" as "Compare table count", 
        RESULTS:"DELTA" as "DELTA"
        FROM {table} dq,
        LATERAL FLATTEN(input => dq.PARTITION_VALUES) PV,
        WHERE ALERT_FLAG = {flag}
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        AND CHECK_TYPE_ID = 1;""").to_pandas()
                            
    c_two = session.sql(f"""
        SELECT 
        PV.value,
        RESULTS:"COUNT_OF_RECORDS_W_VALUE_DIFF" as "Count of records with a difference"
        FROM {table} DQ,
        LATERAL FLATTEN(input => dq.PARTITION_VALUES) PV,
        WHERE ALERT_FLAG = {flag}
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        AND CHECK_TYPE_ID = 2;""").to_pandas()
    
    c_three = session.sql(f"""
        SELECT 
        PV.value,
        Col.value as "Column",
        RESULTS:"TABLE_A_NULL_COUNTS"[Col.index] as "Control table count",
        RESULTS:"TABLE_B_NULL_COUNTS"[Col.index] as "Compare table count",
        RESULTS:"DELTA"[Col.index] as "DELTA"
        FROM {table} DQ,
        LATERAL FLATTEN(input => dq.PARTITION_VALUES) PV,
        LATERAL FLATTEN(input => dq.RESULTS:"TABLE_A_COLUMNS") Col
        WHERE ALERT_FLAG = {flag}
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        AND CHECK_TYPE_ID = 4;""").to_pandas()
    
    c_four = session.sql(f"""
        SELECT 
        PV.value,
        Col.value as "Column",
        RESULTS:"TABLE_A_DISTINCT_VALUE_COUNTS"[Col.index] as "Control table count",
        RESULTS:"TABLE_B_DISTINCT_VALUE_COUNTS"[Col.index] as "Compare table count",
        RESULTS:"DELTA"[Col.index] as "DELTA"
        FROM {table} DQ,
        LATERAL FLATTEN(input => dq.PARTITION_VALUES) PV,
        LATERAL FLATTEN(input => dq.RESULTS:"TABLE_A_COLUMNS") Col
        WHERE ALERT_FLAG = {flag}
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        AND CHECK_TYPE_ID = 5;""").to_pandas()
    
    c_five = session.sql(f"""
        SELECT 
        PV.value,
        Col.value as "Column",
        RESULTS:"VALUES_DROPPED"[Col.index] as "Values Dropped",
        RESULTS:"VALUES_ADDED"[Col.index] as "Values Added"
        FROM {table} DQ,
        LATERAL FLATTEN(input => dq.PARTITION_VALUES) PV,
        LATERAL FLATTEN(input => dq.RESULTS:"TABLE_A_COLUMNS") Col
        WHERE ALERT_FLAG = {flag}
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        AND CHECK_TYPE_ID = 6;""").to_pandas()
    
    c_six = session.sql(f"""
        SELECT 
        PV.value,
        Col.value as "Column",
        RESULTS:"TABLE_A_MIN_VALUES"[Col.index] as "Control Tbl Min",
        RESULTS:"TABLE_B_MIN_VALUES"[Col.index] as "Compare Tbl Min",
        RESULTS:"DELTA_MIN"[Col.index] as "Delta (Mins)",
        RESULTS:"COUNT_OF_RECORDS_BTWN_CONTROL_AND_COMPARE_MINS"[Col.index] as "Count of records that are less than the min of the other table",
        RESULTS:"TABLE_A_MAX_VALUES"[Col.index] as "Control Tbl Max",
        RESULTS:"TABLE_B_MAX_VALUES"[Col.index] as "Compare Tbl Max",
        RESULTS:"DELTA_MAX"[Col.index] as "Delta (Max)",
        RESULTS:"COUNT_OF_RECORDS_BTWN_CONTROL_AND_COMPARE_MAXES"[Col.index] as "Count of records that are greater than the max of the other table",
        FROM {table} DQ,
        LATERAL FLATTEN(input => dq.PARTITION_VALUES) PV,
        LATERAL FLATTEN(input => dq.RESULTS:"TABLE_A_COLUMNS") Col
        WHERE ALERT_FLAG = {flag}
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        AND CHECK_TYPE_ID = 7;""").to_pandas()
    
    c_seven = session.sql(f"""
        SELECT 
        PV.value,
        Col.value as "Column",
        RESULTS:"TABLE_A_SUMS"[Col.index] as "Control table count",
        RESULTS:"TABLE_B_SUMS"[Col.index] as "Compare table count",
        RESULTS:"DELTA"[Col.index] as "DELTA"
        FROM {table} DQ,
        LATERAL FLATTEN(input => dq.PARTITION_VALUES) PV,
        LATERAL FLATTEN(input => dq.RESULTS:"TABLE_A_COLUMNS") Col
        WHERE ALERT_FLAG = {flag}
        and (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'
        AND CHECK_TYPE_ID = 3;""").to_pandas()

    if(len(c_one) > 0):
        st.write("**1. Table/partition total row count**")
        st.dataframe(c_one,use_container_width=True)
    if(len(c_two) > 0):
        st.write("**2. Record-level value change**")
        st.dataframe(c_two,use_container_width=True)
    if(len(c_three) > 0):
        st.write("**3. Column null count**")
        st.dataframe(c_three,use_container_width=True)
    if(len(c_four) > 0):
        st.write("**4. Column distinct value count**")
        st.dataframe(c_four,use_container_width=True)
    if(len(c_five) > 0):
        st.write("**5. Column distinct values dropped or added**")
        st.dataframe(c_five,use_container_width=True)
    if(len(c_six) > 0):
        st.write("**6. Column min/max values**")
        st.dataframe(c_six,use_container_width=True)
    if(len(c_seven) > 0):
        st.write("**7. Column(s) sum**")
        st.dataframe(c_seven,use_container_width=True)


@st.cache_data
def get_schemas(database):
    catalog = st.session_state.catalog_info
    schemas = [obj["schema"] for obj in catalog[database]]
    return schemas

@st.cache_data
def get_tables(database, schema):
    catalog = st.session_state.catalog_info
    tables = [obj["tables"] for obj in catalog[database] if obj["schema"] == schema]
    tables = list(dict.fromkeys(tables[0]))
    return tables

def pag_up(tab):
    interval = st.session_state.pag_interval
    st.session_state[tab+"_paginator"]={"start":st.session_state[tab+"_paginator"]["start"]+interval,"end":st.session_state[tab+"_paginator"]["end"]+interval}

def pag_down(tab):
    interval = st.session_state.pag_interval
    st.session_state[tab+"_paginator"]={"start":st.session_state[tab+"_paginator"]["start"]-interval,"end":st.session_state[tab+"_paginator"]["end"]-interval}

def change_page(page):
    st.session_state.current_page = page


def toggle_button(session_variable):
    # st.write(st.session_state[session_variable])
    st.session_state[session_variable] = not st.session_state[session_variable]
    # st.write(st.session_state[session_variable])
