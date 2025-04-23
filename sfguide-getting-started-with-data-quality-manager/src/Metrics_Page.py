import json

from src.tools import toggle_button, sql_to_pandas, sql_to_dataframe
from src.Page import BasePage, set_page
import streamlit as st
import pandas as pd
from src.globals import APP_OPP_DB, APP_CONFIG_SCHEMA, APP_TEMP_DATA_SCHEMA, APP_RESULTS_SCHEMA, APP_DATA_SCHEMA

class MetricsPage(BasePage):
    def __init__(self):
        self.name = "metrics_page"

    def print_page(self):
        session = st.session_state.session

        dmf_jobs = sql_to_pandas("SELECT * FROM DATA_QUALITY.CONFIG.DQ_JOBS WHERE CHECK_CATEGORY = 'SNOWFLAKE_DMF';")

        st.subheader("DMF jobs dashboard")

        dmf, metadata = st.tabs(["Manually Scheduled Metrics","Column Level Metadata Metrics"])

        with dmf:
            for index,job in dmf_jobs.iterrows():
                with st.expander(job["JOB_NAME"]):
                    job_id = job["JOB_ID"]
                    st.write(f'Created By: {job["CREATE_BY"]}')
                    st.write(f'Active: {str(job["IS_ACTIVE"])}')
                    results = sql_to_pandas(f"SELECT * FROM DATA_QUALITY.RESULTS.DQ_SNOWFLAKE_DMF_RESULTS WHERE JOB_ID = {job_id}")

                    specs = json.loads(job["JOB_SPECS"])

                    data_dict = {}

                    for index,spec in enumerate(specs["COLUMNS"]):
                        data_dict[spec["COLUMN"]] = {}
                        for check in specs["COLUMNS"][index]["CHECKS"]:
                            data_dict[spec["COLUMN"]][check] = []
                    
                    times = []
                    row_counts = []
                    for index, row in results.iterrows():
                        times.append(row["RUN_DATETIME"])
                        results_dict = json.loads(row["RESULTS"])
                        if "row_count" in results_dict:
                            row_counts.append(results_dict["row_count"])
                            del results_dict["row_count"]

                        for column, checks in results_dict.items():
                            for check_name,value in checks.items():
                                data_dict[column][check_name].append(value)
                            
                    if len(row_counts) > 0:
                        st.subheader("Row Count Over Time")
                        rc_json = {}
                        rc_json["TIME"] = times
                        rc_json["ROW_COUNT"] = row_counts
                        df = pd.DataFrame(rc_json)
                        st.line_chart(df, x="TIME", y="ROW_COUNT")
                        st.divider()

                    columns = data_dict.keys()
                    chosen_column = st.selectbox("Column",columns, key=f"{job_id}_column_selector")
                    if chosen_column:
                        checks = data_dict[chosen_column].keys()
                        display_check = st.selectbox("Check", checks, key=f"{job_id}_check_selector")
                        if display_check:
                            st.subheader(f"{display_check} of {chosen_column} over time")
                            df_json = {}
                            df_json["TIME"] = times
                            df_json[display_check] = data_dict[chosen_column][display_check]
                            df = pd.DataFrame(df_json)
                            st.line_chart(df, x="TIME", y=display_check)
        with metadata:
            met_jobs = sql_to_pandas("SELECT * FROM DATA_QUALITY.CONFIG.CONTROL_REPORT;")
            st.write(met_jobs)

            for index, met_job in met_jobs.iterrows():
                m_job_id = met_job["CONTROL_REPORT_ID"]
                active = met_job["ACTIVE_FLG"]
                with st.expander(f"{m_job_id} : Active = {active}"):
                    if "show_flag" + str(m_job_id) not in st.session_state:
                        st.session_state["show_flag" + str(m_job_id)] = False
                    st.button("Show more", key="show" + str(m_job_id) + str(index), on_click=toggle_button,args=("show_flag" + str(m_job_id),), type="primary")
                    if (st.session_state["show_flag" + str(m_job_id)]):
                        met_dict = sql_to_pandas(f"SELECT DISTINCT COLUMN_VALUE FROM DATA_QUALITY.RESULTS.CONTROL_REPORT_RESULT where CONTROL_REPORT_ID = '{m_job_id}'")
                        keys_dict = {}
                        for index,row in met_dict.iterrows():
                            row_json = json.loads(row["COLUMN_VALUE"])
                            for key, value in row_json.items():
                                if key not in keys_dict:
                                    keys_dict[key] = []
                                keys_dict[key].append(value)
                        column = st.selectbox("Select Your Column",keys_dict.keys())
                        value = st.selectbox("Select Your Value",keys_dict[column])
                        run_data = sql_to_pandas(f"""SELECT end_timestamp as time, COLUMN_CNT as count
                            FROM DATA_QUALITY.RESULTS.CONTROL_REPORT_RESULT as cr_res 
                            JOIN DATA_QUALITY.CONFIG.CONTROL_REPORT_RUN as cr_run on cr_res.control_report_run_id = cr_run.control_report_run_id 
                            where CONTROL_REPORT_ID = '{m_job_id}' 
                            and JSON_EXTRACT_PATH_TEXT(COLUMN_VALUE,'{column}') = '{value}'
                            ORDER BY time;""")
                        st.line_chart(run_data, x="TIME", y="COUNT")
                    st.write(met_job)

    def print_sidebar(self):
        super().print_sidebar()
