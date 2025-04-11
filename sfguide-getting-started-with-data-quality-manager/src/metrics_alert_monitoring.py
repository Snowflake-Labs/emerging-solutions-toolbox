import streamlit as st
import pandas as pd
import altair as alt
import json
import time

from src.Page import Page
from src.tools import sql_to_dataframe,sql_to_pandas
from snowflake.snowpark.context import get_active_session

class table_metrics(Page):
    
    def __init__(self):
        self.name = "table_metrics"

    def print_page(self):



        session = st.session_state.session


        col_h1, col_h2, col_h3 = st.columns([8,1,1])
        col_h1.subheader('Data Metrics Monitoring')
        if col_h3.button('↻'):
                st.rerun()

        with st.expander("Raw Results"):
                dmf_results = session.sql("SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS").to_pandas()
                st.dataframe(dmf_results)

        t1,t2 = st.tabs(("Metrics Monitoring","Metric Scan Results"))
        with t1:
            col1, col2, col3 = st.columns([1,1,1])

            def LOAD_RECENT_METRICS():
                METRICS_QUERY = """
                    with LATEST_METRIC as (
                        select
                            METRIC_NAME as QUALITY_CHECK,
                            TABLE_SCHEMA,
                            TABLE_NAME,
                            ARGUMENT_NAMES,
                            max(MEASUREMENT_TIME) as LATEST_MEASUREMENT_TIME,
                            array_agg(VALUE) within group (order by MEASUREMENT_TIME) as VALUE_HISTORY
                        from
                            SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS 
                        where
                            timediff(day, MEASUREMENT_TIME, current_timestamp()) < 30
                        group by
                            TABLE_SCHEMA,
                            TABLE_NAME,
                            QUALITY_CHECK,
                            ARGUMENT_NAMES
                    )
                    select
                        case when D.METRIC_DATABASE = 'SNOWFLAKE' then concat('❄️ ', L.QUALITY_CHECK)
                                when D.METRIC_DATABASE != 'SNOWFLAKE' then concat('💼 ', L.QUALITY_CHECK)
                        end as DMF_NAME,
                        L.TABLE_SCHEMA||'.'||L.TABLE_NAME as TABLE_NAME,
                        L.ARGUMENT_NAMES,
                        case when D.VALUE > 0 then concat('🚨 ', D.VALUE)
                                when D.VALUE = 0 then concat('✅ ', D.VALUE)
                        end as ISSUES_FOUND,
                        L.VALUE_HISTORY,
                        concat(to_char(convert_timezone('Europe/Berlin', D.MEASUREMENT_TIME), 'YYYY-MM-DD at HH:MI:SS'),' (',(timediff(minute, D.MEASUREMENT_TIME, current_timestamp())),' minutes ago)') as LAST_MEASURED
                    from
                        LATEST_METRIC L
                    join
                        SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS  D
                        on  L.TABLE_NAME = D.TABLE_NAME
                        and L.TABLE_SCHEMA = D.TABLE_SCHEMA
                        and L.QUALITY_CHECK = D.METRIC_NAME
                        and L.LATEST_MEASUREMENT_TIME = D.MEASUREMENT_TIME
                    order by
                        LAST_MEASURED desc
                    """
                
                METRICS = session.sql(METRICS_QUERY).collect()
                METRICS_DF = pd.DataFrame(METRICS)
                
                return METRICS_DF


            # Load the DataFrame
            METRICS_DF = LOAD_RECENT_METRICS()

            # Function to add 'ALL' option to the unique values list
            def add_all_option(unique_values):
                return ['All'] + unique_values.tolist()


            # Create multiselect widgets for each column with 'ALL' option
            if len(METRICS_DF) > 0:
                dmf_name_options = add_all_option(METRICS_DF['DMF_NAME'].unique())
                dmf_table_options = add_all_option(METRICS_DF['TABLE_NAME'].unique())

            else:
                dmf_name_options = []
                dmf_table_options = []

            dmf_name_filter = col1.selectbox('DMF', options=dmf_name_options, placeholder='All')
            dmf_table_filter = col2.selectbox('Table', options=dmf_table_options, placeholder='All')
            issues_found_filter = col3.selectbox('Expectations', options= ['Failed', 'All'])


            def filter_dataframe(df, dmf_name_filter, dmf_table_filter, issues_found_filter):
                if len(df) > 0:
                    if dmf_name_filter != 'All':
                        df = df[df['DMF_NAME'] == dmf_name_filter]
                        
                    if dmf_table_filter != 'All':
                        df = df[df['TABLE_NAME'] == dmf_table_filter]
                        
                    if issues_found_filter == 'Failed':
                        df = df[df['ISSUES_FOUND'] != '✅ 0']
                    elif issues_found_filter == 'All':
                        df = df
                return df
                
            FILTERED_DF = filter_dataframe(METRICS_DF, dmf_name_filter, dmf_table_filter, issues_found_filter)


            st.subheader('') #just to add some space

            METRICS_COUNTER = session.sql("""
                with LATEST_METRIC as (
                        select
                            METRIC_NAME as QUALITY_CHECK,
                            TABLE_SCHEMA||'.'||TABLE_NAME as TABLE_NAME,
                            ARGUMENT_NAMES,
                            max(MEASUREMENT_TIME) as LATEST_MEASUREMENT_TIME
                        from
                            SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS 
                        where
                            timediff(day, MEASUREMENT_TIME, current_timestamp()) < 14
                        group by
                            TABLE_NAME,
                            TABLE_SCHEMA,
                            QUALITY_CHECK,
                            ARGUMENT_NAMES
                    ),
                    LATEST_VALUE as(
                        select
                            D.VALUE as VALUE,
                            L.TABLE_NAME
                        from
                            LATEST_METRIC L
                        join
                            SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS  D
                            on  L.TABLE_NAME = D.TABLE_SCHEMA||'.'||D.TABLE_NAME
                            and L.QUALITY_CHECK = D.METRIC_NAME
                            and L.LATEST_MEASUREMENT_TIME = D.MEASUREMENT_TIME
                    )
                select 
                    count(*) as ALL_METRICS,
                    (select count(*) from LATEST_VALUE where VALUE > 0) as FAILED_METRICS,
                    (select count(*) from LATEST_VALUE where VALUE = 0) as OKAY_METRICS,
                    (select count(distinct TABLE_NAME) from LATEST_VALUE where VALUE > 0) as FAILED_TABLES
                from
                    LATEST_VALUE
                """).collect()
            FAILED_METRICS = str(METRICS_COUNTER[0].FAILED_METRICS)
            OKAY_METRICS = str(METRICS_COUNTER[0].OKAY_METRICS)
            FAILED_TABLES = str(METRICS_COUNTER[0].FAILED_TABLES)

            col_m1, col_m2, col_m3 = st.columns([1,1,1])

            col_m1.metric("Failed expectations", '🚨 '+FAILED_METRICS)
            col_m2.metric("Tables failing expectations", '⌗ '+FAILED_TABLES)
            col_m3.metric("Met expectations", '✅ '+OKAY_METRICS)

            st.subheader('') #just to add some space


            ALL_METRICS_HISTOGRAM = session.sql("""
                select
                    count(distinct case when VALUE != 0 then METRIC_NAME || '|' || TABLE_ID || '|' || to_varchar(ARGUMENT_IDS) end) as FAILED_METRICS,
                    count(distinct case when VALUE  = 0 then METRIC_NAME || '|' || TABLE_ID || '|' || to_varchar(ARGUMENT_IDS) end) as SUCCEEDED_METRICS,
                    date_trunc(hour,MEASUREMENT_TIME) as HOUR
                from
                    SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS 
                where
                    timediff(day, MEASUREMENT_TIME, current_timestamp()) < 7
                group by
                    HOUR
                order by
                    HOUR desc
            """).to_pandas()
                
            MELTED_DF = ALL_METRICS_HISTOGRAM.melt('HOUR', var_name='RESULT', value_name='COUNTER')
                
            CHART = alt.Chart(MELTED_DF).mark_bar(size=5).encode(
                    x=alt.X('HOUR:T', axis=alt.Axis(title='Distinct Quality Checks per hour')), 
                    y=alt.Y('COUNTER:Q', axis=alt.Axis(title=None)), 
                    color=alt.Color('RESULT:N', legend=None,
                            scale=alt.Scale(domain=['FAILED_METRICS', 'SUCCEEDED_METRICS'], range=['#FF0000', '#008000']))
                    ).properties(height=240)

            st.altair_chart(CHART, use_container_width=True)



            st.dataframe(FILTERED_DF,
                        column_config={
                            "VALUE_HISTORY": st.column_config.AreaChartColumn("Checks (last 14 days)", y_min=0),
                            "ARGUMENT_NAMES": st.column_config.ListColumn("Columns")
                        },
                            hide_index= True, use_container_width=True)


            with st.expander('Show Metrics History'):
                METRICS_HISTORY = session.sql("""
                    select
                        to_char(convert_timezone('Europe/Berlin', MEASUREMENT_TIME), 'YYYY-MM-DD at HH:MI:SS') as MEASUREMENT_TIME,
                        case when METRIC_DATABASE = 'SNOWFLAKE' then concat('❄️ ', METRIC_NAME)
                                when METRIC_DATABASE != 'SNOWFLAKE' then concat('💼 ', METRIC_NAME)
                        end as METRIC_NAME,
                        VALUE,
                        TABLE_NAME,
                        ARGUMENT_NAMES,
                        TABLE_SCHEMA
                    from 
                        SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS 
                    order by 
                        MEASUREMENT_TIME desc 
                    limit 100
                    """).collect()
                st.dataframe(METRICS_HISTORY, hide_index= True, use_container_width=True)



            st.divider()

        with t2:

            allowed_metrics = [
            "NULL_COUNT",
            "NULL_PERCENT",
            "BLANK_COUNT",
            "BLANK_PERCENT",
            "DUPLICATE_COUNT"]

            def data_metric_scan(table,metric,column):
                return session.sql(f"""
                SELECT *
                  FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
                    REF_ENTITY_NAME  => '{table}',
                    METRIC_NAME  => 'snowflake.core.{metric}',
                    ARGUMENT_NAME => '{column}'
                  ));
                """).collect()



            tables = dmf_results["TABLE_NAME"].unique()
            chosen_table = st.selectbox("Select Table", tables)
            if chosen_table:
                active = True
                scheduled_metrics = dmf_results[dmf_results["TABLE_NAME"]==chosen_table]["METRIC_NAME"].unique()
                schema = dmf_results[dmf_results["TABLE_NAME"]==chosen_table]["TABLE_SCHEMA"].unique()[0]
                database = dmf_results[dmf_results["TABLE_NAME"]==chosen_table]["TABLE_DATABASE"].unique()[0]
                table_path = f'{database}.{schema}.{chosen_table}'
                scheduled_metrics = [metric for metric in scheduled_metrics if metric in allowed_metrics]
                
                chosen_metric = st.selectbox("Select Metric",scheduled_metrics)
                if chosen_metric:
                    metric_columns = dmf_results[(dmf_results["TABLE_NAME"]==chosen_table) & (dmf_results["METRIC_NAME"]==chosen_metric)]["ARGUMENT_NAMES"].unique()
                    metric_columns = metric_columns.tolist()
                    for index,metric in enumerate(metric_columns):
                        metric_columns[index] = json.loads(metric)
                    metric_columns = sum(metric_columns,[])
                    chosen_column = st.selectbox("Select Column",metric_columns)
                    if st.button("Run"):
                        offending_rows = data_metric_scan(table_path,chosen_metric,chosen_column)
                        if len(offending_rows) == 0:
                            st.success("No Violating Rows")
                        else:
                            st.error(f"{len(offending_rows)} Found")
                            st.dataframe(offending_rows)
    def print_sidebar(self):
        pass