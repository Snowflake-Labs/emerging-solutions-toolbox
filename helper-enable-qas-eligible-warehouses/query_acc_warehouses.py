# Import python packages
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")
st.image(
    "https://raw.githubusercontent.com/Snowflake-Labs/emerging-solutions-toolbox/main/banner_emerging_solutions_toolbox.png"
)

st.subheader("Enable Query Acceleration Service for Warehouses with Eligible Queries")
st.markdown(
    """    
    The query acceleration service (QAS) can accelerate parts of the query workload in a warehouse. When it is enabled for a warehouse, it can improve overall warehouse performance by reducing the impact of outlier queries, which are queries that use more resources than the typical query. The query acceleration service does this by offloading portions of the query processing work to shared compute resources that are provided by the service.
    
    For more information, visit:  https://docs.snowflake.com/en/user-guide/query-acceleration-service#label-query-acceleration-eligible-queries.
    
    This notebook identifies warehouses that execute queries that are eligible for QAS, along with the option to enable QAS for each warehouse. This notebook will:
    - check the `QUERY_ACCELERATION_ELIGIBLE` account usage view for warehouses that execute queries that are eligible for QAS.
        - The user can toggle the minimum number of eligible queries to check for, along with the threshold of average execution time is eligible for the service
    - enable QAS for each selected warehouse (optional)
    - allow the user to execute an eligible query on a warehouse once QAS is enabled (optional)"""
)
st.subheader("Prerequisites")
st.markdown(
    """
    - The user executing this notebook, must have access to the `SNOWFLAKE` database.
    - The user's role must either be the warehouse owner, or have the `MANAGE WAREHOUSE` account-level privilge to enable QAS for a selected warehouse."""
)
# Write directly to the app
session = get_active_session()


def paginate_data(df):
    pagination = st.empty()
    batch_size = 20  # Set the number of items per page

    if len(df) > 0:
        bottom_menu = st.columns((4, 1, 1))
        with bottom_menu[2]:
            total_pages = (
                int(len(df) / batch_size) if int(len(df) / batch_size) > 0 else 1
            )
            current_page = st.number_input(
                "Page", min_value=1, max_value=total_pages, step=1
            )
        with bottom_menu[0]:
            st.markdown(f"Page **{current_page}** of **{total_pages}** ")

        pages = split_frame(df, batch_size)
        # dynamically set df height, based on number of rows in data frame
        pagination.dataframe(
            data=pages[current_page - 1],
            height=int(((len(df) / batch_size) + 1.5) * 60 + 3.5),
            use_container_width=True,
        )
    else:
        st.caption("No results to display.")


@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df


@st.cache_data(show_spinner=False)
def query_cache(_session, input_stmt):
    df = pd.DataFrame(_session.sql(input_stmt).collect())
    return df


st.divider()
st.subheader(
    "STEP 1: Show warehouses that have queries eligible for query acceleration"
)
st.write(
    "This step shows the warehouses that have queries that are eligible for query acceleration, based on the criteria set by the user below."
)
# select date range
qa_eligible_date_range_list = [
    "Choose a Date Range",
    "Last day",
    "Last 7 days",
    "Last 28 days",
    "Last 3 months",
    "Last 6 months",
    "Last 12 months",
]
st.selectbox(
    "Select Date Range:", qa_eligible_date_range_list, key="sb_qa_eligible_range"
)

date_time_part = ""
increment = ""
df_query_history_range = None
disable_get_eligible_whs_flag = True
list_qas_eligible_whs = ["Choose Warehouse"]
df_qas_eligible_whs_clmns = [
    "Warehouse Name",
    "# of Eligible Queries",
    "Average Duration (sec)",
    "Average % of Query Available for QA",
]

if st.session_state.sb_qa_eligible_range == "Last day":
    date_time_part = "hours"
    increment = "24"
elif st.session_state.sb_qa_eligible_range == "Last 7 days":
    date_time_part = "days"
    increment = "7"
elif st.session_state.sb_qa_eligible_range == "Last 28 days":
    date_time_part = "days"
    increment = "28"
elif st.session_state.sb_qa_eligible_range == "Last 3 months":
    date_time_part = "months"
    increment = "3"
elif st.session_state.sb_qa_eligible_range == "Last 6 months":
    date_time_part = "months"
    increment = "6"
elif st.session_state.sb_qa_eligible_range == "Last 12 months":
    date_time_part = "months"
    increment = "12"

# set minimum number of eligible queries
st.number_input(
    "Minimum # of Eligible Queries",
    min_value=1,
    value=1,
    step=1,
    key="num_min_qas_queries",
    help="The minimum number of QAS-eligible queries for each warehouse",
)

# select minimum ratio of eligible query acceleration time to total query duration
qa_eligible_time_pct_list = ["Choose a Percentage", "10", "25", "50", "75"]
st.selectbox(
    "Minimum % of Query Eligible for Query Acceleration:",
    qa_eligible_time_pct_list,
    key="sb_qa_eligible_time_pct",
    help="The minimum percentage of the amount of the query's execution time that is eligible for QAS",
)

if (st.session_state.sb_qa_eligible_range != "Choose a Date Range") and (
    st.session_state.sb_qa_eligible_time_pct != "Choose a Percentage"
):
    disable_get_eligible_whs_flag = False

st.button(
    "Get Eligible Warehouses",
    disabled=disable_get_eligible_whs_flag,
    type="primary",
    key="btn_get_eligible_whs",
)

if st.session_state.btn_get_eligible_whs:
    # create a dataframe from eligible warehouses
    df_qas_eligible_whs = pd.DataFrame(
        session.sql(
            f"""SELECT 
                                                            warehouse_name
                                                            ,COUNT(query_id) AS num_eligible_queries
                                                            ,AVG(DATEDIFF(second, start_time, end_time))::number(38,3) AS avg_duration_sec
                                                            ,AVG(eligible_query_acceleration_time / NULLIF(DATEDIFF(second, start_time, end_time), 0))::number(38,3) * 100 as avg_eligible_time_pct
                                                        FROM 
                                                            SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
                                                        WHERE
                                                            end_time > DATEADD({date_time_part}, -{increment}, CURRENT_TIMESTAMP())
                                                        GROUP BY 
                                                            warehouse_name
                                                        HAVING
                                                            num_eligible_queries >= {st.session_state.num_min_qas_queries}
                                                            AND avg_eligible_time_pct >= {st.session_state.sb_qa_eligible_time_pct}
                                                        ORDER BY 
                                                            num_eligible_queries DESC"""
        ).collect(),
        columns=df_qas_eligible_whs_clmns,
    )

    list_qas_eligible_whs.extend(df_qas_eligible_whs["Warehouse Name"].tolist())
    st.session_state.list_qas_eligible_whs = list_qas_eligible_whs

    st.divider()
    st.subheader("Eligible Warehouses:")
    paginate_data(df_qas_eligible_whs)

st.divider()
st.subheader("STEP 2: Inspect QAS-eligible queries (optional).")
st.write(
    "This step allows the user to inspect QAS-eligible queries for the selected warehouse."
)
if "list_qas_eligible_whs" in st.session_state:
    list_qas_eligible_whs = st.session_state.list_qas_eligible_whs

disable_get_eligible_queries_flag = True

# select eligible warehouse
sb_qas_eligible_wh = st.selectbox(
    "Select Warehouse:", list_qas_eligible_whs, key="sb_qas_eligible_wh"
)

if st.session_state.sb_qas_eligible_wh != "Choose Warehouse":
    disable_get_eligible_queries_flag = False

st.button(
    "Get Eligible Queries",
    disabled=disable_get_eligible_queries_flag,
    type="primary",
    key="btn_get_eligible_queries",
)

if st.session_state.btn_get_eligible_queries:
    # create a dataframe for eligible queries
    df_qas_eligible_queries_clmns = [
        "Query ID",
        "Query Text",
        "Warehouse Name",
        "Warehouse Size",
        "Start Time",
        "End Time",
        "Eligible QA Time (sec)",
        "Upper Limit Scale Factor",
        "Total Duration (sec)",
        "% of Query Available for QA",
    ]

    df_qas_eligible_queries = pd.DataFrame(
        session.sql(
            f"""SELECT 
                                                            query_id
                                                            ,query_text
                                                            ,warehouse_name
                                                            ,warehouse_size
                                                            ,start_time
                                                            ,end_time
                                                            ,eligible_query_acceleration_time
                                                            ,upper_limit_scale_factor
                                                            ,DATEDIFF(second, start_time, end_time) AS total_duration
                                                            ,(eligible_query_acceleration_time / NULLIF(DATEDIFF(second, start_time, end_time), 0))::number(38,3) * 100 AS eligible_time_pct
                                                        FROM
                                                            SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
                                                        WHERE
                                                            LOWER(warehouse_name) = '{st.session_state.sb_qas_eligible_wh.lower()}'
                                                            AND end_time > DATEADD({date_time_part}, -{increment}, CURRENT_TIMESTAMP())
                                                        ORDER BY (eligible_time_pct, upper_limit_scale_factor) DESC NULLS LAST
                                                        LIMIT 100
                                                        ;"""
        ).collect(),
        columns=df_qas_eligible_queries_clmns,
    )

    st.divider()
    st.subheader(
        f"Eligible Queries for Warehouse: {st.session_state.sb_qas_eligible_wh.upper()}"
    )
    paginate_data(df_qas_eligible_queries)

st.divider()
st.subheader("STEP 3: Enable QAS")
st.write("This step enables QAS for the selected warehouses.")
disable_enable_qas = True

# select eligible warehouses to enable QAS
st.multiselect("Select Warehouse:", list_qas_eligible_whs, key="ms_qas_eligible_whs")

if st.session_state.ms_qas_eligible_whs:
    disable_enable_qas = False

st.button(
    "Enable QAS", disabled=disable_enable_qas, type="primary", key="btn_enable_qas"
)

if st.session_state.btn_enable_qas:
    for wh in st.session_state.ms_qas_eligible_whs:
        # enable QAS for each warehouse selected
        session.sql(
            f"""ALTER WAREHOUSE {wh} SET ENABLE_QUERY_ACCELERATION = TRUE"""
        ).collect()
        session.sql(
            f"""ALTER WAREHOUSE {wh} SET COMMENT = '{{"origin":"sf_sit","name":"qas_eligible_warehouses","version":{{"major":1, "minor":0}},"attributes":"session_tag"}}'"""
        ).collect()

        st.success(f"QAS enabled for warehouse: {wh.upper()} 🎉")

st.divider()

st.subheader("STEP 4: Execute query on QAS-enabled warehouse (optional)")
st.markdown(
    """
    This step executes a query on the warehouse that has QAS enabled. Perform the following steps:
    
    1. Select one of the warehouses that was enabled in **Step 3**.
    1. Based on the selected warehouse, copy one of the queries from **Step 2** and paste it in the text area below.
    1. Click `Run Query` and notice the improved query performance.
"""
)
current_wh = None
list_qas_enabled_whs = ["Choose Warehouse"]
disable_query_text_area_flag = True
disable_run_query_flag = True
# last_query_id = ""

if "df_run_query" not in st.session_state:
    st.session_state.df_run_query = None

if "last_query_id" not in st.session_state:
    st.session_state.df_run_query = None

df_current_wh = pd.DataFrame(session.sql("""SELECT CURRENT_WAREHOUSE();""").collect())

if not df_current_wh.empty:
    current_wh = df_current_wh.iloc[0, 0]

# get QAS-enabled warehouses
session.sql("""SHOW WAREHOUSES;""").collect()
df_qas_whs = pd.DataFrame(
    session.sql(
        """SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "enable_query_acceleration" = TRUE;"""
    ).collect()
)
list_qas_enabled_whs.extend(df_qas_whs["name"].tolist())

# select QAS-enabled warehouse
st.selectbox("Select Warehouse:", list_qas_enabled_whs, key="sb_qas_enabled_wh")

if st.session_state.sb_qas_enabled_wh != "Choose Warehouse":
    disable_query_text_area_flag = False
    # switch to the QAS-enabled warehouse
    session.sql(f"""USE WAREHOUSE {st.session_state.sb_qas_enabled_wh}""").collect()

st.text_area(
    "Paste Query", key="ta_qas_query", height=300, disabled=disable_query_text_area_flag
)

if st.session_state.ta_qas_query != "":
    disable_run_query_flag = False

btn_run_query = st.button("Run Query", disabled=disable_run_query_flag, type="primary")

if btn_run_query:
    st.session_state.df_run_query = query_cache(session, st.session_state.ta_qas_query)
    st.session_state.last_query_id = pd.DataFrame(
        session.sql("""SELECT LAST_QUERY_ID()""").collect()
    ).iloc[0, 0]

if st.session_state.df_run_query is not None:
    st.divider()
    st.subheader("Query Results")
    paginate_data(st.session_state.df_run_query)
    st.divider()
    st.success(
        f"For more details, check Query History, using Query ID: {st.session_state.last_query_id}"
    )
