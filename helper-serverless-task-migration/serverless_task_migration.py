# Import python packages
import streamlit as st
import pandas as pd
import time
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")
# Write directly to the app
st.image(
    "https://raw.githubusercontent.com/Snowflake-Labs/emerging-solutions-toolbox/main/banner_emerging_solutions_toolbox.png"
)
st.title("Serverless Tasks Migration")
# Get the current credentials
session = get_active_session()
css = """  
<style>  
    /*USES THE HAS SELECTOR TO FILTER BY THE NESTED CONTAINER*/  
div[role="dialog"]:has(.st-key-migrate_container){  
    width:85%;  
}  
</style>"""
st.html(css)


@st.dialog("Migrate Task")
def migrate_task(task_name, wh_name, wh_size):
    with st.container(key="migrate_container"):
        st.write(f"This Migration will remove task {task_name}")
        st.write(
            f"from warehouse {wh_name}, and make it serverless with the below setting"
        )
        wh_size_list = [
            "XSMALL",
            "SMALL",
            "MEDIUM",
            "LARGE",
            "XLARGE",
            "XXLARGE",
            "XXXLARGE",
        ]
        wh_size = st.selectbox(
            "Initial Warehouse Size",
            wh_size_list,
            index=wh_size_list.index(wh_size.replace("-", "").upper()),
        )
        __, col2 = st.columns((10, 2))
        if col2.button("Migrate", type="primary"):
            session.sql(f"ALTER TASK {task_name} SUSPEND").collect()
            session.sql(f"ALTER TASK {task_name} UNSET WAREHOUSE").collect()
            session.sql(
                f"ALTER TASK {task_name} SET USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL';"
            ).collect()
            session.sql(
                f"""ALTER TASK {task_name} SET COMMENT = '{{"origin": "sf_sit","name": "sit_serveless_task_migration","version": "{{major: 1, minor: 0}}"}}'"""
            ).collect()
            session.sql(f"ALTER TASK {task_name} RESUME").collect()
            st.success(f"{task_name} Migrated!")
            time.sleep(5)
            st.rerun()


@st.cache_data
def get_warehouses(wh_names):
    return session.sql(
        f"""  
    SELECT DISTINCT WAREHOUSE_NAME, CONCAT(t.DATABASE_NAME,'.',t.SCHEMA_NAME,'.', t.NAME) as FULL_NAME, t.name FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY t  
  JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q ON t.QUERY_ID = q.QUERY_ID  
  RIGHT JOIN SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY w ON q.warehouse_name = w.name  
  WHERE   
  QUERY_START_TIME BETWEEN w.START_TIME AND w.END_TIME  
  AND NOT CONTAINS(WAREHOUSE_NAME,'COMPUTE_SERVICE')   
  AND SCHEDULED_TIME BETWEEN dateadd(DAY, - 14, CURRENT_TIMESTAMP()) AND CURRENT_TIMESTAMP()  
  AND w.name in ({wh_names});  
    """
    ).to_pandas()


if "qual_tasks" not in st.session_state:
    with st.spinner("Fetching Tasks"):
        st.session_state.qual_tasks = session.sql(
            """SELECT DISTINCT CONCAT(t.DATABASE_NAME,'.',t.SCHEMA_NAME,'.', NAME) as FULL_NAME,NAME, t.QUERY_TEXT, WAREHOUSE_NAME, WAREHOUSE_SIZE, iff(DATEDIFF("MINUTE",QUERY_START_TIME, COMPLETED_TIME) < 1, 'TRUE','FALSE') as SHORT_RUNTIME, iff(DATEDIFF("MINUTE",QUERY_START_TIME, SCHEDULED_TIME) > 1, 'TRUE','FALSE') as RAN_OVER FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY t  
          JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q ON t.QUERY_ID = q.QUERY_ID  
          WHERE   
          (DATEDIFF("MINUTE",QUERY_START_TIME, COMPLETED_TIME) < 1  
          OR  
          DATEDIFF("MINUTE",QUERY_START_TIME, SCHEDULED_TIME) > 1)  
          AND NOT CONTAINS(WAREHOUSE_NAME,'COMPUTE_SERVICE')   
          AND SCHEDULED_TIME BETWEEN dateadd(DAY, - 14, CURRENT_TIMESTAMP()) AND CURRENT_TIMESTAMP();"""
        ).to_pandas()
qual_tasks = st.session_state.qual_tasks
with st.container(border=True):
    st.write(
        """The tasks below are the first glance of tasks that may qualify for migration to serverless tasks \n  
    They either run for under a minute, or run longer than the time between their scheduled run and the next run  
    """
    )
    st.dataframe(
        qual_tasks, use_container_width=True, column_config={"FULL_NAME": None}
    )
with st.container(border=True):
    st.write(
        """The Tasks that run long in the previous list are great candidates for serverless warehouses as they can have a target finish set and the warehouse will scale for the schedule"""
    )
    st.write(
        """For short running tasks, to further qualify them we'll check how many tasks generally run on the same warehouse"""
    )
with st.container(border=True):
    st.write(
        """The below list shows the number of tasks that all run around the same time on each warehouse"""
    )
    warehouses = set(qual_tasks["WAREHOUSE_NAME"].tolist())
    warehouses_l = "'{}'".format("', '".join(warehouses))
    warehouse_list = get_warehouses(warehouses_l)
    st.dataframe(warehouse_list, use_container_width=True)
    st.write(
        "For any tasks that run for less than a minute on a warehouse alone should be migrated to a serverless warehouse"
    )
    st.write(
        "Additionally any tasks scheduled with a total runtime of less than a few minutes would likely each benefit from being serverless"
    )
    st.write(
        "Review each warehouse below and migrate any tasks that are recommended or that you would like to"
    )

    for wh_name in warehouses:
        wh_tasks = warehouse_list[warehouse_list["WAREHOUSE_NAME"] == wh_name][
            "NAME"
        ].tolist()
        wh_size = qual_tasks[qual_tasks["WAREHOUSE_NAME"] == wh_name][
            "WAREHOUSE_SIZE"
        ].values[0]
        with st.expander(wh_name):
            st.write("###")
            col1, __, col2 = st.columns(3)
            col1.subheader("*Task Name*")
            for wh_task in wh_tasks:
                full_task_name = warehouse_list[warehouse_list["NAME"] == wh_task][
                    "FULL_NAME"
                ].values[0]
                describe_task = pd.DataFrame(
                    session.sql(f"DESCRIBE TASK {full_task_name}").collect()
                )
                if describe_task["warehouse"].values[0]:
                    st.divider()
                    col1, __, col2 = st.columns(3)
                    col1.write(wh_task)
                    col2.button(
                        "Migrate",
                        on_click=migrate_task,
                        args=(full_task_name, wh_name, wh_size),
                        key=wh_task + "migrate_button",
                    )
