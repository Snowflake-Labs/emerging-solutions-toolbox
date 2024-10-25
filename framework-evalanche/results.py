import time
import uuid

# Python 3.8 type hints
from typing import Tuple, Union

import pandas as pd
import streamlit as st
from snowflake.snowpark import DataFrame
from streamlit_extras.row import row

from src.app_utils import render_sidebar, select_schema_context
from src.metric_utils import AUTO_EVAL_TABLE, SAVED_EVAL_TABLE
from src.snowflake_utils import save_eval_to_table


def get_result_title() -> str:
    """Returns the title for the results page.

    The title includes the evaluation name if it is available in session state.
    """

    if st.session_state.get("eval_name", None) is not None:
        return f"Evaluation Results: {st.session_state.get('eval_name', '')}"
    else:
        return "Evaluation Results"


TITLE = get_result_title()
if st.session_state.get("metric_result_data", None) is not None:
    INSTRUCTIONS = """Metric evaluation results are shown below.
    You can record these results to a table, save the evaluation for future use, or automate the evaluation.
    Select the checkbox to the left of a row to generate recommendations."""
else:
    INSTRUCTIONS = "Please first select an evaluation from home."
if "eval_funnel" not in st.session_state:
    st.session_state["eval_funnel"] = None


st.set_page_config(
    page_title=TITLE,
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Resolves temporary web socket error in SiS for text input inside of dialog
st.config.set_option("global.minCachedMessageSize", 500 * 1e6)

# We will reset dataframe key to by resetting dataframe key whenever a new dialog boxes opens up.
# Otherwise, we receive error if 2 dialog boxes are opened.
if "dfk" not in st.session_state:
    st.session_state.dfk = str(uuid.uuid4())


def execute_cb() -> None:
    """
    Change the dataframe key.

    This is called when the button with execute label
    is clicked. The dataframe key is changed to unselect rows on it.
    """
    # print(sinfo)
    st.session_state.dfk = str(uuid.uuid4())


def replace_bool_col_with_str() -> DataFrame:
    """Change boolean metric to string so st.dataframe doesn't show checkboxes."""

    from snowflake.snowpark import functions as F
    from snowflake.snowpark.types import StringType

    metric_names = [metric.name for metric in st.session_state["selected_metrics"]]

    df = st.session_state["metric_result_data"]
    for name in metric_names:
        df = df.withColumn(
            name,
            F.iff(
                F.is_boolean(F.to_variant(name)),
                F.cast(F.col(name), StringType()),
                F.col(name),
            ),
        )
    return df


def get_eval_name_desc() -> Tuple[str, str]:
    """
    Returns template to give evaluation name and description.

    Both values are written to tables.

    Args:
        None

    Returns:
        tuple[str, str]

    """
    eval_name = st.text_input("Enter Evaluation Name (no spaces or special characters)")
    eval_description = st.text_input(
        "Enter Evaluation Description", placeholder="Optional", value=None
    )
    return eval_name, eval_description


@st.experimental_dialog("Record Evaluation Results")
def record_evaluation() -> None:
    """Render dialog box to record evaluation results to a table."""

    from src.app_utils import table_data_selector

    st.write("Record evaluation results to a table.")
    table_spec = table_data_selector("write_results", new_table=True)
    table_name = (
        f"{table_spec['database']}.{table_spec['schema']}.{table_spec['table']}"
    )
    if st.button("Save", disabled=True if table_spec is None else False):
        df = st.session_state.get("metric_result_data", None)
        if df is not None:
            try:
                save_eval_to_table(df, table_name)
                msg = "Evaluation results saved to table."
                st.success(msg)
                time.sleep(1.5)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")


@st.experimental_dialog("Add to Saved Evaluations")
def save_eval() -> None:
    """Render dialog box to save evaluation as stored procedure."""

    from src.app_utils import get_stages
    from src.metric_utils import register_saved_eval_sproc
    from src.snowflake_utils import insert_to_eval_table

    st.write("""Source data and metric configuration will be captured as a Snowflake Stored Procedure.
             Select the evaluation from Homepage's **Saved Evaluations** section to run.""")
    name = "save_eval"
    schema_context = select_schema_context(name, on_change=get_stages, args=(name,))
    if f"{name}_stages" not in st.session_state:
        st.session_state[f"{name}_stages"] = []
    stage_name = st.selectbox(
        "Select Stage",
        st.session_state[f"{name}_stages"],
        index=None,
    )
    eval_name, eval_description = get_eval_name_desc()

    if st.button("Save"):
        metrics = st.session_state["selected_metrics"]

        eval_metadata = {
            "EVAL_NAME": eval_name,
            "METRIC_NAMES": [metric.name for metric in metrics],
            "DESCRIPTION": eval_description,  # Not passed to object creation but just inserted into table
            "SOURCE_SQL": st.session_state["source_sql"],
            "PARAM_ASSIGNMENTS": st.session_state["param_selection"],
        }

        try:
            with st.spinner("Registering evaluation...this may take 1-2 minutes."):
                eval_metadata["ASSOCIATED_OBJECTS"] = register_saved_eval_sproc(
                    session=st.session_state["session"],
                    database=schema_context["database"],
                    schema=schema_context["schema"],
                    stage=stage_name,
                    eval_name=eval_metadata["EVAL_NAME"],
                    metrics=metrics,
                    source_sql=eval_metadata["SOURCE_SQL"],
                    param_assignments=eval_metadata["PARAM_ASSIGNMENTS"],
                )
                st.success(
                    "Evaluation registered complete. See On Demand Evaluations to run."
                )
        except Exception as e:
            st.error(f"Error: {e}")
        try:
            with st.spinner("Adding to On Demand Evaluations."):
                msg = insert_to_eval_table(
                    session=st.session_state["session"],
                    table_name=SAVED_EVAL_TABLE,
                    **eval_metadata,
                )
                st.success(msg)
                time.sleep(1.5)
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")


@st.experimental_dialog("Automate Evaluation")
def automate_eval() -> None:
    """Render dialog box to save evaluation as ongoing evaluation.

    Automatically evaluates new records as they are inserted into the source table.
    Process will create a view from source data.
    A stream will be created to capture inserts into the view.
    A stored procedure will be created to run metrics on new records triggered via task with stream.
    Results written to new final table.
    """

    from src.app_utils import get_stages
    from src.metric_utils import automate_eval_objects
    from src.snowflake_utils import insert_to_eval_table

    st.write("""Source data will be tracked and metric(s) calculated for new records.
            Results will be captured in a table.
            Select the evaluation from Homepage's **Automated Evaluations** section to view results.""")
    name = "auto_eval"
    schema_context = select_schema_context(name, on_change=get_stages, args=(name,))
    if f"{name}_stages" not in st.session_state:
        st.session_state[f"{name}_stages"] = []
    stage_name = st.selectbox(
        "Select Stage",
        st.session_state[f"{name}_stages"],
        index=None,
    )
    warehouse = st.selectbox(
        "Select Warehouse",
        st.session_state["warehouses"],  # Set prior to launching dialog box
        index=None,
    )
    warehouse = "WH_XS"
    eval_name, eval_description = get_eval_name_desc()

    if st.button("Save"):
        metrics = st.session_state["selected_metrics"]

        eval_metadata = {
            "EVAL_NAME": eval_name,
            "METRIC_NAMES": [metric.name for metric in metrics],
            "DESCRIPTION": eval_description,  # Not passed to object creation but just inserted into table
            "SOURCE_SQL": st.session_state["source_sql"],
            "PARAM_ASSIGNMENTS": st.session_state["param_selection"],
        }
        try:
            with st.spinner("Automating evaluation...this may take 1-2 minutes."):
                eval_metadata["ASSOCIATED_OBJECTS"] = automate_eval_objects(
                    session=st.session_state["session"],
                    database=schema_context["database"],
                    schema=schema_context["schema"],
                    stage=stage_name,
                    warehouse=warehouse,
                    eval_name=eval_metadata["EVAL_NAME"],
                    metrics=metrics,
                    source_sql=eval_metadata["SOURCE_SQL"],
                    param_assignments=eval_metadata["PARAM_ASSIGNMENTS"],
                )
                st.success(
                    """Evaluation automation complete. Results from current records may be delayed.
                    Select Automated Evaluations from the Homepage to view."""
                )
        except Exception as e:
            st.error(f"Error: {e}")
        try:
            with st.spinner("Adding to Automated Evaluations."):
                msg = insert_to_eval_table(
                    session=st.session_state["session"],
                    table_name=AUTO_EVAL_TABLE,
                    **eval_metadata,
                )
                st.success(msg)
                time.sleep(1.5)
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")


@st.experimental_dialog("Get AI Recommendations")
def give_recommendation_instruction() -> None:
    """Show instructions to get AI recommendations in dialog.

    The dataframe selection checkboxes are not obvious so we provide these explicit instructions.
    """
    st.write(
        "Select a row using the far left checkboxes to generate a recommendation based on the selected metric."
    )


def show_metric() -> None:
    """Renders metric KPIs based on selected metrics."""

    import snowflake.snowpark.functions as F

    if st.session_state.get("metric_result_data", None) is not None:
        df = st.session_state["metric_result_data"]
        metric_names = [metric.name for metric in st.session_state["selected_metrics"]]
        kpi_row = row(6, vertical_align="top")
        for metric_name in metric_names:
            metric_value = df.select(F.avg(F.to_variant(F.col(metric_name)))).collect()[
                0
            ][0]
            kpi_row.metric(label=metric_name, value=round(metric_value, 2))


def show_dataframe_results() -> Tuple[Union[int, None], Union[None, pd.DataFrame]]:
    """
    Renders dataframe output with metrics calculated for each row.

    Dataframe has a single-row selection. The selected row is used to generate AI recommendations.
    If selection is made, the row index and the pandas dataframe are returned.
    If not selection is made, None and the pandas dataframe are returned.
    If no dataframe is available, None and None are returned.

    Args:
        None

    Returns:
        int and pandas Dataframe if selection
        None and pandas Dataframe if no selection
        None and None otherwise

    """
    if st.session_state.get("metric_result_data", None) is not None:
        pandas_df = replace_bool_col_with_str().drop("ROW_ID").to_pandas()
        df_selection = st.dataframe(
            pandas_df,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            use_container_width=True,
            key=st.session_state[
                "dfk"
            ],  # Will be used to reset dataframe if we mean to de-select row.
            # Opening multiple dialog boxes will cause an error so we will reset this.
        )
        selected_row = df_selection["selection"]["rows"]
        if selected_row:
            return selected_row[0], pandas_df
        else:
            return None, pandas_df
    else:
        return None, None


@st.experimental_dialog("🤖 AI Recommendation", width="large")
def show_recommendation(selection: Union[int, None], pandas_df: pd.DataFrame) -> None:
    """Render dialog box to show AI recommendation based on selected row and metric."""

    from src.app_utils import fetch_metrics
    from src.prompts import Recommendation_prompt
    from src.snowflake_utils import get_connection, run_complete

    metrics = fetch_metrics()

    if (
        selection is not None
        and pandas_df is not None
        and st.session_state["selected_metrics"] is not None
    ):
        if "session" not in st.session_state:
            session = get_connection()
        else:
            session = st.session_state["session"]

        selected_row = pandas_df.iloc[selection].to_dict()
        metric_cols = [
            metric.name.upper() for metric in st.session_state["selected_metrics"]
        ]

        selected_metric_name = st.selectbox(
            "Select Metric", metric_cols, index=None, key="metric_selector"
        )
        if selected_metric_name is not None:
            # Get metric object that matches metric name
            matching_metric = next(
                (
                    metric
                    for metric in metrics
                    if metric.name.upper() == selected_metric_name
                ),
                None,
            )
            # Re-add session attribute to metric object
            matching_metric.session = session
            # Re-construct the original prompt from the selected row and param_selections previously saved to session state
            original_prompt_fstrings = {
                key: selected_row[value]
                for key, value in st.session_state["param_selection"][
                    matching_metric.name
                ].items()
            }
            original_prompt = matching_metric.get_prompt(**original_prompt_fstrings)
            recommender_prompt = Recommendation_prompt.format(
                prompt=original_prompt, score=selected_row[selected_metric_name]
            )

            with st.spinner("Thinking..."):
                response = run_complete(session, "llama3.1-8b", recommender_prompt)
                if response is not None:
                    st.write(response)


def trend_avg_metrics() -> None:
    """Render line chart for average metrics by METRIC_DATETIME.

    METRIC_DATETIME is added for every saved evaluation run.
    """

    from snowflake.snowpark.functions import avg, to_variant

    if (
        st.session_state.get("metric_result_data", None) is not None
        and st.session_state.get("selected_metrics", None) is not None
    ):
        metric_cols = [
            metric.name.upper() for metric in st.session_state["selected_metrics"]
        ]

        # We cast to variant in case the metric is a boolean
        # METRIC_DATETIME is batched for every run so there should be many rows per metric calculation set
        df = (
            st.session_state["metric_result_data"]
            .group_by("METRIC_DATETIME")
            .agg(*[avg(to_variant(col)).alias(col) for col in metric_cols])
        )
        st.line_chart(
            df,
            x="METRIC_DATETIME",
            y=metric_cols,
        )


def trend_count_metrics() -> None:
    """Render metric score counts by METRIC_DATETIME in a bar chart.

    METRIC_DATETIME is added for every saved evaluation run.
    """

    if (
        st.session_state.get("metric_result_data", None) is not None
        and st.session_state.get("selected_metrics", None) is not None
    ):
        metric_cols = [
            metric.name.upper() for metric in st.session_state["selected_metrics"]
        ]

        df = st.session_state["metric_result_data"]
        st.bar_chart(
            df,
            x="METRIC_DATETIME",
            y=metric_cols,
        )


def bar_chart_metrics() -> None:
    """Render counts by distinct metric score value in a bar chart.

    This is the default chart if no trendable column is found.
    """

    if (
        st.session_state.get("metric_result_data", None) is not None
        and st.session_state.get("selected_metrics", None) is not None
    ):
        metric_cols = [
            metric.name.upper() for metric in st.session_state["selected_metrics"]
        ]

        df = st.session_state["metric_result_data"]
        chart_df = (
            df.select(metric_cols)
            .unpivot("SCORE", "METRIC", metric_cols)
            .group_by("METRIC", "SCORE")
            .count()
        )

        st.bar_chart(chart_df, x="SCORE", y="COUNT", color="METRIC")


def get_trendable_column() -> Union[None, str]:
    """Returns the first date/timestamp column found in the metric result data.

    Used to ensure that a trendable column is presented which should always be METRIC_DATETIME.
    """

    if (
        st.session_state.get("metric_result_data", None) is not None
        and st.session_state.get("selected_metrics", None) is not None
    ):
        df = st.session_state["metric_result_data"]
        for datatype in df.dtypes:
            if datatype[-1] in ["date", "timestamp"]:
                return datatype[0]
        return None


def chart_expander() -> None:
    """Renders chart area in an expander."""

    with st.expander("Chart Metrics", expanded=False):
        trendable_col = get_trendable_column()
        if trendable_col is not None:
            line, bar = st.columns(2)
            with line:
                trend_avg_metrics()
            with bar:
                trend_count_metrics()
        else:
            bar_chart_metrics()


st.title(TITLE)
st.write(INSTRUCTIONS)
render_sidebar()


def show_results():
    """Main rendering function for page."""

    from src.app_utils import fetch_warehouses

    # Reset metric selector in recommendation if user closes dialog
    if "metric_selector" in st.session_state:
        st.session_state["metric_selector"] = None

    show_metric()
    if st.session_state["eval_funnel"] is not None:
        top_row = row(5, vertical_align="top")
        record_button = top_row.button(
            "Record Results",
            disabled=True
            if st.session_state.get("metric_result_data", None) is None
            else False,
            use_container_width=True,
            help="Record the results to a table.",
            on_click=execute_cb,  # Reset dataframe key to unselect rows.
        )
        save_eval_button = top_row.button(
            "Save Evaluation",
            disabled=st.session_state["eval_funnel"] == ("saved" or "automated"),
            use_container_width=True,
            help="Add the evaluation to your On Demand metrics.",
            on_click=execute_cb,
        )
        automate_button = top_row.button(
            "Automate Evaluation",
            disabled=False,
            use_container_width=True,
            help="Automate and record the evaluation for any new records.",
            on_click=execute_cb,
        )
        recommend_inst = top_row.button(
            "Get AI Recommendations",
            disabled=True
            if st.session_state.get("metric_result_data", None) is None
            else False,
            use_container_width=True,
            help="Select a row to generate a recommendation.",
            on_click=execute_cb,
        )
        row_selection, results_pandas = show_dataframe_results()
        if row_selection is not None:
            show_recommendation(row_selection, results_pandas)
        chart_expander()
        if record_button:
            record_evaluation()
        if save_eval_button:
            save_eval()
        if automate_button:
            st.session_state["warehouses"] = fetch_warehouses()
            automate_eval()
        if recommend_inst:
            give_recommendation_instruction()


show_results()
