import time

# Python 3.8 type hints
from typing import Dict, Optional, Tuple, Union

import pandas as pd
import streamlit as st
from snowflake.snowpark import DataFrame
from src.app_utils import (
    MENU_ITEMS,
    render_sidebar,
    select_model,
)
from src.metrics import Metric, SQLResultsAccuracy
from src.snowflake_utils import (
    AUTO_EVAL_TABLE,
    SAVED_EVAL_TABLE,
    STAGE_NAME,
    run_async_sql_to_dataframe,
    save_eval_to_table,
)
from streamlit_extras.row import row


def get_result_title() -> str:
    """Returns the title for the results page.

    The title includes the evaluation name if it is available in session state.
    """
    
    if st.session_state.get("eval_funnel", "") == "new":
        return "Evaluation Results"
    else:
        if st.session_state.get("eval_name", None) is not None:
            return f"Evaluation Results: {st.session_state.get('eval_name', '')}"
        else:
            return "Evaluation Results"


TITLE = get_result_title()
if st.session_state.get("result_data", None) is not None:
    INSTRUCTIONS = """Evaluation results are shown below.
    Results can be reviewed and saved to a table.
    New evaluations can be saved for future use or automated."""
else:
    INSTRUCTIONS = "Please first select an evaluation from home."
if "eval_funnel" not in st.session_state:
    st.session_state["eval_funnel"] = None


st.set_page_config(
    page_title=TITLE,
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items=MENU_ITEMS,
)

# Resolves temporary web socket error in SiS for text input inside of dialog
st.config.set_option("global.minCachedMessageSize", 500 * 1e6)


# @st.cache_data
def make_downloadable_df():
    if st.session_state.get('include_analysis', False):
        analysis_results = run_full_result_analysis()
        df = pd.concat([st.session_state["result_data"],
                        pd.DataFrame(analysis_results)],
                                axis=1)
    else:
        df = st.session_state["result_data"]
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    # return df.to_csv().encode("utf-8")
    st.session_state['download_result'] = df#.to_csv().encode("utf-8")


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
        df = st.session_state.get("result_data", None)
        if df is not None:
            try:
                save_eval_to_table(df, table_name, st.session_state["session"])
                msg = "Evaluation results saved to table."
                st.success(msg)
                time.sleep(1.5)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")


@st.experimental_dialog("Add to Saved Evaluations")
def save_eval() -> None:
    """Render dialog box to save evaluation as stored procedure."""

    from src.metric_utils import register_saved_eval_sproc
    from src.snowflake_utils import insert_to_eval_table

    st.write("""Source data and metric configuration will be captured as a Snowflake Stored Procedure.
             Select the evaluation from Homepage's **Saved Evaluations** section to run.""")
    # App logic and saved evaluations must resides in same location so we hard-code these values.
    schema_context = {"database": STAGE_NAME.split(".")[0], "schema": STAGE_NAME.split(".")[1]}
    stage_name = STAGE_NAME.split(".")[-1]
    eval_name, eval_description = get_eval_name_desc()

    if st.button("Save"):
        metrics = st.session_state["metrics_in_results"]

        eval_metadata = {
            "EVAL_NAME": eval_name,
            "METRIC_NAMES": [metric.name for metric in metrics],
            "DESCRIPTION": eval_description,  # Not passed to object creation but just inserted into table
            "SOURCE_SQL": st.session_state["source_sql"],
            "MODELS": st.session_state["model_selection"],
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
                    models=eval_metadata["MODELS"],
                    param_assignments=eval_metadata["PARAM_ASSIGNMENTS"],
                )
                st.success(
                    "Evaluation registered complete. See On Demand Evaluations to run."
                )
            # Inserting metadata into table does not have its own try/except block as we 
            # only want to add to the table if everything above is successful
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

    from src.metric_utils import automate_eval_objects
    from src.snowflake_utils import insert_to_eval_table

    st.write("""Source data will be tracked and metric(s) calculated for new records.
            Results will be captured in a table.
            Select the evaluation from Homepage's **Automated Evaluations** section to view results.""")
    # App logic and saved evaluations must resides in same location so we hard-code these values.
    schema_context = {"database": STAGE_NAME.split(".")[0], "schema": STAGE_NAME.split(".")[1]}
    stage_name = STAGE_NAME.split(".")[-1]

    warehouse = st.selectbox(
        "Select Warehouse",
        st.session_state["warehouses"],  # Set prior to launching dialog box
        index=None,
        help="Select the warehouse to power the automation task.",
    )
    eval_name, eval_description = get_eval_name_desc()

    if st.button("Save"):
        metrics = st.session_state["metrics_in_results"]

        eval_metadata = {
            "EVAL_NAME": eval_name,
            "METRIC_NAMES": [metric.name for metric in metrics],
            "DESCRIPTION": eval_description,  # Not passed to object creation but just inserted into table
            "SOURCE_SQL": st.session_state["source_sql"],
            "MODELS": st.session_state["model_selection"],
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
                    models=eval_metadata["MODELS"],
                    source_sql=eval_metadata["SOURCE_SQL"],
                    param_assignments=eval_metadata["PARAM_ASSIGNMENTS"],
                )
                st.success(
                    """Evaluation automation complete. Results from current records may be delayed.
                    Select Automated Evaluations from the Homepage to view."""
                )
            # Inserting metadata into table does not have its own try/except block as we 
            # only want to add to the table if everything above is successful
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
        "Select a row using the far left checkboxes to analyze score results and get recommendations."
    )


def get_metric_cols(current_df: Union[DataFrame, pd.DataFrame]) -> list:
    """Returns list of columns in dataframe that contain metric values.
    
    Some metric names have spaces and Snowpark keeps them in lower case with double quotes.
    Metric names without spaces are capitalized when added to a Snowflake table/dataframe."""

    metric_names = [metric.get_column() for metric in st.session_state["metrics_in_results"]]
    df_columns = current_df.columns
    return [c_name for c_name in df_columns if c_name.upper() in (m_name.upper() for m_name in metric_names)]



def show_metric() -> None:
    """Renders metric KPIs based on selected metrics."""

    # User may navigate away from results and return. 
    # If so, we want to keep the previously viewed metrics and avoid error.
    # When user returns to home page, selected_metric set to empty list by default.
    if (len(st.session_state.get('selected_metrics', [])) > 0 and 
        st.session_state.get('metrics_in_results', []) != st.session_state.get('selected_metrics', [])):
        st.session_state['metrics_in_results'] = st.session_state['selected_metrics']
    
    # Stop page from rendering if user selects new metrics from homepage after viewing previous results
    if (len(st.session_state.get('selected_metrics', [])) > 0 and
        len(st.session_state.get('metrics_in_results', [])) > 0 and
        st.session_state.get('selected_metrics', []) != st.session_state.get('metrics_in_results', [])):
        st.error("""Oops! Looks like you have may have selected new metrics from the homepage. 
                 Please create a new evaluation or select an existing one from the homepage.""")
        st.stop()

    if st.session_state.get("result_data", None) is not None:
        df = st.session_state["result_data"]
        metric_names = [metric.get_column() for metric in st.session_state["metrics_in_results"]]
        kpi_row = row(6, vertical_align="top")
        # Placing entire dataframe in memory seems to be more stable than iterating over columns and averaging in snowpark
        # metric_values = df.select(*metric_names).to_pandas()
        metric_values = df[metric_names]

        for metric_name, metric_value in metric_values.mean().to_dict().items():
            kpi_row.metric(label=metric_name, value=round(metric_value, 2))

def reset_analysis():
    """Reset the analysis attribute in session state to None.
    
    This is necessary so clear out the AI review in the dialog if anything is changed."""

    st.session_state['analysis'] = None

def set_selected_row(selection_df: pd.DataFrame) -> None:
    """Callback function to capture the rows in selection_df with REVIEW == True.
    
    Executed when user selects Review Record."""

    if selection_df is not None:
        reset_analysis()
        first_metric = get_metric_cols(st.session_state.get("result_data", None))[0]
        selected_row = selection_df[selection_df['REVIEW'] == True]

        if selected_row is not None and len(selected_row) >= 1:
            selection = selected_row.to_dict(orient='records')
            st.session_state["selected_dict"] = selection
            st.session_state["selected_score"] = selection[0][first_metric]
        else:
            st.session_state["selected_dict"] = None
            st.session_state["selected_score"] = None


def set_score(selected_record: Dict[str, str]) -> None:
    """Callback function to capture the score in the selected row corresponding to the selected metric."""
    st.session_state["selected_score"] = selected_record[st.session_state['metric_selector']]
    reset_analysis()


def rerun_metric(prompt_inputs: Dict[str, str], metric: Metric) -> None:
    """Callback function to rerun the selected metric with revised required inputs."""

    response = metric.evaluate(model = st.session_state['review_model_selector'], **prompt_inputs)
    if response is not None:
        st.session_state["selected_score"] = response
        reset_analysis()


def analyze_result(prompt_inputs: Dict[str, str],
                   metric: Metric,
                   score: Optional[Union[float, int, bool]] = None,
                   model: Optional[str] = None,
                   return_response: bool = False) -> Union[None, str]:
    """Function to prompt Cortex LLM to review metric's prompt and requried inputs for explanation

    Args:
        prompt_inputs (dict[str, str]): Dictionary of prompt inputs for the metric's evaluation method.
        metric (Metric): Metric object to evaluate.
        score (str): Row's metric score
                     Optional. If None, uses session state selected_score.
        model (str): Model to use for evaluation. Default is None.
                     Optional. If None, uses session state review_model_selector.
        return_response (bool): Flag to return response or write to session state. Default is False.
        

    Returns:
        string or writes to session state
    """

    from src.prompts import Recommendation_prompt
    from src.snowflake_utils import get_connection, run_async_sql_complete

    # Re-establish session in session state as it seems to be lost at times in workflow
    if "session" not in st.session_state:
        st.session_state["session"] = get_connection()

    # When analyzing multiple rows, we don't store score in session state but pass it as an argument for each row
    if score is None:
        score = st.session_state["selected_score"]

    if model is None:
        model = st.session_state['review_model_selector']

    original_prompt = metric.get_prompt(**prompt_inputs)
    recommender_prompt = Recommendation_prompt.format(
        prompt=original_prompt, score=score
    )
    # response = run_complete(st.session_state["session"], model, recommender_prompt)
    response = run_async_sql_complete(st.session_state["session"], model, recommender_prompt)
    if response is not None and not return_response:
        st.session_state['analysis'] = response
    else:
        return response
    

def run_full_result_analysis():
    """Runner function to analyze every row's metric(s) responses with a recommendation prompt.
    
    Rows are analyzed in parallel to speed up the process."""

    import multiprocessing

    from joblib import Parallel, delayed

    metric_cols = get_metric_cols(st.session_state.get("result_data", None))
    metric_analysis = {} # Capture columnar analysis with metric name as key
    for metric_name in metric_cols:
        matching_metric = next(
            (
                metric
                for metric in st.session_state["all_metrics"]
                if metric.get_column() == metric_name.upper()
            ),
            None,
            )
        if matching_metric is not None:
            matching_metric.session = st.session_state["session"]
            # Associates metric param with column containing intended value for evaluation
            prompt_column_specs = st.session_state["param_selection"][matching_metric.name]
        
            responses = Parallel(n_jobs=multiprocessing.cpu_count(), backend="threading")(
                delayed(analyze_result_row)(
                    matching_metric, 
                    row[metric_name], 
                    {key: row[value] for key, value in prompt_column_specs.items()})
                for _, row in st.session_state["result_data"].iterrows()
            )

        metric_analysis[f'{metric_name}_ANALYSIS'] = responses
    return metric_analysis

def analyze_result_row(matching_metric: Metric, score: Union[float, int, bool], prompt_inputs: Dict[str, str]) -> Union[None, str]:
    """Analyzes a given row's metric score given metric evaluation and input arguments.

    Args:
        matching_metric (Metric): Metric object to evaluate.
        score (str): Row's metric score
        prompt_inputs (dict[str, str]): Dictionary of prompt inputs for the metric's evaluation method.
    
    Returns:
        string or writes to session state
    """

    # Use the default model of the metric class if available
    if matching_metric.model is not None:
        model_default = matching_metric.model
    else:
        model_default = "llama3.2-3b"
    return analyze_result(prompt_inputs, matching_metric, score, model_default, True)



@st.experimental_dialog("Download Results", width="small")
def download_dialog() -> None:
    st.write("Download the results to a CSV file. Select **Include Analysis** first to add an AI review of each record.")
    top_row = row(2, vertical_align="top")
    if top_row.button("🤖 Include Analysis",
                      use_container_width=True,
                      help="""Include AI analysis of each record and metric in download.
                              This may take a few minutes."""):
        with st.spinner("Analyzing results...this may take a few minutes."):
            analysis_results = run_full_result_analysis()
            data = pd.concat([st.session_state["result_data"],
                            pd.DataFrame(analysis_results)],
                                    axis=1)
        st.success("Analysis complete. Ready for download.")
    else:
        data = st.session_state["result_data"]

    top_row.download_button(
            label="⬇️ Download Results",
            data=data.to_csv().encode("utf-8"),
            file_name="evalanche_results.csv",
            mime="text/csv",
            use_container_width=True,
        )


def update_record(table_update_inputs: Dict[str, str], selected_metric_name: str, row_id: Union[int,str]) -> None:
    """Callback function to update result_data (display) in session state.
    
    Required input arguments and metric score are editable through this dialog.
    ROW_ID is used as unique identifier to match the record in the dataframe.
    We must first create a temp table to update the record in the dataframe.

    Args:
        table_update_inputs (dict[str,str]): Column name keys with updated values to replace in dataframe.
        selected_metric_name (str): Name of metric to update in dataframe.
    """
    
    # Proper update path once SiS support temp table creation
    # table_update_inputs[selected_metric_name] = st.session_state["selected_score"]
    # st.session_state["result_data"].write.mode("overwrite").save_as_table("tmp_tbl", table_type="temp")
    # temp_df = st.session_state['session'].table("tmp_tbl")
    # temp_df.update(table_update_inputs, temp_df["ROW_ID"] == st.session_state["selected_dict"][0]["ROW_ID"])
    # st.session_state["result_data"] = st.session_state['session'].table("tmp_tbl")

    # Temporary update method using purely pandas
    table_update_inputs[selected_metric_name] = st.session_state["selected_score"]
    # Create shallow copy of dataframe to update is necessary to avoid read-only error
    df = st.session_state["result_data"].copy()
    df.loc[df['ROW_ID'] == row_id, table_update_inputs.keys()] = table_update_inputs.values()
    st.session_state["result_data"] = df


def show_cortex_analyst_sql_results(metric: Metric, prompt_inputs: Dict[str, str]) -> None:
    """Displays data retrieved from SQL used in Cortex Analyst metrics.
    
    Shows results for generated_sql and expected_sql in the prompt_inputs dictionary.
    Only shows results if metric matches the name property of SQLResultsAccuracy.

    Args:
        metric (Metric): Column name keys with updated values to replace in dataframe.
        prompt_inputs (dict[str, str]): Dictionary of prompt inputs for the metric.
    """

    if type(metric).__name__ is (type(SQLResultsAccuracy()).__name__):
        with st.expander("Retrieved Data", expanded=False):
            st.caption("Results limited to 100 rows.")
            for key in ["generated_sql", "expected_sql"]:
                st.write(f"{key.upper()} Result")
                if key in prompt_inputs:
                    try:
                        inference_data = run_async_sql_to_dataframe(metric.session, prompt_inputs[key])
                        st.dataframe(inference_data,
                                    hide_index = True,)
                    except Exception as e:
                        st.write(f"Error: {e}")
                else:
                    st.write("No data returned")

@st.experimental_dialog("Review Record", width="large")
def review_record() -> None:
    """Render dialog box to review a metric result record."""

    st.write("Analyze and explore the selected record. Model selection will be used for analysis and metric rerunning. Updates can be saved to viewed results.")
    if st.session_state["selected_dict"] is None or len(st.session_state["selected_dict"]) == 0:
        st.write("Please select a record to review.")
    elif len(st.session_state["selected_dict"]) > 1:
        st.write("Please select only one record to review at a time.")
    else:
        # Only first record is selected for analysis
        selected_record = st.session_state["selected_dict"][0]
        metric_cols = get_metric_cols(st.session_state.get("result_data", None))

        metric_col, model_col = st.columns(2)
        with metric_col:
            selected_metric_name = st.selectbox(
                    "Select Metric", metric_cols, index=0, 
                    key="metric_selector", 
                    on_change=set_score,
                    args=(selected_record,)
                )
            if selected_metric_name is not None:     
                matching_metric = next(
                    (
                        metric
                        for metric in st.session_state["all_metrics"]
                        if metric.get_column() == selected_metric_name.upper()
                    ),
                    None,
                )
        with model_col:
            # Use the default model of the metric class if available
            if matching_metric is not None:
                if matching_metric.model is not None:
                    model_default = matching_metric.model
                else:
                    model_default = "llama3.2-3b"
            else:
                model_default = "llama3.2-3b"
            select_model('review', default = model_default)
    
        if matching_metric is not None:     
            # Re-add session attribute to metric object
            matching_metric.session = st.session_state["session"]
            
            prompt_inputs = {} # Captures value of f-strings for prompt
            table_update_inputs = {} # Captures table column values for metric evaluation
            for key, value in st.session_state["param_selection"][
                    matching_metric.name
                ].items():
                entered_value = st.text_area(value, 
                                             selected_record[value],
                                             key = value)

                prompt_inputs[key] = entered_value
                table_update_inputs[value] = entered_value
            metric_col, comment_col = st.columns((1, 4))
            with metric_col:
                st.metric(label=selected_metric_name, value=st.session_state['selected_score'])
            with comment_col:
                table_update_inputs['COMMENT'] = st.text_area("Comment", selected_record["COMMENT"])
        
        bottom_selection = row(4, vertical_align="top")
        bottom_selection.button("Analyze", disabled = selected_metric_name is None,
                                          use_container_width=True,
                                          on_click = analyze_result, args = (prompt_inputs, matching_metric))
        bottom_selection.button("Rerun", disabled = selected_metric_name is None,
                                        on_click = rerun_metric, args = (prompt_inputs, matching_metric),
                                        use_container_width=True,)
        save = bottom_selection.button("Save", disabled = selected_metric_name is None,
                                       use_container_width=True,
                                       help = "Save changes to record in current view.")
        
        # Unsaved changes in the dialog may linger if user navigates away and returns.
        # Here we provide a reset button to clear out any unsaved changes.
        reset = bottom_selection.button("Reset", disabled = selected_metric_name is None,
                                       use_container_width=True,
                                       help = "Reset all unsaved changed to selected record.")

        if st.session_state.get('analysis', None) is not None:
            st.write(f"**Analysis:** {st.session_state['analysis']}")

        # If evaluating SQL, show SQL results of current inputs
        show_cortex_analyst_sql_results(matching_metric, prompt_inputs)
  
        if save:
            update_record(table_update_inputs, 
                            selected_metric_name,
                            selected_record['ROW_ID'])
            st.rerun()
        if reset:
            st.rerun()


def show_dataframe_results() -> Optional[pd.DataFrame]:
    """
    Renders dataframe output with metrics calculated for each row.

    Dataframe is rendered in data_editor. Function returns data_editor result as a pandas dataframe.
    Function adds a ROW_ID column to the dataframe for self-joining of dataframes after running metric evaluations.

    Args:
        None

    Returns:
        pandas Dataframe 
    """
    
 
    if st.session_state.get('result_data', None) is not None:    
        df_selection = st.data_editor(
            st.session_state["result_data"],
            hide_index=True,
            disabled=[col for col in st.session_state["result_data"].columns if col != "REVIEW"],
            column_order=['REVIEW'] + [col for col in st.session_state["result_data"].columns if col not in ["REVIEW", 'ROW_ID']],
            use_container_width=True,
        )
        st.caption("Please note that edits made above will not be saved to raw evaluation outputs directly. To save, select Record Results.")
        

        return df_selection
    else:
        return None


def trend_avg_metrics() -> None:
    """Render line chart for average metrics by METRIC_DATETIME.

    METRIC_DATETIME is added for every saved evaluation run.
    """

    if (
        st.session_state.get("result_data", None) is not None
        and st.session_state.get("metrics_in_results", None) is not None
    ):
        metric_cols = get_metric_cols(st.session_state.get("result_data", None))

        df = st.session_state["result_data"].groupby('METRIC_DATETIME', as_index=False)[metric_cols].mean()
        
        # METRIC_DATETIME is batched for every run so there should be many rows per metric calculation set
        st.write("Average Metric Scores over Time")
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
        st.session_state.get("result_data", None) is not None
        and st.session_state.get("metrics_in_results", None) is not None
    ):
        metric_cols = get_metric_cols(st.session_state.get("result_data", None))

        df = st.session_state["result_data"]
        st.write("Metric Scores over Time")
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
        st.session_state.get("result_data", None) is not None
        and len(st.session_state.get("metrics_in_results", []))>0
    ):
        metric_cols = get_metric_cols(st.session_state.get("result_data", None))

        df = pd.melt(st.session_state["result_data"], 
                     value_vars=metric_cols, var_name = 'METRIC', value_name = 'SCORE')\
                        .groupby(['METRIC', 'SCORE']).size().reset_index(name='COUNT')
        st.write("Score Counts by Metric")
        st.bar_chart(df, x="SCORE", y="COUNT", color="METRIC")


def get_trendable_column() -> Union[None, str]:
    """Returns the True if 'METRIC_DATETIME' in result data.

    Used to ensure that the trendable column is presented which should always be METRIC_DATETIME.
    """

    if (
        st.session_state.get("result_data", None) is not None
        and st.session_state.get("metrics_in_results", None) is not None
    ):
        if 'METRIC_DATETIME' in st.session_state["result_data"].columns:
            return True
        else:
            return False


def chart_expander() -> None:
    """Renders chart area in an expander."""

    with st.expander("Chart Metrics", expanded=False):
        trendable_col = get_trendable_column()
        if trendable_col:
            bar_counts, line_trend, bar_trend = st.columns(3)
            with bar_counts:
                bar_chart_metrics()
            with line_trend:
                trend_avg_metrics()
            with bar_trend:
                trend_count_metrics()
        else:
            bar_chart_metrics()


st.title(TITLE)
st.write(INSTRUCTIONS)
render_sidebar()


def show_results():
    """Main rendering function for page."""

    from src.app_utils import fetch_warehouses

    show_metric()
    if st.session_state["eval_funnel"] is not None:
        top_row = row(5, vertical_align="top")
        selection_df = show_dataframe_results()
        recommend_inst = top_row.button(
            "🤖 Review Record",
            disabled=True
            if st.session_state.get("result_data", None) is None
            else False,
            use_container_width=True,
            help="Select a row to review.",
            on_click=set_selected_row,
            args=(selection_df,)
        )
        record_button = top_row.button(
            "📁 Record Results",
            disabled=True
            if st.session_state.get("result_data", None) is None
            else False,
            use_container_width=True,
            help="Record the results to a table.",
        )
        download_results = top_row.button(
            label="⬇️ Download Results",
            disabled=True
            if st.session_state.get("result_data", None) is None
            else False,
            use_container_width=True,
            help="Download results to csv.",
        )
        save_eval_button = top_row.button(
            "💾 Save Evaluation",
            disabled=st.session_state["eval_funnel"] == ("saved" or "automated"),
            use_container_width=True,
            help="Add the evaluation to your On Demand metrics.",
        )
        automate_button = top_row.button(
            "⌛ Automate Evaluation",
            disabled=False,
            use_container_width=True,
            help="Automate and record the evaluation for any new records.",
        )

        chart_expander()
        if record_button:
            record_evaluation()
        if download_results:
            download_dialog()
        if save_eval_button:
            save_eval()
        if automate_button:
            st.session_state["warehouses"] = fetch_warehouses()
            automate_eval()
        if recommend_inst:
            review_record()

show_results()
