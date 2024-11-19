# Python 3.8 type hints
from typing import Any, Dict, List, Optional
import time

import streamlit as st
from streamlit_extras.grid import grid
from streamlit_extras.row import row
from streamlit_extras.stylable_container import stylable_container

from src.app_utils import (
    css_yaml_editor,
    fetch_evals,
    fetch_metric_display,
    fetch_metrics,
    get_metric_preview,
    render_sidebar,
    set_session_var_to_none,
    MENU_ITEMS,
    ABOUT,
)
from src.snowflake_utils import (
    AUTO_EVAL_TABLE,
    SAVED_EVAL_TABLE,
    STAGE_NAME,
    CUSTOM_METRIC_TABLE,
)
from src.snowflake_utils import (
    call_sproc,
    get_connection,
)

TITLE = "⛰️Evalanche: GenAI Evaluation"
INSTRUCTIONS = """
Welcome to the Evalanche dashboard!
Here you can create, run, and view GenAI evaluations.

To get started, select a metric in New Evaluations and follow the prompts to evaluate existing GenAI outputs.
If you already have saved or auomated evaluations, you can run or view them from below.
Select **Help** to learn more.
"""

st.set_page_config(
    page_title=TITLE,
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items=MENU_ITEMS,
     )

@st.experimental_dialog("About", width="large")
def show_about():
    st.write(ABOUT)

if "session" not in st.session_state:
    st.session_state["session"] = get_connection()

st.title(TITLE)
st.write(INSTRUCTIONS)
render_sidebar()


@st.experimental_dialog("Create New Metric",
                        width="large")
def add_new_metric():
    from src.app_utils import (
        select_model,
        vars_entry,
        upload_staged_pickle,
        add_metric_to_table,
    )
    from src.metric_utils import (
        create_custom_metric,
        DEFAULT_CUSTOM_METRIC_DESC,
        DEFAULT_CUSTOM_METRIC_NAME,
        DEFAULT_CUSTOM_METRIC_PROMPT,
    )
    from src.metrics import provided_metrics
    
    st.write("""Want to create your own LLM-as-a-Judge metric? Let's get started!
             Please provide the below required information and we'll do the heavy lifting for you.
             We've added some example prompts to get you started.""")
    
    metric_name = st.text_input("Metric Name", value=DEFAULT_CUSTOM_METRIC_NAME)
    metric_description = st.text_area("Metric Description",
                                      value=DEFAULT_CUSTOM_METRIC_DESC.strip())
    model = select_model('custom_metric_model',
                         default = "llama3.1-8b")
    st.caption("""Variables should be enclosed in brackets { } like f-strings. 
               For example, '{question}'. These variables will be filled with column values.
               We suggest prompts that return an integer score, such as 1 - 5.
               True/False results should be returned as 1 or 0.""")
    metric_prompt = st.text_area("LLM-as-a-Judge Prompt",
                                 value= DEFAULT_CUSTOM_METRIC_PROMPT,
                                 height = 200)
    
    if metric_prompt:
        metric_required = vars_entry(metric_prompt)
    else:
        st.write("No variables found.")
    
    if st.button("Create Metric"):
        new_metric = create_custom_metric(metric_name, metric_description, metric_prompt, metric_required, model)
        if new_metric.name in [metric.name for metric in provided_metrics]:
            st.error("Metric name cannot match provided metrics.")
            st.stop()
        # Pickled file name should match the class name for convenience
        file_name = type(new_metric).__name__ + ".pkl"
        upload_staged_pickle(st.session_state["session"], new_metric, file_name, STAGE_NAME)
        
        add_metric_to_table(st.session_state["session"],
                            new_metric.name,
                            f"@{STAGE_NAME}/{file_name}", # We want to track back to full file path
                            CUSTOM_METRIC_TABLE)
        st.success("New metric created successfully!")
        time.sleep(1.5)
        st.rerun()


@st.experimental_dialog("Manage Metrics", width="large")
def manage_metric_dialog():
    
    # Going straight from snowpark df into data_editor causes issues with rendering home in st. dialog
    # Instead we go directly to pandas and then back to table
    schema = st.session_state['session'].table(CUSTOM_METRIC_TABLE).schema
    current_table = st.session_state['session'].table(CUSTOM_METRIC_TABLE).to_pandas()
    if current_table.shape[0] == 0:
        st.write("No custom metrics available.")
    else:
        st.write("Below are the custom metrics currently available. Uncheck to hide a custom metric on the account.")
        new_table = st.data_editor(current_table,
                       key = "custom_metrics_table",
                       use_container_width=True,
                       hide_index=True,
                       column_order= ["SHOW_METRIC"] + [col for col in current_table.columns if col != "SHOW_METRIC"],
                       column_config= {"SHOW_METRIC": st.column_config.CheckboxColumn(
                                        "SHOW",
                                        help="Checked metrics will be displayed as available to users.",
                                    )},
                       disabled = [col for col in current_table.columns if col != "SHOW_METRIC"],
                       )
        if st.button("Save"):
            with st.spinner("Saving changes..."):
                time.sleep(3)
                new_df = st.session_state['session'].create_dataframe(new_table,
                                                                  schema = schema)
                _ = new_df.write.save_as_table(table_name = CUSTOM_METRIC_TABLE.split("."),
                                            mode = "overwrite",
                                            column_order = "name",)
                st.success("Changes saved successfully.")
            # A fetch_metrics will be called in the next rerun so we don't need to add it here
            st.rerun()
                
            

def delete_evaluation(evaluation: Dict[str, Any], eval_tablename: str) -> None:
    """
    Deletes evaluation from eval_tablename and ASSOCIATED_OBJECTS.

    Args:
        evaluation (dict): Evaluation metadata.
        eval_tablename (str): Name of table where evaluation is stored.

    Returns:
        None

    """
    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    with st.spinner("Removing evaluation..."):
        for object_type, object_name in evaluation["ASSOCIATED_OBJECTS"].items():
            # Stored procedures need to be called with arg types
            # Our evaluations have no arguments no empty () is appended
            try:
                if object_type == "PROCEDURE":
                    object_name = f"{object_name}()"
                session.sql(f"DROP {object_type} IF EXISTS {object_name}").collect()
            except:
                st.warning(f"Could not delete {object_type} {object_name}.")
                continue
        session.sql(
            f"DELETE FROM {eval_tablename} WHERE EVAL_NAME = '{evaluation['EVAL_NAME']}'"
        ).collect()
        st.rerun()


@st.experimental_dialog("Evaluation Details")
def show_eval_details(
    evaluation: Dict[str, Any], click_func, eval_tablename: str
) -> None:
    """
    Presents evaluation details in dialog box.

    Args:
        evaluation (dict): Evaluation metadata.

    Returns:
        None

    """
    st.write(f"**Name**: {evaluation['EVAL_NAME']}")
    st.write(f"**Description**: {evaluation['DESCRIPTION']}")
    with st.expander("Source SQL"):
        with stylable_container(
            css_styles=css_yaml_editor, key=f"{evaluation['EVAL_NAME']}_source_sql"
        ):
            st.text_area(
                value=evaluation["SOURCE_SQL"],
                label="code",
                label_visibility="collapsed",
                height=200,
            )
    st.write("**Metrics**:")
    for metric_name in evaluation["METRIC_NAMES"]:
        with st.expander(f"{metric_name}"):
            st.write(f"Model: {evaluation['MODELS'][metric_name]}")
            st.write(evaluation["PARAM_ASSIGNMENTS"][metric_name])
    button_container = row(5, vertical_align="center")
    if button_container.button("Run", use_container_width=True):
        # Set result_data to None so first rendering on results
        # page will create it as pandas dataframe from Snowpark result dataframe
        set_session_var_to_none('result_data')
        click_func(evaluation)
    if button_container.button("Delete", use_container_width=True):
        delete_evaluation(evaluation, eval_tablename)


def run_saved_eval(evaluation: Dict[str, Any]) -> None:
    """
    Executes stored procedure for saved evaluation.

    Sets session state variables for results page.
    Switches page to results page.

    Args:
        evaluation (dict): Evaluation metadata.

    Returns:
        None

    """
    st.session_state["selected_metrics"] = [
            metric for metric in st.session_state['all_metrics'] if metric.name in evaluation["METRIC_NAMES"]
        ]
    # Evaluations may correspond to previously hidden/removed metrics
    # If they are selected, we want to stop the user the ability to run them
    # If the metrics exist but hidden, they can select to show them before running
    if len(st.session_state["selected_metrics"]) != len(evaluation["METRIC_NAMES"]):
        st.error("Metric(s) used in evaluations have been hidden and/or deleted. Please ensure they exist and are set to show via Manage Metrics.")
        st.stop()
    else:
        with st.spinner("Running evaluation..."):
            result = call_sproc(
                st.session_state["session"],
                evaluation["ASSOCIATED_OBJECTS"]["PROCEDURE"],
            )
            st.session_state["metric_result_data"] = result
            st.session_state["eval_name"] = evaluation["EVAL_NAME"]
            st.session_state["eval_funnel"] = "existing"
            # We also extract source_sql and param_assignments here in case user
            # wants to automate an already saved evaluation
            st.session_state["source_sql"] = evaluation["SOURCE_SQL"]
            st.session_state["param_selection"] = evaluation["PARAM_ASSIGNMENTS"]
            st.session_state["model_selection"] = evaluation["MODELS"]
            st.switch_page("pages/results.py")


def run_auto_eval(evaluation: Dict[str, Any]) -> None:
    """
    Extracts everything from automated evaluation for viewing

    Sets session state variables for results page.
    Switches page to results page.

    Args:
        evaluation (dict): Evaluation metadata.

    Returns:
        None

    """
    st.session_state["selected_metrics"] = [
            metric for metric in st.session_state['all_metrics'] if metric.name in evaluation["METRIC_NAMES"]
        ]
    # Evaluations may correspond to previously hidden/removed metrics
    # If they are selected, we want to stop the user the ability to run them
    # If the metrics exist but hidden, they can select to show them before running
    if len(st.session_state["selected_metrics"]) != len(evaluation["METRIC_NAMES"]):
        st.error("Metric(s) used in evaluations have been hidden and/or deleted. Please ensure they exist and are set to show via Manage Metrics.")
        st.stop()
    else:
        with st.spinner("Running evaluation..."):
            st.session_state["param_selection"] = evaluation["PARAM_ASSIGNMENTS"]
            st.session_state["model_selection"] = evaluation["MODELS"]
            st.session_state["eval_funnel"] = "automated"
            try:
                result = st.session_state["session"].table(
                    evaluation["ASSOCIATED_OBJECTS"]["TABLE"]
                )
                # If automation process is not complete, table may not exist throwing error
                # in which case we also don't want to switch pages to results as they won't exist
                # We force an immediate action on dataframe to check if it exists. Otherwise lazy evaluation will hide it.
                result.count()
                st.session_state["metric_result_data"] = result
                st.session_state["eval_name"] = evaluation["EVAL_NAME"]
                st.switch_page("pages/results.py")
            except Exception as e:
                st.session_state["metric_result_data"] = None
                st.warning(
                    f"Table {evaluation['ASSOCIATED_OBJECTS']['TABLE']} does not have results yet. Please try again shortly."
                )


def eval_button_grid(evaluations: List[Any], suffix = Optional[str]) -> Any:
    """Creates a grid of evaluations buttons given list of evaluation metadata.

    Args:
        evaluations (list[]): List of evaluation metadata to be displayed in button grid.
        suffix (string): Optional suffix to add to button key names to avoid duplicate widget key error in streamlit.

    Returns:
       buttons or empty list if no evaluations are available.

    """

    if len(evaluations) > 0:
        eval_grid = grid(4, 4, 4, 4, vertical_align="center")
        eval_buttons = []
        for i, eval in enumerate(evaluations):
            eval_buttons.append(
                eval_grid.button(
                    eval["EVAL_NAME"].split(".")[-1],
                    use_container_width=True,
                    help=eval["DESCRIPTION"],
                    key=eval["EVAL_NAME"] + (suffix or ''),
                )
            )
        return eval_buttons
    else:
        return []


def add_to_selected_metrics(metric_name: str) -> None:
    """Handles adding or removing metrics from selected metrics based on metric checkboxes.

    Users may select checkboxes from home or selected metrics set from selecting evaluations.
    Function sets initial metric trackers if not already set.
    Function is also used as callback for when metric checkboxes are changed.

    Args:
        metric_name (string): Name of metric that corresponds to metric.name attribute.

    Returns:
        None
    """

    matching_metric = next(
        (metric for metric in st.session_state['all_metrics'] if metric.name == metric_name),
        None,
    )

    if st.session_state.get(f"{metric_name}_checkbox", False) is True:
        if metric_name not in st.session_state["selected_metric_names"]:
            st.session_state["selected_metric_names"].append(metric_name)
        if metric_name not in [metric.name for metric in st.session_state["selected_metrics"]]:
            st.session_state["selected_metrics"].append(matching_metric)
    else:
        st.session_state["selected_metric_names"] = [
            j
            for i, j in enumerate(st.session_state["selected_metric_names"])
            if j != metric_name
        ]
        st.session_state["selected_metrics"] = [
            j
            for i, j in enumerate(st.session_state["selected_metrics"])
            if j.name != metric_name
        ]

    st.session_state["selected_metric_names"] = list(set(st.session_state["selected_metric_names"]))
    st.session_state["selected_metrics"] = list(set(st.session_state["selected_metrics"]))


def new_eval_section() -> None:
    """Renders the New Evaluations section of the home page."""
    import uuid
    metric_display = fetch_metric_display()

    with st.container(border=True):
        selection, preview = st.columns(2)
        with selection:
            st.subheader("📐 New Evaluations")
            st.write("To create a new evaluation, start by selecting a metric.")
            for metric_category in metric_display:
                if len(metric_category["metrics"]) > 0:  # Custom metrics may be empty
                    st.write(
                        f'**{metric_category["section_name"]}**: {metric_category["caption"]}'
                    )
                    metric_grid = grid(2, 2, 2, 2, vertical_align="center")
                    for metric in metric_category["metrics"]:
                        metric_grid.checkbox(
                            metric.name,
                            key=f"{metric.name}_checkbox",
                            value=False,
                            on_change=add_to_selected_metrics,
                            args=(metric.name,),
                        )

        with preview:
            with st.container(border=True):
                preview_metric = st.selectbox(
                    "Metric Preview",
                    options=[metric.name for metric in st.session_state['all_metrics']],
                    index=None,
                )
                if preview_metric is not None:
                    display_metric = next(
                        (metric for metric in st.session_state['all_metrics'] if metric.name == preview_metric),
                        None,
                    )
                    st.code(
                        get_metric_preview(display_metric).replace("*", ""),
                        language="yaml",
                    )

        button_container = row(6, vertical_align="center")
        help_button = button_container.button(
            "ℹ️ Help",
            use_container_width=True,
        )
        new_metric_button = button_container.button(
            "➕ Add Metrics",
            use_container_width=True,
        )
        del_metric_button = button_container.button(
            "🎛️ Manage Metrics",
            use_container_width=True,
        )
        continue_button = button_container.button(
            "▶️ Continue",
            use_container_width=True,
            disabled=bool(st.session_state["selected_metrics"] is None),
            type = "primary",
        )
        if continue_button:
            st.session_state["eval_funnel"] = "new"
            st.switch_page("pages/data.py")
        if new_metric_button:
            add_new_metric()
        if del_metric_button:
            manage_metric_dialog()
        if help_button:
            show_about()


def saved_eval_section() -> None:
    """Renders the Saved Evaluations section of the home page."""

    with st.container(border=True):
        st.subheader("📌 Saved Evaluations")
        st.write("Select a saved evaluation to run.")
        saved_evaluations = fetch_evals(SAVED_EVAL_TABLE)
        if len(saved_evaluations) > 0:
            eval_buttons = eval_button_grid(saved_evaluations, suffix='_saved')
            # Need result of session call so cannot use a callback here
            # Instead, we iterate over the buttons and evaluations and call the SPROC if the button is clicked
            for i, button in enumerate(eval_buttons):
                if button is True:
                    try:
                        selected_eval = saved_evaluations[i]
                        show_eval_details(
                            selected_eval, run_saved_eval, SAVED_EVAL_TABLE
                        )
                    except Exception as e:
                        st.error(f"Error: {e}")
        else:
            st.write("No saved evaluations available.")


def automated_eval_section() -> None:
    """Renders the Automated Evaluations section of the home page."""
    with st.container(border=True):
        st.subheader("📡 Automated Evaluations")
        st.write("Select an automated evaluation to see results.")
        auto_evaluations = fetch_evals(AUTO_EVAL_TABLE)
        if len(auto_evaluations) > 0:
            eval_buttons = eval_button_grid(auto_evaluations, suffix='_auto')
            # Need to extract the table name corresponding to the automated evaluation to show in results
            for i, button in enumerate(eval_buttons):
                if button is True:
                    try:
                        selected_eval = auto_evaluations[i]
                        show_eval_details(selected_eval, run_auto_eval, AUTO_EVAL_TABLE)

                    except Exception as e:
                        st.error(f"Error: {e}")
        else:
            st.write("No automated evaluations available.")

def set_metric_states():
    """Sets necessary metric trackers for the home page."""
    if "selected_metric_names" not in st.session_state:
        st.session_state["selected_metric_names"] = []
    if "selected_metrics" not in st.session_state:
        st.session_state["selected_metrics"] = []

    st.session_state['all_metrics'] = fetch_metrics(st.session_state["session"], STAGE_NAME)
    # User may return from results where selected metrics already set
    # If user selects a new metric, we want to refresh selected_metrics to match
    for metric in st.session_state['all_metrics']:
        add_to_selected_metrics(metric.name)

set_metric_states()
new_eval_section()
saved_eval_section()
automated_eval_section()