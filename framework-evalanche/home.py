# Python 3.8 type hints
from typing import Any, Dict, List

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
)
from src.metric_utils import AUTO_EVAL_TABLE, SAVED_EVAL_TABLE
from src.snowflake_utils import (
    call_sproc,
    get_connection,
)

TITLE = "Evalanche: GenAI Evaluation"
INSTRUCTIONS = """
Welcome to the Evalanche dashboard!
Here you can create, run, and view GenAI evaluations.

To start, select a metric in New Evaluations and follow the prompts to evaluate existing GenAI outputs.
If you already have saved evaluations, you can run them from Saved Evaluations.
Lastly, Automated Evaluations shows evaluations that are currently running.
"""

st.set_page_config(
    page_title=TITLE,
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)


if "session" not in st.session_state:
    st.session_state["session"] = get_connection()

st.title(TITLE)
st.write(INSTRUCTIONS)
render_sidebar()


@st.experimental_dialog("Create New Metric")
def add_new_metric():
    st.write("TO DO - Add new metric wizard")


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
    for metric_name, assignments in evaluation["PARAM_ASSIGNMENTS"].items():
        with st.expander(f"Parameter Assignments for **{metric_name}**"):
            st.write(assignments)
    button_container = row(5, vertical_align="center")
    if button_container.button("Run", use_container_width=True):
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
    with st.spinner("Running evaluation..."):
        result = call_sproc(
            st.session_state["session"],
            evaluation["ASSOCIATED_OBJECTS"]["PROCEDURE"],
        )
        st.session_state["selected_metrics"] = [
            metric for metric in metrics if metric.name in evaluation["METRIC_NAMES"]
        ]
        st.session_state["metric_result_data"] = result
        st.session_state["eval_name"] = evaluation["EVAL_NAME"]
        st.session_state["eval_funnel"] = "existing"
        # We also extract source_sql and param_assignments here in case user
        # wants to automate an already saved evaluation
        st.session_state["source_sql"] = evaluation["SOURCE_SQL"]
        st.session_state["param_selection"] = evaluation["PARAM_ASSIGNMENTS"]
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
    with st.spinner("Running evaluation..."):
        st.session_state["selected_metrics"] = [
            metric for metric in metrics if metric.name in evaluation["METRIC_NAMES"]
        ]
        st.session_state["param_selection"] = evaluation["PARAM_ASSIGNMENTS"]
        st.session_state["eval_funnel"] = "automated"
        try:
            result = st.session_state["session"].table(
                evaluation["ASSOCIATED_OBJECTS"]["TABLE"]
            )
            # If automation process is not complete, table may not exist throwing error
            # in which case we also don't want to switch pages to results as they won't exist
            st.session_state["metric_result_data"] = result
            st.session_state["eval_name"] = evaluation["EVAL_NAME"]
            st.switch_page("pages/results.py")
        except Exception as e:
            st.session_state["metric_result_data"] = None
            st.warning(
                f"Table {evaluation['ASSOCIATED_OBJECTS']['TABLE']} does not have results yet."
            )


def eval_button_grid(evaluations: List[Any]) -> Any:
    """Creates a grid of evaluations buttons given list of evaluation metadata.

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
                    key=eval["EVAL_NAME"],
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

    # Initialize metric_name and metric trackers
    if "selected_metric_names" not in st.session_state:
        st.session_state["selected_metric_names"] = []
    if "selected_metrics" not in st.session_state:
        st.session_state["selected_metrics"] = []

    # Get metric object that matches metric name
    matching_metric = next(
        (metric for metric in metrics if metric.name == metric_name),
        None,
    )

    # Add or remove metric from selected metrics
    if st.session_state.get(f"{metric_name}_checkbox", False) is True:
        if metric_name not in st.session_state["selected_metric_names"]:
            st.session_state["selected_metric_names"].append(metric_name)
        if matching_metric not in st.session_state["selected_metrics"]:
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
            if j != matching_metric
        ]

    # If no metrics are selected such as user returned to Home, reset selected_metrics
    if len(st.session_state["selected_metric_names"]) == 0:
        st.session_state["selected_metrics"] = []


def new_eval_section() -> None:
    """Renders the New Evaluations section of the home page."""

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
                    options=[metric.name for metric in metrics],
                    index=None,
                )
                if preview_metric is not None:
                    display_metric = next(
                        (metric for metric in metrics if metric.name == preview_metric),
                        None,
                    )
                    st.code(
                        get_metric_preview(display_metric).replace("*", ""),
                        language="yaml",
                    )

        button_container = row(9, vertical_align="center")
        continue_button = button_container.button(
            "Continue",
            use_container_width=True,
            disabled=bool(st.session_state["selected_metrics"] is None),
        )
        if continue_button:
            st.session_state["eval_funnel"] = "new"
            st.switch_page("pages/data.py")


def saved_eval_section() -> None:
    """Renders the Saved Evaluations section of the home page."""

    with st.container(border=True):
        st.subheader("📌 Saved Evaluations")
        st.write("Select a saved evaluation to run.")
        saved_evaluations = fetch_evals(SAVED_EVAL_TABLE)
        if len(saved_evaluations) > 0:
            eval_buttons = eval_button_grid(saved_evaluations)
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
            eval_buttons = eval_button_grid(auto_evaluations)
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


metrics = fetch_metrics()
# Initialize selected metric trackers and reset if returning to page
for metric in metrics:
    add_to_selected_metrics(metric.name)
new_eval_section()
saved_eval_section()
automated_eval_section()
