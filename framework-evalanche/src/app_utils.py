# Python 3.8 type hints
from typing import Any, Dict, List, Optional, Union

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
import pandas as pd
import snowflake.snowpark.functions as F

from src.metrics import Metric
from src.snowflake_utils import (
    get_connection,
    CUSTOM_METRIC_TABLE,
    models,
    add_row_id,
)

ABOUT = """
Evalanche is a Streamlit in Snowflake (SiS) app that provides a single location to evaluate and compare LLM use case outputs in a streamlined, on demand, and automated fashion.

Evalanche's primary structure is based on 2 components: 1) Metrics and 2) Data Sources.
Together, Metrics and Data Sources can be combined to make an Evaluation.
In other words, an Evaluation will calculate 1 or more Metrics for every record in a Data Source.

### Metrics

Metrics rely on LLMs to Judge (or Score) results.
A Metric mostly consists of required input(s) and a prompt asking the LLM to assess a specific quality of these required input(s).

### Data Sources

Data Sources provide the values to Metrics’ required inputs. Data Sources can be a Snowflake table or a SQL SELECT statement.

### Example

The Answer Relevancy Metric will rate the relevancy of an LLM response to a question. A Data Source for Answer Relevancy could look like the below:

| QUESTION        | LLM_RESPONSE |
|----------------|----------------|
| What does SCUBA stand for? | SCUBA stands for Self Contained Underwater Breathing Apparatus  |
| Do woodchucks chuck wood? | Woodchucks don’t actually chuck wood, they do in fact chuck quite a bit of dirt when digging out a burrow.  |

### Getting Started

To create your own evaluation:
1) Select from the out of the box Metrics or create your own.
2) Select or create a Data Source that provides the required inputs of the desired Metric(s).
"""

MENU_ITEMS = {
             'Get Help': "https://github.com/Snowflake-Labs/emerging-solutions-toolbox/tree/main/framework-evalanche",
             'Report a bug': "https://github.com/Snowflake-Labs/emerging-solutions-toolbox/tree/main/framework-evalanche",
             'About': ABOUT
         }

# Style text_area to mirror st.code
css_yaml_editor = """
    textarea{
        font-size: 14px;
        color: #2e2e2e;
        font-family:Menlo;
        background-color: #fbfbfb;
    }
    """

def select_model(
        keyname: str,
        default: Optional[str] = None,
                 ) -> List[str]:
    """Renders selectbox for model selection.

    Args:
        default (string): Default model to select.

    Returns:
        string: Selected model.
    """

    return st.selectbox("Select model",
                        models,
                        index=models.index(default) if default in models else None,
                        key=f"{keyname}_model_selector",)


def test_complete(session, model, prompt = "Repeat the word hello once and only once. Do not say anything else.") -> bool:
    from snowflake.cortex import Complete
    from snowflake.snowpark.exceptions import SnowparkSQLException

    """Returns True if selected model is supported in region and returns False otherwise."""
    try:
        response = Complete(model, prompt, session = session)
        return True
    except SnowparkSQLException as e:
        if 'unknown model' in str(e):
            return False

def upload_staged_pickle(session: Session, instance: Any, file_name: str, stage_name: str) -> None:
    """Pickles object and uploads to Snowflake stage."""
    import cloudpickle as pickle
    import os

    # Pickle the instance
    with open(file_name, "wb") as f:
        pickle.dump(instance, f)
    # Upload the pickled instance to the stage
    session.file.put(file_name, f"@{stage_name}", auto_compress=False, overwrite=True)
    os.remove(file_name)

def load_staged_pickle(session: Session, staged_file_path: str) -> Any:
    """Loads pickled object from Snowflake stage.

    LS @stage results do not include database and schema so we have to create the full path to the file manually.
    """
    import cloudpickle as pickle

    session.file.get(f"{staged_file_path}","/tmp")
    # Load the pickled instance from the file
    local_file = '/tmp/' + staged_file_path.split("/")[-1]
    with open(local_file, "rb") as f:
        loaded_instance = pickle.load(f)
        return loaded_instance


def delete_metric(session: Session, metric: Metric, stage_name: str):
    """Deletes metric pickle file from Snowflake stage."""
    file_name = type(metric).__name__ + ".pkl"

    query = f"rm @{stage_name}/{file_name}"
    session.sql(query).collect()

    # Remove metric from all_metrics list
    # We want to avoid re-querying stage for latency
    st.session_state['all_metrics'] = [
                metric if metric.name != metric.name else metric
                for metric in st.session_state['all_metrics']
            ]


def add_metric_to_table(session: Session,
                        metric_name: str,
                        stage_file_path: str,
                        table_name: str = CUSTOM_METRIC_TABLE,
                        ):
    """Adds metric to CUSTOM_METRICS table in Snowflake."""
    import datetime
    from snowflake.snowpark.functions import current_timestamp, when_matched, when_not_matched

    metric_table = session.table(table_name)
    metric_columns = metric_table.columns
    key_col = 'METRIC_NAME'
    other_cols = [col for col in metric_columns if col != key_col]

    if session.get_current_user() is None:
        username = st.experimental_user.user_name
    else:
        username = session.get_current_user()

    new_record_df = session.create_dataframe([[metric_name,
                                               stage_file_path,
                                               datetime.datetime.now(),
                                               True,
                                               username.replace('"', '')]]).to_df(*metric_columns)

    update_dict = {col: new_record_df[col] for col in other_cols}
    insert_dict = {col: new_record_df[col] for col in metric_columns}

    _ = metric_table.merge(new_record_df, metric_table[key_col] == new_record_df[key_col],
            [when_matched().update(update_dict),
            when_not_matched().insert(insert_dict)])




def get_custom_metrics_table_results(session: Session,
                                     table_name: str = CUSTOM_METRIC_TABLE
                                     ) -> List[Dict[str, Any]]:
    """Retrieves list of staged file names from CUSTOM_METRICS table in Snowflake where show is True."""
    df = session.table(table_name)
    if "STAGE_FILE_PATH" not in df.columns or "SHOW_METRIC" not in df.columns:
        return []
    else:
        return [i.as_dict()['STAGE_FILE_PATH'] for i in df.collect() if i.as_dict()['SHOW_METRIC'] is True]


def fetch_custom_metrics_from_stage(session: Session,
                                    stage_name: str,
                                    metrics_table: str = CUSTOM_METRIC_TABLE,
                                    ) -> List[Union[str, Metric, None]]:
    """Returns list of custom metrics from Snowflake stage."""

    # Get metric list from table
    metrics_to_show = get_custom_metrics_table_results(session, metrics_table)

    query = f"ls @{stage_name} pattern='.*\\pkl'"
    result = session.sql(query)
    files = [file[0] for file in result.collect()]
    if len(files) > 0:
        custom_metrics = []
        for f in files:
            stage_file_path = f"@{stage_name}/{f.split('/')[-1]}" # Non-qualified stage name in LS from @stage results
            if stage_file_path in metrics_to_show:
                try:
                    custom_metrics.append(load_staged_pickle(session, stage_file_path))
                except TypeError:
                    st.warning(f"Error loading metric {stage_file_path}. This can occur if the metric was created with a different python version than what is being used.")
                    continue
        return custom_metrics
    else:
        return []

def fetch_metrics(session: Session,
                  custom_metrics_stage_name: str,
                  metrics_table: str = CUSTOM_METRIC_TABLE,
                  ) -> List[Metric]:
    """Combines metrics and custom metrics, if any, and returns list of metrics."""
    from src.metrics import provided_metrics

    custom_metrics = fetch_custom_metrics_from_stage(session, custom_metrics_stage_name, metrics_table)
    if len(custom_metrics) > 0:
        return provided_metrics + custom_metrics
    else:
        return provided_metrics


def fetch_metric_display() -> List[Dict[str, Any]]:
    """Combines metrics and custom metrics displays, if any, and returns list of displays."""

    from src.metrics import metric_display
    from src.metrics import provided_metrics


    custom_metrics = [metric for metric in st.session_state['all_metrics']
                      if metric.name not in [provided_metric.name for provided_metric in provided_metrics]]

    if len(custom_metrics) > 0:
        custom_metrics_section = {
            "section_name": "Custom Metrics",
            "caption": """Metrics created by user base.""",
            "metrics": custom_metrics,
        }
        return metric_display + [custom_metrics_section]
    else:
        return metric_display


def format_required_args(required: Dict[str, str]) -> str:
    """Formats required arguments as markdown list."""

    return "\n".join([f"- **{name}**: {value}" for name, value in required.items()])


def get_metric_preview(metric: Metric) -> str:
    """Returns markdown formatted preview of metric."""

    return f"""
**Metric**: {metric.name}

**Description:**
{metric.description}

**Required Inputs**:
{format_required_args(metric.required)}
"""


@st.cache_data()
def fetch_databases() -> List[str]:
    """Returns list of databases from Snowflake account."""

    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql("SHOW DATABASES").collect()
    return [row["name"] for row in results]


def fetch_schemas(database: str) -> List[str]:
    """Returns list of schemas in database from Snowflake account."""

    if database is None:
        return []
    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return [row["name"] for row in results]


def get_schemas(name: str):
    """Call back function to associate schema selector with corresponding database selector."""

    if st.session_state[f"{name}_database"] is not None:
        st.session_state[f"{name}_schemas"] = fetch_schemas(
            st.session_state[f"{name}_database"]
        )
    else:
        st.session_state[f"{name}_schemas"] = []


def get_stages_and_views(name: str):
    """Call back function to associate database and schema selector with corresponding stage
    and semantic viewoptions."""

    if (
        st.session_state[f"{name}_database"] is not None
        and st.session_state[f"{name}_schema"] is not None
    ):
        st.session_state[f"{name}_stages"] = fetch_stages(
            st.session_state[f"{name}_database"], st.session_state[f"{name}_schema"]
        )
        st.session_state[f"{name}_semantic_views"] = fetch_semantic_views(
            st.session_state[f"{name}_database"], st.session_state[f"{name}_schema"]
        )

    else:
        st.session_state[f"{name}_stages"] = []
        st.session_state[f"{name}_semantic_views"] = []

def get_semantic_models(name: str):
    """Call back function to associate available semantic model selector with corresponding stage selection."""

    if (
        st.session_state[f"{name}_database"] is not None
        and st.session_state[f"{name}_schema"] is not None
        and st.session_state[f"{name}_stage"] is not None
    ):
        if "session" not in st.session_state:
            session = get_connection()
        else:
            session = st.session_state["session"]
        stage = f'{st.session_state[f"{name}_database"]}.{st.session_state[f"{name}_schema"]}.{st.session_state[f"{name}_stage"]}'
        query = f"ls @{stage} pattern='.*\\yaml'"
        result = session.sql(query)
        files = [file[0].split("/")[-1] for file in result.collect()]
        if len(files) > 0:
            st.session_state[f"{name}_models"] = files
        else:
            st.session_state[f"{name}_models"] = []


def get_sprocs(name: str):
    """Call back function to associate database and schema selector with corresponding stored procedures."""

    if (
        st.session_state[f"{name}_database"] is not None
        and st.session_state[f"{name}_schema"] is not None
    ):
        st.session_state[f"{name}_sprocs"] = fetch_sprocs(
            st.session_state[f"{name}_database"], st.session_state[f"{name}_schema"]
        )
    else:
        st.session_state[f"{name}_sprocs"] = []


def fetch_tables(database: str, schema: str) -> List[str]:
    """Returns list of tables in database and schema from Snowflake account."""

    if database is None or schema is None:
        return []
    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql(f"SHOW TABLES IN SCHEMA {database}.{schema}").collect()
    return [row["name"] for row in results]


def fetch_stages(database: str, schema: str) -> List[str]:
    """Returns list of stages in database and schema from Snowflake account."""

    if database is None or schema is None:
        return []
    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql(f"SHOW STAGES IN SCHEMA {database}.{schema}").collect()
    return [row["name"] for row in results]

def fetch_semantic_views(database: str, schema: str) -> List[str]:
    """Returns list of semantic views in database and schema from Snowflake account."""

    if database is None or schema is None:
        return []
    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql(f"SHOW SEMANTIC VIEWS IN SCHEMA {database}.{schema}").collect()
    return [row["name"] for row in results]


def fetch_sprocs(database: str, schema: str) -> List[str]:
    """Returns list of stored procedures in database and schema from Snowflake account."""

    if database is None or schema is None:
        return []
    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql(f"SHOW PROCEDURES IN SCHEMA {database}.{schema}").collect()
    return [row["arguments"] for row in results if row["schema_name"] == schema]


def fetch_warehouses() -> List[str]:
    """Returns list of warehouses from Snowflake account."""

    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql(f"SHOW WAREHOUSES").collect()
    return [row["name"] for row in results]


def fetch_columns(database: str, schema: str, table: str) -> List[str]:
    """Returns list of columns in database, schema, and table from Snowflake account."""

    if database is None or schema is None or table is None:
        return []
    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    results = session.sql(
        f"SHOW COLUMNS IN TABLE {database}.{schema}.{table}"
    ).collect()
    return [row["column_name"] for row in results]


def select_schema_context(name: str, **kwargs) -> Dict[str, str]:
    """Renders selectboxes for database and schema selection.

    **kwargs can be used to pass callbacks and args for callbacks to filter subsequent selectboxes from database + schema selection.

    Args:
        name (string): Used to create unique session state keys for widgets.

    Returns:
        dict: A dictionary with keys: database, schema.

    """

    # Set database and schema session state if not already set at first initialization
    if f"{name}_database" not in st.session_state:
        st.session_state[f"{name}_database"] = None
    if f"{name}_schemas" not in st.session_state:
        st.session_state[f"{name}_schemas"] = []

    database = st.selectbox(
        "Select Database",
        fetch_databases(),
        index=None,
        key=f"{name}_database",
        on_change=get_schemas,
        args=(name,),
    )

    schema = st.selectbox(
        "Select Schema",
        st.session_state[f"{name}_schemas"],
        index=None,
        key=f"{name}_schema",
        **kwargs,
    )
    return {"database": database, "schema": schema}


def table_data_selector(name: str, new_table: bool = False) -> Dict[str, str]:
    """
    Renders selectboxes for database, schema, and table selection.

    If new_table is True, a text input field will be rendered for the user to enter a new table name.
    Results are returned as a dictionary with keys: database, schema, table.

    Args:
        name (string): Used to create unique session state keys for widgets.
        new_table (bool): If True, a text input field will be rendered for the user to enter a new table name.
                          If False, a selectbox will be rendered for the user to select an existing table.

    Returns:
        dict: A dictionary with keys: database, schema, table.

    """
    context = select_schema_context(name)
    database = context["database"]
    schema = context["schema"]
    if new_table:
        table = st.text_input("Enter Table Name", key=f"new_table_{name}")
    else:
        table = st.selectbox(
            "Select Table",
            fetch_tables(database, schema),
            index=None,
            key=f"tables_{name}",
        )
    return {"database": database, "schema": schema, "table": table}


def try_parse_json(value: str) -> Any:
    """Attempts to parse a string as JSON for unloading semi-structured data from evaluation tables.

    If parsing fails, returns None to avoid streamlit errors.
    This means that some rows may not be displayed in the evaluation table or pieces missing but errors mitigated.
    """

    import json

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None
    except TypeError:
        return None


def fetch_evals(
    table_name: str,
    json_cols=["METRIC_NAMES", "PARAM_ASSIGNMENTS", "MODELS", "ASSOCIATED_OBJECTS"],
) -> List[Optional[Dict[str, Optional[str]]]]:
    """
    Returns evaluation metadata from tables.
    JSON parsing is applied to specific columns. If errors occur, None is returned for the specific row.
    If an error occurs with reading the table, an empty list is returned.

    Args:
        table_name (string): Fully-qualified Snowflake table name.
        json_cols (list[str]): List of columns to parse as JSON.

    Returns:
        List of dictionaries with column names as keys and column values values as dict values.

    """

    if "session" not in st.session_state:
        session = get_connection()
    else:
        session = st.session_state["session"]
    try:
        results = session.table(table_name).collect()
        keys = results[0].asDict().keys()
        return [
            {
                key: try_parse_json(row[key]) if key in json_cols else row[key]
                for key in keys
            }
            for row in results
        ]
    except Exception as e:
        return []


def render_sidebar():
    """Renders the sidebar of selected metrics for the application across all pages."""

    if st.session_state.get("selected_metrics", None) is not None:
        with st.sidebar:
            for metric in st.session_state["selected_metrics"]:
                st.write(get_metric_preview(metric))


def count_words_in_braces(prompt: str):
    import re
    # Create a regex pattern that matches any word between the braces
    pattern = re.compile(r'\{([^{}]+)\}')
    # Find all non-overlapping matches of the pattern
    matches = pattern.findall(prompt)
    # Return the number of matches
    return set(matches)


def vars_entry(prompt):
    variables = count_words_in_braces(prompt)
    required_params = {}
    if len(variables) > 0:
        st.caption("Enter description for variable(s).")
        for i, var in enumerate(variables):
            required_params[var] = st.text_input(label=var,
                                key = var,)
        return required_params


def format_query_tag(query_tag: Dict[str, Any]) -> str:
    """
    Formats dict query tag as string.

    Args:
        query_tag (dict): Semi-structured dictionary query tag.

    Returns:
        Query tag formatted as string.
    """
    import json

    return json.dumps(query_tag)


def set_session_var_to_none(variable: str):
    """Sets session state to None for all session state variables."""
    if variable in st.session_state:
        st.session_state[variable] = None

def prep_metric_results_for_display(df: DataFrame) -> pd.DataFrame:
    """Prepares metric results DataFrame for display in Streamlit.

    Args:
        df (DataFrame): Snowflake DataFrame containing metric results.

    Returns:
        pd.DataFrame: Pandas DataFrame ready for display.
    """

    return add_row_id(df)\
                        .withColumn("REVIEW", F.lit(False))\
                        .withColumn("COMMENT", F.lit(None)).to_pandas()
