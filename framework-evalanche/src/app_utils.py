# Python 3.8 type hints
from typing import Any, Dict, List, Optional

import streamlit as st

from src.metrics import Metric
from src.snowflake_utils import get_connection

QUERY_TAG = {
    "origin": "sf_sit",
    "name": "evalanche",
    "version": {"major": 1, "minor": 0},
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


def fetch_metrics() -> List[Metric]:
    """Combines metrics and custom metrics, if any, and returns list of metrics."""
    from src.custom_metrics import custom_metrics
    from src.metrics import metrics

    if len(custom_metrics) > 0:
        return metrics + custom_metrics
    else:
        return metrics


def fetch_metric_display() -> List[Dict[str, Any]]:
    """Combines metrics and custom metrics displays, if any, and returns list of displays."""
    from src.custom_metrics import custom_metrics
    from src.metrics import metric_display

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


def get_stages(name: str):
    """Call back function to associate database and schema selector with corresponding stage options."""

    if (
        st.session_state[f"{name}_database"] is not None
        and st.session_state[f"{name}_schema"] is not None
    ):
        st.session_state[f"{name}_stages"] = fetch_stages(
            st.session_state[f"{name}_database"], st.session_state[f"{name}_schema"]
        )
    else:
        st.session_state[f"{name}_stages"] = []


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
    json_cols=["METRIC_NAMES", "PARAM_ASSIGNMENTS", "ASSOCIATED_OBJECTS"],
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


def print_session_state():
    """Troubleshooting helper function to print session state keys and values.

    Note: This function is not intended for production use.
    """

    for key, value in st.session_state.items():
        print(f"{key}: {value}\n")


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
