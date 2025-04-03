import time
from collections import OrderedDict

# Python 3.8 type hints
from typing import List, Union, Dict, Tuple, Any

import streamlit as st
from snowflake.snowpark import DataFrame
from snowflake.snowpark.session import Session
from streamlit_extras.row import row
from streamlit_extras.stylable_container import stylable_container
import snowflake.snowpark.functions as F

from src.app_utils import (
    css_yaml_editor,
    fetch_columns,
    render_sidebar,
    table_data_selector,
    select_model,
    test_complete,
    set_session_var_to_none,
    MENU_ITEMS,
    fetch_metrics,
)
from src.metric_utils import metric_runner
from src.snowflake_utils import (
    get_connection,
    join_data,
    add_row_id,
    STAGE_NAME,
)

TITLE = "Data Selection"

INSTRUCTIONS = """
Select your evaluation data below.
The evaluation data should contain all metric inputs and any additional columns to retain through evaluation.
You can specify a single dataset or separate datasets for expected and actual results, if applicable."""

st.set_page_config(
    page_title=TITLE,
    page_icon="⚒️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items=MENU_ITEMS,
)

# Resolves temporary web socket error in SiS for text input inside of dialog
st.config.set_option("global.minCachedMessageSize", 500 * 1e6)

if "session" not in st.session_state:
    st.session_state["session"] = get_connection()

CODE_PLACEHOLDER = """SELECT
    DATA
FROM
"""

BESPOKE_INSTRUCTIONS = """Before you start, your LLM pipeline must be encapsulated in a stored procedure that takes a VARIANT input and returns a single value.
            Every row of the reference table will be passed through the stored procedure as a dictionary.
            Every column in the reference table will be passed to the stored procedure but only those columns selected will be passed to the stored procedure.
            Please see [Snowflake Stored Procedure documentation](https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-overview)
            for details on stored procedures and these [specific instructions](https://github.com/sfc-gh-jsummer/evalanche#crafting-a-llm-pipeline-stored-procedure) on crafting these stored procedures."""

CORTEX_ANALYST_INSTRUCTIONS = """Have reference questions to run through Cortex Analyst? 
        Select the Semantic Model in stage, table containing the reference questions, and a destination table.
        We will do the rest. Take note of the table name as it will be used in the next step to evaluate the results."""


def check_models(models: List[str]) -> None:
    """Check if models are available in the Snowflake account region."""

    for model in models:
        available = test_complete(st.session_state["session"], model)
        if not available:
            st.error(f"Model {model} not available in region. Please select another.")
            st.stop()


def run_sql(sql: str) -> Union[None, DataFrame]:
    """Run SQL query and return DataFrame or surfaces Streamlit error."""

    if not sql:
        st.warning("Please enter a SQL query.")
    else:
        try:
            return st.session_state["session"].sql(sql.replace(";", ""))
        except Exception as e:
            st.error(f"Error: {e}")


def source_data_selector(name: str) -> Union[None, DataFrame]:
    """
    Returns dataframe of user selected/specified database, schema, table and column selection.

    Args:
        name (string): Used to create unique session state keys for widgets.

    Returns:
        Dataframe: Snowpark dataframe of selected data.

    """
    table_spec = table_data_selector(name)
    columns = fetch_columns(
        table_spec["database"], table_spec["schema"], table_spec["table"]
    )
    selected_columns = st.multiselect(
        "Select Columns", columns, default=None, key=f"columns_{name}"
    )
    if selected_columns:
        return (
            st.session_state["session"]
            .table(
                f'{table_spec["database"]}.{table_spec["schema"]}.{table_spec["table"]}'
            )
            .select(*selected_columns)
        )


def validate_data_inputs() -> None:
    """Validate that all required data inputs for separate inference and expected sources are present."""

    if st.session_state.get("inference_data", None) is None:
        st.error("No inference data selected.")
        st.stop()
    if st.session_state.get("ground_data", None) is None:
        st.error("No ground truth data selected.")
        st.stop()
    if st.session_state.get("inference_join_column", None) is None:
        st.error("No inference join column selected.")
        st.stop()
    if st.session_state.get("ground_join_column", None) is None:
        st.error("No ground truth join column selected.")
        st.stop()


@st.dialog("Joined Data Preview", width="large")
def preview_merge_data() -> None:
    """Preview joined data from selected data sources."""

    limit = 50
    if st.session_state.get("single_source_data", None) is None:
        validate_data_inputs()
        try:
            data = join_data(
                inference_data=st.session_state["inference_data"],
                ground_data=st.session_state["ground_data"],
                inference_key=st.session_state["inference_join_column"],
                ground_key=st.session_state["ground_join_column"],
                limit=limit,
            )
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        try:
            data = st.session_state["single_source_data"].limit(limit)
        except Exception as e:
            st.error(f"Error: {e}")
    if data is not None:
        st.write(f"Limited to {limit} rows.")
        st.dataframe(data, hide_index=True, use_container_width=False)


def data_spec(key_name: str, instructions: str, height=200, join_key=True) -> None:
    """Renders a data selection interfaced with a custom SQL toggle or Snowflake object selectors.

    join_key not used if user toggles for single source data.

    Args:
        key_name (string): Used to create unique session state keys for widgets.
        instructions (string): Instructions to display to user.
        height (int): Height of text_area for custom SQL input.
        join_key (bool): Whether to display a selectbox for join key column.

    """
    instruct_col, checkbox_col = st.columns([1.5, 1])
    with instruct_col:
        st.write(instructions)
    with checkbox_col:
        custom_sql = st.toggle(
            "Custom SQL",
            help="Select this option if you want to write your own SQL queries.",
            key=f"{key_name}_custom_sql",
        )
    if custom_sql:
        with stylable_container(
            css_styles=css_yaml_editor, key=f"{key_name}_styled_code"
        ):
            code_input = st.text_area(
                label="code",
                label_visibility="collapsed",
                height=height,
                key=f"{key_name}_code_input",
                placeholder=CODE_PLACEHOLDER + key_name.upper(),
            )
            if code_input:
                st.session_state[f"{key_name}_data"] = run_sql(code_input)
    else:
        st.session_state[f"{key_name}_data"] = source_data_selector(key_name)
    if join_key:
        if st.session_state[f"{key_name}_data"] is not None:
            columns = st.session_state[f"{key_name}_data"].columns
        else:
            columns = []
        _ = st.selectbox(
            "Select Join Column",
            options=columns,
            index=None,
            key=f"{key_name}_join_column",
            kwargs={"key_name": key_name},
        )


def sproc_runner(
    session: Session, sproc_name: str, inputs: Dict[str, Any]
) -> Tuple[Union[int, float], Any]:
    start_time = time.time()
    record_result = (
        session.sql(f"""CALL {sproc_name}({inputs})""").collect_nowait().result()[0][0]
    )
    # record_result = session.call(sproc_name, inputs) # Once Snowpark supports thread-safe calls without parameter change
    elapsed_time = time.time() - start_time
    return (record_result, elapsed_time)


def cortex_analyst_sproc_runner(
    session: Session, sproc_name: str, question: str, semantic_model_path: str
) -> Tuple[Union[int, float], Any]:
    start_time = time.time()
    record_result = (
        session.sql(f"""CALL {sproc_name}('{question}', '{semantic_model_path}')""")
        .collect_nowait()
        .result()[0][0]
    )
    # record_result = session.call(sproc_name, inputs) # Once Snowpark supports thread-safe calls without parameter change
    elapsed_time = time.time() - start_time
    return (record_result, elapsed_time)


def pipeline_runner(
    session: Session,
    sproc: str,
    input_tablename: str,
    output_tablename: str,
    columns: List[str],
    cortex_analyst: bool = False,
    semantic_model: str = None,
) -> str:
    """Runs stored procedures asynchronously over input from Snowflake table.

    Stored procedures may not be asynchronous but calling of them is done asynchronously in the app.
    Stored procedures must have one input that is a string and return a single value.
    Results are written to a table in Snowflake.
    Write mode is set to append so that multiple evaluations can be saved to the same table.
    Note that all columns in table will be kept but only those passed in columns will be
    passed to stored procedure to mitigate errors from other columns.

    Args:
        session (Session): Snowpark session
        sproc (string): Fully-qualified name of stored procedure.
        input_tablename (string): Fully-qualified name of table with input values.
        output_tablename (string): Fully-qualified name of table to write results to.
        columns (list): List of columns to pass to stored procedure.
        cortex_analyst (bool): Whether to run Cortex Analyst SQL generation.
        semantic_model (string): Fully-qualified path to semantic model for Cortex Analyst.

    Returns:
        string: Fully-qualified name of table where results are written.
    """

    import multiprocessing
    from joblib import Parallel, delayed

    from snowflake.snowpark.functions import lit

    from src.snowflake_utils import add_row_id, save_eval_to_table

    df = add_row_id(session.table(input_tablename))
    first_column = columns[
        0
    ]  # We will use this to pass to Cortex Analyst sproc as the semantic file path
    columns = columns + ["ROW_ID"]

    # Add semantic model as additional column for tracking purposes
    if cortex_analyst:
        if semantic_model is not None:
            df = df.withColumn("MODEL", lit(semantic_model))
            columns = columns + ["MODEL"]

            for pandas_df in df.select(*columns).to_pandas_batches():
                results = []
                for _, row in pandas_df.iterrows():
                    result = {
                        "ROW_ID": row["ROW_ID"],  # Capture ROW_ID
                        "CORTEX_ANALYST_SQL": (
                            response := cortex_analyst_sproc_runner(
                                session,
                                sproc,
                                row.to_dict()[first_column],
                                semantic_model,
                            )
                        )[0],
                        "ELAPSED_TIME": response[1],
                    }
                    results.append(result)
                    time.sleep(
                        3
                    )  # Add a 3-second delay between processing each record to avoid overloading the system

    else:
        for pandas_df in df.select(*columns).to_pandas_batches():
            # for pandas_df in df.to_pandas_batches():
            results = Parallel(n_jobs=multiprocessing.cpu_count(), backend="threading")(
                delayed(
                    lambda row: {
                        "ROW_ID": row["ROW_ID"],  # Capture ROW_ID
                        "RESPONSE": (
                            response := sproc_runner(session, sproc, row.to_dict())
                        )[0],
                        "ELAPSED_TIME": response[1],
                    }
                )(row)
                for _, row in pandas_df.iterrows()
            )

    result = session.create_dataframe(results).join(df, on="ROW_ID", how="left")
    save_eval_to_table(result, output_tablename)
    return output_tablename


@st.dialog("Run your LLM Pipeline", width="large")
def pipeline_runner_dialog() -> None:
    """Dialog to run reference data through LLM pipeline and record results for evaluation."""

    from src.app_utils import (
        get_sprocs,
        select_schema_context,
        get_stages,
        get_semantic_models,
    )

    st.write(
        """Have reference questions or inputs but still need to run them through your LLM pipeline?
             Use this dialog to run a reference set through your LLM pipeline and record the results."""
    )

    pipeline_selection = st.selectbox(
        "Do you want to run **Cortex Analyst SQL Generation** or a **custom LLM Pipeline**?",
        options=["Cortex Analyst", "Custom"],
        index=None,
    )

    if pipeline_selection is not None:
        if pipeline_selection == "Custom":
            st.write(f"**Instructions:** {BESPOKE_INSTRUCTIONS}")
        else:
            st.write(f"**Instructions:** {CORTEX_ANALYST_INSTRUCTIONS}")

        name = "runner"
        if pipeline_selection == "Custom":
            st.write("Select the stored procedure that encapsulates your LLM pipeline.")
            schema_context = select_schema_context(
                name, on_change=get_sprocs, args=(name,)
            )

            if f"{name}_sprocs" not in st.session_state:
                st.session_state[f"{name}_sprocs"] = []
            sproc_name = st.selectbox(
                "Select Stored Procedure",
                st.session_state[f"{name}_sprocs"],
                index=None,
            )
            sproc_name = (
                f"{schema_context['database']}.{schema_context['schema']}.{sproc_name}"
            )

        else:
            st.write(
                "Select the stage that contains your semantic model for Cortex Analyst."
            )
            schema_context = select_schema_context(
                name, on_change=get_stages, args=(name,)
            )

            if f"{name}_stages" not in st.session_state:
                st.session_state[f"{name}_stages"] = []
            if f"{name}_models" not in st.session_state:
                st.session_state[f"{name}_models"] = []
            stage_name = st.selectbox(
                "Select Snowflake Stage",
                st.session_state[f"{name}_stages"],
                index=None,
                key=f"{name}_stage",
                on_change=get_semantic_models,
                args=(name,),
            )
            semantic_model = st.selectbox(
                "Select Semantic Model",
                st.session_state[f"{name}_models"],
                index=None,
                key=f"{name}_model",
            )
            qualified_semantic_model = f"{schema_context['database']}.{schema_context['schema']}.{stage_name}/{semantic_model}"

        table = st.text_input(
            "Enter Name for Generated Table",
            key=f"new_table_{name}",
            help="Fully qualify if you would like to save in a different database/schema than above.",
        )
        if "." not in table:
            new_tablename = (
                f"{schema_context['database']}.{schema_context['schema']}.{table}"
            )
        else:
            new_tablename = table
        st.divider()

        st.write("Select the reference question set.")
        name = "runner_output"
        table_spec = table_data_selector(name, new_table=False)
        data_table = (
            f'{table_spec["database"]}.{table_spec["schema"]}.{table_spec["table"]}'
        )
        available_columns = fetch_columns(
            table_spec["database"], table_spec["schema"], table_spec["table"]
        )

        if pipeline_selection == "Custom":
            selected_columns = st.multiselect(
                "Select Columns",
                available_columns,
                default=None,
                key=f"columns_{name}",
                help="Select the columns to be explicitly passed to the stored procedure.",
            )
        else:
            selected_columns = st.selectbox(
                "Select Column containing Reference Question",
                available_columns,
                index=None,
                key=f"columns_{name}",
                help="Select the column that contains the reference questions for Cortex Analyst.",
            )
        if st.button("Run"):
            try:
                if pipeline_selection == "Custom":
                    with st.spinner("Running pipeline..."):
                        st.session_state["returned_tablename"] = pipeline_runner(
                            st.session_state["session"],
                            sproc_name.split("(")[0],
                            data_table,
                            new_tablename,
                            selected_columns,
                        )
                else:
                    with st.spinner("Running Cortex Analyst..."):
                        st.session_state["returned_tablename"] = pipeline_runner(
                            session=st.session_state["session"],
                            sproc="GENAI_UTILITIES.EVALUATION.CORTEX_ANALYST_SQL",
                            input_tablename=data_table,
                            output_tablename=new_tablename,
                            columns=[selected_columns],
                            cortex_analyst=True,
                            semantic_model=qualified_semantic_model,
                        )
                # Set result_data to None so first rendering on results
                # page will create it as pandas dataframe from Snowpark result dataframe
                set_session_var_to_none("result_data")
                st.success(f"Results written to {new_tablename}.")
            except Exception as e:
                st.error(f"Error: {e}")
                st.stop()
            time.sleep(2)
            st.rerun()


@st.dialog("Configure Metrics", width="large")
def configure_metrics() -> None:
    """Dialog to configure metric parameters/inputs to data source columns."""

    st.write("Select a model and a column for each required parameter.")
    limit = 5
    if st.session_state.get("single_source_data", None) is None:
        validate_data_inputs()
        try:
            columns = join_data(
                inference_data=st.session_state["inference_data"],
                ground_data=st.session_state["ground_data"],
                inference_key=st.session_state["inference_join_column"],
                ground_key=st.session_state["ground_join_column"],
                limit=limit,
            ).columns
        except Exception as e:
            st.error(f"Error in pulling data: {e}")
    else:
        try:
            columns = st.session_state["single_source_data"].columns
        except Exception as e:
            st.error(f"Error in pulling data: {e}")
    param_selection = {}  # Track parameter-column assignments for each metric
    model_selection = {}  # Track model selection for each metric
    for metric in st.session_state["selected_metrics"]:
        st.divider()
        st.write(f"**{metric.name}**: {metric.description}")
        model = select_model(default=metric.model, keyname=metric.name)
        metric_params = (
            OrderedDict()
        )  # Track each parameter assignment for a single metric
        required_params = metric.required
        for param, desc in required_params.items():
            metric_params[param] = st.selectbox(
                f"Select column for **{param}**",
                columns,
                index=None,
                key=f"{metric.name}_{param}_selection",
                help=desc,
            )
        param_selection[metric.name] = metric_params
        model_selection[metric.name] = model
    st.session_state["param_selection"] = param_selection
    st.session_state["model_selection"] = model_selection
    if st.button("Run"):
        check_models(st.session_state["model_selection"].values())
        run_eval()


def run_eval() -> None:
    """
    Runs metric calculation on selected data sources after metric parameter-column association completed.
    """

    if st.session_state.get("param_selection", None) is None:
        st.error("Please select columns for all required parameters.")
    else:
        with st.spinner("Calculating metric..."):
            if st.session_state.get("single_source_data", None) is None:
                st.session_state["metric_result_data"] = join_data(
                    inference_data=st.session_state["inference_data"],
                    ground_data=st.session_state["ground_data"],
                    inference_key=st.session_state["inference_join_column"],
                    ground_key=st.session_state["ground_join_column"],
                    limit=None,
                )
            else:
                st.session_state["metric_result_data"] = st.session_state[
                    "single_source_data"
                ]
            # Get source_sql for joined dataset in case we need to save a sproc in subsequent page
            st.session_state["source_sql"] = st.session_state[
                "metric_result_data"
            ].queries["queries"][0]

            # metric_result_data contains raw form of metric results
            st.session_state["metric_result_data"] = metric_runner(
                session=st.session_state["session"],
                metrics=st.session_state["selected_metrics"],
                models=st.session_state["model_selection"],
                param_assignments=st.session_state["param_selection"],
                source_df=st.session_state["metric_result_data"],
                source_sql=None,
            )

            # Prepare view for user to review and comment on results page
            st.session_state["result_data"] = (
                add_row_id(st.session_state["metric_result_data"])
                .withColumn("REVIEW", F.lit(False))
                .withColumn("COMMENT", F.lit(None))
                .to_pandas()
            )

            # metric_funnel will capture where user came from and dictate next steps allowed
            st.session_state["eval_funnel"] = "new"
            st.switch_page("pages/results.py")


# Mitigate dropping of session state on various page refresh
if "selected_metrics" in st.session_state:
    st.session_state["selected_metrics"] = st.session_state["selected_metrics"]

st.title(TITLE)
st.write(INSTRUCTIONS)
render_sidebar()


def pick_data() -> None:
    """Main rendering function for page."""

    data_split, runner_col, _ = st.columns([1, 1, 2])
    # Show table if results were written to a table in stored procedure runner
    if "returned_tablename" in st.session_state:
        st.info(f"Recent results written to {st.session_state['returned_tablename']}.")
    with data_split:
        data_toggle = st.toggle(
            "Separate Expected & Actual",
            help="""Turn on to specify expected and actual datasets separately.
                    A join key will be necessary to compare the two datasets.""",
            value=False,
        )
    with runner_col:
        runner_button = st.button(
            "Need to generate results?",
            use_container_width=True,
            help="""Have reference questions or inputs but still need to run them through your LLM pipeline?
            Use this dialog to run a reference set through your LLM pipeline and record the results to evaluate.""",
        )
        if runner_button:
            pipeline_runner_dialog()
    if not data_toggle:
        single_col, _ = st.columns(2)
        with single_col:
            data_spec(
                key_name="single_source",
                instructions="Select your evaluation dataset.",
                join_key=False,
            )
    else:
        inf_col, ground_col = st.columns(2)
        with inf_col:
            data_spec(key_name="ground", instructions="Select your expected results.")
        with ground_col:
            data_spec(key_name="inference", instructions="Select your actual results.")
    button_container = row(10, vertical_align="center")
    preview_button = button_container.button(
        ":mag_right: Preview", use_container_width=True
    )
    configure_button = button_container.button(
        "▶️ Configure",
        use_container_width=True,
        help="Select metrics and data to configure your evaluation.",
        type="primary",
        disabled=len(st.session_state.get("selected_metrics", [])) == 0,
    )

    if preview_button:
        preview_merge_data()
    if configure_button:
        configure_metrics()


pick_data()
