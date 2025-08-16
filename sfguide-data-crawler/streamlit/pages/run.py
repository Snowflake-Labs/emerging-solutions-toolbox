import time
import streamlit as st
import pandas as pd
from snowflake.cortex import Complete
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

def get_available_models(session):
    """Returns list of available models in Snowflake account."""

    if 'available_models' not in st.session_state:
        try:
            query = "SHOW MODELS IN SNOWFLAKE.MODELS"
            model_results = session.sql(query).collect()
            models = [row['name'].lower() for row in model_results]
            st.session_state['available_models'] = models
        except Exception as e:
            st.error(f"Error getting available Cortex models: {str(e)}."
                     "Visit https://docs.snowflake.com/user-guide/snowflake-cortex/aisql?lang=de%2F#refresh-model-objects-and-application-roles for  more information.")
            # Set default and only model to llama3.1-8b
            st.session_state['available_models'] = ["llama3.1-8b"]


def termination_callback():
    """Callback function to terminate the running query."""

    if 'async_job' in st.session_state and st.session_state.get('async_job') is not None:
        if st.session_state.async_job.is_done():
            st.info('Crawling already completed', icon=":material/check_circle:")
        else:
            st.session_state.async_job.cancel()
            st.toast(':red-background[Crawling cancelled]')


def test_complete(session, model, prompt = "Repeat the word hello once and only once. Do not say anything else.") -> bool:
    """Returns True if selected model is supported in region and returns False otherwise."""
    try:
        response = Complete(model, prompt, session = session)
        return True
    except SnowparkSQLException as e:
        if 'unknown model' in str(e):
            return False


def make_table_list(session,
                    target_database,
                    target_schema = None):
    """Returns list of selectable tables in database and, optionally schema."""
    target_schema_clause = f"AND TABLE_SCHEMA='{target_schema}'" if target_schema else ""
    query = f"""
    SELECT
       TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
        FROM {target_database}.INFORMATION_SCHEMA.tables
        WHERE 1=1
            AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA' {target_schema_clause}
            AND (ROW_COUNT >= 1 OR ROW_COUNT IS NULL)
            AND IS_TEMPORARY = 'NO'
            AND NOT STARTSWITH(TABLE_NAME, '_')
    """
    table_results = session.sql(query).collect()
    tables = [row['TABLENAME'] for row in table_results]
    return tables

@st.cache_data
def get_databases(_session):
    """Returns list of available databases."""
    database_result = _session.sql("SHOW DATABASES").collect()
    return [row['name'] for row in database_result]

def get_schemas(session):
    """Returns list of available schemas in database."""
    if st.session_state['db']:
        schema_result = session.sql(f"SHOW SCHEMAS IN DATABASE {st.session_state['db']}").collect()
        return [row['name'] for row in schema_result]
    else:
        return []


# @st.experimental_dialog("Table selection.") # Coming soon with experimental_dialog GA
def specify_tables(session):
    """Creates a table selection interface for the user."""
    with st.expander("Table Selection (optional)"):
        st.caption("Specify tables to include or exclude.")
        if st.session_state['db']:
            split_selection = 2 if st.session_state['schema'] else 1
            selectable_tables = make_table_list(session, st.session_state['db'], st.session_state['schema'])
        else:
            selectable_tables = []
        exclude_flag = st.toggle("Exclude tables")
        specified_tables = st.multiselect("",
                                        options = selectable_tables,
                                        format_func = lambda x: ".".join(x.split(".")[split_selection:]),
                                        default = [])
        st.session_state['include_tables'] = []
        st.session_state['exclude_tables'] = []
        if specified_tables:
            if exclude_flag:
                st.session_state['exclude_tables'] = specified_tables
            else:
                st.session_state['include_tables'] = specified_tables


# Streamlit App Page Layout
st.set_page_config(layout="wide", page_title="Data Catalog Runner", page_icon="🧮")
get_available_models(session)
st.title("Catalog Tables ❄️")
st.subheader("Specify databases or schemas to crawl")

st.caption("Specify Snowflake data to crawl.")
d_col1, d_col2 = st.columns(2)
with d_col1:
    st.session_state['db'] = st.selectbox("Database",
                                          options = get_databases(session),
                                          index = None,
                                          placeholder="Select a database")
with d_col2:
    st.session_state['schema'] = st.selectbox("Schema (optional)",
                                               options = get_schemas(session),
                                               index = None,
                                               placeholder="Select a schema")
specify_tables(session)
st.divider()
st.caption("Select crawling parameters.")

# Description generation method selection
st.subheader("Description Generation Mode")
st.write("Choose how you want to generate table descriptions:")

mode_options = ["**Standard**: ⚡ Native AI Function", "**Advanced**: 🤖 LLM with Custom Prompts"]

# Default to Standard mode
generation_method = st.radio(
    "",
    options=mode_options
)

if generation_method == mode_options[1]:
    st.info("📝 **LLM with Custom Prompts**: Uses Cortex LLM models with customizable prompts and data sampling for detailed, context-aware descriptions.")
else:
    st.info("⚡ **Native AI Function**: Uses Snowflake's built-in AI_GENERATE_TABLE_DESC function for fast, automated table analysis.")

# Set use_native_feature based on selection
use_native_feature = (generation_method == mode_options[0])

st.divider()

# Common options that apply to both methods
st.subheader("Additional Options")
col1, col2 = st.columns([1, 1], gap="small")
with col1:
    replace_catalog = st.checkbox("Replace catalog descriptions",
                                help = "Select True to regenerate and replace table descriptions.")
with col2:
    update_comment = st.checkbox("Replace table comments",
                                help = "Select True to update table comments with generated descriptions.")

# Show detailed parameters for non-native approach
if not use_native_feature:
    st.caption("Configure LLM parameters for custom description generation.")
    p_col1, p_col2, p_col3 = st.columns(3)
    with p_col1:
        sampling_mode = st.selectbox("Sampling strategy",
                                    ("fast", "nonnull"),
                                    placeholder="fast",
                                    help = "Select fast to randomly sample or non-null to prioritize non-empty values.")
    with p_col2:
        n = st.number_input("Sample rows",
                           min_value = 1,
                           max_value = 10,
                           value = 5,
                           step = 1,
                           format = '%i')
    with p_col3:
        model = st.selectbox("Cortex LLM",
                        st.session_state['available_models'],
                        index=st.session_state['available_models'].index("llama3.1-8b") if "llama3.1-8b" in st.session_state['available_models'] else 0,
                        help = "Select LLM to generate table descriptions.")
else:
    st.success("✅ **Ready to go!** The Native AI Function requires no additional configuration.")
    # Set default values for native approach (these will be ignored but are required for the procedure call)
    sampling_mode = "fast"
    n = 5
    model = "mistral-7b"

st.divider()

submit_button = st.button("Submit",
                          disabled = False if st.session_state.get('db', None) else True)
# Once the button is clicked, check if the model is available in the region
# and then crawl the data
# and generate descriptions
# and update the catalog
# and update the comments
# and show the results

if submit_button:
    # Initialize async_job state if not already set
    if 'async_job' not in st.session_state:
        st.session_state['async_job'] = None

    # Create status container
    status = st.status('Initializing...', expanded=True)

    # Skip model check for native feature since it doesn't use LLM models
    model_available = True if use_native_feature else test_complete(session, model)

    if not use_native_feature:
        status.update(label='Checking model availability', expanded=True)
        if model_available:
            status.update(
            label="Model available", expanded=True
            )
        else:
            status.update(
            label="Model not available in your region. Please select another model.", state="error", expanded=False
            )

    if model_available:
        status.update(label="Crawling data..", expanded=True)

        spinner_message = 'Using native AI to analyze tables' if use_native_feature else 'Generating descriptions with LLM'
        with st.spinner(spinner_message, show_time=True):
            if not st.session_state['schema']: # Fix sending schema as string None
                st.session_state['schema'] = ''

            df_results = None
            try:
                query = f"""
                CALL DATA_CATALOG(target_database => '{st.session_state["db"]}',
                                        catalog_database => 'DATA_CATALOG',
                                        catalog_schema => 'TABLE_CATALOG',
                                        catalog_table => 'TABLE_CATALOG',
                                        target_schema => '{st.session_state["schema"]}',
                                        include_tables => {st.session_state["include_tables"]},
                                        exclude_tables => {st.session_state["exclude_tables"]},
                                        replace_catalog => {bool(replace_catalog)},
                                        sampling_mode => '{sampling_mode}',
                                        update_comment => {bool(update_comment)},
                                        use_native_feature => {bool(use_native_feature)},
                                        n => {int(n)},
                                        model => '{model}'
                                        )
                """
                # Start the procedure asynchronously
                df = session.sql(query)
                async_job = df.collect_nowait() # returns immediately with query ID
                st.session_state['async_job'] = async_job

                # Button for user to terminate the running query
                cancel_button = st.empty()
                with cancel_button.container():
                    st.button('Terminate Crawl', key='terminate_run', on_click=termination_callback, icon=":material/stop_circle:", type="primary")

                # Wait for results and reset the async_job state
                df_results = async_job.result()
                st.session_state['async_job'] = None # Reset the async_job state

                # Configure DataFrame display based on update_comment
                column_order = ['COMMENT_UPDATED', 'TABLENAME', 'DESCRIPTION']
                column_config = {
                    "COMMENT_UPDATED": st.column_config.Column(
                        "Comment Updated",
                        help="Comment was updated successfully",
                        width="small",
                        required=True,
                    ),
                    "TABLENAME": st.column_config.Column(
                        "Table Names",
                        help="Snowflake Table Names",
                        width="medium",
                        required=True,
                    ),
                    "DESCRIPTION": st.column_config.Column(
                        "Table Descriptions",
                        help="LLM-generated table descriptions",
                        width="large",
                        required=True,
                    )
                }

            except Exception as e:
                st.session_state['async_job'] = None  # Reset after error
                st.warning(f"Error generating descriptions. Error: {str(e)}")
                status.update(label="Crawl Incomplete", state="error", expanded=False)

            # Remove the cancel button
            cancel_button.empty()

            # Display the results if successful
            if df_results is not None and len(df_results) > 0:
                st.dataframe(df_results,
                            use_container_width=True,
                            hide_index=True,
                            column_order=column_order,
                            column_config=column_config)
                # time.sleep(5)
                st.write("Visit **manage** to update descriptions.")
                status.update(label="Done", state="complete", expanded=False)
