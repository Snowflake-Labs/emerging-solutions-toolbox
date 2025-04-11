import time
import streamlit as st
import pandas as pd
from snowflake.cortex import Complete
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()
models = [
    'llama3.2-1b',
    'llama3.2-3b',
    'llama3.1-8b',
    'llama3.1-70b',
    'llama3.1-405b',
    'snowflake-arctic',
    'reka-core',
    'reka-flash',
    'mistral-large2',
    'mixtral-8x7b',
    'mistral-7b',
    'jamba-instruct',
    'jamba-1.5-mini',
    'jamba-1.5-large',
    'gemma-7b',
]

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
    database_result = _session.sql("SHOW DATABASES").collect()
    return [row['name'] for row in database_result]

def get_schemas(session):
    if st.session_state['db']:
        schema_result = session.sql(f"SHOW SCHEMAS IN DATABASE {st.session_state['db']}").collect()
        return [row['name'] for row in schema_result]
    else:
        return []

# @st.experimental_dialog("Table selection.") # Coming soon with experimental_dialog GA
def specify_tables(session):
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

st.set_page_config(layout="wide", page_title="Data Catalog Runner", page_icon="🧮")
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

replace_catalog = st.checkbox("Replace catalog descriptions",
                            help = "Select True to regenerate and replace table descriptions.")
update_comment = st.checkbox("Replace table comments",
                            help = "Select True to update table comments with generated descriptions.")
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
                                models,
                                placeholder="mistral-7b",
                                help = "Select LLM to generate table descriptions.")

submit_button = st.button("Submit",
                          disabled = False if st.session_state.get('db', None) else True)

if submit_button:
    with st.status('Checking model availability') as status:
        model_available = test_complete(session, model)
        if model_available:
            status.update(
            label="Model available", state="complete", expanded=False
        )
        else:
            status.update(
            label="Model not available in your region. Please select another model.", state="error", expanded=False
            )
    if model_available:    
        with st.spinner('Crawling data...generating descriptions'):
            if not st.session_state['schema']: # Fix sending schema as string None
                st.session_state['schema'] = ''
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
                                        n => {int(n)},
                                        model => '{model}'
                                        )
                """
                df = session.sql(query)
                st.dataframe(df,
                            use_container_width=True,
                            hide_index = True,
                            column_order=['TABLENAME', 'DESCRIPTION'],
                            column_config={
                "TABLENAME": st.column_config.Column(
                    "Table Names",
                    help="Snowflake Table Names",
                    width=None,
                    required=True,
                ),
                "DESCRIPTION": st.column_config.Column(
                    "Table Descriptions",
                    help="LLM-generated table descriptions",
                    width="large",
                    required=True,
                )                   
                })
                # time.sleep(5)
                st.write("Visit **manage** to update descriptions.")
            except Exception as e:
                st.warning(f"Error generating descriptions. Error: {str(e)}")