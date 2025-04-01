import streamlit as st
import pandas as pd
import math
import time
import json
import sqlparse

def read_sql(query, session, index):
    """
    Execute a SQL query and return a specific column from the result.

    Parameters:
    query (str): The SQL query to be executed.
    session (object): The session object to interact with the database.
    index (int): The index of the column to be returned from the query result.

    Returns:
    list: A list containing the values of the specified column from the query result.
    """
    result = session.sql(query).collect()
    return [row[index] for row in result]

def check_has_data(variable):
    """
    Check if a variable has data, i.e., it is not an empty string and not None.

    Parameters:
    variable: The variable to check.

    Returns:
    bool: True if the variable has data (is not an empty string and not None), False otherwise.
    """
    return variable != '' and variable is not None


def read_table(query, session):
    """
    Execute a SQL query and return the result as a pandas DataFrame.

    Parameters:
    query (str): The SQL query to be executed.
    session (object): The session object to interact with the database.

    Returns:
    pandas.DataFrame: The query result as a pandas DataFrame.
    """
    return session.sql(query).to_pandas()

def get_databases(session):
    """
    Retrieve the list of databases from the session.

    Parameters:
    session (object): The session object to interact with the database.

    Returns:
    list: A list of database names.
    """
    query = "SHOW DATABASES"
    return read_sql(query, session, 1)

def get_schemas(session, database):
    """
    Retrieve the list of schemas within a specific database.

    Parameters:
    session (object): The session object to interact with the database.
    database (str): The name of the database to retrieve schemas from.

    Returns:
    list: A list of schema names within the specified database.
    """
    query = f"SHOW SCHEMAS IN DATABASE {database}"
    return read_sql(query, session, 1)

def convert_to_k_m(value):
    """
    Convert a numerical value into a string representation with 'K' for thousands and 'M' for millions.

    Parameters:
    value (int or float): The numerical value to be converted. Can be None.

    Returns:
    str: The value converted to a string with 'K' for thousands or 'M' for millions. If the value is None, returns 'N/A'.
    """
    if value is None:
        return 'N/A'
    
    if value >= 1_000_000:
        # If the value is 1,000,000 or greater, convert to millions and append 'M'
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        # If the value is 1,000 or greater, but less than 1,000,000, convert to thousands and append 'K'
        return f"{value / 1_000:.1f}K"
    else:
        # If the value is less than 1,000, return it as a string
        return str(value)
    
def get_tables(session, database, schema):
    """
    Retrieve the list of tables within a specific schema of a database.

    Parameters:
    session (object): The session object used to interact with the database.
    database (str): The name of the database containing the schema.
    schema (str): The name of the schema from which to retrieve tables.

    Returns:
    list: A list of table names within the specified schema.
    """
    # SQL query to show all tables in the specified schema
    query = f"""
    SELECT TABLE_NAME, TABLE_TYPE FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'
    """
    
    # Execute the query and return the list of table names (first column in the result)
    return read_sql(query, session, 0)

def reset_session_state_on_selection_change():
    """
    Reset session state values when the selected table changes.
    """
    keys_to_reset = [
        'table_data', 'metadata_raw', 'dataset', 'row_count', 'table_size_mb',
        'dynamic_row_count', 'metadata', 'primary_filters_df', 'secondary_filters_df',
        'filter_values', 'primary_where_clause', 'final_where_clause', 'text_filter_conditions',
        'secondary_filter_values', 'cohort_row_count', 'base_filter_query', 'selected_table_full', 
        'save_changes_pressed'
    ]
    
    for key in keys_to_reset:
        st.session_state.pop(key, None)

def create_metadatacard_html(header="Header", value="Value", description="Description", card_bg_color="#ffffff", header_bg_color="#000000"):
    """
    Create an HTML string for a styled card component.

    Parameters:
    header (str): The text to display in the card header. Default is "Header".
    value (str): The main value to display in the card. Default is "Value".
    description (str): A description to display below the main value. Default is "Description".
    card_bg_color (str): The background color for the card. Default is "#ffffff".
    header_bg_color (str): The background color for the card header. Default is "#000000".

    Returns:
    str: An HTML string representing the styled card.
    """
    return f"""
    <div style="background-color: {card_bg_color}; border-radius: 10px; padding: 10px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1); margin: 10px; width: 180px; height: 150px;">
        <div style="background-color: {header_bg_color}; color: white; padding: 5px 5px 15px; border-radius: 5px 5px 0 0; text-align: center; height: 30px;">
            <h5 style="margin: 0; color:white; font-size: 0.9em; line-height: 30px;">{header}</h5>
        </div>
        <div style="padding: 10px; text-align: center;">
            <h4 style="margin: 0; color: black; font-size: 2em;">{value}</h4>
            <p style="margin: 0; color: gray; font-size: 0.8em;">{description}</p>
        </div>
    </div>
    """

@st.cache_data
def fetch_table_data(_session, table_name):
    """
    Fetch data from the specified table using the provided session object.

    Parameters:
    _session (object): The session object used to interact with the database.
    table_name (str): The name of the table from which to fetch data.

    Returns:
    pd.DataFrame: A pandas DataFrame containing the data from the specified table.
    """
    return load_table_data(_session, table_name)

def load_table_data(session, table):
    """
    Load data from the specified table using the provided session object.

    Parameters:
    session (object): The session object used to interact with the database.
    table (str): The name of the table from which to load data.

    Returns:
    tuple: A tuple containing:
        - pd.DataFrame: A pandas DataFrame containing a sample of rows from the specified table.
        - int: The total number of rows in the specified table.
    """
    # SQL query to fetch a sample of rows from the table
    query = f"SELECT * FROM {table} SAMPLE (10 ROWS)"
    
    # SQL query to count the total number of rows in the table
    row_count_query = f"SELECT count(*) FROM {table}"
    
    # Execute the queries and return the results as a tuple
    return read_table(query, session), read_sql(row_count_query, session, 0)

def get_table_metadata(session, table_name):
    """
    Retrieve metadata for columns in the specified table.

    Parameters:
    session (object): The session object used to interact with the database.
    table_name (str): The fully qualified name of the table (in the format 'database.schema.table').

    Returns:
    pd.DataFrame: A pandas DataFrame containing metadata for the columns in the specified table.
    """
    # Split the fully qualified table name into database, schema, and table parts
    parts = table_name.split(".")
    if len(parts) == 3:
        database_name, schema_name, table_name = parts

    # SQL query to retrieve column metadata from the information schema
    query = f"""
        SELECT COLUMN_NAME,
               DATA_TYPE,
               IFNULL(COMMENT, '') AS DESCRIPTION,
               '' AS FILTER_TYPE 
        FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'
    """
    
    # Execute the query and return the result as a pandas DataFrame
    return read_table(query, session)

def fetch_table_size(session, database, schema, table):
    """
    Fetch the size of the specified table in a human-readable format.

    Parameters:
    session (object): The session object used to interact with the database.
    database (str): The name of the database containing the table.
    schema (str): The name of the schema containing the table.
    table (str): The name of the table for which to fetch the size.

    Returns:
    str: The size of the table in a human-readable format (B, KB, MB, GB, TB).
    """
    try:
        # SQL query to fetch the active bytes of the specified table from the information schema
        query = f"""
            SELECT
                "ACTIVE_BYTES"
            FROM {database}.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
            WHERE table_catalog = '{database}'
            AND table_schema = '{schema}'
            AND table_name = '{table}'
        """
        
        # Execute the query and get the result
        result = read_sql(query, session, 0)[0]

        # If the result is 0, return "0B"
        if result == 0:
            return "0B"
        
        # Define size units
        size_units = ("B", "KB", "MB", "GB", "TB")
        
        # Calculate the appropriate unit
        i = int(math.floor(math.log(float(result), 1024)))
        p = math.pow(1024, i)
        s = round(float(result) / p, 0)
        
        # Return the size in a human-readable format
        return f"{s} {size_units[i]}"
    
    except:

        return f"0 MB"
    
def generate_columns_info_json(table_df, table_metadata_df, session, num_samples=3):
    """
    Generate a JSON representation of columns info using sample values from the dataset.

    Parameters:
    - table_df (pd.DataFrame): Dataframe containing the table data.
    - table_metadata_df (pd.DataFrame): Dataframe containing metadata of the table.
    - session: Database session.
    - num_samples (int): Number of sample values to extract.

    Returns:
    - pd.DataFrame: Dataframe with enriched metadata including LLM descriptions.
    """
    # Convert datetime columns to string format
    for col in table_df.columns:
        if pd.api.types.is_datetime64_any_dtype(table_df[col]):
            table_df[col] = table_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            table_df[col] = table_df[col].astype(str)

    # Extract sample values from the dataframe
    sample_values = table_df.head(num_samples).transpose().reset_index()
    sample_values.columns = ['COLUMN_NAME'] + [f'Sample Value {i+1}' for i in range(sample_values.shape[1] - 1)]
    sample_values = sample_values.applymap(str)
    sample_values['SAMPLE_VALUES'] = sample_values.apply(lambda row: row[1:].values.tolist(), axis=1)
    
    # Merge sample values with the metadata
    merged_df = pd.merge(table_metadata_df, sample_values[['COLUMN_NAME', 'SAMPLE_VALUES']], on='COLUMN_NAME')

    # Construct JSON output for LLM processing
    columns_info = [
        {
            'COLUMN_NAME': row['COLUMN_NAME'],
            'DATA_TYPE': row['DATA_TYPE'],
            'SAMPLE_VALUES': row['SAMPLE_VALUES']
        }
        for index, row in merged_df.iterrows()
    ]

    json_output = json.dumps(columns_info, indent=4)
    df = get_llm_data_dict(json_output, session)

    # Merge LLM output with the original metadata
    return pd.merge(df, table_metadata_df[['COLUMN_NAME', 'DATA_TYPE']], on='COLUMN_NAME')

def get_llm_data_dict(data_json, session):
    """
    Get column descriptions and metadata using an LLM model.

    Parameters:
    - data_json (str): JSON string of column data.
    - session: Database session.

    Returns:
    - pd.DataFrame: Dataframe with LLM-enriched metadata.
    """
    data_json = json.dumps(data_json).replace("'", "''")
    datadict_query = f"""
        SELECT
            SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            [
                {{
                    'role': 'system', 'content': '
                    You are an AI Business Intelligence Assistant. Given a JSON input, process it and return a JSON output with the following attributes for each column:

                    - COLUMN_NAME: The column name from the JSON input.
                    - DESCRIPTION: A brief explanation of the column purpose, excluding data type.
                    - COLUMN_TYPE: Specify if the column is a Fact or Dimension.
                    - FILTER_TYPE: Applicable filter type (e.g., dropdown, text, multi-select, boolean, slider, top N, range, date). Label as Unknown if unclear.
                    - PRIMARY_FILTER: Identify 2 primary filters among the columns and set this attribute to True for them. Set to False for others.

                    '
                }},
                {{
                    'role': 'user',
                    'content': '
                        Here is a Json. Process it and return a JSON with COLUMN_NAME, DESCRIPTION, COLUMN_TYPE and FILTER_TYPE attributes filled.
                        <json>{data_json}</json>'
                }}
            ],
            {{
                'temperature': 1,
                'max_tokens': 1000000
            }}
        ) as output,
        try_parse_json(parse_json(output:choices)[0]):messages as response,
        charindex('[', response,1) as start_index,
        charindex(']', response,1) as end_index,
        try_parse_json(SUBSTRING(response, start_index, end_index - start_index +2)) as response_json;
    """
    
    with st.spinner("Please wait..."):
        # Execute the query and get the response JSON
        response_json = read_table(datadict_query, session)['RESPONSE_JSON'][0]

    df = pd.DataFrame(json.loads(response_json))

    return df

def process_columns(session, df, where_clause=''):
    """
    Process columns to generate min, max, and distinct values for primary filters.

    Parameters:
    - session: Database session.
    - df (pd.DataFrame): Dataframe with column metadata.
    - where_clause (str): Additional SQL where clause.

    Returns:
    - pd.DataFrame: Dataframe with processed column values.
    """
    table_name = st.session_state.selected_table_full

    # Initialize an empty list to store the results
    result_dfs = []

    for index, row in df.iterrows():
        column_name = row['COLUMN_NAME']
        data_type = row['DATA_TYPE']
        filter_type = row['FILTER_TYPE']

        # Call the stored procedure for each column and get the result
        result_df = session.call('cohort_builder_for_snowflake.app.get_column_stats', column_name, data_type, filter_type, table_name, where_clause)
        
        # Convert the result to a pandas DataFrame and append to the list
        result_dfs.append(result_df.to_pandas())

    # Concatenate all the result DataFrames
    combined_df = pd.concat(result_dfs, ignore_index=True)

    # Merge the result with the original dataframe
    combined_df = pd.merge(df, combined_df, on=['COLUMN_NAME', 'DATA_TYPE', 'FILTER_TYPE'], how='inner')

    return combined_df

def render_header(title, description):
        """
        Render the header section of the app with custom styling.
        """
        st.markdown("""
            <style>
            .header {
                font-size: 32px;
                font-weight: 600;
                color: #87ceeb;
                display: flex;
                align-items: center;
            }
            .header img {
                margin-right: 10px;
            }
            .description {
                color: #ede8e8;
                font-size: 16px;
                font-weight: bold;
            }
            .horizontal-line {
                border: none;
                height: 50px;
                background: #e6e6fa;
                margin-top: 0px;
                margin-bottom: 20px;
            }
            div[data-testid="stExpander"]  {
                background-color: #626868;  
                color: #F9F7F3;  
                border: none;  
            }
            </style>
            """, unsafe_allow_html=True)

        st.markdown(f"""
            <div class="header">
                <span style=font-weight: bold;">{title}</span>
            </div>
            <hr class="horizontal-line"/>
            <p class="description">
                {description}
            </p>
            """, unsafe_allow_html=True)

def gen_where_clause_for_non_text_filters(df, filter_values):
    """
    Generate WHERE clause for non-text filters.

    Parameters:
    - df (pd.DataFrame): DataFrame containing column metadata.
    - filter_values (dict): Dictionary with filter values for columns.

    Returns:
    - str: WHERE clause for non-text filters.
    """
    clauses = []
    st.write(filter_values)
    for column_name, value in filter_values.items():

        if value is None:
            continue

        # Getting the column from the dataframe
        column_df = df[df['COLUMN_NAME'] == column_name]

        if not column_df.empty:
            row = column_df.iloc[0]
            filter_type = row['FILTER_TYPE']

            if filter_type == "dropdown":
                if value != "":
                    clauses.append(f"{column_name} = '{value}'")

            elif filter_type == "multi-select dropdown":
                if value:
                    # Filter out empty strings
                    filtered_values = [v for v in value if v != ""]
                    if filtered_values:
                        values_str = ', '.join([f"'{v}'" for v in filtered_values])
                        clause = f"{column_name} IN ({values_str})"
                        clauses.append(clause)

            elif filter_type == "boolean filter":
                clauses.append(f"{column_name} = {value}")

            elif filter_type == "slider" or filter_type == "range filter":
                min_val, max_val = value
                clauses.append(f"{column_name} BETWEEN {min_val} AND {max_val}")

            elif filter_type == "advanced date filter":
                condition = value.get("condition")
                if condition == "between":
                    start_date, end_date = value.get("value1"), value.get("value2")
                    clauses.append(f"{column_name} BETWEEN '{start_date}' AND '{end_date}'")
                elif condition == "greater than or equal":
                    date_val = value.get("value")
                    clauses.append(f"{column_name} >= '{date_val}'")
                elif condition == "less than or equal":
                    date_val = value.get("value")
                    clauses.append(f"{column_name} <= '{date_val}'")

            elif filter_type == 'date filter':
                min_date, max_date = value
                clauses.append(f"{column_name} BETWEEN '{min_date}' AND '{max_date}'")

            elif filter_type == "advanced numeric filter":
                condition = value.get("condition")
                if condition == "between":
                    min_val, max_val = value.get("value1"), value.get("value2")
                    clauses.append(f"{column_name} BETWEEN {min_val} AND {max_val}")
                elif condition == "greater than":
                    val = value.get("value")
                    clauses.append(f"{column_name} > {val}")
                elif condition == "less than":
                    val = value.get("value")
                    clauses.append(f"{column_name} < {val}")
                elif condition == "equal to":
                    val = value.get("value")
                    clauses.append(f"{column_name} = {val}")

    return " AND ".join(clauses)



def gen_where_clause_for_text_filter(conditions):
    """
    Generate WHERE clause for text filters.

    Parameters:
    - conditions (dict): Dictionary with text filter conditions.

    Returns:
    - str: WHERE clause for text filters.
    """
    where_clauses = []
    for column, (condition_tuples, logical_operator) in conditions.items():
        clauses = []
        for condition_type, condition_value in condition_tuples:
            if condition_type == "Contains":
                clauses.append(f"{column} LIKE '%{condition_value}%'")
            elif condition_type == "Does not contain":
                clauses.append(f"{column} NOT LIKE '%{condition_value}%'")
            elif condition_type == "Equals":
                clauses.append(f"{column} = '{condition_value}'")
            elif condition_type == "Does not equal":
                clauses.append(f"{column} != '{condition_value}'")
            elif condition_type == "Begins with":
                clauses.append(f"{column} LIKE '{condition_value}%'")
            elif condition_type == "Ends with":
                clauses.append(f"{column} LIKE '%{condition_value}%'")
            elif condition_type == "Blank":
                clauses.append(f"{column} IS NULL")
            elif condition_type == "Not blank":
                clauses.append(f"{column} IS NOT NULL")

        if clauses:
            where_clauses.append(f" ({f' {logical_operator} '.join(clauses)}) ")

    where_clause =  " AND ".join(where_clauses) if where_clauses else ""
    
    return where_clause if where_clause else "true"


def call_upsert_cohorts(session, cohort_name, table_name, column_list, cohort_selections_json):
    """
    Call the UPSERT_COHORTS stored procedure using an existing Snowpark session.

    :param session: Snowpark session object
    :param cohort_name: Name of the cohort
    :param table_name: Name of the table
    :param column_list: List of columns or pandas Series
    :param cohort_selections_json: JSON string of filter values
    :return: The result of the procedure call
    """
    try:
        # Prepare the call to the stored procedure
        proc_name = "COHORT_BUILDER_FOR_SNOWFLAKE.APP.UPSERT_COHORTS"
        
        # Ensure column_list is a list
        if not isinstance(column_list, list):
            column_list = column_list.tolist()

        # Call the stored procedure
        result = session.call(proc_name, cohort_name, table_name, column_list, cohort_selections_json)

        # Print the result
        return result

    except Exception as e:
        # Handle any exceptions that may occur
        st.warning(e)
        return None

    
def format_sql(sql_statement):
    """
    Formats a SQL statement using sqlparse and returns the formatted SQL.

    Args:
    sql_statement (str): The SQL statement to format.

    Returns:
    str: The formatted SQL statement.
    """
    formatted_sql = sqlparse.format(sql_statement, reindent=True, keyword_case='upper')
    return formatted_sql

class BuildCohort:
    def __init__(self):
        # Initialize session state variables
        self.header_bg_color = st.session_state.header_bg_color
        self.card_bg_color = st.session_state.card_bg_color

    def set_page_config(self):
        """
        Set the configuration for the Streamlit page.
        """
        st.set_page_config(
            page_title="Build Cohort",
            page_icon="👨‍👩‍👦",
            layout="wide",
            initial_sidebar_state="expanded"
        )

    def run(self):
        """
        Main method to run the application.
        """
        self.set_page_config()
        col1, col2, col3, col4 = st.columns([1,1,1,1])

        select_dataset = SelectDataset()
        data_dictionary = DataDictionary()
        finalize_cohort = FinalizeCohort()
        schedule_cohort = ScheduleCohorts()

        if col1.button("Select Dataset", use_container_width = True, type = "primary" if st.session_state.build_cohort_selected_tab == 1 else "secondary"):
            st.session_state.build_cohort_selected_tab = 1
            st.rerun()
        if col2.button("Data Dictionary", use_container_width = True, type = "primary" if st.session_state.build_cohort_selected_tab == 2 else "secondary"):
            st.session_state.build_cohort_selected_tab = 2
            st.rerun()
        if col3.button("Build Cohort", use_container_width = True, type = "primary" if st.session_state.build_cohort_selected_tab == 3 else "secondary"):
            st.session_state.build_cohort_selected_tab = 3
            st.rerun()
        if col4.button("Schedule Cohort", use_container_width = True, type = "primary" if st.session_state.build_cohort_selected_tab == 4 else "secondary"):
            st.session_state.build_cohort_selected_tab = 4
            st.rerun()

        if st.session_state.build_cohort_selected_tab == 1:
            render_header('Data Model', '')
            select_dataset.run()

        if st.session_state.build_cohort_selected_tab == 2:
            render_header('Data Dictionary', 
                          'Choose primary and secondary filters from the columns below. Primary filters will be applied first to reduce the dataset size, followed by secondary filters to finalize your cohort. Click ''Process Metadata'' to start processing the data.'
                          )
            data_dictionary.run()   

        if st.session_state.build_cohort_selected_tab == 3:
            st.session_state.show_dialog = False
            render_header('Build Cohort', 'First use the Primary Filters on the sidebar to filter down the dataset. The Secondary Filters are then populated for you to build your Cohort.')
            finalize_cohort.run()  

        if st.session_state.build_cohort_selected_tab == 4:
            st.session_state.show_dialog = False
            render_header('Schedule Cohort', 'First use the Primary Filters on the sidebar to filter down the dataset. The Secondary Filters are then populated for you to build your Cohort.')
            schedule_cohort.run()  


class SelectDataset:

    def render_table_selection(self):

        database_list = ['']
        schema_list = ['']
        table_list = ['']

        database_list[1:] = get_databases(st.session_state.session)

        def on_database_selection():
            """
            Callback function to handle database selection change.
            """
            st.session_state.selected_database = st.session_state.databases
            st.session_state.selected_schema = ''
            st.session_state.selected_table = ''
            schema_list.clear()
            table_list.clear()

        # Database selection
        db_ind = database_list.index(st.session_state.selected_database) if st.session_state.selected_database in database_list else 0
        st.session_state.selected_database = st.selectbox(
            'Database:', 
            database_list, 
            index=db_ind,
            key="databases",
            on_change=on_database_selection
        )

        # Fetch schemas if a database is selected
        if check_has_data(st.session_state.selected_database):
            schema_list[1:] = get_schemas(st.session_state.session, st.session_state.selected_database)

            def on_schema_selection():
                """
                Callback function to handle schema selection change.
                """
                st.session_state.selected_schema = st.session_state.schemas
                st.session_state.selected_table = ''
                table_list.clear()

            # Schema selection
            sch_ind = schema_list.index(st.session_state.selected_schema) if st.session_state.selected_schema in schema_list else 0
            st.session_state.selected_schema = st.selectbox(
                'Schema:', 
                schema_list, 
                key="schemas",
                index=sch_ind,
                on_change=on_schema_selection
            )

            # Fetch tables if a schema is selected
            if check_has_data(st.session_state.selected_schema):
                table_list[1:] = get_tables(st.session_state.session, st.session_state.selected_database, st.session_state.selected_schema)

                def on_table_selection():
                    """
                    Callback function to handle table selection change.
                    """
                    st.session_state.selected_table = st.session_state.tables

                # Table selection
                tbl_ind = table_list.index(st.session_state.selected_table) if st.session_state.selected_table in table_list else 0
                st.session_state.selected_table = st.selectbox(
                    'Table:', 
                    table_list, 
                    key="tables",
                    index=tbl_ind,
                    on_change=on_table_selection
                )

    def display_data(self):
        """
        Display the data and metadata of the selected table, including summary cards.
        """
        with st.container():
            # Header with icon
            c = st.container()

            # Generate cards
            row_card = create_metadatacard_html(
                header="Rows",
                value=convert_to_k_m(st.session_state.row_count),
                description="Rows",
                card_bg_color=st.session_state.card_bg_color,
                header_bg_color=st.session_state.header_bg_color
            )

            memory_card = create_metadatacard_html(
                header="Memory",
                value=f"{st.session_state.table_size_mb}" if st.session_state.table_size_mb is not None else "N/A",
                description="Memory",
                card_bg_color=st.session_state.card_bg_color,
                header_bg_color=st.session_state.header_bg_color
            )

            column_card = create_metadatacard_html(
                header="Columns",
                value=st.session_state.dataset.shape[1] if st.session_state.dataset is not None else "N/A",
                description="Columns",
                card_bg_color=st.session_state.card_bg_color,
                header_bg_color=st.session_state.header_bg_color
            )

            # Display the cards
            col1, col2, col3 = c.columns(3)
            col1.markdown(row_card, unsafe_allow_html=True)
            col2.markdown(memory_card, unsafe_allow_html=True)
            col3.markdown(column_card, unsafe_allow_html=True)

            st.markdown("""---""") 
            # Display the dataset
            if st.session_state.dataset is not None:
                st.dataframe(st.session_state.dataset, hide_index=True)

    def check_dataset_selection(self):
        """
        Check if the selected dataset has changed and fetch the corresponding data if it has.
        """
        dataset_selection_changed = (
            st.session_state.selected_database != st.session_state.prev_selected_database or
            st.session_state.selected_schema != st.session_state.prev_selected_schema or
            st.session_state.selected_table != st.session_state.prev_selected_table
        )

        if dataset_selection_changed:

            reset_session_state_on_selection_change()

            st.session_state.prev_selected_database = st.session_state.selected_database
            st.session_state.prev_selected_schema = st.session_state.selected_schema
            st.session_state.prev_selected_table = st.session_state.selected_table

            if check_has_data(st.session_state.selected_table):
                st.session_state.selected_table_full = f"{st.session_state.selected_database}.{st.session_state.selected_schema}.{st.session_state.selected_table}"
                st.session_state.table_data = fetch_table_data(st.session_state.session, st.session_state.selected_table_full)
                st.session_state.metadata_raw = get_table_metadata(st.session_state.session, st.session_state.selected_table_full)
                st.session_state.metadata_raw['IS_PRIMARY'] = False
                st.session_state.metadata_raw['IS_SECONDARY'] = False        
                st.session_state.dataset = st.session_state.table_data[0]
                st.session_state.row_count = st.session_state.table_data[1][0]
                st.session_state.table_size_mb = fetch_table_size(st.session_state.session, st.session_state.selected_database, st.session_state.selected_schema, st.session_state.selected_table)
                st.session_state.dynamic_row_count = st.session_state.row_count

                st.session_state.metadata = st.session_state.metadata_raw
                return True
        return False

    def run(self):

        self.render_table_selection()
        if st.session_state.selected_table:
            if self.check_dataset_selection():
                self.display_data()
            else:
                if 'dataset' in st.session_state:
                    self.display_data()        
        else:
            st.write("Please select a database, schema, and table from the sidebar to view data.")


class DataDictionary:

    def display_sidebar(self):
        """
        Display the sidebar for LLM toggle and form submission.
        """
        with st.sidebar:
            with st.form("llm_toggle_form"):
                llm_data_dict = st.checkbox(
                    "Use LLM for Data Dictionary?",
                    value=st.session_state.llm_data_dict,
                    help="Selecting this option will utilize the **mistral-large** LLM model to identify column types and filter types for all columns in the dataset. It will also populate the descriptions using the LLM. This process takes approximately one minute."
                )
                llm_toggle_submit = st.form_submit_button("Update Dictionary")

                if llm_toggle_submit:
                    st.session_state.llm_data_dict = llm_data_dict  # Update session state with the checkbox value
                    if llm_data_dict:
                        start_time = time.time()
                        max_retries = 2
                        retry_attempts = 0

                        while retry_attempts <= max_retries:
                            try:
                                st.session_state.metadata = generate_columns_info_json(
                                    table_df=st.session_state.dataset,
                                    table_metadata_df=st.session_state.metadata_raw,
                                    session=st.session_state.session
                                )
                                st.success("Metadata successfully updated using the **mistral-large** LLM model.")
                                break  # Exit loop if successful
                            except Exception as e:
                                st.warning(f"Attempt {retry_attempts + 1} failed: {e}")
                                retry_attempts += 1
                                elapsed_time = time.time() - start_time
                                if elapsed_time >= 180:  # 3 minutes
                                    st.error("Exceeded maximum allowed time for processing.")
                                    break
                    else:
                        st.session_state.metadata = st.session_state.metadata_raw
                        st.success("Metadata successfully updated without using the LLM model.")

    def display_expander(self):
        """
        Display the expander section for filter selection and data editing.
        """
        df = st.session_state.metadata
        # Ensure all expected columns are present
        expected_columns = ['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION', 'FILTER_TYPE']
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # Add PRIMARY_FILTER and SECONDARY_FILTER columns if they do not exist
        if 'IS_PRIMARY' not in df.columns:
            df['IS_PRIMARY'] = False
        if 'IS_SECONDARY' not in df.columns:
            df['IS_SECONDARY'] = False

        # Reorder the columns as specified
        df = df[['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION', 'FILTER_TYPE', 'IS_PRIMARY', 'IS_SECONDARY']]

        st.subheader("Select Filters")
        with st.container():
            # Display editable Data Editor
            edited_df = st.data_editor(
                df,
                column_config={
                    "COLUMN_NAME": st.column_config.TextColumn("Column Name", disabled=True),
                    "DATA_TYPE": st.column_config.TextColumn("Data Type", width="small", disabled=True),
                    "DESCRIPTION": st.column_config.TextColumn("Description", width="medium"),
                    "FILTER_TYPE": st.column_config.SelectboxColumn("Filter Type", options=["dropdown", "text filter", "multi-select dropdown", "boolean filter", "range filter", "advanced numeric filter", "advanced date filter", "date filter"]),
                    "IS_PRIMARY": st.column_config.CheckboxColumn("Primary Filter"),
                    "IS_SECONDARY": st.column_config.CheckboxColumn("Secondary Filter"),
                },
                use_container_width=True,
                height=800,
                hide_index=True
            )

            if st.button("Confirm Selection"):
                # Show a spinner while processing the metadata
                with st.spinner("Processing metadata..."):
                    # Save the edited dataframe back to session state
                    st.session_state.metadata = edited_df
                    # Validation check for filter type
                    missing_filter_type = edited_df[
                        ((edited_df["IS_PRIMARY"] == True) | (edited_df["IS_SECONDARY"] == True)) &
                        (edited_df["FILTER_TYPE"].isnull() | (edited_df["FILTER_TYPE"] == "") | (edited_df["FILTER_TYPE"] == "Unknown"))
                    ]

                    if not missing_filter_type.empty:
                        st.error("Please select filter type for all primary and secondary filters.")
                    else:
                        st.session_state.primary_filters_df = process_columns(
                            st.session_state.session, 
                            edited_df[edited_df["IS_PRIMARY"] == True][["COLUMN_NAME", "DATA_TYPE", "FILTER_TYPE"]]
                        )

                        st.session_state.primary_filters_df["IS_PRIMARY"] = True

                        st.session_state.secondary_filters_df = edited_df[
                            (edited_df["IS_SECONDARY"] == True) & (edited_df["IS_PRIMARY"] == False)
                        ][["COLUMN_NAME", "DATA_TYPE", "FILTER_TYPE"]]

                        st.session_state.secondary_filters_df["IS_PRIMARY"] = False

                        self.reset_session_states()

                        st.rerun()


    def reset_session_states(self):
        """
        Reset session states after saving changes and preparing for a new cohort build.
        Clears existing data and configurations, and sets up necessary defaults.
        """

        # Initializing or resetting simple session states to default values
        defaults = {
            'filter_values': {},
            'primary_where_clause': '',
            'final_where_clause': '',
            'text_filter_conditions': {},
            'secondary_filter_values': {},
            'show_secondary_filters': False,
            'cohort_row_count': st.session_state.row_count,
            'base_filter_query': f'SELECT COUNT(*) FROM {st.session_state.selected_table_full} WHERE true',
            'final_query': '',
            'save_changes_pressed': True,
            'col_list': st.session_state.dataset.columns,
            'selected_column_list': st.session_state.dataset.columns,
            'filter_df': pd.DataFrame(),
            'selected_filters': {},  # Assuming initialization is needed
            'preview_dataset': None,
            'is_cohort_saved': False,
            'cohort_name': ''
        }

        # Apply default values to session states, creating them if they don't exist
        for state, value in defaults.items():
            st.session_state[state] = value

        # List of session states to remove, if they exist
        removable_states = [
            'additional_state1',  # Example state names that might need removal
            'additional_state2'
        ]

        # Remove session states that are no longer needed
        for state in removable_states:
            st.session_state.pop(state, None)  # Use pop with None to avoid KeyError if the state does not exist



    def run(self):
        # Ensure 'selected_table' and 'dataset' are set in session state
        if 'selected_table' in st.session_state and st.session_state.selected_table:
            self.display_sidebar()
            self.display_expander()
        else:
            st.warning("No table selected or dataset is not available in the session state.")


class FinalizeCohort:

    def initialize_session_state(self):
        # Initialize session state variables
        session_state_defaults = {
            "filter_values": {},
            "selected_filters": {},
            "preview_dataset": pd.DataFrame(),
            "show_secondary_filters": False,
            "is_cohort_saved": False,
            "show_dialog": False,
            "cohort_name": ""
        }

        for key, default_value in session_state_defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    def update_selected_filters(self):
        for key, value in st.session_state.selected_filters.items():
            st.session_state.filter_values[key] = value

    def remove_secondary_filters_from_session_state(self):
        columns_to_remove = st.session_state.secondary_filters_df["COLUMN_NAME"].tolist()
        
        # Remove columns from selected_filters
        for column in columns_to_remove:
            if column in st.session_state.filter_values :
                del st.session_state.filter_values[column]

        st.session_state.selected_filters = st.session_state.filter_values
        
        # Remove columns from text_filter_conditions
        for column in columns_to_remove:
            if column in st.session_state.text_filter_conditions:
                del st.session_state.text_filter_conditions[column]
        
        return True

    def prepare_filter_data_frames(self, filter_type):
        if filter_type == "Primary":
            st.session_state.secondary_filters_df = st.session_state.secondary_filters_df[["COLUMN_NAME", "DATA_TYPE", "FILTER_TYPE"]]
            self.remove_secondary_filters_from_session_state()
            st.session_state.filter_df = st.session_state.primary_filters_df
        else:
            st.session_state.filter_df = pd.concat([st.session_state.secondary_filters_df, st.session_state.primary_filters_df], ignore_index=True)
    
    def generate_filter_clause(self, filter_type):
        non_text_filters_clause = gen_where_clause_for_non_text_filters(st.session_state.filter_df, st.session_state.filter_values)
        text_filters_clause = gen_where_clause_for_text_filter(st.session_state.text_filter_conditions)
        return f" AND {non_text_filters_clause} AND {text_filters_clause}"


    def apply_filters(self, filter_type):
        self.update_selected_filters()
        self.prepare_filter_data_frames(filter_type)
        filter_clause = self.generate_filter_clause(filter_type)
    

        if filter_type == "Primary":
            if not st.session_state.secondary_filters_df.empty:
                st.session_state.secondary_filters_df = process_columns(
                    st.session_state.session,
                    st.session_state.secondary_filters_df,
                    filter_clause
                )
        st.session_state.final_query = st.session_state.base_filter_query + filter_clause

        st.session_state.cohort_row_count = read_sql(
            st.session_state.final_query,
            st.session_state.session,
            0
        )[0]

        st.session_state.preview_dataset = read_table(
            st.session_state.final_query.replace('COUNT(*)', '*') + ' LIMIT 1000',
            st.session_state.session
        )

        st.session_state.show_secondary_filters = True
        st.rerun()

    def render_sidebar(self):
        with st.container():
            with st.sidebar:
                st.header("Primary Filters")

                # Obtain and sort the dataframe of filters
                primary_filters_df = st.session_state.primary_filters_df.sort_values(by="FILTER_TYPE")

                # Render filters that are not numeric or date advanced filters
                for index, row in primary_filters_df.iterrows():
                    if row["FILTER_TYPE"] not in ['advanced numeric filter', 'advanced date filter']:
                        self.render_filter(row)

                st.divider()

                # Section for Advanced Numeric Filters
                for index, row in primary_filters_df.iterrows():
                    if row["FILTER_TYPE"] == 'advanced numeric filter':
                        self.render_filter(row)

                st.divider()

                # Section for Advanced Date Filters
                for index, row in primary_filters_df.iterrows():
                    if row["FILTER_TYPE"] == 'advanced date filter':
                        self.render_filter(row)

                # Button to apply primary filters
                if st.button("Apply - Primary Filters", type="primary"):
                    self.apply_filters("Primary")
                    st.session_state.filter_df = st.session_state.primary_filters_df
                    st.session_state.show_secondary_filters = True


    def render_filter(self, row):
        filter_type = row["FILTER_TYPE"]
        column_name = row["COLUMN_NAME"]
        distinct_values = json.loads(row["DISTINCT_VALUES"]) if row.get("DISTINCT_VALUES") else []
        min_value = row.get("MIN_VALUE", None)
        max_value = row.get("MAX_VALUE", None)
        where_clauses = []

        try:
            bold_column_name = f"**{column_name}**"

            if filter_type == "dropdown":
                current_value = st.session_state.filter_values.get(column_name, None)
                default_index = distinct_values.index(current_value) if current_value in distinct_values else None

                st.session_state.selected_filters[column_name] = st.selectbox(
                    bold_column_name,
                    options=distinct_values,
                    index=default_index,
                    key=f"{column_name}_dropdown"
                )

            elif filter_type == 'multi-select dropdown':
                st.session_state.selected_filters[column_name] = st.multiselect(
                    bold_column_name,
                    options=distinct_values,
                    default=st.session_state.filter_values.get(column_name, []),
                    key=f"{column_name}_multiselect"
                )

            elif filter_type == 'date filter':
                if not min_value or not max_value:
                    st.warning(f"Min or Max date not specified for {column_name}")
                    return

                min_date = pd.to_datetime(min_value).date()
                max_date = pd.to_datetime(max_value).date()

                column_filters = st.session_state.filter_values.get(column_name)

                st.session_state.selected_filters[column_name] = st.slider(
                    bold_column_name,
                    min_value=min_date,
                    max_value=max_date,
                    value=(
                        column_filters[0] if column_filters else min_date,
                        column_filters[1] if column_filters else max_date
                    ),
                    format="MM/DD/YYYY",
                    key=f"{column_name}_datefilter"
                )

            elif filter_type == 'text filter':
                st.markdown(f"**{column_name}:**")
                text_filter = TextFilterDialog(column_name)
                text_filter.display()

            elif filter_type == 'boolean filter':
                current_value = st.session_state.filter_values.get(column_name, False)
                st.write("")
                st.session_state.selected_filters[column_name] = st.checkbox(
                    bold_column_name,
                    value=current_value,
                    key=f"{column_name}_booleanfilter"
                )

            elif filter_type == 'range filter':
                if min_value is None or max_value is None:
                    st.warning(f"Min or Max value not specified for {column_name}")
                    return
                try:
                    min_value = int(min_value)
                    max_value = int(max_value)
                except ValueError:
                    try:
                        min_value = float(min_value)
                        max_value = float(max_value)
                    except ValueError:
                        st.warning(f"Invalid Min or Max value for {column_name}")
                        return

                if min_value == max_value:
                    st.markdown(f"**{column_name}:**")
                    st.markdown(f":red[Has only one value: **{min_value}**]")
                else:
                    column_filters = st.session_state.filter_values.get(column_name)
                    try:
                        st.session_state.selected_filters[column_name] = st.slider(
                            bold_column_name,
                            min_value=min_value,
                            max_value=max_value,
                            value=(
                                int(column_filters[0]) if column_filters else min_value,
                                int(column_filters[1]) if column_filters else max_value
                            ),
                            key=f"{column_name}_rangefilter"
                        )
                    except:
                        st.session_state.selected_filters[column_name] = st.slider(
                            bold_column_name,
                            min_value=min_value,
                            max_value=max_value,
                            value=(
                                float(column_filters[0]) if column_filters else min_value,
                                float(column_filters[1]) if column_filters else max_value
                            ),
                            key=f"{column_name}_rangefilter"
                        )

            elif filter_type == 'advanced date filter':
                distinct_values = ["", "greater than or equal", "less than or equal"]

                selected_filter = st.session_state.selected_filters.get(column_name, None)
                if selected_filter is not None:
                    # Check if selected_filter is a Python dictionary
                    if isinstance(selected_filter, dict):
                        current_value = selected_filter.get("condition", None)
                    else:
                        st.error(f"Selected filter for {column_name} is not a valid Python dictionary.")
                        return
                else:
                    current_value = None

                if current_value is not None and current_value in distinct_values:
                    default_index = distinct_values.index(current_value)
                else:
                    default_index = None

                if not min_value or not max_value:
                    st.warning(f"Min or Max date not specified for {column_name}")
                    return
                try:
                    min_date = pd.to_datetime(min_value)
                    max_date = pd.to_datetime(max_value)
                except ValueError:
                    st.warning(f"Invalid Min or Max date for {column_name}")
                    return

                st.markdown(bold_column_name)
                condition = st.selectbox(
                    f"{column_name}_condition",
                    options=distinct_values,
                    key=f"{column_name}_condition",
                    index = default_index
                )

                try:
                    selected_date = pd.to_datetime(st.session_state.selected_filters[column_name].get("value", min_date))
                except (KeyError, TypeError, ValueError):
                    selected_date = min_date

                selected_date = st.date_input(
                    f"{column_name}_value",
                    min_value=min_date,
                    max_value=max_date,
                    value=selected_date
                )
                date_string = selected_date.strftime('%Y-%m-%d')
                st.session_state.selected_filters[column_name] = {"condition": condition, "value": date_string}


            elif filter_type == 'advanced numeric filter':
                if min_value is None or max_value is None:
                    st.warning(f"Min or Max value not specified for {column_name}")
                    return
                try:
                    min_value = float(min_value)
                    max_value = float(max_value)
                except ValueError:
                    st.warning(f"Invalid Min or Max value for {column_name}")
                    return

                distinct_values = ["","greater than", "less than", "equal to", "between"]

                selected_filter = st.session_state.selected_filters.get(column_name, None)
                if selected_filter is not None:
                    current_value = selected_filter.get("condition", None)
                else:
                    current_value = None

                if current_value is not None and current_value in distinct_values:
                    default_index = distinct_values.index(current_value)
                else:
                    default_index = None

                st.markdown(bold_column_name)
                condition = st.selectbox(
                    f"{column_name}_condition",
                    options=distinct_values,
                    key=f"{column_name}_condition",
                    index=default_index
                )

                if condition == "between":
                    try:
                        values = st.session_state.selected_filters[column_name]
                        value1 = float(values.get("value1", min_value))
                        value2 = float(values.get("value2", max_value))
                    except (KeyError, TypeError):
                        value1 = min_value
                        value2 = max_value

                    col1, col2 = st.columns(2)
                    with col1:
                        value1 = st.number_input(f"{column_name}_value1", min_value=min_value, max_value=max_value, value=value1)
                    with col2:
                        value2 = st.number_input(f"{column_name}_value2", min_value=min_value, max_value=max_value, value=value2)
                    st.session_state.selected_filters[column_name] = {"condition": condition, "value1": value1, "value2": value2}
                else:
                    try:
                        value = float(st.session_state.selected_filters[column_name].get("value", min_value))
                    except (KeyError, TypeError):
                        value = min_value

                    st.session_state.selected_filters[column_name] = {"condition": condition, "value": st.number_input(f"{column_name}_value", min_value=min_value, max_value=max_value, value=value)}

        except Exception as e:
            st.error(f"Error processing {column_name}: {e}")
            st.write(e)

    def render_main_content(self):
        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            # Ensure cohort_name is in session state
            if "cohort_name" not in st.session_state:
                st.session_state.cohort_name = ""

            cohort_name = st.text_input(
                "Cohort Name: ",
                value=st.session_state.cohort_name,
                disabled=st.session_state.is_cohort_saved,
            )

            # Update the session state with the current cohort name
            st.session_state.cohort_name = cohort_name

            if st.session_state.is_cohort_saved:
                st.info("If you scheduled your cohort as a Dynamic table, updating the Cohort will require you to re-create your schedule.")
                save_button_text = "Update Cohort"
            else:
                save_button_text = "Save Cohort"

            schedule_cohort = ScheduleCohorts()   

            if st.button(save_button_text):

                if save_button_text == "Update Cohort":

                    result = schedule_cohort.check_existing_table()

                    if result and len(result) > 0:
                        table_type = result[0]["TYPE"]
                        table_name = result[0]["TABLE_NAME"]
                        database = result[0]["TABLE_CATALOG"]
                        schema = result[0]["TABLE_SCHEMA"]
                        type = result[0]["SCHEDULE_TYPE"]

                        if type == 'dynamictable':

                            query = f"DROP {table_type} {database}.{schema}.{table_name}"

                            read_sql(query, st.session_state.session, 0)

                        elif type == 'snapshot':

                            create_schedule = CreateSnapshotTable()
                            
                            cadence = result[0]["CADENCE"]
                            create_schedule.create_snapshot_table(table_name, cadence, st.session_state.cohort_name)

                        elif type == 'onetime':
                            create_schedule = CreateOneTimeTable()
                            
                            create_schedule.create_table(table_name, st.session_state.cohort_name)


                if len(cohort_name) <= 2:

                    st.markdown(":red[The entered cohort_name is too small; it has to be at least 3 characters long.]")

                else: 

                    st.info('Saving')

                    columns_data = st.session_state.filter_df[
                        ["COLUMN_NAME", 
                         "IS_PRIMARY", 
                         "FILTER_TYPE",
                         "DATA_TYPE"
                         ]
                    ]
       
                    filter_values = {
                        "filters": {**st.session_state.filter_values, **st.session_state.text_filter_conditions}
                    }
                    
                    # Prepare the dictionary from filter_values for easier mapping
                    filter_dict = filter_values['filters']

                    # Define a function to apply to the COLUMN_NAME column to fetch values from filter_dict
                    def apply_filters(column_name):
                        value = filter_dict.get(column_name)
                        # Convert value to a string if it is not None
                        return str(value) if value is not None else None

                    # Apply the function to the 'COLUMN_NAME' column
                    columns_data['FILTER_VALUES'] = columns_data['COLUMN_NAME'].apply(apply_filters)

                    save_cohort_status = call_upsert_cohorts(
                        st.session_state.session, 
                        cohort_name, 
                        st.session_state.selected_table_full, 
                        st.session_state.selected_column_list, 
                        columns_data.to_json(orient='records', indent=4)
                        )

                    if save_cohort_status != None:
                        st.session_state.is_cohort_saved = True

                    st.rerun()

        with col3:
            row_card = create_metadatacard_html(
                header="Rows",
                value=convert_to_k_m(st.session_state.cohort_row_count),
                description="Rows",
                card_bg_color=st.session_state.card_bg_color,
                header_bg_color=st.session_state.header_bg_color,
            )

            st.markdown(row_card, unsafe_allow_html=True)

        tab1, tab2 = st.tabs(["Secondary Filters", "Select Columns"])

        with tab1:
            self.render_secondary_filters()
            self.render_column_select()

        with tab2:
            self.render_preview_cohort()

    def render_column_select(self):

        if not st.session_state.show_secondary_filters:
            return

        st.subheader("Cohort Query")
        
        try:
            selected_columns = st.session_state.selected_column_list
            if len(selected_columns)>0:
                final_query = st.session_state.final_query.replace(
                    'COUNT(*)',
                    ", ".join(map(str, selected_columns))
                )
            else:
                final_query = st.session_state.final_query.replace(
                    'COUNT(*)',
                    '*'
                )
        except: 
            final_query = st.session_state.base_filter_query

        st.session_state.cohort_query = final_query
        
        st.code(format_sql(final_query))


    def render_preview_cohort(self):

        if not st.session_state.show_secondary_filters:
                st.markdown(":red[Apply primary filters to display secondary filters.]")
                return
            
        with st.form("select_columns"):
            
            try:
                selected_column_list = st.multiselect(
                "Select columns you want in your cohort:",
                st.session_state.col_list,
                st.session_state.selected_column_list
            )
            except:
                selected_column_list = st.multiselect(
                "Select columns you want in your cohort:",
                st.session_state.col_list,
                #st.session_state.selected_column_list
            )

            if st.form_submit_button("Select Columns", type= "primary"):
                st.session_state.selected_column_list = selected_column_list
                st.rerun()
        
        st.divider()

        try:
            selected_columns = st.session_state.selected_column_list
            if len(selected_columns)>0:
                final_query = st.session_state.final_query.replace(
                    'COUNT(*)',
                    ", ".join(map(str, selected_columns))
                )
            else:
                final_query = st.session_state.final_query.replace(
                    'COUNT(*)',
                    '*'
                )
            
        except: 
            final_query = st.session_state.base_filter_query

        with st.expander("Cohort Query",expanded = False):
            st.code(format_sql(final_query))

        selected_columns = st.session_state.selected_column_list
        if len(selected_columns)>0:
            st.dataframe(st.session_state.preview_dataset[selected_columns], 
                     height = 500,
                     use_container_width = True)
        else:
            st.dataframe(st.session_state.preview_dataset, 
                     height = 500,
                     use_container_width = True)

    def render_secondary_filters(self):
        if not st.session_state.show_secondary_filters:
            st.markdown(":red[Apply primary filters to display secondary filters.]")
            return

        if st.session_state.secondary_filters_df is None or st.session_state.secondary_filters_df.empty:
            st.markdown(":red[No secondary filters available.]")
            return

        # Check for the presence of text filters, other filters, advanced numeric filters, and advanced date filters
        text_filters_count = sum(1 for _, row in st.session_state.secondary_filters_df.iterrows() if row["FILTER_TYPE"] == "text filter")
        advanced_numeric_filters_count = sum(1 for _, row in st.session_state.secondary_filters_df.iterrows() if row["FILTER_TYPE"] == "advanced numeric filter")
        advanced_date_filters_count = sum(1 for _, row in st.session_state.secondary_filters_df.iterrows() if row["FILTER_TYPE"] == "advanced date filter")
        other_filters_count = sum(1 for _, row in st.session_state.secondary_filters_df.iterrows() if row["FILTER_TYPE"] not in ["text filter", "advanced numeric filter", "advanced date filter"])

        # Text Filters Section
        if text_filters_count > 0:
            with st.container():
                st.subheader("Text Filters")
                cols = st.columns(3)  # maintain the 3-column layout for text filters
                current_filter_count = 0

                for index, row in st.session_state.secondary_filters_df.iterrows():
                    if row["FILTER_TYPE"] == "text filter":
                        with cols[current_filter_count]:
                            self.render_filter(row)
                        current_filter_count += 1
                        if current_filter_count >= 3:
                            current_filter_count = 0
                            cols = st.columns(3)
        else:
            st.write(":orange[No Text Filters]")

        # Other Filters Section
        if other_filters_count > 0:
            st.subheader("Other Filters")
            with st.container(border=True):
                cols = st.columns(3)  # maintain the 3-column layout for other filters
                current_filter_count = 0

                for index, row in st.session_state.secondary_filters_df.iterrows():
                    if row["FILTER_TYPE"] not in ["text filter", "advanced numeric filter", "advanced date filter"]:
                        with cols[current_filter_count]:
                            self.render_filter(row)
                        current_filter_count += 1
                        if current_filter_count >= 3:
                            current_filter_count = 0
                            cols = st.columns(3)

        else:
            st.write("No Other Filters")

        # Advanced Numeric Filters Section
        if advanced_numeric_filters_count > 0:
            st.subheader("Advanced Numeric Filters")
            with st.container(border=True):
                cols = st.columns(3)  # maintain the 3-column layout for advanced numeric filters
                current_filter_count = 0

                for index, row in st.session_state.secondary_filters_df.iterrows():
                    if row["FILTER_TYPE"] == "advanced numeric filter":
                        with cols[current_filter_count]:
                            self.render_filter(row)
                        current_filter_count += 1
                        if current_filter_count >= 3:
                            current_filter_count = 0
                            cols = st.columns(3)

        else:
            st.write("No Advanced Numeric Filters")

        # Advanced Date Filters Section
        if advanced_date_filters_count > 0:
            st.subheader("Advanced Date Filters")
            with st.container(border=True):
                cols = st.columns(3)  # maintain the 3-column layout for advanced date filters
                current_filter_count = 0

                for index, row in st.session_state.secondary_filters_df.iterrows():
                    if row["FILTER_TYPE"] == "advanced date filter":
                        with cols[current_filter_count]:
                            self.render_filter(row)
                        current_filter_count += 1
                        if current_filter_count >= 3:
                            current_filter_count = 0
                            cols = st.columns(3)

        else:
            st.write("No Advanced Date Filters")

        # Apply button for all filters
        st.divider()
        if st.button("Apply - Secondary Filters", use_container_width=True, type="primary"):
            self.apply_filters("Secondary")
            st.rerun()


    def run(self):
        
        if st.session_state.primary_filters_df is not None and not st.session_state.primary_filters_df.empty:

            if st.session_state.primary_filters_df.shape[0] > 0:
            
                self.render_sidebar()
                self.render_main_content() 
        else:

            st.warning("Please select the Primary & Secondary Filters in the Data Dictionary.")


class ScheduleCohorts:

    def render_main_content(self):
        st.subheader("How do you want to store this Cohort?")
        storage_option = st.radio("", ['Dynamic Table', 'Snapshot tables', 'One-time Table'])

        st.divider()

        if storage_option == 'Dynamic Table':
            table_creator = CreateDynamicTable()
            table_creator.dynamictable_inputs()
        elif storage_option == 'Snapshot tables':
            table_creator = CreateSnapshotTable()
            table_creator.snapshottable_inputs()
        elif storage_option == 'One-time Table':
            table_creator = CreateOneTimeTable()
            table_creator.onetimetable_inputs()

    def check_existing_table(self):
        query = f"""
        SELECT TABLE_NAME,
               TABLE_CATALOG,
               TABLE_SCHEMA,
               PARSE_JSON(COMMENT) as COMMENT_JSON,
               COMMENT_JSON:attributes:cohort::STRING as COHORT_NAME,
               COMMENT_JSON:attributes:type::STRING as SCHEDULE_TYPE,
               COMMENT_JSON:attributes:cadence::STRING as CADENCE,
               CREATED,
               LAST_ALTERED,
               LAST_DDL,
               CASE WHEN IS_DYNAMIC = 'YES'
                THEN 'DYNAMIC TABLE'
                ELSE 'TABLE'
               END as TYPE
        FROM COHORT_BUILDER_FOR_SNOWFLAKE.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'APP'
        AND TABLE_CATALOG = 'COHORT_BUILDER_FOR_SNOWFLAKE'
        AND COMMENT_JSON:name = 'cohort-builder'
        AND COMMENT_JSON:attributes:cohort = '{st.session_state.cohort_name}';
        """
        result = st.session_state.session.sql(query).collect()
        return result

    def run(self):
        if len(st.session_state.cohort_name) > 2:
            result = self.check_existing_table()
            if result:
                st.info(f"A table already exists for the cohort '{st.session_state.cohort_name}'.")

                # Convert the result to a DataFrame
                df = pd.DataFrame(result)
                
                # Unpivot the DataFrame
                df_unpivoted = df.melt(var_name='Attribute', value_name='Value')

                df = df_unpivoted[df_unpivoted["Attribute"] == "TABLE_NAME"]

                # Display the unpivoted DataFrame
                st.dataframe(df,use_container_width = True, hide_index= True)

            else:
                self.render_main_content()
        else:
            st.warning("Please build a new Cohort and save or Load an existing Cohort")


class CreateDynamicTable:

    def dynamictable_inputs(self):
        st.title('Create Dynamic Table in Snowflake')
        
        # Input for Dynamic Table Name
        table_name = st.text_input('Dynamic Table Name')
        
        # Display Lag Value and Lag Unit in two columns
        st.write('Specify Lag:')
        col1, col2 = st.columns(2)
        with col1:
            lag_value = st.number_input('Lag Value', min_value=1, step=1, value=1)
        with col2:
            lag_unit = st.selectbox('Lag Unit', ['minutes', 'hours', 'days'])
        target_lag = f"{lag_value} {lag_unit}"
        
        # Selectbox for REFRESH_MODE
        refresh_mode = st.selectbox('REFRESH_MODE', ['AUTO', 'FULL', 'INCREMENTAL'])

        if st.button('Create Table'):
            self.create_dynamic_table(table_name, target_lag, refresh_mode)

    def create_dynamic_table(self, table_name, target_lag, refresh_mode):
        try:
            # Generate SQL for creating the dynamic table
            cohort_name = st.session_state.cohort_name 

            create_table_sql = f"""
            CREATE OR REPLACE DYNAMIC TABLE COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name}
            COMMENT = '{{"origin":"sf_sit","name":"cohort-builder","version":{{"major":1, "minor":1}},"attributes":{{"cohort":"{cohort_name}", "type":"dynamictable"}}}}'
            REFRESH_MODE = {refresh_mode}
            TARGET_LAG = '{target_lag}'
            WAREHOUSE = 'COHORT_BUILDER_LOAD_WH'
            AS
            {st.session_state.cohort_query};
            """

            # Execute the SQL command in Snowflake
            st.session_state.session.sql(create_table_sql).collect()
            st.success(f"Dynamic table '{table_name}' created successfully with target lag '{target_lag}' and refresh mode '{refresh_mode}'!")
        except Exception as e:
            st.error(f"Error creating table: {e}")

class CreateSnapshotTable:

    def snapshottable_inputs(self):
        st.title('Create Snapshot Table in Snowflake')
        
        # Input for Snapshot Table Name
        table_name = st.text_input('Snapshot Table Name')
        
        # Selectbox for Cadence
        cadence = st.selectbox('Select Cadence', ['Daily', 'Weekly', 'Monthly'])
        
        if st.button('Create Table'):
            self.create_snapshot_table(table_name, cadence, st.session_state.cohort_name)

    def create_snapshot_table(self, table_name, cadence, cohort_name):
        try:
            # Mapping of cadences to CRON expressions
            cadence_to_cron = {
                'Daily': 'USING CRON 0 0 * * * UTC',  # Every day at midnight UTC
                'Weekly': 'USING CRON 0 0 * * 0 UTC', # Every Sunday at midnight UTC
                'Monthly': 'USING CRON 0 0 1 * * UTC' # First day of every month at midnight UTC
            }

            # Get the CRON expression from the mapping based on the user-selected cadence
            cron_schedule = cadence_to_cron.get(cadence, 'USING CRON 0 0 * * * UTC')  # Default to daily if not found

            # Generate SQL for creating the stored procedure
            query = st.session_state.cohort_query
            escaped_query = query.replace("'", "''")  # Properly escape single quotes

            create_proc_sql = f"""
            CREATE OR REPLACE PROCEDURE COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name}_snapshot_proc()
            RETURNS STRING
            LANGUAGE SQL
            COMMENT = '{{"origin":"sf_sit","name":"cohort-builder","version":{{"major":1, "minor":1}},"attributes":{{"cohort":"{cohort_name}", "type":"snapshot", "cadence":"{cadence}"}}}}'
            AS
            $$
            DECLARE
                current_datetime VARCHAR;
                create_table_sql VARCHAR;
            BEGIN
                -- Generate a datetime string in the format 'YYYYMMDDHH24MISS'
                current_datetime := TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');

                -- Build the SQL to create a new table with the datetime postfix
                create_table_sql := 'CREATE TABLE COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name}_' || current_datetime || ' COMMENT = \\'{{"origin":"sf_sit","name":"cohort-builder","version":{{"major":1, "minor":1}},"attributes":{{"cohort":"{cohort_name}", "type":"snapshot", "cadence":"{cadence}"}}}}\\' AS {escaped_query}';

                -- Execute the SQL statement
                EXECUTE IMMEDIATE create_table_sql;

                -- Return a success message
                RETURN 'Table created with name: COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name}_' || current_datetime;
            END;
            $$;
            """
            # Execute the SQL command to create the stored procedure in Snowflake
            st.session_state.session.sql(create_proc_sql).collect()
            st.success(f"Stored procedure '{table_name}_snapshot_proc' created successfully!")

            # Generate SQL for creating the task
            create_task_sql = f"""
            CREATE OR REPLACE TASK COHORT_BUILDER_FOR_SNOWFLAKE.APP.snapshot_task_{table_name}
                COMMENT = '{{"origin":"sf_sit","name":"cohort-builder","version":{{"major":1, "minor":1}},"attributes":{{"cohort":"{cohort_name}", "type":"snapshot", "cadence":"{cadence}"}}}}'
                WAREHOUSE = 'COHORT_BUILDER_LOAD_WH'
                SCHEDULE = '{cron_schedule}'
            AS
                CALL COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name}_snapshot_proc();
            """

            # Execute the SQL command to create the task in Snowflake
            st.session_state.session.sql(create_task_sql).collect()
            st.success(f"Snapshot task for '{table_name}' has been set up successfully with a {cadence} cadence!")

            # Generate SQL to resume the task
            resume_task_sql = f"ALTER TASK COHORT_BUILDER_FOR_SNOWFLAKE.APP.snapshot_task_{table_name} RESUME;"

            # Execute the SQL command to resume the task in Snowflake
            st.session_state.session.sql(resume_task_sql).collect()
            st.success(f"Snapshot task for '{table_name}' has been resumed!")


            # Call procedure
            call_proc_sql = f"CALL COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name}_snapshot_proc()"
            st.session_state.session.sql(call_proc_sql).collect()

        except Exception as e:
            st.error(f"Error setting up snapshot table and task: {e}")

class CreateOneTimeTable:

    def onetimetable_inputs(self):
        st.title('Create One-Time Table in Snowflake')
        
        # Input for One-Time Table Name
        table_name = st.text_input('One-Time Table Name')
        
        if st.button('Create Table'):
            self.create_table(table_name,  st.session_state.cohort_name)

    def create_table(self, table_name, cohort_name):
        # Generate SQL for one-time table creation
        create_table_sql = f"""
        CREATE OR REPLACE DYNAMIC TABLE COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name}
        TARGET_LAG = 'downstream'
        WAREHOUSE = 'COHORT_BUILDER_LOAD_WH'
        COMMENT = '{{"origin":"sf_sit","name":"cohort-builder","version":{{"major":1, "minor":1}},"attributes":{{"cohort":"{cohort_name}", "type":"onetime"}}}}'
        AS
        {st.session_state.cohort_query};
        """
        
        # Execute the SQL command in Snowflake
        try:
            st.session_state.session.sql(create_table_sql).collect()
        except Exception as e:
            st.error(f"Error creating table: {e}")

        suspend_query = f"ALTER DYNAMIC TABLE COHORT_BUILDER_FOR_SNOWFLAKE.APP.{table_name} SUSPEND"
        
        # Execute the SQL command in Snowflake
        try:
            st.session_state.session.sql(suspend_query).collect()
        except Exception as e:
            st.error(f"Error creating table: {e}")

        st.success("Table created Sucessfully")

class TextFilterDialog:
    def __init__(self, column_name):
        self.column_name = column_name
        self.condition_options = ["", "Contains", "Does not contain", "Equals", "Does not equal", "Begins with", "Ends with", "Blank", "Not blank"]

        if column_name not in st.session_state.text_filter_conditions.keys():
            condition_list = [["",""],["",""]]
            logical_operator = "OR"
            st.session_state.text_filter_conditions[column_name] = [condition_list, logical_operator]

    def display(self):

        condition_list = st.session_state.text_filter_conditions[self.column_name][0]
        logical_operator = st.session_state.text_filter_conditions[self.column_name][1]
        

        with st.expander(f"Set {self.column_name} conditions:"):
            for i in range(2):
                condition_type = st.selectbox(
                    f"Condition Type {i+1}",
                    self.condition_options,
                    index=self.condition_options.index(condition_list[i][0]), 
                    key=f"{self.column_name}_type_{i}") 
                
                condition_value = st.text_input(
                    f"Value {i+1}",
                    value=condition_list[i][1], 
                    key=f"{self.column_name}_value_{i}",
                    disabled=condition_type in ["Blank", "Not blank"])
                
                condition_list[i] = [condition_type, condition_value]

                if i == 0:
                    logical_operator = st.radio(
                        f"Logical Operator {i}", ["AND", "OR"], 
                        index=["AND", "OR"].index(logical_operator), 
                        key=f"{self.column_name}_operator_{i}")

            if st.button(f"Submit - {self.column_name}"):
                st.session_state.text_filter_conditions[self.column_name] = [condition_list, logical_operator]
                st.session_state.show_dialog = False
                st.rerun()
            

if __name__ == "__main__":
    app = BuildCohort()
    app.run()