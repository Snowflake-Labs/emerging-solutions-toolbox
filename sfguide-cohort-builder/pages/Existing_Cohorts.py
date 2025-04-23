import streamlit as st
import json
import ast
import datetime
import math
import pandas as pd
import sqlparse

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


class ExistingCohorts:
    def __init__(self):
        self.header_bg_color = "#37474f"
        self.card_bg_color = "#eceff1"
        self.setup_page()


    def setup_page(self):
        # Set page configuration
        st.set_page_config(
            page_title="Existing Cohorts",
            page_icon="📦",
            layout="wide",
            initial_sidebar_state="expanded"
        )


    def render_header(self):
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
                background-color: #BFC8AD;  /* Set your desired color */
                color: white;  /* Set text color if needed */
                border: none;  /* Optional: remove border */
            }
            </style>
            """, unsafe_allow_html=True)

        st.markdown("""
            <div class="header">
                <span style=font-weight: bold;">Existing Cohorts</span>
            </div>
            <hr class="horizontal-line"/>
            <p class="description">
                
            </p>
            """, unsafe_allow_html=True)
        
    def load_data_model(self):

        # # Split the table name by the period character
        parts = st.session_state.selected_table_full.split('.')

        # Assign the parts to variables
        st.session_state.selected_database,st.session_state.selected_schema,st.session_state.selected_table = parts

        st.session_state.prev_selected_database = st.session_state.selected_database
        st.session_state.prev_selected_schema = st.session_state.selected_schema
        st.session_state.prev_selected_table = st.session_state.selected_table

        st.session_state.table_data = fetch_table_data(st.session_state.session, st.session_state.selected_table_full)
        st.session_state.metadata_raw = get_table_metadata(st.session_state.session, st.session_state.selected_table_full)
        st.session_state.metadata_raw['IS_PRIMARY'] = False
        st.session_state.metadata_raw['IS_SECONDARY'] = False          
        st.session_state.dataset = st.session_state.table_data[0]
        st.session_state.col_list = st.session_state.dataset.columns
        st.session_state.row_count = st.session_state.table_data[1][0]
        st.session_state.table_size_mb = fetch_table_size(st.session_state.session, st.session_state.selected_database, st.session_state.selected_schema, st.session_state.selected_table)
        st.session_state.dynamic_row_count = st.session_state.row_count

        st.session_state.metadata = st.session_state.metadata_raw

    def load_data_dictionary(self):

        st.session_state.primary_filters_df = process_columns(
            st.session_state.session, 
            st.session_state.metadata[st.session_state.metadata["IS_PRIMARY"] == True][["COLUMN_NAME", "DATA_TYPE", "FILTER_TYPE"]]
        )

        st.session_state.primary_filters_df["IS_PRIMARY"] = True

        st.session_state.secondary_filters_df = st.session_state.metadata[
            (st.session_state.metadata["IS_SECONDARY"] == True) & (st.session_state.metadata["IS_PRIMARY"] == False)
        ][["COLUMN_NAME", "DATA_TYPE", "FILTER_TYPE"]]

        st.session_state.secondary_filters_df["IS_PRIMARY"] = False
        st.session_state.expander_open = False  # Close the expander

        self.reset_session_states()

        st.session_state.save_changes_pressed = True


    def load_build_cohort(self):

        test_filter = st.session_state.filter_values

        non_text_filters_clause = gen_where_clause_for_non_text_filters(
            st.session_state.primary_filters_df, 
            st.session_state.filter_values)

        text_filters_clause = gen_where_clause_for_text_filter(st.session_state.text_filter_conditions)

        st.session_state.secondary_filters_df = process_columns(
                        st.session_state.session, 
                        st.session_state.secondary_filters_df,
                        " AND " + non_text_filters_clause + " AND " + text_filters_clause
                    )
        
        st.session_state.filter_df = pd.concat([st.session_state.secondary_filters_df, st.session_state.primary_filters_df], ignore_index=True)
        
        non_text_filters_clause = gen_where_clause_for_non_text_filters(
            st.session_state.filter_df, 
            st.session_state.filter_values)

        text_filters_clause = gen_where_clause_for_text_filter(st.session_state.text_filter_conditions)

        filter_clause = " AND " + non_text_filters_clause + " AND " + text_filters_clause

        st.session_state.final_query = st.session_state.base_filter_query + filter_clause

        selected_columns = st.session_state.selected_column_list

        if len(selected_columns)>0:
            final_query = str(st.session_state.final_query).replace(
                'COUNT(*)',
                ", ".join(map(str, selected_columns))
            )
        else:
            final_query = st.session_state.final_query.replace(
                    'COUNT(*)',
                    '*'
                )
        
        st.code(format_sql(final_query))

        st.session_state.cohort_row_count = read_sql(
            st.session_state.final_query,
            st.session_state.session,
            0
        )[0]

        st.session_state.preview_dataset = read_table(final_query, st.session_state.session)
        
        st.session_state.is_cohort_saved = True
        

    def render_main_content(self):

        # Function to handle conversion based on FILTER_TYPE
        def convert_filter_values(filter_value, filter_type):
            try:
                # Convert to tuple if FILTER_TYPE is 'date filter'
                if filter_type == 'date filter':
                    # Remove unnecessary characters from the string
                    clean_string = filter_value.strip()[1:-1]
                    # Convert the string to a tuple of datetime.date objects
                    evaluated = eval(clean_string)
                elif isinstance(filter_value, str):
                    evaluated = ast.literal_eval(filter_value)
                else:
                    evaluated = filter_value
            except:
                return filter_value
            return evaluated

        query = "SELECT * FROM COHORT_BUILDER_FOR_SNOWFLAKE.APP.COHORTS;"
        cohorts_data = read_table(query, st.session_state.session)

        existing_cohorts = cohorts_data["COHORT_NAME"].to_list()

        selected_cohort = st.selectbox("Select Cohort:", existing_cohorts, index=None)

        if st.button("Load Cohort"):

            self.reset_session_states()

            with st.spinner("Please wait..."):

                st.session_state.cohort_name = selected_cohort

                selected_cohort_data = cohorts_data[cohorts_data["COHORT_NAME"] == selected_cohort]

                st.session_state.selected_table_full = selected_cohort_data["TABLE_NAME"].iloc[0]

                self.load_data_model()

                st.session_state.selected_column_list = ast.literal_eval(selected_cohort_data["COLUMN_LIST"].iloc[0])

                if len(st.session_state.col_list.to_list()) != len(st.session_state.selected_column_list):
                    st.session_state.selected_column_list = ast.literal_eval(selected_cohort_data["COLUMN_LIST"].iloc[0])

                else:
                    st.session_state.selected_column_list =[]

                

                filter_values = json.loads(json.loads(selected_cohort_data["COHORT_SELECTIONS_JSON"].iloc[0]))

                for filter in filter_values:
                    column_name = filter["COLUMN_NAME"]
                    is_primary = filter["IS_PRIMARY"]
                    filter_type = filter["FILTER_TYPE"]

                    # Check if the column exists in the DataFrame
                    if column_name in st.session_state.metadata['COLUMN_NAME'].values:
                        if is_primary is True:
                            st.session_state.metadata.loc[st.session_state.metadata['COLUMN_NAME'] == column_name, 'IS_PRIMARY'] = True
                        else:
                            st.session_state.metadata.loc[st.session_state.metadata['COLUMN_NAME'] == column_name, 'IS_SECONDARY'] = True

                        st.session_state.metadata.loc[st.session_state.metadata['COLUMN_NAME'] == column_name, 'FILTER_TYPE'] = filter_type

                self.load_data_dictionary()

                # Create the dictionary
                filter_dict = {}
                text_filter_conditions = {}
                filter_values_df = pd.DataFrame(filter_values)
                filter_values_df['IS_PRIMARY'] = filter_values_df['IS_PRIMARY'].fillna(False)
                for index, row in filter_values_df.iterrows():
                    if row["FILTER_TYPE"] == 'text filter':
                        text_filter_conditions[row['COLUMN_NAME']] = convert_filter_values(row["FILTER_VALUES"], row["FILTER_TYPE"])
                    else:
                        filter_dict[row['COLUMN_NAME']] = convert_filter_values(row["FILTER_VALUES"], row["FILTER_TYPE"])

                st.session_state.filter_values = filter_dict
                st.session_state.selected_filters = st.session_state.filter_values
                st.session_state.text_filter_conditions = text_filter_conditions

                self.load_build_cohort()        
                st.success("Metadata processed successfully.")


    def reset_session_states(self):
        """
        Reset specific session states after saving changes.
        """
        st.session_state.filter_values = {}
        # #Not being used?
        # st.session_state.primary_where_clause = ''
        # #Not being used?
        # st.session_state.final_where_clause = ''
        # st.session_state.text_filter_conditions = {}
        # #Not being used?
        # st.session_state.secondary_filter_values = {}
        st.session_state.show_secondary_filters = True
        # #st.session_state.cohort_row_count = st.session_state.row_count
        st.session_state.base_filter_query = f'SELECT COUNT(*) FROM {st.session_state.selected_table_full} WHERE true '
        st.session_state.final_query = '',
        st.session_state.save_changes_pressed = True
        #st.session_state.col_list = st.session_state.dataset.columns
        #st.session_state.selected_column_list =  st.session_state.col_list # Track if save changes is pressed

        #self.reset_session_states_for_build_cohort()

    def run(self):
        """
        Main method to run the application.
        """
        self.render_header()
        self.render_main_content()

if __name__ == "__main__":
    app = ExistingCohorts()
    app.run()
