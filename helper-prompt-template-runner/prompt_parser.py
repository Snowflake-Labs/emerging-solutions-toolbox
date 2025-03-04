from typing import Optional, Union, Any
import datetime
import logging

import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame
from snowflake.snowpark import Session

major, minor = 1, 2
QUERY_TAG = {
    "origin": "sf_sit",
    "name": "prompt_template_runner",
    "version": {"major": major, "minor": minor},
}

def set_query_tag(session: Session) -> None:
    """
    Sets the query tag for the Snowflake session.
    Args:
        session (Session): The Snowflake session object.
        
    Returns:
        None
    """
    try:
        session.query_tag = QUERY_TAG
    except Exception:
        return
    
class PromptParser:
    def __init__(self):
        self.configuration_loaded = False
        self.prompt_config = None

    def process(self, row_data: dict, prompt_template_file: str, prompt_config: dict, include_metadata: bool = False):
        if not self.configuration_loaded:
            if prompt_template_file:
                self.prompt_config = extract_prompt(prompt_template_file)
            else:
                self.prompt_config = prompt_config
 
            if not self.prompt_config:
                raise ValueError(f"Error: prompt_config is required in configuration file or explicitly.")
            self.configuration_loaded = True
        
        messages = self.prompt_config.get('messages', [])
        literal_vars = self.prompt_config.get('literal_variables', {})
        column_vars = self.prompt_config.get('column_variables', {})
        prompt_name = self.prompt_config.get('name', str(datetime.date.today()))
        prompt_version = self.prompt_config.get('version', str(datetime.datetime.now().time()))
            
        # Replace column names with column values in column_variables
        mapped_column_vals = {} # Make new mapping in case we need to exlude missing row data
        if column_vars:
            for var_name, col_name in column_vars.items():
                if col_name in row_data:
                    mapped_column_vals[var_name] = row_data.get(col_name, '')
    
        replace_dict = {**(literal_vars or {}), **(mapped_column_vals or {})} # Combine dictionaries if not empty

        # Process messages
        updated_messages = []
        if messages:
            for message in messages:
                updated_message = {}
                for key, value in message.items():
                    mapped_value = value
                    for source_value, target_value in replace_dict.items():
                        mapped_value = mapped_value.replace(f'{{{source_value}}}', target_value)
                    updated_message[key] = mapped_value
                updated_messages.append(updated_message)
    
        # Yield as a tuple, since UDTFs must return tuples
        if include_metadata:
            yield ({'messages': updated_messages, 
                    'prompt_name': prompt_name, 
                    'prompt_version': prompt_version},)
        else:
            yield (updated_messages,)

    
def test_model(
        session: Session, 
        model: str, 
        prompt = "Repeat the word hello once and only once. Do not say anything else."
        ) -> bool:
    from snowflake.cortex import Complete
    from snowflake.snowpark.exceptions import SnowparkSQLException

    """Returns True if selected model is supported in region and returns False otherwise.
    
    Args:
        session (Session): The Snowflake session object.
        model (str): The model to use for the SNOWFLAKE.CORTEX.COMPLETE function.
        prompt (str): The prompt to test the model with. 
            Defaults to "Repeat the word hello once and only once. Do not say anything else."
    """
    try:
        response = Complete(model, prompt, session = session)
        return True
    except SnowparkSQLException as e:
        if 'unknown model' in str(e):
            return False

def run_complete_options(
        session: Session,
        source_table: str,
        model: str,
        model_options: dict[str, Union[str, float, int]] = {},
        prompt_column: str = 'PROMPT',
        response_column: str = 'RESPONSE',
        ) -> DataFrame:
    """
    Executes a SQL query to run the SNOWFLAKE.CORTEX.COMPLETE function on a specified table.
    Args:
        session (Session): The Snowflake session object.
        source_table (str): The name of the source table to query.
        model (str): The model to use for the SNOWFLAKE.CORTEX.COMPLETE function.
        model_options (Optional[dict[str, Union[str, float, int]]]): Options for the model in a dictionary format. 
            Defaults to an empty dictionary.
        prompt_column (Optional[str]): The name of the column containing the prompts. Defaults to 'PROMPT'.
        response_column (Optional[str]): The name of the column to store the responses. Defaults to 'RESPONSE'.
    Returns:
        DataFrame: The result of the SQL query as a DataFrame.
    Raises:
        Exception: If there is an error executing the SQL query.
    """

    import json
    
    query = f"""
        SELECT * EXCLUDE ({prompt_column}), 
        SNOWFLAKE.CORTEX.COMPLETE(
        ?,
            {prompt_column}, PARSE_JSON(?)
            ) as {response_column}          
        FROM {source_table}
        """
    
    # If error occurs, immediately raise exception as error may be across multiple records
    try:
        return session.sql(
            query.format(
                prompt_column = prompt_column,
                response_column = response_column,
                source_table = source_table
                    ), 
            params = [
                model,
                json.dumps(model_options),
                ]
            )
    except Exception as e:
        raise Exception(f"Error running Cortex Complete query: {e}")

def parse_prompt_template(
        session: Session,
        prompt_config: dict[str, Any],
        origin_table: Optional[str] = None,
        output_table_name: Optional[str] = 'TEMP',
        table_type: Optional[str] = 'temporary',
        table_write_mode: str = 'errorifexists',
        prompt_column: str = 'PROMPT'
        ) -> None:
    """
    Parses a prompt template and saves the results to a specified table.
    Args:
        session (Session): The Snowflake session object.
        origin_table (str): The name of the origin table to read data from.
        prompt_config (dict[str, Any]): Prompt template dictionary.
        output_table_name (Optional[str]): The name of the output table. Defaults to 'TEMP'.
        table_type (Optional[str]): The type of the output table (temporary, transient, or permanent).
            Defaults to 'temporary'.
        table_write_mode (Optional[str]): The mode to write the table (append, overwrite, truncate, errorifexists, ignore). 
            Defaults to 'errorifexists'.
        prompt_column (Optional[str]): The column name for the prompt. Defaults to 'PROMPT'.
    Returns:
        None
        Saves dataframe to Snowflake table.
    """

    if origin_table is None:
        origin_table = prompt_config.get('origin_table', None)

    # Final check for required parameters
    if origin_table is None:
        raise ValueError(f"Error: origin_table is required in configuration file or explicitly.")
    
    # Must first create object construct column as separate request to avoid selection error
    df = session.table(origin_table).with_column('ROW_DATA', F.object_construct('*'))

    # Calling the UDTF using call_table_function from snowpark.functions
    udtf = 'PROMPT_TEMPLATE_PARSER'

    # We set metadata to exclude so we can add it in once later in stored procedure
    df = df.with_column(prompt_column, F.call_table_function(
            udtf,
            F.col('ROW_DATA'),
            F.lit(''),
            F.to_variant(F.lit(prompt_config)),
            F.lit(False))).drop('ROW_DATA')
    
    # Save results to table
    if output_table_name is None:
        output_table_name = prompt_config['name'].replace(' ', '_').upper()
    if table_type not in ['temporary', 'transient', '']:
        table_type = 'temporary' # Default to temporary table
    if table_write_mode not in ['append', 'overwrite', 'truncate', 'errorifexists', 'ignore']:
        table_write_mode = 'errorifexists' # Default to error before overriding
    df.write.mode(table_write_mode).save_as_table(output_table_name, table_type=table_type)

def extract_prompt(prompt_template_file: str) -> dict[str, Any]:
    """
    Extracts the prompt from a YAML file.

    Uses SnowflakeFile if the file is a URL corresponding to Snowflake stage.
    Otherwise, uses the standard Python open function for a local file.

    Args:
        prompt_template_file (str): The path to the YAML file containing the prompt template.
    Returns:
        dict[str, Any]: The prompt dictionary.
    """

    import yaml
    
    if prompt_template_file.startswith('https://'): # BUILD_SCOPED_FILE_URL returns URL
        from snowflake.snowpark.files import SnowflakeFile
        open_file = SnowflakeFile.open(prompt_template_file)
    else:
        open_file = open(prompt_template_file, 'r')
        
    with open_file as file:
        try:
            return yaml.safe_load(file)['prompt']
        except KeyError:
            raise KeyError(f"Error parsing YAML file {prompt_template_file}. Ensure that the file contains a 'prompt' key.")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file {prompt_template_file}. Error: {e}")
        
def add_metadata(df: DataFrame, column: str, metadata: dict[str, Any]) -> DataFrame:
    """
    Adds metadata to a specified column in the DataFrame. 
    
    If the metadata contains nested dictionaries, they are unnested and added as separate keys.
    
    Args:
        df (DataFrame): The DataFrame to which metadata will be added.
        column (str): The name of the column to which metadata will be added.
        metadata (dict[str, Any]): A dictionary containing metadata to be added. Nested dictionaries are supported.
    Returns:
        DataFrame: The DataFrame with the added metadata.
    """
    
    try:
        for key, value in metadata.items():
                
                if value is None:
                    continue
                elif key == 'model_options':
                    df = df.with_column(column,
                            F.sql_expr(f"OBJECT_INSERT({column}, '{key}', TO_JSON({value}))")
                            )
                else:
                    df = df.with_column(
                        column,
                        F.sql_expr(f"OBJECT_INSERT({column}, '{key}', '{value}')")
                    )
        return df
    except Exception as e:
        raise Exception(f"Error adding metadata to DataFrame: {e}")

def run_prompt_template(
        session: Session,
        prompt_template_file: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        messages: Optional[list] = None,
        literal_variables: Optional[dict] = None,
        column_variables: Optional[dict] = None,
        origin_table: Optional[str] = None,
        model: Optional[str] = None,
        model_options: Optional[dict] = None,
        response_column: str = 'RESPONSE',
        output_table_name: Optional[str] = 'TEMP',
        table_type: Optional[str] = 'temporary',
        table_write_mode: str = 'overwrite',
        prompt_column: str = 'PROMPT'
        ) -> DataFrame:
    
    """
    Executes a prompt template by extracting the prompt configuration, 
        preparing the prompt table, running the model with the specified options, 
        and adding metadata to the response.
    
    Args:
        session (Session): The Snowflake session object.
        prompt_template_file (str): The file path to the prompt template configuration.
            If the file is in stage, use BUILD_SCOPED_FILE_URL to get the URL.
            If the file is local, use the local file path.
        name (Optional[str]): The name of the prompt template.
        version (Optional[str]): The version of the prompt template.
        messages (Optional[list]): The list of messages to use in the prompt template.
        literal_variables (Optional[dict]): The dictionary of literal variables to use in the prompt template.
        column_variables (Optional[dict]): The dictionary of column variables to use in the prompt template.
        origin_table (Optional[str]): The name of the origin table containing the prompts. 
            If None, it will be extracted from the prompt configuration.
        model (Optional[str]): The model to use for generating responses. 
            If None, it will be extracted from the prompt configuration.
        model_options (Optional[dict]): Additional options to pass to the model. 
            If None or empty, it will be extracted from the prompt configuration.
            model_options are optional in Cortex calls.
        response_column (str): The name of the column to store the model responses. Default is 'RESPONSE'.
        output_table_name (Optional[str]): The name of the output table to create. Default is 'TEMP'.
        table_type (Optional[str]): The type of the table to create (e.g., 'temporary'). Default is 'temporary'.
        table_write_mode (str): The mode to use when writing the output_table_name table (e.g., 'overwrite'). Default is 'overwrite'.
        prompt_column (str): The name of the column to write prepared prompts to in output_table_name. Default is 'PROMPT'.
            Column will not be included in output table. 
            Column name should not conflict wiht existing columns in origin_table.
    
    Returns:
        DataFrame: The DataFrame containing the model responses with added metadata.
    Raises:
        ValueError: If required parameters (origin_table or model) are missing.
    """

    logger = logging.getLogger("prompt_template_runner")

    # Set query tag for usage
    set_query_tag(session)

    # Start by extracting the prompt configuration if it exists
    if prompt_template_file is not None:
        prompt_config = extract_prompt(prompt_template_file)
    else: # Will populate with explicitly passed parameters in signature
        prompt_config = {}

    if name:
        prompt_config['name'] = name
    
    if version:
        prompt_config['version'] = version
    
    if isinstance(messages, list) and len(messages) > 0:
        prompt_config['messages'] = messages
    elif prompt_config.get('messages', None) is not None:
        prompt_config['messages'] = prompt_config.get('messages')
    else:
        logger.warning("No messages provided in prompt_config or signature.")
        raise ValueError("Error: No messages provided in prompt_config or signature.") # messages are required
    
    if isinstance(literal_variables, dict) and len(literal_variables) > 0:
        prompt_config['literal_variables'] = literal_variables
    else:
        prompt_config['literal_variables'] = prompt_config.get('literal_variables', {})
    
    if isinstance(column_variables, dict) and len(column_variables) > 0:
        prompt_config['column_variables'] = column_variables
    else:
        prompt_config['column_variables'] = prompt_config.get('column_variables', {})
    
    if origin_table is None: # If origin_table is passed in signature, it will take priority
        origin_table = prompt_config.get('origin_table', None)
        if origin_table is None:
            logger.warning("No origin_table provided in prompt_config or signature.")
            logger.info("Creating empty temporary table.")

            # Create empty temporary table for non-tabular generation
            session.range(1).write.mode('overwrite').save_as_table('temp_placeholder_tbl', table_type='temporary')
            origin_table = 'temp_placeholder_tbl' # Set origin_table to empty temporary table
    
    if model is None:
        model = prompt_config.get('model', None)
        if model is None:
            logger.info("No model provided in prompt_config or signature.")
            logger.info("Defaulting to model llama3.2-3b.")
            model = 'llama3.2-3b'

    # Check model availability
    available = test_model(session, model)

    if not available:
        raise ValueError(f"Error: Model {model} is not available in the current region.")

    if isinstance(model_options, dict) and len(model_options) > 0:
        model_options = model_options
    else:
        model_options = prompt_config.get('model_options', {})
    
    # Creates table with prompt column so we can use SQL complete with options against prepped prompt
    parse_prompt_template(
        session,
        prompt_config,
        origin_table,
        output_table_name,
        table_type,
        table_write_mode,
        prompt_column
    )

    # Run complete with options to add the response column
    df = run_complete_options(
        session,
        output_table_name,
        model,
        model_options,
        prompt_column,
        response_column
    )
    
    metadata = {
        'prompt_name': prompt_config.get('name', ''),
        'prompt_version': prompt_config.get('version', ''),
        'model_options': model_options
    }

    df = add_metadata(df, response_column, metadata)

    return df