# Python 3.8 type hints
from typing import Any, Dict, Optional, Union

import streamlit as st
from snowflake.snowpark import DataFrame
from snowflake.snowpark.session import Session


@st.cache_resource()
def get_connection() -> Session:
    """Returns a Snowpark session object.

    Operates on environment variables SiS method."""

    try:
        from snowflake.snowpark.context import get_active_session

        session = get_active_session()
    except:
        import os

        from dotenv import load_dotenv

        load_dotenv(override=True)
        # Snowpark session
        connection_parameters = {
            "user": os.getenv("USER"),
            "password": os.getenv("PASSWORD"),
            "account": os.getenv("ACCOUNT"),
            "role": os.getenv("ROLE"),
            "warehouse": os.getenv("WAREHOUSE"),
            "database": os.getenv("DATABASE"),
            "schema": os.getenv("SCHEMA"),
        }
        session = Session.builder.configs(connection_parameters).create()
    return session


def get_sql(df: DataFrame, index: Optional[int] = 0) -> str:
    """
    Returns the SQL query/queries that generated the Snowpark dataframe.

    Args:
        df (Dataframe): Snowpark dataframe
        index (int): Index of SQL query to return. Default is 0.

    Returns:
        List[str] or str of SQL queries.

    """

    if index is not None:
        return df.queries["queries"][index]
    else:
        return df.queries["queries"]


def run_async_sql_complete(session: Session, model: str, prompt: str):
    """
    Returns Cortex COMPLETE via async .sql method of snowpark session.

    Args:
        session (Session): Snowpark session
        model (str): Cortex Complete supported LLM.
        prompt (str): User input to prompt LLM.

    Returns:
        str

    """

    # Mitigates Snowpark SQL parsing error due to single quotes
    prompt = prompt.replace("'", "\\'")
    query = f"""SELECT
    TRIM(snowflake.cortex.complete('{model}',
    '{prompt}'))
    """
    return session.sql(query).collect_nowait().result()[0][0]


def run_complete(session: Session, model: str, prompt: str) -> str:
    """
    Returns Cortex COMPLETE LLM response.

    Args:
        session (Session): Snowpark session
        model (str): Cortex Complete supported LLM.
        prompt (str): User input to prompt LLM.

    Returns:
        str

    """

    from snowflake.cortex import Complete

    prompt = prompt.replace("'", "\\'")

    return Complete(model=model, prompt=prompt, session=session)


def return_sql_result(session: Session, sql: str) -> Union[str, None]:
    """
    Returns the result of a SQL query as a JSON-like string.

    Args:
        session (Session): Snowpark session
        sql (str): SELECT statement

    Returns:
        str

    """

    from snowflake.snowpark import functions as F

    result = (
        session.sql(sql.replace(";", ""))
        .limit(100)
        .select(F.to_varchar(F.array_agg(F.object_construct("*"))))
    )
    try:
        return result.collect_nowait().result()[0][0]
    except Exception as e:
        st.error(f"Error: {e}")


def join_data(
    inference_data: DataFrame,
    ground_data: DataFrame,
    inference_key: str,
    ground_key: str,
    limit: Optional[int] = 50,
) -> DataFrame:
    """
    Joins inference_data and ground_data dataframes using inference_key column and ground_key column and returns joined dataset.

    Args:
        inference_data (DataFrame): Inference data
        ground_data (DataFrame): Ground truth data
        inference_key (str): Column name to join on in inference_data
        ground_key (str): Column name to join on in ground_data
        limit (int): Number of rows to return. Default is 50.

    Returns:
        Snowpark dataframe

    """

    # Snowpark will duplicate join keys if they match unless it's a simple single-key join
    if inference_key == ground_key:
        on = inference_key
    else:
        inference_data[inference_key] == ground_data[ground_key]

    data = inference_data.join(
        ground_data,
        on=on,
        lsuffix="_INFERENCE",
        rsuffix="_GROUND",
    )
    if limit:
        return data.limit(limit)
    else:
        return data


def add_row_id(snpk_df: DataFrame) -> DataFrame:
    """Adds a ROW_ID column to a Snowpark dataframe for self-joining of dataframes after running metric evaluations."""

    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    return snpk_df.with_column("ROW_ID", F.row_number().over(Window.order_by(F.lit(1))))


def save_eval_to_table(snpk_df: DataFrame, table_name: str) -> str:
    """Saves evaluation data to a table in Snowflake.

    Write mode is set to append so that multiple evaluations can be saved to the same table.
    New rows will be appended. Table will be created if it does not exist.
    METRIC_DATETIME will be added if not already present in the DataFrame.

    Args:
        snpk_df (DataFrame): DataFrame to save to table.
        table_name (string): Fully-qualified Snowflake table to write data to.

    Returns:
        string: Confirmation message.
    """

    from snowflake.snowpark.functions import current_timestamp

    columns = snpk_df.columns
    if "ROW_ID" in columns:
        snpk_df = snpk_df.drop("ROW_ID")
    if "METRIC_DATETIME" not in columns:
        snpk_df = snpk_df.with_column("METRIC_DATETIME", current_timestamp())
    snpk_df.write.save_as_table(table_name, mode="append")
    return f"Metric evaluation results saved to {table_name}."


def insert_to_eval_table(
    session: Session,
    table_name: str,
    join_key: str = "EVAL_NAME",
    **metadata: Dict[str, Optional[str]],
) -> str:
    """
    Inserts/updates evaluation table with metadata.

    If join_key value exists, the row will be updated. Otherwise, the row will be added.

    Args:
        session (Session): Snowpark session.
        table_name (str): Fully-qualified Snowflake table name.
        join_key (str): Name of evaluation to check if it already exists. Default is "EVAL_NAME".
        metadata (dict[str, str]): Metadata to insert/update in the table.

    Returns:
        String confirmation message

    """
    import snowflake.snowpark.functions as F

    new_df = session.create_dataframe([metadata])

    current_df = session.table(table_name)

    # Dynamically create the dictionaries for when_matched and when_not_matched
    update_dict = {key: new_df[key] for key in metadata if key != join_key}
    insert_dict = {key: new_df[key] for key in metadata}

    _ = current_df.merge(
        new_df,
        current_df[join_key] == new_df[join_key],
        [
            F.when_matched().update(update_dict),
            F.when_not_matched().insert(insert_dict),
        ],
    )

    return "Added to Evaluation Homepage."


def call_sproc(session: Session, name: str) -> Any:
    """Calls a stored procedure in Snowflake and returns the result."""
    return session.call(name)


def call_async_sproc(session: Session, sproc: str, input_value: dict[str, Any]) -> Any:
    return session.sql(f"CALL {sproc}({input_value})").collect_nowait().result()[0][0]
