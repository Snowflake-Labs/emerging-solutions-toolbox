# Python 3.8 type hints
from typing import Dict, List, Optional

from snowflake.snowpark import DataFrame
from snowflake.snowpark.session import Session

from src.app_utils import format_query_tag
from src.metrics import Metric, AnswerRelevancy
from src.snowflake_utils import QUERY_TAG


DEFAULT_CUSTOM_METRIC_NAME = "MyRelevancy"
DEFAULT_CUSTOM_METRIC_DESC = AnswerRelevancy().description
DEFAULT_CUSTOM_METRIC_PROMPT = AnswerRelevancy().prompt.strip()\
                                        .replace("\n", " ")\
                                        .replace("[", "\n[")\
                                        .replace("]", "]\n")


def run_metric(
    metrics: List[Metric],  # List of metrics
    metric_result_data: DataFrame,
    models: Dict[str, str],  # Dictionary of metric model names
    params: Dict[str, Dict[str, str]],  # Nested dictionary {metric_name: {key: value}}
    session: Session,
) -> DataFrame:
    """
    Executes child Metric class' evaluate method on a Snowpark dataframe.

    Calls the evaluate method are multi-threaded and multiple metrics are run in parallel.
    Results are returned as a Snowpark dataframe.
    Dataframe must have ROW_ID already in the dataframe.

    params is a nested dictionary with the following structure:
    {
        metric_name: {
            input_name: column_name
        }
    )
    models is a dictionary with the following structure:
    {
        metric_name: model_name
    )

    Args:
        metrics (list[Metric]): Metric child classes
        metric_result_data (Dataframe): Snowpark dataframe with data to evaluate.
        models (dict[str, str]): Dictionary of metric model names.
        params (dict[str, dict[str, str]]): Nested dictionary of metric parameter-column associations.
        session (Session): Snowpark session.

    Returns:
        Dataframe: Snowpark dataframe of selected data with metric results.

    """

    import multiprocessing

    from joblib import Parallel, delayed

    pandas_df = metric_result_data.to_pandas()
    # Parallel processing for each row in the dataframe
    results = Parallel(multiprocessing.cpu_count(), 
                        backend="threading")(
        delayed(
            lambda row: {
                "ROW_ID": row["ROW_ID"],  # Capture ROW_ID
                # Loop over each metric in the metrics list
                **{
                    metric.get_column(): metric.evaluate(
                        models[metric.name],  # Pass the model for each metric
                        **{
                            key: row[params[metric.name][key]]
                            for key in params[metric.name]
                        }
                    )  # Pass params as **kwargs for each metric
                    for metric in metrics
                },
            }
        )(row)
        for _, row in pandas_df.iterrows()
    )

    # Return the results as a dataframe in the session
    return session.create_dataframe(results)


def apply_metric(
    metrics: List[Metric],
    metric_result_data: DataFrame,
    models: Dict[str, str],
    params: Dict[str, Dict[str, str]],
    session: Session,
):
    """
    Applies Metric.evaluate calculation and join back to beginning dataframe based on ROW_ID.

    Results are returned as a Snowpark dataframe.
    Dataframe must have ROW_ID already in the dataframe.

    params is a nested dictionary with the following structure:
    {
        metric_name: {
            input_name: column_name
        }
    )
    models is a dictionary with the following structure:
    {
        metric_name: model_name
    )

    Args:
        metrics (list[Metric]): Metric child classes
        metric_result_data (Dataframe): Snowpark dataframe with data to evaluate.
        models (dict[str, str]): Dictionary of metric model names.
        params (dict[str, dict[str, str]]): Nested dictionary of metric parameter-column associations.
        session (Session): Snowpark session.

    Returns:
        Dataframe: Snowpark dataframe of selected data with metric results.

    """
    metric_df = run_metric(metrics, metric_result_data, models, params, session)

    return metric_result_data.join(metric_df, on="ROW_ID", how="left")


def metric_runner(
    session: Session,
    metrics: List[Metric],
    models: Dict[str, str],
    param_assignments: Dict[str, Dict[str, str]],
    source_sql: Optional[str] = None,
    source_df: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Runner for metric.evaluate against Snowpark dataframe.

    Results are returned as a Snowpark dataframe.
    Either source_sql or source_df must be provided.

    param_assignments is a nested dictionary with the following structure:
    {
        metric_name: {
            input_name: column_name
        }
    )
    models is a dictionary with the following structure:
    {
        metric_name: model_name
    )

    Args:
        session (Session): Snowpark session.
        metrics (list[Metric]): Metric child classes
        models (dict[str, str]): Dictionary of metric model names.
        param_assignments (Dataframe): Snowpark dataframe with data to evaluate.
        source_sql (str, Optional): SQL to derive source data.
        source_df (Dataframe, Optional): Snowpark dataframe with source data.

    Returns:
        Dataframe: Snowpark dataframe of selected data with metric results.

    """

    from src.snowflake_utils import add_row_id

    # Metric class is instantiated with session = None for compatibility across SiS, OSS streamlit, and SPROC
    for idx in range(len(metrics)):
        metrics[idx].session = session
    if source_sql:
        df = session.sql(source_sql.replace(";", ""))
    elif source_df:
        df = source_df
    else:
        raise ValueError(
            "No source data provided. Please provide either a SQL query or a Snowpark DataFrame."
        )

    df = add_row_id(df)

    df = apply_metric(
        metrics=metrics,
        metric_result_data=df,
        models=models,
        params=param_assignments,
        session=session,
    ).drop("ROW_ID")

    return df


def set_procedure_comment(
    session: Session,
    sproc_name: str,
    QUERY_TAG: Dict[str, str] = format_query_tag(QUERY_TAG),
) -> None:
    """
    Applied QUERY_TAG as a comment to an already created stored procedure.

    Args:
        session (Session): Snowpark session.
        sproc_name (string): Fully-qualified name of stored procedure.
        QUERY_TAG (dict[str, str]): JSON-like query tag.

    Returns:
        None

    """
    # PROCEDURE must have no arguments cause PROCEDUREs are identified by name + arguments
    # Below we pass empty paranthesese indicating no arguments.
    query = f"COMMENT ON PROCEDURE {sproc_name}() IS '{format_query_tag(QUERY_TAG)}'"
    session.sql(query).collect()


def register_saved_eval_sproc(
    session: Session,
    database: str,
    schema: str,
    stage: str,
    eval_name: str,
    metrics: List[Metric],
    source_sql: str,
    models: Dict[str, str],
    param_assignments: Dict[str, Dict[str, str]],
) -> Dict[str, str]:
    """
    Registers a stored procedure for running a metric evaluation.

    Captures source data and metric configuration as a Snowflake Stored Procedure.
    param_assignments is a nested dictionary with the following structure:
    {
        metric_name: {
            input_name: column_name
        }
    )
    models is a dictionary with the following structure:
    {
        metric_name: model_name
    )

    Returned ASSOCIATED_OBJECTS will contain the name of the stored procedure:
    {
        "PROCEDURE": fully-qualified sproc_name
    }

    Args:
        session (Session): Snowpark session.
        database (str): Snowflake database name.
        schema (str): Snowflake schema name.
        stage (str): Non-qualified Snowflake stage name.
        eval_name (str): Name of the evaluation.
                         Should not have spaces or special characters.
                         Characters must follow Snowflake object naming rules.
        metrics (list[Metric]): Metric child classes
        source_sql (str, Optional): SQL to derive source data.
        models (dict[str, str]): Dictionary of metric model names.
        param_assignments (Dataframe): Snowpark dataframe with data to evaluate.


    Returns:
        Dictionary of ASSOCIATED OBJECTS of evaluation

    """

    from snowflake.snowpark.functions import sproc
    from snowflake.snowpark.types import StructType

    object_prefix = f"{database}.{schema}.{eval_name}"
    sproc_name = object_prefix + "_SAVED_EVAL_SPROC"
    ASSOCIATED_OBJECTS = {"PROCEDURE": sproc_name}
    stage = f"@{database}.{schema}.{stage}"

    # Session attribute cannot be serialized, so set to None for SPROC
    for idx in range(len(metrics)):
        if metrics[idx].session is not None:
            metrics[idx].session = None

    @sproc(
        name=sproc_name,
        stage_location=stage,
        session=session,
        imports=[
            f"{stage}/src.zip",
        ],
        packages=[
            "joblib==1.4.2",
            "pandas==2.0.3",
            "snowflake-snowpark-python==1.22.1",
            "snowflake-ml-python==1.6.2",
            "streamlit==1.35.0",
        ],
        is_permanent=True,
        replace=True,
        return_type=StructType(),
    )
    def metric_sproc(session: Session):
        from src.metric_utils import metric_runner

        df = metric_runner(
            session=session,
            metrics=metrics,
            models=models,
            source_sql=source_sql,
            source_df=None,
            param_assignments=param_assignments,
        )
        return df

    set_procedure_comment(session, sproc_name)

    return ASSOCIATED_OBJECTS


def register_auto_eval_sproc(
    session: Session,
    stage: str,
    sproc_name: str,
    output_table_name: str,
    metrics: List[Metric],
    source_sql: str,
    models: Dict[str, str],
    param_assignments: Dict[str, Dict[str, str]],
) -> None:
    """
    Registers a Snowflake stored procedure for running a metric evaluation for automated evaluations.

    Note: Stored procedures for saved and automated evaluations are different.

    Captures source data and metric configuration as a Snowflake Stored Procedure.
    Results written to a table in Snowflake.
    param_assignments is a nested dictionary with the following structure:
    {
        metric_name: {
            input_name: column_name
        }
    )
    models is a dictionary with the following structure:
    {
        metric_name: model_name
    )


    Args:
        session (Session): Snowpark session.
        stage (str): Fully-qualified Snowflake stage name.
        sproc_name (str): Fully-qualified Snowflake stored procedure name.
        output_table_name (str): Fully-qualified Snowflake stored table name.
        metrics (list[Metric]): Metric child classes
        source_sql (str, Optional): SQL to derive source data.
        models (dict[str, str]): Dictionary of metric model names.
        param_assignments (Dataframe): Snowpark dataframe with data to evaluate.


    Returns:
        None

    """

    from snowflake.snowpark.functions import sproc

    # Session attribute cannot be serialized, so set to None for SPROC
    for idx in range(len(metrics)):
        if metrics[idx].session is not None:
            metrics[idx].session = None

    @sproc(
        name=sproc_name,
        stage_location=stage,
        session=session,
        imports=[
            f"{stage}/src.zip",
        ],
        packages=[
            "joblib==1.4.2",
            "pandas==2.0.3",
            "snowflake-snowpark-python==1.22.1",
            "snowflake-ml-python==1.6.2",
            "streamlit==1.35.0",
        ],
        is_permanent=True,
        replace=True,
    )
    def metric_sproc(session: Session) -> None:
        from src.metric_utils import metric_runner
        from src.snowflake_utils import save_eval_to_table

        df = metric_runner(
            session=session,
            metrics=metrics,
            models=models,
            source_sql=source_sql,
            source_df=None,
            param_assignments=param_assignments,
        )

        remove_cols = [col for col in df.columns if col.startswith("METADATA$")]
        df = df.drop(*remove_cols)
        _ = save_eval_to_table(df, output_table_name)

    set_procedure_comment(session, sproc_name)


def make_eval_view(
    session: Session,
    source_sql: str,
    view_name: str,
) -> None:
    """Creates Snowflake view using CREATE VIEW AS with source_sql SELECT statement.

    Args:
        session (Session): Snowpark session.
        source_sql (str): Source SQL to use in the SELECT of the CREATE VIEW AS SELECT statement
        view_name (str): Fully-qualified Snowflake view name to create.
    """

    query = f"""
    CREATE OR REPLACE VIEW {view_name}
    COMMENT = '{format_query_tag(QUERY_TAG)}'
    AS {source_sql}
    """
    session.sql(query).collect()


def create_eval_stream(
    session: Session,
    view_name: str,
    stream_name: str,  # Will get from database, schema, and eval name
) -> None:
    """Creates Snowflake stream on view.

    Args:
        session (Session): Snowpark session.
        view_name (str): Fully-qualified Snowflake view name.
        stream_name (str): Fully-qualified Snowflake stream name to create.
    """

    query = f"""
    CREATE OR REPLACE STREAM {stream_name}
    ON VIEW {view_name}
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = '{format_query_tag(QUERY_TAG)}'
    """
    session.sql(query).collect()


def get_stream_inserts_sql(
    session: Session,
    stream_name: str,  # Fully-qualified stream name
) -> str:
    """Returns underlying SQL to extract and filter a STREAM table to capture INSERTS.

    Args:
        session (Session): Snowpark session.
        stream_name (str): Fully-qualified Snowflake stream name to create.
    """

    from snowflake.snowpark.functions import col

    from src.snowflake_utils import get_sql

    df = session.table(stream_name).filter(col("METADATA$ACTION") == "INSERT")
    return get_sql(df, 0)


def create_eval_task(
    session: Session,
    warehouse: str,
    task_name: str,
    stream_name: str,
    sproc_name: str,
) -> None:
    """Creates Snowflake task with stream trigger to call stored procedure.

    Args:
        session (Session): Snowpark session.
        warehouse (str): Snowflake warehouse name for task.
        task_name (str): Fully-qualified Snowflake task name to create.
        stream_name (str): Fully-qualified Snowflake stream name to create.
        sproc_name (str): Fully-qualified Snowflake stored procedure name to call.
    """

    create_query = f"""
    CREATE OR REPLACE TASK {task_name}
    WAREHOUSE = {warehouse}
    COMMENT = '{format_query_tag(QUERY_TAG)}'
    WHEN SYSTEM$STREAM_HAS_DATA('{stream_name}')
    AS CALL {sproc_name}()
    """
    # Creates tasks start in SUSPENDED state
    resume_query = f"ALTER TASK {task_name} RESUME"
    for query in [create_query, resume_query]:
        session.sql(query).collect()


def automate_eval_objects(
    session: Session,
    stage: str,
    warehouse: str,
    database: str,
    schema: str,
    eval_name: str,  # Just the name
    source_sql: str,
    metrics: List[Metric],
    models: Dict[str, str],
    param_assignments: Dict[str, Dict[str, str]],
) -> Dict[str, str]:
    """
    Orchestrates all object creation for setting an automated evaluation.

    param_assignments is a nested dictionary with the following structure:
    {
        metric_name: {
            input_name: column_name
        }
    )
    models is a dictionary with the following structure:
    {
        metric_name: model_name
    )

    Returned ASSOCIATED_OBJECTS will contain the following:
    {
        "PROCEDURE": fully-qualified sproc_name,
        "STREAM": fully-qualified stream_name,
        "TABLE": fully-qualified table_name,
        "TASK": fully-qualified task_name,
        "VIEW": fully-qualified view_name
    }

    Args:
        session (Session): Snowpark session.
        stage (str): Non-qualified Snowflake stage name.
        warehouse (str): Snowflake warehouse name for task.
        database (str): Snowflake database name.
        schema (str): Snowflake schema name.
        eval_name (str): Name of the evaluation.
                         Should not have spaces or special characters.
                         Characters must follow Snowflake object naming rules.
        source_sql (str, Optional): SQL to derive source data.
        metrics (list[Metric]): Metric child classes
        models (dict[str, str]): Dictionary of metric model names.
        param_assignments (Dataframe): Snowpark dataframe with data to evaluate.

    Returns:
        Dictionary of ASSOCIATED OBJECTS of evaluation

    """
    # All first class objects should be fully qualified with suffix to avoid conflicts
    stage = f"@{database}.{schema}.{stage}"
    object_prefix = f"{database}.{schema}.{eval_name}"
    tag = "_AUTO_EVAL_"

    def set_name(object_type: str):
        return object_prefix + tag + object_type

    ASSOCIATED_OBJECTS = {}
    for object_type in ["PROCEDURE", "VIEW", "STREAM", "TASK", "TABLE"]:
        ASSOCIATED_OBJECTS[object_type] = set_name(object_type)

    # Create view and stream on original source SQL
    make_eval_view(session, source_sql, ASSOCIATED_OBJECTS["VIEW"])
    create_eval_stream(
        session, ASSOCIATED_OBJECTS["VIEW"], ASSOCIATED_OBJECTS["STREAM"]
    )

    # Capture SQL from stream inserts to serve as source for metric evaluation
    metric_source_sql = get_stream_inserts_sql(session, ASSOCIATED_OBJECTS["STREAM"])

    # Session attribute cannot be serialized, so set to None for SPROC
    for idx in range(len(metrics)):
        if metrics[idx].session is not None:
            metrics[idx].session = None
    # Create SPROC that will be triggered when stream has inserts and run metric
    register_auto_eval_sproc(
        session,
        stage,
        ASSOCIATED_OBJECTS["PROCEDURE"],
        ASSOCIATED_OBJECTS["TABLE"],
        metrics,
        metric_source_sql,  # This is the SQL that captures Stream INSERTS
        models,
        param_assignments,
    )

    create_eval_task(
        session,
        warehouse,
        ASSOCIATED_OBJECTS["TASK"],
        ASSOCIATED_OBJECTS["STREAM"],
        ASSOCIATED_OBJECTS["PROCEDURE"],
    )

    return ASSOCIATED_OBJECTS


def create_custom_metric(metric_name: str,
                         description: str,
                         prompt: str,
                         required_args: Dict[str, str],
                         default_model: str = "llama3.1-8b",
                         ):
    """
    Creates a custom metric class that inherits from the Metric class.

    required_args is a dictionary with the following structure: {input_name: description}
    f-string, e.g. {question} in the prompt should match the keys of require_args.

    Args:
        metric_name (str): Metric name.
        description (str): Metric description.
        prompt (str): LLM-as-a-Judge prompt with f-strings that match required_args keys.
        required_args (dict[str, str]): Input arguments required for the metric with a description of each as the value.
        default_model (str): Snowflake Cortex LLM model name.

    Returns:
        CustomClass: Custom metric class

    """

    class_name = ''.join(t.title().replace(" ","") for t in metric_name.split())
        
    CustomClass = type(
        class_name,
        (Metric,),
        {
            "__init__": lambda self, model=default_model: Metric.__init__(
                self,
                name=metric_name,
                description=description,
                prompt=prompt,
                required=required_args,
            ) or setattr(self, 'model', model)
        }
    )
    return CustomClass()