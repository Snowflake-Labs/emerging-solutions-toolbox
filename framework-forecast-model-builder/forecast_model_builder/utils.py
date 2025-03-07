from __future__ import annotations

import importlib.metadata
import json
import logging
import pkgutil
import sys
from textwrap import dedent

import snowflake.snowpark
from snowflake.cortex import Complete
from snowflake.ml.registry import registry
from snowflake.snowpark import Session, exceptions
from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session


def connect(connection_name: str = "default") -> Session:
    """Establish a Snowpark session.

    If the "snowflake_import_directory" option is set, it attempts to get the active session.
    If the active session is not available, it creates a new session.
    If the "snowflake_import_directory" option is not set, it attempts to create a new session.
    Logs an error message if session creation fails.

    Parameters
    ----------
    connection_name : str, optional
        The name of the connection to use, by default 'default'

    Returns
    -------
    Session
        A Snowpark session object

    Raises
    ------
    exceptions.SnowparkSessionException
        If unable to create a new session.

    """
    if sys._xoptions.get("snowflake_import_directory"):
        try:
            session = get_active_session()
        except exceptions.SnowparkSessionException:
            session = Session.builder.config(
                "connection_name", connection_name
            ).getOrCreate()
    else:
        try:
            session = Session.builder.config(
                "connection_name", connection_name
            ).getOrCreate()
        except exceptions.SnowparkSessionException as e:
            logging.error(f"Failed to connect: {e}")
            print(f"Error: {e}")
    return session


def predict_baseline(
    df: snowflake.snowpark.DataFrame,
    target_column: str,
    lag_column: str,
    lag_interval: int,
) -> snowflake.snowpark.DataFrame:
    """Predict the target column using a simple lagged value.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The DataFrame containing the target column.
    target_column : str
        The name of the column to predict.
    lag_column : str
        The name of the column use for ordering the lag.
    lag_interval : int
        The number of periods to lag the target column.

    Returns
    -------
    snowflake.snowpark.DataFrame
        The input DataFrame with a new column containing the predictions.

    Examples
    --------
    >>> from ml_forecasting_incubator.utils import predict_baseline
    >>> from snowflake.snowpark import Session
    >>> from snowflake.snowpark import types as T
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"ID": 1, "VALUE": 1.0},
    ...     {"ID": 2, "VALUE": 2.0},
    ...     {"ID": 3, "VALUE": 3.0},
    ... ]
    >>> df = session.create_dataframe(
    ...     d,
    ...     schema=T.StructType(
    ...         [
    ...             T.StructField("ID", T.LongType()),
    ...             T.StructField("VALUE", T.FloatType()),
    ...         ]
    ...     ),
    ... )
    >>> result_df = predict_baseline(df, "VALUE", "ID", 1)
    >>> result_df.show()
    ---------------------------------
    |"ID"  |"VALUE"  |"PREDICTION"  |
    ---------------------------------
    |1     |1.0      |NULL          |
    |2     |2.0      |1.0           |
    |3     |3.0      |2.0           |
    ---------------------------------

    """
    window = snowflake.snowpark.Window().orderBy(F.col(lag_column))
    prediction_expr = F.coalesce(
        F.lag(F.col(target_column), lag_interval).over(window),
        F.col(target_column),
    )
    return df.with_column(
        "PREDICTION",
        prediction_expr,
    )


def explain_metrics(  # noqa: D103
    model: str = "llama3.1-70b", metrics: dict = {}
) -> str:
    metrics = json.dumps(metrics)
    prompt = dedent(
        f"""
    [INST] You are analyzing the performance of a forecasting model.
    [INST] Here are various regression metrics to evaluate the model.
    [INST] The model has been trained on a dataset and made predictions.
    [INST] Do not mention anything about metrics not provided in the JSON data.
    {metrics}
    """
    )
    return Complete(
        model,
        prompt,
    )


def obtain_env_specs() -> dict:
    """Obtain the versions of the installed packages.

    Returns
    -------
    dict
        A dictionary containing the package names and their versions.

    """
    module_dict = {}
    for finder, module_name, is_pkg in pkgutil.iter_modules():
        try:
            distribution = importlib.metadata.distribution(module_name)
            version = distribution.version
            module_dict[module_name] = version
        except importlib.metadata.PackageNotFoundError:
            continue

    return module_dict


def get_next_model_version_from_storage_tbl(
    session: Session, model_schema: str, model_storage_tbl_nm: str, model_name: str
) -> int:
    """Get the name of the next version of a model from the model binary storage table.

    The function will increment the maximum version number by 1. This assumes that the version naming convention is "V1", "V2", etc.

    Parameters
    ----------
    session : Session
        The Snowpark session object.
    model_schema : str
        The name of the schema where the model binary storage table is located.
    model_storage_tbl_nm : str
        The name of the model binary storage table.
    model_name : str
        The name of the model.

    Returns
    -------
    str
        The next version of the model, which is 1 greater then the maximum version.

    """
    if (
        session.sql(
            f"SHOW TABLES LIKE '{model_storage_tbl_nm}' in SCHEMA {model_schema}"
        ).count()
        <= 0
    ):
        return "V1"
    model_storage_sdf = session.table(f"{model_schema}.{model_storage_tbl_nm}").filter(
        F.col("MODEL_NAME") == model_name
    )
    if model_storage_sdf.count() == 0:
        return "V1"
    versions = [
        row["MODEL_VERSION"]
        for row in model_storage_sdf.select("MODEL_VERSION").distinct().collect()
    ]
    max_version = max(int(v[1:]) for v in versions)
    return f"V{max_version + 1}"


def get_next_model_version_from_registry(reg: registry, model_name: str) -> str:
    """Get the name of the next version of a model.

    The function will increment the maximum version number by 1. This assumes that the version naming convention is "V1", "V2", etc.

    Parameters
    ----------
    reg : registry
        The registry object.
    model_name : str
        The name of the model.

    Returns
    -------
    str
        The next version of the model, which is 1 greater then the maximum version.

    """
    models = reg.show_models()
    if models.empty:
        return "V1"
    models = models[models["name"] == model_name]
    if len(models.index) == 0:
        return "V1"
    versions = json.loads(models["versions"][0])
    max_version = max(int(v[1:]) for v in versions)
    return f"V{max_version + 1}"


def get_next_model_version(
    session: Session,
    model_schema: str,
    model_storage_tbl_nm: str,
    reg: registry,
    model_name: str,
) -> dict:
    """Get the name of the next version of a model.

    The function compares the maximum version number from the model binary storage table and the registry, and returns the greater of the two.
    This assumes that the version naming convention is "V1", "V2", etc.

    Parameters
    ----------
    session : Session
        The Snowpark session object.
    model_schema : str
        The name of the schema where the model binary storage table is located.
    model_storage_tbl_nm : str
        The name of the model binary storage table.
    reg : registry
        The registry object.
    model_name : str
        The name of the model.

    Returns
    -------
    str
        The next version of the model, which is 1 greater then the maximum version.

    """
    storage_tbl_next_version = get_next_model_version_from_storage_tbl(
        session, model_schema, model_storage_tbl_nm, model_name
    )
    registry_next_version = get_next_model_version_from_registry(reg, model_name)
    new_model_version = max(
        int(v[1:]) for v in [storage_tbl_next_version, registry_next_version]
    )

    output_dict = {
        "storage_tbl_next_version": storage_tbl_next_version,
        "registry_next_version": registry_next_version,
        "new_model_version": f"V{new_model_version}",
    }
    return output_dict
