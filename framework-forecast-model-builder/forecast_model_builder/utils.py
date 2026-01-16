from __future__ import annotations

import importlib.metadata
import json
import logging
import pkgutil
import sys
import hashlib
import json
import pickle
import math
from textwrap import dedent

import snowflake.snowpark
from snowflake.cortex import Complete
from snowflake.ml.registry import registry
from snowflake.ml.feature_store import FeatureView, FeatureStore
from snowflake.snowpark import Session, exceptions, DataFrame
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
    >>> from forecast_model_builder.utils import predict_baseline
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

def version_featureview(feature_store: FeatureStore, feature_view: FeatureView) -> str:
    """
    Determine the appropriate version for a feature view based on its definition.

    Implements a smart versioning strategy that:
      - Creates a new version (increments) when breaking changes are detected
        (changes to entities, query, timestamp column, or clustering)
      - Updates the existing version in-place for non-breaking metadata changes
        (refresh frequency, warehouse, or description)
      - Returns version "1" for brand new feature views

    This approach minimizes unnecessary version proliferation while ensuring
    that downstream consumers are protected from breaking schema changes.

    Args:
        feature_store (FeatureStore): The initialized Snowflake Feature Store instance
        feature_view (FeatureView): The new feature view to version

    Returns:
        str: The version string to use when registering the feature view.
             Either a new incremented version or the existing version number.

    Examples:
        >>> version = _version_featureview(fs, my_feature_view)
        >>> fs.register_feature_view(my_feature_view, version=version)

    """
    name = str(feature_view.name)

    # Check if any versions of this feature view already exist
    existing = feature_store.list_feature_views().filter(F.col("NAME") == name).collect()

    if existing:
        # Find the highest (most recent) version number
        last_version = max([int(row.VERSION) for row in existing])
        last_feature_view = feature_store.get_feature_view(name=name, version=str(last_version))

        # Compare entities - a change in entities is a breaking change
        last_ent = [e.name for e in last_feature_view.entities]
        new_ent = [e.name for e in feature_view.entities]
        if last_ent != new_ent:
            return str(last_version+1)

        # Check for breaking changes in core feature view attributes
        # These attributes affect the data schema or query logic
        breaking_change_keys = ['_query', '_name','_timestamp_col','_cluster_by']
        for k in breaking_change_keys:
            if getattr(last_feature_view, k) != getattr(feature_view, k):
                return str(last_version+1)

        # For non-breaking metadata changes, update the existing version in-place
        # These changes don't affect the data schema, so no new version is needed
        metadata_keys = ["refresh_freq", "warehouse", "desc"]
        updates = {
            k: getattr(feature_view, k)
            for k in metadata_keys
            if getattr(feature_view, k) != getattr(last_feature_view, k)
        }
        if updates:
            feature_store.update_feature_view(name=name, version=str(last_version), **updates)

        return str(last_version)

    # No existing versions found - this is a new feature view
    return str(1)

def version_data(df:DataFrame) -> str:

    """Computes an md5 hash on the data itself to be used as a dataset version."""

    return hashlib.md5(str(df.select_expr("HASH_AGG(*)")).encode('utf-8')).hexdigest().upper()

def perform_inference(session: Session, inference_input_df: DataFrame, model_version: ModelVersion):
    # If the inference dataset does not have the TARGET column already, add it and fill it with null values

    constants = model_version.show_metrics()["user_settings"]
    target_col = constants['TARGET_COLUMN']
    use_context = constants['USE_CONTEXT']
    batch_size = constants['INFERENCE_APPROX_BATCH_SIZE']
    time_col = constants['TIME_PERIOD_COLUMN']

    if target_col not in inference_input_df.columns:
        inference_input_df = inference_input_df.with_column(target_col, F.lit(None).cast(T.FloatType()))

    if not use_context:
        storage_tbl_nm = constants['MODEL_BINARY_STORAGE_TBL_NM']
        model_bytes_table = (
            session.table(storage_tbl_nm)
            .filter(F.col("MODEL_NAME") == model_version.model_name)
            .filter(F.col("MODEL_VERSION") == model_version.version_name)
            .select("GROUP_IDENTIFIER_STRING", "MODEL_BINARY")
        )

        # NOTE: We inner joint to the model bytes table to ensure that we only try run inference on partitions that have a model.
        inference_input_df = inference_input_df.join(
            model_bytes_table, on=["GROUP_IDENTIFIER_STRING"], how="inner"
        )

    # Add a column called BATCH_GROUP,
    #   which has the property that for each unique value there are roughly the number of records specified in batch_size.
    # Use that to create a PARTITION_ID column that will be used to run inference in batches.
    # We do this to avoid running out of memory when performing inference on a large number of records.
    largest_partition_record_count = (
        inference_input_df.group_by("GROUP_IDENTIFIER_STRING")
        .agg(F.count("*").alias("PARTITION_RECORD_COUNT"))
        .agg(F.max("PARTITION_RECORD_COUNT").alias("MAX_PARTITION_RECORD_COUNT"))
        .collect()[0]["MAX_PARTITION_RECORD_COUNT"]
    )

    number_of_batches = math.ceil(largest_partition_record_count / batch_size)
    inference_input_df = (
        inference_input_df.with_column(
            "BATCH_GROUP", F.abs(F.random(123)) % F.lit(number_of_batches)
        )
        .with_column(
            "PARTITION_ID",
            F.concat_ws(
                F.lit("__"), F.col("GROUP_IDENTIFIER_STRING"), F.col("BATCH_GROUP")
            ),
        )
        .drop("RANDOM_NUMBER", "BATCH_GROUP")
    )

    # Look at a couple rows of the inference input data
    print(f"Inference input data row count: {inference_input_df.count()}")
    print(
        f"Number of end partition invocations to expect in the query profile: {inference_input_df.select('PARTITION_ID').distinct().count()}"
    )
    # Use the model to score the input data
    inference_result = model_version.run(inference_input_df, partition_column="PARTITION_ID").select(
        "_PRED_",
        F.col("GROUP_IDENTIFIER_STRING_OUT_").alias("GROUP_IDENTIFIER_STRING"),
        F.col(f"{time_col}_OUT_").alias(time_col),
    )

    return inference_result
