from __future__ import annotations

from decimal import Decimal

import snowflake.snowpark
from snowflake.snowpark import functions as F


def mean_absolute_error(
    df: snowflake.snowpark.DataFrame, y_true: str, y_pred: str
) -> Decimal:
    """Calculate the mean absolute error between two columns in a DataFrame.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The DataFrame containing the columns to compare.
    y_true : str
        The name of the column containing the true values.
    y_pred : str
        The name of the column containing the predicted values.

    Returns
    -------
    Decimal
        The mean absolute error between the two columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.metrics import mean_absolute_error
    >>> from snowflake.snowpark import Session
    >>> from snowflake.snowpark import types as T
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"ACTUAL": 5.0, "PREDICTION": 5.0},
    ...     {"ACTUAL": 6.0, "PREDICTION": 7.0},
    ...     {"ACTUAL": 7.0, "PREDICTION": 7.5},
    ... ]
    >>> df = session.create_dataframe(
    ...     d,
    ...     schema=T.StructType(
    ...         [
    ...             T.StructField("ACTUAL", T.FloatType()),
    ...             T.StructField("PREDICTION", T.FloatType()),
    ...         ]
    ...     ),
    ... )
    >>> mean_absolute_error(df, "ACTUAL", "PREDICTION")
    0.5

    """
    expr = F.mean(F.abs(F.col(y_true) - F.col(y_pred)))
    return df.select(expr).collect()[0][0]


def mean_squared_error(  # noqa: D103
    df: snowflake.snowpark.DataFrame, y_true: str, y_pred: str
) -> Decimal:
    """Calculate the mean absolute error between two columns in a DataFrame.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The DataFrame containing the columns to compare.
    y_true : str
        The name of the column containing the true values.
    y_pred : str
        The name of the column containing the predicted values.

    Returns
    -------
    Decimal
        The mean absolute error between the two columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.metrics import mean_squared_error
    >>> from snowflake.snowpark import Session
    >>> from snowflake.snowpark import types as T
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"ACTUAL": 5.0, "PREDICTION": 5.0},
    ...     {"ACTUAL": 6.0, "PREDICTION": 7.0},
    ...     {"ACTUAL": 7.0, "PREDICTION": 7.5},
    ... ]
    >>> df = session.create_dataframe(
    ...     d,
    ...     schema=T.StructType(
    ...         [
    ...             T.StructField("ACTUAL", T.FloatType()),
    ...             T.StructField("PREDICTION", T.FloatType()),
    ...         ]
    ...     ),
    ... )
    >>> mean_squared_error(df, "ACTUAL", "PREDICTION")
    0.4166666666666667

    """
    expr = F.mean((F.col(y_true) - F.col(y_pred)) ** 2)
    return df.select(expr).collect()[0][0]


def root_mean_squared_error(  # noqa: D103
    df: snowflake.snowpark.DataFrame, y_true: str, y_pred: str
) -> Decimal:
    """Calculate the root mean absolute error between two columns in a DataFrame.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The DataFrame containing the columns to compare.
    y_true : str
        The name of the column containing the true values.
    y_pred : str
        The name of the column containing the predicted values.

    Returns
    -------
    Decimal
        The root mean absolute error between the two columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.metrics import root_mean_squared_error
    >>> from snowflake.snowpark import Session
    >>> from snowflake.snowpark import types as T
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"ACTUAL": 5.0, "PREDICTION": 5.0},
    ...     {"ACTUAL": 6.0, "PREDICTION": 7.0},
    ...     {"ACTUAL": 7.0, "PREDICTION": 7.5},
    ... ]
    >>> df = session.create_dataframe(
    ...     d,
    ...     schema=T.StructType(
    ...         [
    ...             T.StructField("ACTUAL", T.FloatType()),
    ...             T.StructField("PREDICTION", T.FloatType()),
    ...         ]
    ...     ),
    ... )
    >>> root_mean_squared_error(df, "ACTUAL", "PREDICTION")
    0.6454972243679028

    """
    expr = F.sqrt(F.mean((F.col(y_true) - F.col(y_pred)) ** 2))
    return df.select(expr).collect()[0][0]


def r2_score(df: snowflake.snowpark.DataFrame, y_true: str, y_pred: str) -> Decimal:
    """Calculate the R-squared score between two columns in a DataFrame.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The DataFrame containing the columns to compare.
    y_true : str
        The name of the column containing the true values.
    y_pred : str
        The name of the column containing the predicted values.

    Returns
    -------
    Decimal
        The R-squared score between the two columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.metrics import r2_score
    >>> from snowflake.snowpark import Session
    >>> from snowflake.snowpark import types as T
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"ACTUAL": 5.0, "PREDICTION": 5.0},
    ...     {"ACTUAL": 6.0, "PREDICTION": 7.0},
    ...     {"ACTUAL": 7.0, "PREDICTION": 7.5},
    ... ]
    >>> df = session.create_dataframe(
    ...     d,
    ...     schema=T.StructType(
    ...         [
    ...             T.StructField("ACTUAL", T.FloatType()),
    ...             T.StructField("PREDICTION", T.FloatType()),
    ...         ]
    ...     ),
    ... )
    >>> r2_score(df, "ACTUAL", "PREDICTION")
    0.375

    """
    mean_y_true = df.agg(F.mean(F.col(y_true))).collect()[0][0]
    ss_tot = df.agg(F.sum((F.col(y_true) - mean_y_true) ** 2)).collect()[0][0]
    ss_res = df.agg(F.sum((F.col(y_true) - F.col(y_pred)) ** 2)).collect()[0][0]
    r2 = 1 - (ss_res / ss_tot)
    return r2


def mean_absolute_percentage_error(
    df: snowflake.snowpark.DataFrame, y_true: str, y_pred: str
) -> Decimal:
    """Calculate the mean absolute percentage error between two columns in a DataFrame.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The DataFrame containing the columns to compare.
    y_true : str
        The name of the column containing the true values.
    y_pred : str
        The name of the column containing the predicted values.

    Returns
    -------
    Decimal
        The mean absolute percentage error between the two columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.metrics import mean_absolute_percentage_error
    >>> from snowflake.snowpark import Session
    >>> from snowflake.snowpark import types as T
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"ACTUAL": 5.0, "PREDICTION": 5.0},
    ...     {"ACTUAL": 6.0, "PREDICTION": 7.0},
    ...     {"ACTUAL": 7.0, "PREDICTION": 7.5},
    ... ]
    >>> df = session.create_dataframe(
    ...     d,
    ...     schema=T.StructType(
    ...         [
    ...             T.StructField("ACTUAL", T.FloatType()),
    ...             T.StructField("PREDICTION", T.FloatType()),
    ...         ]
    ...     ),
    ... )
    >>> mean_absolute_percentage_error(df, "ACTUAL", "PREDICTION")
    0.07936507936507936

    """
    expr = F.mean(F.abs(F.col(y_true) - F.col(y_pred)) / F.col(y_true))
    return df.select(expr).collect()[0][0]


def get_metrics(df: snowflake.snowpark.DataFrame, y_true: str, y_pred: str) -> dict:
    """Calculate multiple regression metrics between two columns in a DataFrame.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The DataFrame containing the columns to compare.
    y_true : str
        The name of the column containing the true values.
    y_pred : str
        The name of the column containing the predicted values.

    Returns
    -------
    dict
        A dictionary containing the calculated metrics.

    Examples
    --------
    >>> from ml_forecasting_incubator.metrics import get_metrics
    >>> from snowflake.snowpark import Session
    >>> from snowflake.snowpark import types as T
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"ACTUAL": 5.0, "PREDICTION": 5.0},
    ...     {"ACTUAL": 6.0, "PREDICTION": 7.0},
    ...     {"ACTUAL": 7.0, "PREDICTION": 7.5},
    ... ]
    >>> df = session.create_dataframe(
    ...     d,
    ...    schema=T.StructType(
    ...         [
    ...             T.StructField("ACTUAL", T.FloatType()),
    ...             T.StructField("PREDICTION", T.FloatType()),
    ...         ]
    ...     ),
    ... )
    >>> get_metrics(df, "ACTUAL", "PREDICTION")
    {'mean_absolute_error': 0.5,
    'mean_absolute_percentage_error': 0.07936507936507936,
    'mean_squared_error': 0.4166666666666667,
    'root_mean_squared_error': 0.6454972243679028,
    'r2_score': 0.375}

    """
    return {
        "mean_absolute_error": mean_absolute_error(df, y_true, y_pred),
        "mean_absolute_percentage_error": mean_absolute_percentage_error(
            df, y_true, y_pred
        ),
        "mean_squared_error": mean_squared_error(df, y_true, y_pred),
        "root_mean_squared_error": root_mean_squared_error(df, y_true, y_pred),
        "r2_score": r2_score(df, y_true, y_pred),
    }
