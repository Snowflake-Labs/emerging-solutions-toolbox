from __future__ import annotations

from enum import Enum
from typing import Optional, Union

import snowflake.snowpark
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import DateType, TimestampType, TimeType


def expand_dates(
    df: snowflake.snowpark.DataFrame,
    column_names: Optional[Union[list[str], set[str]]] = None,
    output_names: str = "{col}_{date_component}",
) -> snowflake.snowpark.DataFrame:
    """Expand date columns into separate temporal components.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The input DataFrame.
    column_names : Optional[Union[list[str], set[str]]], optional
        The names of the columns to expand, by default None
    output_names : str, optional
        The output column names, by default "{col}_{date_component}"

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame with the expanded date columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.transformations import expand_dates
    >>> from datetime import date
    >>> from snowflake.snowpark import Session
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"date_col": date(2021, 1, 1)},
    ...     {"date_col": date(2021, 1, 2)},
    ...     {"date_col": date(2021, 1, 3)},
    ... ]
    >>> df = session.create_dataframe(d)
    >>> expand_dates(df).show()
    --------------------------------------------------------------------------------------------------
    |"DATE_COL"  |"DATE_COL_YEAR"  | ... | ... | ... |"DATE_COL_WEEKOFYEAR"  |"DATE_COL_IS_WEEKEND"  |
    --------------------------------------------------------------------------------------------------
    |2021-01-01  |2021             | ... | ... | ... |53                     |False                  |
    |2021-01-02  |2021             | ... | ... | ... |53                     |True                   |
    |2021-01-03  |2021             | ... | ... | ... |53                     |True                   |
    --------------------------------------------------------------------------------------------------

    """
    if column_names is None:
        fields = [field for field in df.schema if isinstance(field.datatype, DateType)]
        for field in fields:
            field_name = field.name
            df = df.with_column(f"{field_name}_year", F.year(field_name))
            df = df.with_column(f"{field_name}_month", F.month(field_name))
            df = df.with_column(f"{field_name}_dayofweek", F.dayofweek(field_name))
            df = df.with_column(f"{field_name}_dayofyear", F.dayofyear(field_name))
            df = df.with_column(f"{field_name}_weekofyear", F.weekofyear(field_name))
            df = df.with_column(
                f"{field_name}_is_weekend",
                F.iff(F.dayofweek(field_name).isin([6, 0]), True, False),
            )
    return df


def expand_times(  # noqa: D103
    df: snowflake.snowpark.DataFrame,
    column_names: Optional[Union[list[str], set[str]]] = None,
    output_names: str = "{col}_{time_component}",
) -> snowflake.snowpark.DataFrame:
    """Expand date columns into separate temporal components.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The input DataFrame.
    column_names : Optional[Union[list[str], set[str]]], optional
        The names of the columns to expand, by default None
    output_names : str, optional
        The output column names, by default "{col}_{time_component}"

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame with the expanded time columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.transformations import expand_datetimes
    >>> from datetime import datetime
    >>> from snowflake.snowpark import Session
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"datetime_col": datetime(2021, 1, 1, 1, 2, 3)},
    ...     {"datetime_col": datetime(2021, 1, 2, 13, 14, 15)},
    ...     {"datetime_col": datetime(2021, 1, 3, 22, 23, 24)},
    ... ]
    >>> df = session.create_dataframe(d)
    >>> expand_times(df).show()
    ------------------------------------------------------------------------
    |"TIME_COL"  |"TIME_COL_HOUR"  |"TIME_COL_MINUTE"  |"TIME_COL_SECOND"  |
    ------------------------------------------------------------------------
    |01:02:03    |1                |2                  |3                  |
    |13:14:15    |13               |14                 |15                 |
    |22:23:24    |22               |23                 |24                 |
    ------------------------------------------------------------------------

    """
    if column_names is None:
        fields = [field for field in df.schema if isinstance(field.datatype, TimeType)]
        for field in fields:
            field_name = field.name
            df = df.with_column(f"{field_name}_hour", F.hour(field_name))
            df = df.with_column(f"{field_name}_minute", F.minute(field_name))
            df = df.with_column(f"{field_name}_second", F.second(field_name))
    return df


def expand_datetimes(  # noqa: D103
    df: snowflake.snowpark.DataFrame,
    column_names: Optional[Union[list[str], set[str]]] = None,
    output_names: str = "{col}_{datetime_component}",
) -> snowflake.snowpark.DataFrame:
    """Expand datetime columns into separate temporal components.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The input DataFrame.
    column_names : Optional[Union[list[str], set[str]]], optional
        The names of the columns to expand, by default None
    output_names : str, optional
        The output column names, by default "{col}_{datetime_component}"

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame with the expanded datetime columns.

    Examples
    --------
    >>> from ml_forecasting_incubator.transformations import expand_times
    >>> from datetime import time
    >>> from snowflake.snowpark import Session
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"time_col": time(1, 2, 3)},
    ...     {"time_col": time(13, 14, 15)},
    ...     {"time_col": time(22, 23, 24)},
    ... ]
    >>> df = session.create_dataframe(d)
    >>> expand_datetimes(df).show()
    ----------------------------------------------------------------------------------------------------
    |"DATETIME_COL"       |"DATETIME_COL_YEAR"  | ... | "DATETIME_COL_MINUTE"  |"DATETIME_COL_SECOND"  |
    ----------------------------------------------------------------------------------------------------
    |2021-01-01 01:02:03  |2021                 | ... |2                      |3                       |
    |2021-01-02 13:14:15  |2021                 | ... |14                     |15                      |
    |2021-01-03 22:23:24  |2021                 | ... |23                     |24                      |
    ----------------------------------------------------------------------------------------------------

    """
    if column_names is None:
        fields = [
            field for field in df.schema if isinstance(field.datatype, TimestampType)
        ]
        for field in fields:
            field_name = field.name
            df = df.with_column(f"{field_name}_year", F.year(field_name))
            df = df.with_column(f"{field_name}_month", F.month(field_name))
            df = df.with_column(f"{field_name}_dayofweek", F.dayofweek(field_name))
            df = df.with_column(f"{field_name}_dayofyear", F.dayofyear(field_name))
            df = df.with_column(f"{field_name}_weekofyear", F.weekofyear(field_name))
            df = df.with_column(
                f"{field_name}_is_weekend",
                F.iff(F.dayofweek(field_name).isin([6, 0]), True, False),
            )
            df = df.with_column(f"{field_name}_hour", F.hour(field_name))
            df = df.with_column(f"{field_name}_minute", F.minute(field_name))
            df = df.with_column(f"{field_name}_second", F.second(field_name))
    return df


class Frequency(Enum):  # noqa: D101
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


def compute_lags(
    df: snowflake.snowpark.DataFrame,
    target_column: str,
    order_column: str,
    frequency: Frequency,
    periods: int,
) -> snowflake.snowpark.DataFrame:
    """Compute lagged values of a target column.

    Parameters
    ----------
    df : snowflake.snowpark.DataFrame
        The input DataFrame.
    target_column : str
        The name of the target column.
    order_column : str
        The name of the column to order by.
    frequency : Frequency
        The frequency of the lag.
    periods : int
        The number of periods to lag by.

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame with the lagged values.

    Examples
    --------
    >>> from ml_forecasting_incubator.transformations import compute_lags, Frequency
    >>> from datetime import date
    >>> from snowflake.snowpark import Session
    >>> session = Session.builder.getOrCreate()
    >>> d = [
    ...     {"date_col": date(2021, 1, 1), "value_col": 1},
    ...     {"date_col": date(2021, 1, 2), "value_col": 2},
    ...     {"date_col": date(2021, 1, 3), "value_col": 3},
    ... ]
    >>> df = session.create_dataframe(d)
    >>> compute_lags(df, "value_col", "date_col", Frequency.DAY, 1).show()
    ----------------------------------------------------
    |"DATE_COL"  |"VALUE_COL"  |"VALUE_COL_LAG_DAY_1"  |
    ----------------------------------------------------
    |2021-01-01  |1            |1                      |
    |2021-01-02  |2            |1                      |
    |2021-01-03  |3            |2                      |
    ----------------------------------------------------

    """
    return df.with_column(
        f"{target_column}_lag_{frequency.value}_{periods}",
        F.coalesce(
            F.lag(F.col(target_column), periods).over(
                snowflake.snowpark.Window.order_by(F.col(order_column))
            ),
            F.col(target_column),
        ),
    )
