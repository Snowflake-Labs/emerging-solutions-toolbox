from __future__ import annotations

import math
from functools import reduce

import holidays
import pandas as pd
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark import Session, Window
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T


def verify_valid_rollup_spec(
    current_frequency: str,
    rollup_frequency: str,
    rollup_aggs: dict,
    exogenous_variables: list,
) -> None:
    """Verify that the rollup specification is valid.

    The rollup frequency must less granular than the current frequency.
    Also there needs to be an aggregate function assigned to the TARGET and each EXOGENOUS column.

    Parameters
    ----------
    current_frequency : str
        The current frequency specified by the user for the dataset. It can be one of the following: "second", "minute", "hour", "day", "week", "month", or "other".
    rollup_frequency : str
        The desired frequency to rollup to. It can be one of the following: "minute", "hour", "day", "week", "month", or None.
    rollup_aggs : dict
        The aggregations to apply to the columns. The key is the column name and the value is the aggregation function.
    exogenous_variables : list
        The names of the exogenous variable columns.

    Returns
    -------
    None

    Raises
    ------
    ValueError

    """
    if (
        (
            rollup_frequency == "second"
            and current_frequency in ["minute", "hour", "day", "week", "month"]
        )
        or (
            rollup_frequency == "minute"
            and current_frequency in ["hour", "day", "week", "month"]
        )
        or (
            rollup_frequency == "hour" and current_frequency in ["day", "week", "month"]
        )
        or (rollup_frequency == "day" and current_frequency in ["week", "month"])
        or (rollup_frequency == "week" and current_frequency in ["month"])
    ):
        raise ValueError("Cannot roll up from lower granularity to higher granularity")

    if (rollup_frequency is not None) & (
        len(rollup_aggs) < len(exogenous_variables) + 1
    ):
        raise ValueError(
            "There must be a ROLLUP_AGGREGATION defined for the TARGET_COLUMN and each of the EXOGENOUS_COLUMNS."
        )
    elif (rollup_frequency is not None) & (
        len(rollup_aggs) > len(exogenous_variables) + 1
    ):
        raise ValueError(
            "Too many rollup aggregations are defined. Only define aggregations for the TARGET_COLUMN and the EXOGENOUS_COLUMNS."
        )


def verify_current_frequency(
    input_sdf: SnowparkDataFrame,
    time_period_column: str,
    w_spec: Window,
    current_frequency: str,
):
    """Approximate the granularity of the dataset by calculating the most common time difference between consecutive records.

    Parameters
    ----------
    input_sdf : snowflake.snowpark.DataFrame
        The input DataFrame.
    time_period_column : str
        The names of the column to expand
    w_spec : snowflake.snowpark.Window
        The window specification
    current_frequency : str
        The current frequency specified by the user for the dataset. It can be one of the following: "second", "minute", "hour", "day", "week", "month", or "other".

    Returns
    -------
    Does not return anything.

    """
    # First only get the first 5 rows within each partition since we only need to check a few datetime steps to roughly verify current granularity
    sdf_freq_verification = (
        input_sdf.withColumn("ROW_NUMBER", F.row_number().over(w_spec))
        .filter(F.col("ROW_NUMBER") <= 5)
        .drop("ROW_NUMBER")
    )

    # Create a new column that calculates the time difference between the current row and the previous row
    sdf_freq_verification = sdf_freq_verification.withColumn(
        "TIME_DIFF",
        F.datediff(
            "millisecond",
            F.lag(F.col(time_period_column), 1).over(w_spec),
            F.col(time_period_column),
        ),
    )

    # Remove null value rows
    sdf_freq_verification = sdf_freq_verification.filter(F.col("TIME_DIFF").isNotNull())

    # # Count distinct time difference values
    # distinct_time_step_values_count = (
    #     sdf_freq_verification.groupBy("TIME_DIFF").count().count()
    # )

    # # Print the number of distinct time step values
    # print(f"Distinct time step values: {distinct_time_step_values_count} \n")

    # Most common time step value
    time_diff_stats = sdf_freq_verification.agg(
        F.min("TIME_DIFF").alias("MIN_TIME_DIFF"),
        F.mode("TIME_DIFF").alias("MODE_TIME_DIFF"),
        F.max("TIME_DIFF").alias("MAX_TIME_DIFF"),
    ).collect()[0].as_dict()

    # Time step value in different units
    time_step_funcs = {
        "millisecond": lambda x: x,
        "second": lambda x: x / 1000,
        "minute": lambda x: x / 1000 / 60,
        "hour": lambda x: x / 1000 / 60 / 60,
        "day": lambda x: x / 1000 / 60 / 60 / 24,
        "week": lambda x: x / 1000 / 60 / 60 / 24 / 7,
        "month": lambda x: x / 1000 / 60 / 60 / 24 / 30,
    }

    time_step_values = {k:v(time_diff_stats['MODE_TIME_DIFF']) for k,v in time_step_funcs.items()}

    # The unit that converts the most common time step value closest to 1 is probably the granularity of the dataset
    closest_unit_to_1 = min(
        time_step_values, key=lambda k: abs(time_step_values[k] - 1)
    )
    conversion_func = time_step_funcs[closest_unit_to_1]

    min_diff = conversion_func(time_diff_stats['MIN_TIME_DIFF'])
    max_diff = conversion_func(time_diff_stats['MAX_TIME_DIFF'])

    print(
        f"""Most common time between consecutive records (frequency): {time_step_values[closest_unit_to_1]} {closest_unit_to_1}(s)
    The current frequency appears to be in {closest_unit_to_1.upper()} granularity.
    The range of values is {min_diff} - {max_diff} {closest_unit_to_1}(s)
    """
    )

    if (current_frequency != "other") & (closest_unit_to_1 != current_frequency):
        raise ValueError(
            f"The observed frequency ({closest_unit_to_1}) does not match the current_frequency configuration value ({current_frequency})."
        )


def roll_up(
    input_sdf: SnowparkDataFrame,
    time_period_column: str,
    partition_columns: list,
    target_column: str,
    exogenous_columns: list,
    rollup_to_frequency: str,
    rollup_aggregations: dict,
) -> SnowparkDataFrame:
    """Rollup to the specified granularity.

    Parameters
    ----------
    input_sdf : snowflake.snowpark.DataFrame
        The input DataFrame.
    time_period_column : str
        The names of the column to expand
    partition_columns : list
        The names of the columns to partition by.
    target_column : str
        The name of the target column.
    exogenous_columns : list
        The names of the exogenous variable columns.
    rollup_to_frequency : str
        The desired frequency to rollup to. It can be one of the following: "minute", "hour", "day", "week", "month", or None.
    rollup_aggregations : dict
        The aggregations to apply to the columns. The key is the column name and the value is the aggregation function.

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame rolledup to the specified frequency.

    """
    if rollup_to_frequency is None:
        sdf_rollup = input_sdf

    else:
        # First convert the time period column to the specified rollup granularity
        sdf_rollup = input_sdf.with_column(
            time_period_column,
            F.date_trunc(rollup_to_frequency, F.col(time_period_column)),
        )

        # Then group by the time period column and aggregate
        sdf_rollup = sdf_rollup.group_by(time_period_column, *partition_columns).agg(
            rollup_aggregations
        )

        # Rename the new aggregated columns to their original names and ensure the datetime column is of type timestamp
        sdf_rollup = sdf_rollup.to_df(
            time_period_column, *partition_columns, target_column, *exogenous_columns
        ).select(
            F.cast(time_period_column, T.TimestampType()).alias(time_period_column),
            *partition_columns,
            target_column,
            *exogenous_columns,
        )

    return sdf_rollup


def expand_datetime(
    input_sdf: SnowparkDataFrame, time_period_column: str, time_step_frequency: str
) -> SnowparkDataFrame:
    """Expand datetime columns into separate temporal components and then perform sinusoidal transformation.

    Parameters
    ----------
    input_sdf : snowflake.snowpark.DataFrame
        The input DataFrame.
    time_period_column : str
        The names of the column to expand
    time_step_frequency : str
        The frequency of the dataset. It can be one of the following: "second", "minute", "hour", "day", "week", "month", "other".

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame with the expanded datetime columns.

    """
    # First ensure that the time period column is TimestampType so that we can do the hour, minute, and second calculations
    input_col_names = input_sdf.columns
    sdf_engineered = input_sdf.with_column(
        time_period_column, F.col(time_period_column).cast(T.TimestampType())
    ).select(input_col_names)

    # Calculate the temporal components
    sdf_engineered = (
        sdf_engineered.with_column("YEAR", F.year(time_period_column))
        .with_column("MONTH", F.month(time_period_column))
        .with_column("WEEK_OF_YEAR", F.weekofyear(time_period_column))
        .with_column("DAY_OF_WEEK", F.dayofweek(time_period_column))
        .with_column("DAY_OF_YEAR", F.dayofyear(time_period_column))
        .with_column("HOUR_OF_DAY", F.hour(time_period_column))
        .with_column("MINUTE_OF_HOUR", F.minute(time_period_column))
        .with_column(
            "MINUTE_OF_DAY", F.hour(time_period_column) * 60 + F.col("MINUTE_OF_HOUR")
        )
        .with_column(
            "SECOND_OF_DAY",
            F.hour(time_period_column) * 3600
            + F.minute(time_period_column) * 60
            + F.second(time_period_column),
        )
    )

    # Sinusoidal transformation of time features
    sdf_engineered = sdf_engineered.with_column(
        "MONTH_SIN", F.sin(2 * math.pi * F.col("MONTH") / 12)
    ).with_column("MONTH_COS", F.cos(2 * math.pi * F.col("MONTH") / 12))

    if time_step_frequency in ["second", "minute", "hour", "day", "week", "other"]:
        sdf_engineered = sdf_engineered.with_column(
            "WEEK_OF_YEAR_SIN", F.sin(2 * math.pi * F.col("WEEK_OF_YEAR") / 52)
        ).with_column(
            "WEEK_OF_YEAR_COS", F.cos(2 * math.pi * F.col("WEEK_OF_YEAR") / 52)
        )

    if time_step_frequency in ["second", "minute", "hour", "day", "other"]:
        # Create one-hot encoding for DAY_OF_WEEK instead of sinusoidal transformation
        sdf_engineered = (
            sdf_engineered.with_column(
                "DAY_OF_WEEK_SUN", (F.col("DAY_OF_WEEK") == 0).cast(T.IntegerType())
            )
            .with_column(
                "DAY_OF_WEEK_MON", (F.col("DAY_OF_WEEK") == 1).cast(T.IntegerType())
            )
            .with_column(
                "DAY_OF_WEEK_TUE", (F.col("DAY_OF_WEEK") == 2).cast(T.IntegerType())
            )
            .with_column(
                "DAY_OF_WEEK_WED", (F.col("DAY_OF_WEEK") == 3).cast(T.IntegerType())
            )
            .with_column(
                "DAY_OF_WEEK_THU", (F.col("DAY_OF_WEEK") == 4).cast(T.IntegerType())
            )
            .with_column(
                "DAY_OF_WEEK_FRI", (F.col("DAY_OF_WEEK") == 5).cast(T.IntegerType())
            )
            .with_column(
                "DAY_OF_WEEK_SAT", (F.col("DAY_OF_WEEK") == 6).cast(T.IntegerType())
            )
        )

        sdf_engineered = sdf_engineered.with_column(
            "DAY_OF_YEAR_SIN", F.sin(2 * math.pi * F.col("DAY_OF_YEAR") / 365)
        ).with_column(
            "DAY_OF_YEAR_COS", F.cos(2 * math.pi * F.col("DAY_OF_YEAR") / 365)
        )

    if time_step_frequency in ["second", "minute", "hour", "other"]:
        sdf_engineered = sdf_engineered.with_column(
            "HOUR_OF_DAY_SIN", F.sin(2 * math.pi * F.col("HOUR_OF_DAY") / 24)
        ).with_column("HOUR_OF_DAY_COS", F.cos(2 * math.pi * F.col("HOUR_OF_DAY") / 24))

    if time_step_frequency in ["second", "minute", "other"]:
        sdf_engineered = (
            sdf_engineered.with_column(
                "MINUTE_OF_HOUR_SIN", F.sin(2 * math.pi * F.col("MINUTE_OF_HOUR") / 60)
            )
            .with_column(
                "MINUTE_OF_HOUR_COS", F.cos(2 * math.pi * F.col("MINUTE_OF_HOUR") / 60)
            )
            .with_column(
                "MINUTE_OF_DAY_SIN", F.sin(2 * math.pi * F.col("MINUTE_OF_DAY") / 1440)
            )
            .with_column(
                "MINUTE_OF_DAY_COS", F.cos(2 * math.pi * F.col("MINUTE_OF_DAY") / 1440)
            )
        )

    if time_step_frequency in ["second", "other"]:
        sdf_engineered = sdf_engineered.with_column(
            "SECOND_OF_DAY_SIN", F.sin(2 * math.pi * F.col("SECOND_OF_DAY") / 86400)
        ).with_column(
            "SECOND_OF_DAY_COS", F.cos(2 * math.pi * F.col("SECOND_OF_DAY") / 86400)
        )

    # Create a feature to capture trend
    if time_step_frequency != "other":
        sdf_engineered = sdf_engineered.with_column(
            f"{time_step_frequency}S_SINCE_JAN2020",
            F.datediff(
                time_step_frequency, F.lit("2020-01-01"), F.col(time_period_column)
            ),
        )
    else:
        sdf_engineered = sdf_engineered.with_column(
            "DAYS_SINCE_JAN2020",
            F.datediff("day", F.lit("2020-01-01"), F.col(time_period_column)),
        )

    # Drop the pre-transformed datetime columns
    sdf_engineered = sdf_engineered.drop(
        "MONTH",
        "WEEK_OF_YEAR",
        "DAY_OF_WEEK",
        "DAY_OF_YEAR",
        "HOUR_OF_DAY",
        "MINUTE_OF_HOUR",
        "MINUTE_OF_DAY",
        "SECOND_OF_DAY",
    )

    return sdf_engineered


def recent_rolling_avg(
    input_sdf: SnowparkDataFrame,
    columns_to_avg: list,
    w_spec: Window,
    time_step_frequency: str,
) -> SnowparkDataFrame:
    """Create rolling averages for each of the specified columns over a recent time period.

    For every column listed in columns_to_avg, two new columns are added: <col_nm>_ROLLING_MEAN and <col_nm>_ROLLING_MEAN_SHORT.

    Parameters
    ----------
    input_sdf : snowflake.snowpark.DataFrame
        The input DataFrame.
    columns_to_avg : list
        The names of the columns for which to calculate the rolling average.
    w_spec : snowflake.snowpark.Window
        The window specification to use for the rolling average.
    time_step_frequency : str
        The frequency of the dataset. It can be one of the following: "second", "minute", "hour", "day", "week", "month", "other".

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame with the rolling average column(s) added.

    """
    for col_nm in columns_to_avg:
        if time_step_frequency == "second":  # average of last 1 hour
            output_sdf = input_sdf.with_column(
                f"{col_nm}_ROLLING_MEAN",
                F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-3600, 0)),
            )
        elif time_step_frequency == "minute":  # average of last 1 day
            output_sdf = input_sdf.with_column(
                f"{col_nm}_ROLLING_MEAN",
                F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-1440, 0)),
            )
        elif time_step_frequency == "hour":  # average of last 1 week
            output_sdf = input_sdf.with_column(
                f"{col_nm}_ROLLING_MEAN",
                F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-168, 0)),
            )
        elif time_step_frequency == "day":  # average of last 1 month
            output_sdf = input_sdf.with_column(
                f"{col_nm}_ROLLING_MEAN",
                F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-30, 0)),
            )
        elif time_step_frequency == "week":  # average of last 1 month
            output_sdf = input_sdf.with_column(
                f"{col_nm}_ROLLING_MEAN",
                F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-4, 0)),
            )
        elif time_step_frequency == "month":  # average of last 4 months
            output_sdf = input_sdf.with_column(
                f"{col_nm}_ROLLING_MEAN",
                F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-4, 0)),
            )
        else:
            output_sdf = input_sdf.with_column(
                f"{col_nm}_ROLLING_MEAN",
                F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-10, 0)),
            )

        # Remove rows where _ROLLING_MEAN is null
        output_sdf = output_sdf.filter(F.col(f"{col_nm}_ROLLING_MEAN").isNotNull())

        # Also calculate a shorter rolling average over the most recent 4 time periods
        output_sdf = output_sdf.with_column(
            f"{col_nm}_ROLLING_MEAN_SHORT",
            F.avg(F.col(col_nm)).over(w_spec.rowsBetween(-3, 0)),
        )

        # Remove rows where _ROLLING_MEAN_SHORT is null
        output_sdf = output_sdf.filter(
            F.col(f"{col_nm}_ROLLING_MEAN_SHORT").isNotNull()
        )

    return output_sdf


def create_holiday_dataframe(
    session: Session,
    start_year: int = 2020,
    end_year: int = 2030,
    country: str = "US",
    language: str = None,
) -> SnowparkDataFrame:
    """Create a DataFrame with all dates from start_year to end_year.

    Parameters
    ----------
    session : Session
        The Snowpark session.
    start_year : int
        The start year for the date range.
    end_year : int
        The end year for the date range.
    country : str
        The country code for the country to get holidays for.
    language : str
        The language code for the language to get holidays for.

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame with the DATE and HOLIDAY columns

    """
    # Generate date range
    date_range = pd.date_range(
        start=f"{start_year}-01-01", end=f"{end_year}-12-31", freq="D"
    )

    # Instantiate the holidays object based on the country and language
    # For list of valid countries and languages, see documentation for holidays package: https://holidays.readthedocs.io/en/latest/
    try:
        country_holidays = holidays.country_holidays(country, language=language)
    except KeyError:
        raise ValueError(
            f"Invalid country code '{country}' or language '{language}'. Check supported options in the holidays package."
        )

    # Create pandas DataFrame
    df = pd.DataFrame({"DATE": date_range})

    # Add HOLIDAY column with holiday names (or None if not a holiday)
    df["HOLIDAY"] = df["DATE"].map(lambda x: country_holidays.get(x, None))

    # Regex to replace any spaces or non-alphanumeric characters with underscores and to uppercase all characters
    df["HOLIDAY"] = df["HOLIDAY"].str.replace(r"\W+", "_", regex=True).str.upper()

    # Convert to Snowpark DataFrame
    sdf = session.createDataFrame(df).select(
        F.col("DATE").cast(T.DateType()).alias("DATE"), "HOLIDAY"
    )

    return sdf


def determine_lag_value(time_step_frequency: str, forecast_horizon: int) -> int:
    """Calculate the lag value based on the time step frequency and forecast horizon.

    Parameters
    ----------
    time_step_frequency: str
        The frequency of the time steps. Supported values are "hour", "day", "week", and "month".
    forecast_horizon: int
        The forecast horizon in the same units as the time step frequency.

    Returns
    -------
    int
        The calculated lag value.

    Raises
    ------
    ValueError

    """
    if time_step_frequency == "hour":
        horizon_days = math.ceil(forecast_horizon / 24)
        next_multiple_of_7 = ((horizon_days // 7) + 1) * 7
        lag = next_multiple_of_7 * 24
    elif time_step_frequency == "day":
        lag = 364
    elif time_step_frequency == "week":
        lag = 52
    elif time_step_frequency == "month":
        lag = 12
    else:
        raise ValueError("Unsupported time step frequency")

    if lag * 2 <= forecast_horizon:
        raise ValueError("Try a smaller forecast horizon if using lag features.")
    return lag


def lag_and_target_prep(
    input_sdf: SnowparkDataFrame,
    target_column: str,
    time_step_frequency: str,
    forecast_horizon: int,
    w_spec: Window,
    lead: int = None,
    create_lag_feature: bool = True,
) -> SnowparkDataFrame:
    """Create lag (and possibly lead) of the target column.

    If we are training separate models for each future time step, the lead feature will be the target used for modeling.
    If we are not training separate models for each future time step, the original target column will be used for modeling.

    Parameters
    ----------
    input_sdf : snowflake.snowpark.DataFrame
        The input DataFrame.
    target_column : str
        The names of the target column
    time_step_frequency : str
        The frequency of the dataset. It can be one of the following: "second", "minute", "hour", "day", "week", "month", "other".
    forecast_horizon : int
        The number of time periods in the forecast horizon.
    w_spec : snowflake.snowpark.Window
        The window specification.
    lead : int
        The number of time periods in the future to lead. Default is None.
        If a positive integer value is supplied, a lead of the target will be created for use cases that require separate models for each future time period.
        If the value is None, then the target column will be used as is for modeling (suggesting a single model for all future time periods).
    create_lag_feature : bool
        If True, a lag feature of the target column will be created. Default is True.

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame containing the Lag and Lead features.

    """
    output_sdf = input_sdf

    if create_lag_feature:
        # Establish lag value for LAG feature(s).
        # NOTE: The number of time periods in the past to lag ("lag" variable below) MUST be greater than the forecast_horizon.
        lag = determine_lag_value(time_step_frequency, forecast_horizon)

        # Get the record count before adding lag features to ensure we don't lose too much data after creating them.
        record_count_before_lag = output_sdf.count()

    if lead is None:
        # if lead is None, we are not training separate models for each future time period.
        if create_lag_feature:
            # Create LAG feature of target column.
            # NOTE: We also look at the lag 2, for those time periods near the end of the forecast horizon
            #       where the lag 1 is not available.
            output_sdf = (
                input_sdf.with_column(
                    "LAG_1_TARGET", F.lag(target_column, lag).over(w_spec)
                )
                .with_column("LAG_2_TARGET", F.lag(target_column, lag * 2).over(w_spec))
                .with_column(
                    "LAG_TARGET",
                    F.coalesce(F.col("LAG_1_TARGET"), F.col("LAG_2_TARGET")),
                )
            )

            # Create an AVG of the last 3 lags
            # NOTE: This will only average non-null values.
            #       If for example, the lag 3 is not available, it will average the lag 1 and lag 2 values.
            output_sdf = (
                output_sdf.with_column(
                    "LAG_3_TARGET", F.lag(target_column, lag * 3).over(w_spec)
                )
                .with_column(
                    "AVG_LAST_3_LAGS",
                    F.when(
                        (F.col("LAG_3_TARGET").isNotNull())
                        & (F.col("LAG_2_TARGET").isNotNull())
                        & (F.col("LAG_1_TARGET").isNotNull()),
                        (
                            F.col("LAG_1_TARGET")
                            + F.col("LAG_2_TARGET")
                            + F.col("LAG_3_TARGET")
                        )
                        / 3,
                    )
                    .when(
                        (F.col("LAG_2_TARGET").isNotNull())
                        & (F.col("LAG_1_TARGET").isNotNull()),
                        (F.col("LAG_1_TARGET") + F.col("LAG_2_TARGET")) / 2,
                    )
                    .otherwise(F.col("LAG_1_TARGET")),
                )
                .drop("LAG_1_TARGET", "LAG_2_TARGET", "LAG_3_TARGET")
                .filter(F.col("LAG_TARGET").isNotNull())
            )

            # Get the record count after adding lag features to ensure we don't lose too much data after creating them.
            record_count_after_lag = output_sdf.count()
            if record_count_after_lag / record_count_before_lag < 0.25:
                raise ValueError(
                    "Adding lag features removes more than 75 percent of the training data."
                )
            elif record_count_after_lag / record_count_before_lag < 0.5:
                print(
                    "WARNING: Adding lag features removes more than 50 percent of the training data."
                )

        # Create a new name for the target column,
        #   so that regardless of modeling pattern, our model training code will train on the same Y variable name.
        output_sdf = output_sdf.with_column("MODEL_TARGET", F.col(target_column))

    elif lead > 0:
        if create_lag_feature:
            # Create lag feature of target column for situations where we are building separate models for each future time period.
            # # NOTE: We subtract lead number from the lag number so that we are lagging from the future date we are forecasting.
            output_sdf = (
                output_sdf.with_column(
                    "LAG_1_TARGET", F.lag(target_column, lag - lead).over(w_spec)
                )
                .with_column(
                    "LAG_2_TARGET", F.lag(target_column, lag * 2 - lead).over(w_spec)
                )
                .with_column(
                    "LAG_TARGET",
                    F.coalesce(F.col("LAG_1_TARGET"), F.col("LAG_2_TARGET")),
                )
            )

            # Create an AVG of the last 3 lags
            # NOTE: This will only average non-null values.
            #       If for example, the lag 3 is not available, it will average the lag 1 and lag 2 values.
            output_sdf = (
                output_sdf.with_column(
                    "LAG_3_TARGET", F.lag(target_column, lag * 3 - lead).over(w_spec)
                )
                .with_column(
                    "AVG_LAST_3_LAGS",
                    F.when(
                        (F.col("LAG_3_TARGET").isNotNull())
                        & (F.col("LAG_2_TARGET").isNotNull())
                        & (F.col("LAG_1_TARGET").isNotNull()),
                        (
                            F.col("LAG_1_TARGET")
                            + F.col("LAG_2_TARGET")
                            + F.col("LAG_3_TARGET")
                        )
                        / 3,
                    )
                    .when(
                        (F.col("LAG_2_TARGET").isNotNull())
                        & (F.col("LAG_1_TARGET").isNotNull()),
                        (F.col("LAG_1_TARGET") + F.col("LAG_2_TARGET")) / 2,
                    )
                    .otherwise(F.col("LAG_1_TARGET")),
                )
                .drop("LAG_1_TARGET", "LAG_2_TARGET", "LAG_3_TARGET")
                .filter(F.col("LAG_TARGET").isNotNull())
            )

            # Get the record count after adding lag features to ensure we don't lose too much data after creating them.
            record_count_after_lag = output_sdf.count()
            if record_count_after_lag / record_count_before_lag < 0.25:
                raise ValueError(
                    "Adding lag features removes more than 75 percent of the training data."
                )
            elif record_count_after_lag / record_count_before_lag < 0.5:
                print(
                    "WARNING: Adding lag features removes more than 50 percent of the training data."
                )

        # Create TARGET column which is a LEAD of the original target column
        output_sdf = output_sdf.with_column(
            "MODEL_TARGET", F.lead(target_column, lead).over(w_spec)
        )

        # Create a column that contains the lead number
        output_sdf = output_sdf.with_column("LEAD", F.lit(lead))

    else:
        raise ValueError("The lead value must be a positive integer or None")

    return output_sdf


def apply_functions_in_a_loop(
    train_separate_lead_models: bool,
    partition_column_list: list,
    input_sdf: SnowparkDataFrame,
    target_column: str,
    time_step_frequency: str,
    forecast_horizon: int,
    w_spec: Window,
    create_lag_feature: bool = True,
) -> SnowparkDataFrame:
    """Run the lag_and_target_prep and add_holidays functions once for each lead value.

    If we are training separate models for each future time step, the functions are run separately for each lead value, and the resulting dfs are UNIONED together.
    If we are not training separate models for each future time step, the functions are only run once.

    Parameters
    ----------
    train_separate_lead_models : bool
        If True, a separate model will be trained for each future time period.
    partition_column_list : list
        The names of the partition columns.
    input_sdf : snowflake.snowpark.DataFrame
        The input DataFrame.
    target_column : str
        The names of the target column
    time_step_frequency : str
        The frequency of the dataset. It can be one of the following: "second", "minute", "hour", "day", "week", "month", "other".
    forecast_horizon : int
        The number of time periods in the forecast horizon.
    w_spec : snowflake.snowpark.Window
        The window specification.
    create_lag_feature : bool
        If True, a lag feature of the target column will be created. Default is True.

    Returns
    -------
    snowflake.snowpark.DataFrame
        The DataFrame containing the Lag and Lead features as well as the GROUP_IDENTIFIER and GROUP_IDENTIFIER_STRING columns.

    """
    # NOTE: If not training separate lead models, then the new target column ('MODEL_TARGET') will be the same as the original target column.

    if not train_separate_lead_models:
        # Create final SDF for modeling which includes lagged target if desired
        final_sdf = lag_and_target_prep(
            input_sdf,
            target_column,
            time_step_frequency,
            forecast_horizon,
            w_spec,
            lead=None,
            create_lag_feature=create_lag_feature,
        )

        # Combine all the partition columns into a single partition column
        if len(partition_column_list) > 0:
            group_id_list = []
            for colnm in partition_column_list:
                group_id_list.append(F.lit(colnm))
                group_id_list.append(F.col(colnm))
        else:
            # If len(partition_column_list) == 0, then we need to make sure the group identifier is not null
            group_id_list = [F.lit("SERIES"), F.lit("SINGLE")]

        final_sdf = final_sdf.with_column(
            "GROUP_IDENTIFIER",
            F.object_construct_keep_null(*group_id_list).astype(T.VariantType()),
        ).with_column(
            "GROUP_IDENTIFIER_STRING", F.concat_ws(F.lit("_"), *group_id_list)
        )

        if len(partition_column_list) > 0:
            final_sdf = final_sdf.drop(*partition_column_list)

    elif train_separate_lead_models:
        # Because we have exogenous variables that we don't have future values for, we need to create a separate model for each future time period.
        # In the following code we create separate copies of the data for each lead we want to forecast. So if the forecast horizon is 48 hours, we will union together 48 separate dataframes.
        # For each LEAD value, the target will be the target column that many days into the future.
        # Also the LAG column is created from the new target column date (as opposed to lagging from the date the forecast is being made).

        # Create a union of the different LEAD dataframes
        training_sdf_list = []
        for i in range(1, forecast_horizon + 1):
            training_sdf_list.append(
                lag_and_target_prep(
                    input_sdf,
                    target_column,
                    time_step_frequency,
                    forecast_horizon,
                    w_spec,
                    lead=i,
                    create_lag_feature=create_lag_feature,
                )
            )

        final_sdf = reduce(SnowparkDataFrame.union_all, training_sdf_list)

        # Combine all the partition columns into a single partition column
        if len(partition_column_list) > 0:
            group_id_list = []
            for colnm in partition_column_list:
                group_id_list.append(F.lit(colnm))
                group_id_list.append(F.col(colnm))
        else:
            # If len(partition_column_list) == 0, then we need to make sure the group identifier is not null
            group_id_list = [F.lit("SERIES"), F.lit("SINGLE")]

        group_id_list.append(F.lit("LEAD"))
        group_id_list.append(F.col("LEAD"))

        final_sdf = (
            final_sdf.with_column(
                "GROUP_IDENTIFIER",
                F.object_construct_keep_null(*group_id_list).astype(T.VariantType()),
            )
            .with_column(
                "GROUP_IDENTIFIER_STRING", F.concat_ws(F.lit("_"), *group_id_list)
            )
            .drop(*partition_column_list, "LEAD")
        )

    return final_sdf
