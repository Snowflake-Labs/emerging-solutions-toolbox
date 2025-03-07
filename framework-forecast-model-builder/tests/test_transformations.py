from datetime import date

from forecast_model_builder.transformations import (
    Frequency,
    compute_lags,
    expand_dates,
    expand_datetimes,
    expand_times,
)


def test_expand_dates(alltypes):
    df = alltypes.select("DATE_COL")
    result_df = expand_dates(df)

    expected_columns = [
        "DATE_COL",
        "DATE_COL_YEAR",
        "DATE_COL_MONTH",
        "DATE_COL_DAYOFYEAR",
        "DATE_COL_DAYOFWEEK",
        "DATE_COL_WEEKOFYEAR",
        "DATE_COL_IS_WEEKEND",
    ]
    assert set(map(str.upper, result_df.columns)) == set(
        map(str.upper, expected_columns)
    )

    result_data = result_df.collect()
    assert result_data[0]["DATE_COL_YEAR"] == 2023
    assert result_data[0]["DATE_COL_MONTH"] == 1
    assert result_data[0]["DATE_COL_DAYOFYEAR"] == 1
    assert result_data[0]["DATE_COL_IS_WEEKEND"]


def test_expand_times(alltypes):
    df = alltypes.select("TIME_COL")
    result_df = expand_times(df)

    expected_columns = [
        "TIME_COL",
        "TIME_COL_HOUR",
        "TIME_COL_MINUTE",
        "TIME_COL_SECOND",
    ]
    assert set(map(str.upper, result_df.columns)) == set(
        map(str.upper, expected_columns)
    )

    result_data = result_df.collect()
    assert result_data[0]["TIME_COL_HOUR"] == 1
    assert result_data[0]["TIME_COL_MINUTE"] == 2
    assert result_data[0]["TIME_COL_SECOND"] == 3


def test_expand_datetimes(alltypes):
    df = alltypes.select("TIMESTAMP_COL")
    result_df = expand_datetimes(df)

    expected_columns = [
        "TIMESTAMP_COL",
        "TIMESTAMP_COL_YEAR",
        "TIMESTAMP_COL_MONTH",
        "TIMESTAMP_COL_DAYOFWEEK",
        "TIMESTAMP_COL_DAYOFYEAR",
        "TIMESTAMP_COL_WEEKOFYEAR",
        "TIMESTAMP_COL_IS_WEEKEND",
        "TIMESTAMP_COL_HOUR",
        "TIMESTAMP_COL_MINUTE",
        "TIMESTAMP_COL_SECOND",
    ]
    assert set(map(str.upper, result_df.columns)) == set(
        map(str.upper, expected_columns)
    )

    result_data = result_df.collect()
    assert result_data[0]["TIMESTAMP_COL_YEAR"] == 2023
    assert result_data[0]["TIMESTAMP_COL_MONTH"] == 1
    assert result_data[0]["TIMESTAMP_COL_DAYOFWEEK"] == 0
    assert result_data[0]["TIMESTAMP_COL_DAYOFYEAR"] == 1
    assert result_data[0]["TIMESTAMP_COL_IS_WEEKEND"]
    assert result_data[0]["TIMESTAMP_COL_HOUR"] == 1
    assert result_data[0]["TIMESTAMP_COL_MINUTE"] == 2
    assert result_data[0]["TIMESTAMP_COL_SECOND"] == 3


def test_compute_lags(session):
    d = [
        {"date_col": date(2021, 1, 1), "value_col": 1},
        {"date_col": date(2021, 1, 2), "value_col": 2},
        {"date_col": date(2021, 1, 3), "value_col": 3},
    ]
    df = session.create_dataframe(d)
    result = compute_lags(df, "value_col", "date_col", Frequency.DAY, 1)
    assert "VALUE_COL_LAG_DAY_1" in result.columns
    assert result.select("VALUE_COL_LAG_DAY_1").to_pandas()[
        "VALUE_COL_LAG_DAY_1"
    ].tolist() == [1, 1, 2]
