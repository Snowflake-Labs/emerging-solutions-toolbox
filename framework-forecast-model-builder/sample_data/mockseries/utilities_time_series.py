from datetime import datetime, timedelta
from typing import List, Tuple

import numpy as np
import pandas as pd
from mockseries.noise import RedNoise
from mockseries.seasonality import SinusoidalSeasonality
from mockseries.trend import LinearTrend
from mockseries.utils import datetime_range


def generate_time_series_data(
    grain: str = "daily",
    start_year: int = 2021,
    trend_coeff: float = 0.1,
    trend_flat_base: int = 100,
    seasonality_daily_amp: int = 5,
    seasonality_weekly_amp: int = 10,
    seasonality_6mo_amp: int = 5,
    seasonality_yearly_amp: int = 25,
    noise_mean: int = 0,
    noise_std: int = 3,
    noise_corr: float = 0.5,
) -> Tuple[List, np.ndarray]:
    """Generate a single mock time series with specified trend, seasonality, and noise.

    Parameters
    ----------
    grain : str
        'hourly', 'daily', or 'weekly' granularity. Default is 'daily'.
    start_year : int
        The year to start the time series. Default is 2021.
    trend_coeff : float
        Specifies the slope of the linear trend. higher values create steeper trends. Default is 0.1.
    trend_flat_base : int
        Specifies the base value of the linear trend (i.e. the y-intercept). Default is 100.
    seasonality_daily_amp : int
        Amplitude of the daily seasonality. Default is 5.
    seasonality_weekly_amp : int
        Amplitude of the weekly seasonality. Default is 10.
    seasonality_6mo_amp : int
        Amplitude of the 6-month seasonality. Default is 5.
    seasonality_yearly_amp : int
        Amplitude of the yearly seasonality. Default is 25.
    noise_mean : int
        Mean of the random noise. This will be used to ensure the series contain random noise. Default is 0.
    noise_std : int
        Standard deviation of the random noise. Default is 3.
    noise_corr : float
        Correlation of the random noise. Default is 0.5.

    Returns
    -------
    Tuple
        Tuple containing the time points and the target values of the time series.

    """
    # Define trend
    trend = LinearTrend(
        coefficient=trend_coeff, time_unit=timedelta(days=4), flat_base=trend_flat_base
    )

    # Define random noise
    noise = RedNoise(mean=noise_mean, std=noise_std, correlation=noise_corr)

    # Define seasonality
    seasonality = SinusoidalSeasonality(
        amplitude=seasonality_6mo_amp, period=timedelta(days=182.5)
    ) + SinusoidalSeasonality(
        amplitude=seasonality_yearly_amp, period=timedelta(days=365)
    )

    # Depending on the desired granularity, establish a time delta for time_points and adjust the seasonality
    if grain[0].lower() == "w":
        granularity_time_delta = timedelta(weeks=1)
    elif grain[0].lower() == "h":
        granularity_time_delta = timedelta(hours=1)
        seasonality = (
            seasonality
            + SinusoidalSeasonality(
                amplitude=seasonality_weekly_amp, period=timedelta(days=7.0)
            )
            + SinusoidalSeasonality(
                amplitude=seasonality_daily_amp, period=timedelta(days=1.0)
            )
        )
    else:  # case when grain[0].lower() == 'd'
        granularity_time_delta = timedelta(days=1)
        seasonality = seasonality + SinusoidalSeasonality(
            amplitude=seasonality_weekly_amp, period=timedelta(days=7.0)
        )

    timeseries = trend + seasonality + noise

    time_points = datetime_range(
        granularity=granularity_time_delta,
        start_time=datetime(start_year, 1, 1),
        end_time=datetime.today(),
    )
    ts_values = timeseries.generate(time_points=time_points)

    return time_points, ts_values


def create_dataframe(time_points: list, ts_values: np.ndarray) -> pd.DataFrame:
    """Create a pandas DataFrame from the list and numpy array that come from the generate_time_series_data function. This dataframe will represent a single time series.

    Parameters
    ----------
    time_points : list
        List of datetime objects representing the time points of the time series.
    ts_values : np.ndarray
        Numpy array of the target values of the time series.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing time points and target values of the time series.

    """
    df_single_partition = pd.DataFrame(
        {"ORDER_TIMESTAMP": time_points, "TARGET": ts_values}
    )
    return df_single_partition
