import pytest
import sklearn.metrics

import forecast_model_builder.metrics


@pytest.mark.parametrize(
    "metric_name",
    [
        pytest.param("mean_absolute_error", id="mean_absolute_error"),
        pytest.param("mean_squared_error", id="mean_squared_error"),
        pytest.param("root_mean_squared_error", id="root_mean_squared_error"),
        pytest.param("r2_score", id="r2_score"),
        pytest.param(
            "mean_absolute_percentage_error", id="mean_absolute_percentage_error"
        ),
    ],
)
def test_regression_metrics(predictions, metric_name):
    ml_forecasting_incubator_func = getattr(forecast_model_builder.metrics, metric_name)
    sklearn_func = getattr(sklearn.metrics, metric_name)
    df = predictions.to_pandas()
    result = ml_forecasting_incubator_func(predictions, "ACTUAL", "PREDICTION")
    expected = sklearn_func(df["ACTUAL"], df["PREDICTION"])
    assert result == pytest.approx(expected, abs=1e-4)


@pytest.mark.parametrize(
    "metric_name",
    [
        ("mean_absolute_error"),
        ("mean_squared_error"),
        ("root_mean_squared_error"),
        ("r2_score"),
        ("mean_absolute_percentage_error"),
    ],
)
def test_get_metrics(predictions, metric_name):
    df = predictions.to_pandas()
    result = forecast_model_builder.metrics.get_metrics(
        predictions, "ACTUAL", "PREDICTION"
    )
    sklearn_func = getattr(sklearn.metrics, metric_name)
    expected = sklearn_func(df["ACTUAL"], df["PREDICTION"])
    assert result[metric_name] == pytest.approx(expected, abs=1e-4)
