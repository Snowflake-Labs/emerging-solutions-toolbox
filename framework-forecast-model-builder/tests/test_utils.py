from forecast_model_builder.utils import predict_baseline


def test_predict_baseline(predictions):
    result_df = predict_baseline(predictions, "ACTUAL", "ID", 1)
    result_data = result_df.collect()

    assert result_data[0]["PREDICTION"] == 5.0
    assert result_data[1]["PREDICTION"] == 5.0
    assert result_data[2]["PREDICTION"] == 6.0
