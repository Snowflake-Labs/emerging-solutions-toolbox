import os
from datetime import date, datetime, time

import boto3
import pandas as pd
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T

load_dotenv(override=True)


@pytest.fixture(scope="session")
def session():
    session = boto3.session.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION_NAME"),
    )
    client = session.client(service_name="secretsmanager")
    p_key = serialization.load_pem_private_key(
        client.get_secret_value(SecretId=os.environ.get("AWS_SECRET_PRIVATE_KEY_NAME"))[
            "SecretString"
        ].encode("utf-8"),
        password=client.get_secret_value(
            SecretId=os.environ.get("AWS_SECRET_PRIVATE_KEY_PASSPHRASE_NAME")
        )["SecretBinary"],
        backend=default_backend(),
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    session = Session.builder.configs(
        {
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "user": os.environ.get("SNOWFLAKE_USER"),
            "private_key": pkb,
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "database": os.environ.get("SNOWFLAKE_DATABASE"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
        }
    ).getOrCreate()
    yield session
    session.close()


@pytest.fixture(scope="session")
def alltypes(session):
    schema = T.StructType(
        [
            T.StructField("TIMESTAMP_COL", T.TimestampType()),
            T.StructField("DATE_COL", T.DateType()),
            T.StructField("TIME_COL", T.TimeType()),
            T.StructField("INT_COL", T.LongType()),
            T.StructField("FLOAT_COL", T.FloatType()),
            T.StructField("STRING_COL", T.StringType()),
        ]
    )
    data = [
        {
            "TIMESTAMP_COL": datetime(2023, 1, 1, 1, 2, 3),
            "DATE_COL": date(2023, 1, 1),
            "TIME_COL": time(1, 2, 3),
            "INT_COL": 1,
            "FLOAT_COL": 1.0,
            "STRING_COL": "a",
        }
    ]
    df = session.create_dataframe(data, schema)
    yield df


@pytest.fixture(scope="session")
def predictions(session):
    # The first "prediction" is correct. The others are slightly higher.
    data = {
        "ID": range(1, 4),
        "ACTUAL": [5.0, 6.0, 7.0],
        "PREDICTION": [5.0, 7.0, 7.5],
    }
    df = session.create_dataframe(pd.DataFrame(data))
    yield df


@pytest.fixture(scope="session")
def temporal_df(session):
    data = {
        "ID": range(1, 7),
        "DATETIME_COL": [
            datetime(2024, 1, 1, 0, 0),
            datetime(2024, 1, 2, 2, 1),
            datetime(2024, 1, 2, 3, 3),
            datetime(2025, 2, 2, 3, 4),
            datetime(2025, 3, 4, 5, 6),
            datetime(2025, 4, 5, 6, 7),
        ],
    }
    df = session.create_dataframe(pd.DataFrame(data))
    df = df.with_columns(
        [
            "DATETIME_YEAR_COL",
            "DATETIME_MONTH_COL",
            "DATETIME_WEEK_COL",
            "DATETIME_DAY_COL",
            "DATETIME_HOUR_COL",
            "DATETIME_MINUTE_COL",
            "DATETIME_SECOND_COL",
        ],
        [
            F.date_trunc("YEAR", F.col("DATETIME_COL")),
            F.date_trunc("MONTH", F.col("DATETIME_COL")),
            F.date_trunc("WEEK", F.col("DATETIME_COL")),
            F.date_trunc("DAY", F.col("DATETIME_COL")),
            F.date_trunc("HOUR", F.col("DATETIME_COL")),
            F.date_trunc("MINUTE", F.col("DATETIME_COL")),
            F.date_trunc("SECOND", F.col("DATETIME_COL")),
        ],
    )
    yield df
