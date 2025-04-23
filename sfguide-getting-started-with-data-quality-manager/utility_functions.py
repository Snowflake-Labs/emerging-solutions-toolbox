from snowflake.snowpark import types as T
from snowflake.snowpark import functions as F
from snowflake.snowpark import DataFrame as SnowparkDataFrame

import snowflake.ml.modeling.preprocessing as sml_pre
import snowflake.ml.modeling.pipeline as sml_pipe


# ********************************
# ANOMALY DETECTION FUNCTIONS
# ********************************


def obtain_col_type_lists(sdf: SnowparkDataFrame, COLUMNS_TO_CHECK: list) -> tuple:
    """Creates lists of column names. Each list represents a different type of data.
    NOTE: CATEGORICAL_COLUMNS right now is simply the same list as STRING_COLUMNS, but this could change if needed.
    """
    numeric_datatypes = (
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
    )
    time_datatypes = (T.DateType, T.TimestampType, T.TimeType)
    # numeric_datatypes = [ByteType(), ShortType(), IntegerType(), LongType(), FloatType(), DoubleType(), DecimalType()]
    # time_datatypes = [DateType(), TimestampType(), TimeType()]

    # Create lists to hold all of the numeric/boolean/string column names. All columns eventually need to be numeric for modeling.
    NUMERIC_COLUMNS = [
        field.name
        for field in sdf.schema.fields
        if (isinstance(field.datatype, numeric_datatypes))
        & (field.name in COLUMNS_TO_CHECK)
    ]
    TIME_COLUMNS = [
        field.name
        for field in sdf.schema.fields
        if (isinstance(field.datatype, time_datatypes))
        & (field.name in COLUMNS_TO_CHECK)
    ]
    BOOLEAN_COLUMNS = [
        field.name
        for field in sdf.schema.fields
        if (isinstance(field.datatype, T.BooleanType))
        & (field.name in COLUMNS_TO_CHECK)
    ]
    STRING_COLUMNS = [
        field.name
        for field in sdf.schema.fields
        if (isinstance(field.datatype, T.StringType)) & (field.name in COLUMNS_TO_CHECK)
    ]
    # # NOTE: Must use isinstance() instead of == to check datatypes, because StringType(1) does not equal StringType()
    # NUMERIC_COLUMNS = [field.name for field in sdf.schema.fields if (field.datatype in numeric_datatypes) & (field.name in COLUMNS_TO_CHECK)]
    # TIME_COLUMNS = [field.name for field in sdf.schema.fields if (field.datatype in time_datatypes) & (field.name in COLUMNS_TO_CHECK)]
    # BOOLEAN_COLUMNS = [field.name for field in sdf.schema.fields if (field.datatype == BooleanType()) & (field.name in COLUMNS_TO_CHECK)]
    # STRING_COLUMNS = [field.name for field in sdf.schema.fields if (field.datatype == StringType()) & (field.name in COLUMNS_TO_CHECK)]
    CATEGORICAL_COLUMNS = STRING_COLUMNS

    return (
        NUMERIC_COLUMNS,
        TIME_COLUMNS,
        BOOLEAN_COLUMNS,
        STRING_COLUMNS,
        CATEGORICAL_COLUMNS,
    )


def obtain_prepro_pipeln_new_col_names(
    preprocessing_pipeline: sml_pipe.Pipeline,
) -> list:
    """Obtain the new column names that were created by the Snowpark ML pipeline"""
    NEW_COLUMN_LIST_OF_LISTS = [
        step[1].get_output_cols() for step in preprocessing_pipeline.steps
    ]
    NEW_COLUMNS = []
    for sublist in NEW_COLUMN_LIST_OF_LISTS:
        NEW_COLUMNS.extend(sublist)

    return NEW_COLUMNS


def anomaly_detection_preprocessing(
    sdf: SnowparkDataFrame, COLUMNS_TO_CHECK: list
) -> tuple:
    """Takes a supplied Snowpark dataframe and performs data-type-specific transformations on a supplied list of columns.
    Uses Snowpark ML methods, so processing uses Snowflake compute.
    Right now the only transformations are
    (1) convert Boolean variables into numeric variables,
    (2) one-hot-encode string columns
    """
    # Get lists of column names for each data type
    (
        NUMERIC_COLUMNS,
        TIME_COLUMNS,
        BOOLEAN_COLUMNS,
        STRING_COLUMNS,
        CATEGORICAL_COLUMNS,
    ) = obtain_col_type_lists(sdf, COLUMNS_TO_CHECK)

    # CONVERT Boolean Columns to Numeric and UPPER CASE values in all String Columns
    sdf2 = sdf.select(
        [
            F.when(F.col(col_nm) == True, 1)
            .otherwise(0)
            .cast(T.IntegerType())
            .alias(col_nm)
            if (col_nm in BOOLEAN_COLUMNS)
            else F.upper(F.col(col_nm)).alias(col_nm)
            if (
                col_nm in STRING_COLUMNS
            )  # NOTE: Making strings upper so their dummy col names are all upper
            else F.col(col_nm)
            for col_nm in sdf.columns
        ]
    )

    if len(CATEGORICAL_COLUMNS) > 0:
        # PREPROCESSING PIPELINE: One-Hot Encode categorical features
        preprocessing_pipeline = sml_pipe.Pipeline(
            steps=[
                (
                    "OHE",
                    sml_pre.OneHotEncoder(
                        input_cols=CATEGORICAL_COLUMNS, output_cols=CATEGORICAL_COLUMNS
                    ),
                )
            ]
        )

        transformed_df = preprocessing_pipeline.fit(sdf2).transform(sdf2)

        # Obtain the new column names that were created
        NEW_COLUMNS = obtain_prepro_pipeln_new_col_names(preprocessing_pipeline)
    else:
        transformed_df = sdf2
        NEW_COLUMNS = []

    return (transformed_df, CATEGORICAL_COLUMNS, NEW_COLUMNS)
