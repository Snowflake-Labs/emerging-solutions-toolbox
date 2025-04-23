from snowflake.snowpark import functions as F
from snowflake.snowpark import DataFrame as SnowparkDataFrame


# ********************************
# NON-STATISTICAL TEST FUNCTIONS
# ********************************


# def compare_column_aggregates(
#     check_type_id: int,
#     tbla: SnowparkDataFrame,
#     tblb: SnowparkDataFrame,
#     a_columns_to_check: list,
#     b_columns_to_check: list,
#     a_agg_list: list,
#     b_agg_list: list,
#     a_output_key: str,
#     b_output_key: str,
#     udf_nm: str,
# ) -> SnowparkDataFrame:
#     # Obtain aggregate value (ex: sums, null counts, distinct value counts, etc) of relevant columns and place all aggregated values into an ARRAY column
#     a_values_df = (
#         tbla.group_by("PARTITION_VALUES")
#         .agg(*a_agg_list)
#         .with_column("TABLE_A_AGG_VALUES", F.array_construct(*a_columns_to_check))
#         .select("PARTITION_VALUES", "TABLE_A_AGG_VALUES")
#     )

#     b_values_df = (
#         tblb.group_by("PARTITION_VALUES")
#         .agg(*b_agg_list)
#         .with_column("TABLE_B_AGG_VALUES", F.array_construct(*b_columns_to_check))
#         .select("PARTITION_VALUES", "TABLE_B_AGG_VALUES")
#     )

#     joined_df = a_values_df.join(b_values_df, on=["PARTITION_VALUES"])

#     # Subtract each value in the two arrays to get a third array containing the changes
#     delta_df = joined_df.withColumn(
#         "DELTA",
#         F.call_udf(udf_nm, F.col("TABLE_A_AGG_VALUES"), F.col("TABLE_B_AGG_VALUES")),
#     )

#     result_df = delta_df.select(
#         F.lit(check_type_id).alias("CHECK_TYPE_ID"),
#         "PARTITION_VALUES",
#         # "TABLE_A_AGG_VALUES",
#         # "TABLE_B_AGG_VALUES",
#         # "DELTA",
#         F.object_construct_keep_null(
#             F.lit("TABLE_A_COLUMNS"),
#             F.lit(a_columns_to_check),
#             F.lit("TABLE_B_COLUMNS"),
#             F.lit(b_columns_to_check),
#             F.lit(a_output_key),
#             "TABLE_A_AGG_VALUES",
#             F.lit(b_output_key),
#             "TABLE_B_AGG_VALUES",
#             F.lit("DELTA"),
#             "DELTA",
#         ).alias("RESULTS"),
#     )

#     return result_df


def get_group_aggregates(
    tbla: SnowparkDataFrame,
    tblb: SnowparkDataFrame,
    a_columns_to_check: list,
    b_columns_to_check: list,
    a_agg_list: list,
    b_agg_list: list
) -> SnowparkDataFrame:
    """
    Groups by the PARTITION_VALUES column and calculates the specified aggregate function over the specified columns to check. 
    The results of the for the Table A and Table B are joined together and returned as a Snowpark DataFrame
    """
    # Obtain aggregate value (ex: sums, null counts, distinct value counts, etc) of relevant columns and place all aggregated values into an ARRAY column
    a_values_df = (
        tbla.group_by("PARTITION_VALUES")
        .agg(*a_agg_list)
        .with_column("TABLE_A_AGG_VALUES", F.array_construct(*a_columns_to_check))
        .select("PARTITION_VALUES", "TABLE_A_AGG_VALUES")
    )

    b_values_df = (
        tblb.group_by("PARTITION_VALUES")
        .agg(*b_agg_list)
        .with_column("TABLE_B_AGG_VALUES", F.array_construct(*b_columns_to_check))
        .select("PARTITION_VALUES", "TABLE_B_AGG_VALUES")
    )

    joined_df = a_values_df.join(b_values_df, on=["PARTITION_VALUES"])

    return joined_df


def create_result_df_for_simple_summary_check(
    joined_df: SnowparkDataFrame,
    check_type_id: int,
    a_columns_to_check: list,
    b_columns_to_check: list,
    a_output_key: str,
    b_output_key: str,
    udf_nm: str,
) -> SnowparkDataFrame:
    """
    Calculates the delta between the aggregated values of two tables and returns the results as a dictionary in Snowpark DataFrame.
    """

    # Subtract each value in the two arrays to get a third array containing the changes
    delta_df = joined_df.withColumn(
        "DELTA",
        F.call_udf(udf_nm, F.col("TABLE_A_AGG_VALUES"), F.col("TABLE_B_AGG_VALUES")),
    )

    result_df = delta_df.select(
        F.lit(check_type_id).alias("CHECK_TYPE_ID"),
        "PARTITION_VALUES",
        # "TABLE_A_AGG_VALUES",
        # "TABLE_B_AGG_VALUES",
        # "DELTA",
        F.object_construct_keep_null(
            F.lit("TABLE_A_COLUMNS"),
            F.lit(a_columns_to_check),
            F.lit("TABLE_B_COLUMNS"),
            F.lit(b_columns_to_check),
            F.lit(a_output_key),
            F.col("TABLE_A_AGG_VALUES"),
            F.lit(b_output_key),
            F.col("TABLE_B_AGG_VALUES"),
            F.lit("DELTA"),
            F.col("DELTA"),
        ).alias("RESULTS"),
    )

    return result_df



def compare_column_aggregates(
    check_type_id: int,
    tbla: SnowparkDataFrame,
    tblb: SnowparkDataFrame,
    a_columns_to_check: list,
    b_columns_to_check: list,
    a_agg_list: list,
    b_agg_list: list,
    a_output_key: str,
    b_output_key: str,
    udf_nm: str,
) -> SnowparkDataFrame:

    joined_df = get_group_aggregates(
        tbla,
        tblb,
        a_columns_to_check,
        b_columns_to_check,
        a_agg_list,
        b_agg_list
    )

    result_df = create_result_df_for_simple_summary_check(
        joined_df,
        check_type_id,
        a_columns_to_check,
        b_columns_to_check,
        a_output_key,
        b_output_key,
        udf_nm
    )
    return result_df