CATALOG_RESULT_SCHEMA = ['TABLENAME', 'DESCRIPTION', 'COMMENT_UPDATED']

def run_table_catalog(session,
                      target_database,
                      catalog_database,
                      catalog_schema,
                      catalog_table,
                      target_schema,
                      include_tables,
                      exclude_tables,
                      replace_catalog,
                      sampling_mode,
                      update_comment,
                      n,
                      model,
                      use_native_feature):

    """
    Catalogs data contained in Snowflake Database/Schema.

    Writes results to table. Output table contains:
    - TABLENAME: Name of the table
    - DESCRIPTION: Description of data contained in respective table
    - COMMENT_UPDATED: Boolean indicating if table comment was successfully updated (when update_comment=True)
    - EMBEDDINGS: Vector embeddings of the description
    - CREATED_ON: Timestamp when the record was created

    Args:
        target_database (string): Snowflake database to catalog.
        catalog_database (string): Snowflake database to store table catalog.
        catalog_schema (string): Snowflake schemaname to store table catalog.
        catalog_table (string): Snowflake tablename to store table catalog.
        target_schema (string, Optional): Snowflake schema to catalog.
        include_tables (list, Optional): Explicit list of tables to include in catalog.
        exclude_tables (list, Optional): Explicit list of tables to exclude in catalog.
                                         include_tables takes precedence over exclude_tables.
        replace_catalog (bool): If True, replace existing catalog table records. Defaults to False.
        sampling_mode (string): How to retrieve sample data records for table.
                                One of ['fast' (Default), 'nonnull']
                                - Pass 'fast' or omit to randomly sample records from each table.
                                - Pass 'nonnull' to prioritize least null records for table samples.
                                - Passing 'nonnull' will take considerably longer to run.
        update_comment (bool): If True, update table's current comments. Defaults to False
        n (int): Number of records to sample from table. Defaults to 5.
        model (string): Cortex model to generate table descriptions. Defaults to 'mistral-7b'.
        use_native_feature (bool): If True, use native feature for processing. Defaults to False.

    Returns:
        Snowpark DataFrame containing the catalog results
    """

    import json
    import multiprocessing
    import joblib
    from joblib import Parallel, delayed
    import pandas as pd
    import snowflake.snowpark.functions as F
    from tables import get_crawlable_tbls, apply_table_filters, get_unique_context, get_all_tables, add_records_to_catalog
    from prompts import start_prompt

    def process_single_table(table, schema_df, catalog_info, sampling_mode, n, model, update_comment, use_native_feature):
        """Process a single table with all necessary parameters

        Args:
            table (str): Fully qualified table name to process
            schema_df (pd.DataFrame): DataFrame containing schema information for all tables
            catalog_info (dict): Dictionary containing catalog database and schema info
            sampling_mode (str): How to retrieve sample data records ('fast' or 'nonnull')
            n (int): Number of records to sample from table
            model (str): Cortex model to generate table descriptions
            update_comment (bool): Whether to update table comments with generated descriptions
            use_native_feature (bool): Whether to use Snowflake's native AI_GENERATE_TABLE_DESC function

        Returns:
            Dictionary containing:
            - TABLENAME: Name of the table
            - DESCRIPTION: Description or error message
            - COMMENT_UPDATED: Boolean indicating if comment was updated (when update_comment=True)
        """
        try:
            # Prepare table metadata
            current_schema = table.split('.')[1]
            prompt_args = {
                'tablename': table,
                'table_columns': schema_df[schema_df.TABLENAME == table]['COLUMN_INFO'].to_numpy().item(),
                'table_comment': schema_df[schema_df.TABLENAME == table]['TABLE_COMMENT'].to_numpy().item(),
                'schema_tables': schema_df[schema_df.TABLE_SCHEMA == current_schema]\
                                .groupby('TABLE_SCHEMA')['TABLE_DDL']\
                                .apply(list).to_numpy().item()[0]
            }

            try:
                # Only generate prompt if not using native feature
                if not use_native_feature:
                    try:
                        prompt = start_prompt.format(**prompt_args)
                        prompt = prompt.replace("'", "\\'")  # Simple quote escaping
                    except Exception as e:
                        return {
                            'TABLENAME': table,
                            'DESCRIPTION': f'Error creating prompt: {str(e)}',
                            'COMMENT_UPDATED': False if update_comment else None
                        }
                else:
                    prompt = ''  # Empty prompt when using native feature

                # Execute the catalog procedure
                async_job = session.sql(f"""
                    CALL {catalog_info['database']}.{catalog_info['schema']}.CATALOG_TABLE(
                        tablename => '{table}',
                        prompt => '{prompt}',
                        sampling_mode => '{sampling_mode}',
                        n => {n},
                        model => '{model}',
                        update_comment => {update_comment},
                        use_native_feature => {use_native_feature})
                """).collect()

                # Wait for and get the result
                result = async_job

                # Added debug logging
                if not bool(result):
                    return {
                        'TABLENAME': table,
                        'DESCRIPTION': f'Error: Empty result from CATALOG_TABLE',
                        'COMMENT_UPDATED': False if update_comment else None
                    }

                # Get the dictionary result from generate_description
                result_data = json.loads(result[0][0])
                return result_data

            except Exception as e:
                # Return failed result - will still be included in catalog
                return {
                    'TABLENAME': table,
                    'DESCRIPTION': f'Error processing table: {str(e)}',
                    'COMMENT_UPDATED': False if update_comment else None
                }

        except Exception as e:
            # Catch any other unexpected errors
            return {
                'TABLENAME': table,
                'DESCRIPTION': f'Unexpected error: {str(e)}',
                'COMMENT_UPDATED': False if update_comment else None
            }

    # Get tables to crawl
    tables = get_crawlable_tbls(session, target_database, target_schema,
                                catalog_database, catalog_schema, catalog_table,
                                replace_catalog)

    # Apply table filters if provided
    tables = apply_table_filters(tables, include_tables, exclude_tables)

    # If no tables to crawl, return empty dataframe
    if not tables:
        return session.create_dataframe([['No new tables to crawl', '', None]],
                                      schema=CATALOG_RESULT_SCHEMA)

    # Get context and schema information
    context_db, context_schemas = get_unique_context(tables)
    schema_df = get_all_tables(session, context_db, context_schemas)

    # Prepare catalog info for reuse
    catalog_info = {
        'database': catalog_database,
        'schema': catalog_schema
    }

    # Parallel processing of tables using the cpu count available
    results = Parallel(n_jobs=multiprocessing.cpu_count(), backend="threading")(
        delayed(process_single_table)(
            table,
            schema_df,
            catalog_info,
            sampling_mode,
            n,
            model,
            update_comment,
            use_native_feature
        ) for table in tables
    )

    # Create DataFrame from results (list -> pd dataframe -> snowpark dataframe -> add embeddings and timestamp)
    try:
        df = session.create_dataframe(pd.DataFrame.from_records(results),
                                    schema=CATALOG_RESULT_SCHEMA)\
               .withColumn('EMBEDDINGS',
                          F.call_udf('SNOWFLAKE.CORTEX.EMBED_TEXT_768',
                                    'e5-base-v2',
                                    F.col('DESCRIPTION')))\
               .withColumn('CREATED_ON', F.current_timestamp())
    except Exception as e:
        return session.create_dataframe([['Error creating results dataframe', str(e), None]],
                                      schema=CATALOG_RESULT_SCHEMA)

    # Write to catalog table
    add_records_to_catalog(session,
                          catalog_database,
                          catalog_schema,
                          catalog_table,
                          df,
                          replace_catalog)

    return df
