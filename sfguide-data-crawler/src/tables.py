import snowflake.snowpark.functions as F
import pandas as pd

def get_table_comment(tablename, session):
    """Returns current comment on table"""

    tbl_context = tablename.split('.')
    tbl, schema = tbl_context[-1], '.'.join(tbl_context[:-1])
    return session.sql(f"SHOW TABLES LIKE '{tbl}' IN SCHEMA {schema} LIMIT 1").collect()[0]['comment']\
                  .replace("'", "\\'")

def convert_vec2array(tablename, session):
    """Converts vector type columns of dataframe to arrays before sampling"""

    import snowflake.snowpark.types as T

    df = session.table(tablename)
    vec_cols = [c.name for c in df.schema.fields if (type(c.datatype) == T.VectorType)]
    if vec_cols:
        return df.select([F.array_slice(F.to_array(x), F.lit(0), F.lit(10)).as_(x) if x in vec_cols else x for x in df.columns])
    else:
        return df
    
def pctg_nonnulls(df):
    """Returns float per row in dataframe indicating proportion of row values non-null/non-empty"""

    import pandas
    from _snowflake import vectorized
    return 1 - sum(el in [None, ''] for el in df)/len(df)

def sample_tbl(tablename, sampling_mode, n, session):
    """Returns n samples of table based on sampling_mode"""

    from snowflake.snowpark.window import Window

    df = convert_vec2array(tablename, session) # VectorType cannot be used in object_construct
    if sampling_mode == "fast": # Randomly sample
        samples = df.sample(n = n)\
            .select(F.to_varchar(F.array_agg(F.object_construct('*'))))\
            .to_pandas().values[0][0]
    elif sampling_mode == 'nonnull': # Sort by least null and take first n
        samples = df.withColumn('ROWNUMBER', 
                            F.row_number().over(Window.partition_by().order_by(F.desc(F.call_udf('PCTG_NONNULL', F.array_construct('*'))))))\
                .sort(F.col('ROWNUMBER')).drop(F.col('ROW_NUMBER'))\
                .select(F.to_varchar(F.array_slice(F.array_agg(F.object_construct('*')), F.lit(0), F.lit(n))))\
                .to_pandas().values[0][0]
    else:
        raise ValueError("sampling_mode must be one of ['fast' (Default), 'nonnull'].") 
    return samples.replace("'", "\\'")

def cortex_sql(session, model, prompt, temperature):
    """Executes CORTEX COMPLETE using SQL in Python API.
    
    Use if temperature passed. Python API does not support temperature.
    """
    query = f"""
    SELECT TRIM(SNOWFLAKE.CORTEX.COMPLETE(
    '{model}', 
    [
        {{
            'role': 'user',
            'content': '{prompt}'
        }}
    ],
    {{
        'temperature': {temperature}
    }}
    ):choices[0]:messages) AS RESPONSE
    """
    result = session.sql(query).collect()[0][0]
    return result

def run_complete(session, tablename, model, sampling_mode, n, prompt, temperature = None):
    
    """Returns (success/failed LLM-generated description) of table given least empty sample records."""

    import textwrap
    from snowflake.cortex import Complete
    from snowflake.snowpark.exceptions import SnowparkSQLException

    samples = sample_tbl(tablename, sampling_mode, n, session)

    try:
        # Escape curly braces for SQL translation to avoid error
        prompt = textwrap.dedent(prompt.format(table_samples = samples))
        
        if isinstance(temperature, float):
            if temperature > 0 and temperature < 1: 
                response = cortex_sql(session,
                                    model,
                                    prompt,
                                    temperature)
            else: # Use default temperature if non-valid temperature passed
                response = Complete(model, 
                                    prompt,
                                    session = session)
        else:
            response = Complete(model, 
                                prompt,
                                session = session)
        response = str(response).strip().replace("'", "\\'")
        
        return ("success", response)
    except SnowparkSQLException as e:
        if 'max tokens' in str(e):
            raise NotImplementedError(f"{e}.\nCortex token counter will be added once available. Try a different model or fewer sample rows.")
        else:
            return ("fail", f"""LLM-generation Error Encountered: {e}""")
    except Exception as e:
        return ("fail", f"""LLM-generation Error Encountered: {e}""")
    
def get_crawlable_tbls(session,
                     database,
                     schema,
                     catalog_database,
                     catalog_schema,
                     catalog_table,
                     ignore_catalog = False):
    """Returns list of tables in database/schema that have not been cataloged."""

    if schema:
        schema_qualifier = f"= '{schema}'"
    else:
        schema_qualifier = "<> 'INFORMATION_SCHEMA'"

    if ignore_catalog:
        catalog_constraint = ""
        
    else:
        catalog_constraint = f"""NATURAL FULL OUTER JOIN {catalog_database}.{catalog_schema}.{catalog_table}
                                 WHERE {catalog_database}.{catalog_schema}.{catalog_table}.TABLENAME IS NULL"""
    
    query = f"""
    WITH T AS (
        SELECT 
            TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
            FROM {database}.INFORMATION_SCHEMA.tables 
            WHERE TABLE_SCHEMA {schema_qualifier}
            AND (ROW_COUNT >= 1 OR ROW_COUNT IS NULL)
            AND IS_TEMPORARY = 'NO'
            AND NOT STARTSWITH(TABLE_NAME, '_')
            )
    SELECT 
        T.TABLENAME
    FROM T 
    {catalog_constraint}
    """
    return session.sql(query).to_pandas()['TABLENAME'].values.tolist()

def get_unique_context(tablenames):
    """Returns target database and unique set of qualified schema names for crawling context."""
    schemas = {".".join(t.split(".")[:-1]) for t in tablenames}
    db = tablenames[0].split('.')[0]
    return db, schemas

def get_all_tables(session, target_database, target_schemas):
    """Returns pandas dataframe of [schema, table, table comment, column info]."""
    target_schema_str = ','.join(f"'{t.split('.')[1]}'" for t in target_schemas)
    query = f"""
        WITH T AS 
    (
        SELECT 
            TABLE_SCHEMA
            , TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
            ,REGEXP_REPLACE(COMMENT, '{{|}}','') AS TABLE_COMMENT
        FROM {target_database}.INFORMATION_SCHEMA.tables 
        WHERE 1=1 
            AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
            AND TABLE_SCHEMA IN ({target_schema_str})
            AND (ROW_COUNT >= 1 OR ROW_COUNT IS NULL)
            AND IS_TEMPORARY = 'NO'
            AND NOT STARTSWITH(TABLE_NAME, '_')
        )
    , C AS (
        SELECT
            TABLE_SCHEMA
            ,TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
            ,LISTAGG(CONCAT(COLUMN_NAME, ' ', DATA_TYPE, COALESCE(concat(' (', REGEXP_REPLACE(COMMENT, '{{|}}',''), ')'), '')), ', ') as COLUMN_INFO
        FROM {target_database}.INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1 
            AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
            AND TABLE_SCHEMA IN ({target_schema_str})
            AND NOT STARTSWITH(TABLE_NAME, '_')
        GROUP BY TABLE_SCHEMA, TABLENAME
        )
    SELECT
        *
        , 'Table: ' || TABLENAME || ', Comment: ' || COALESCE(TABLE_COMMENT, 'No comment') || ', Columns: ' || COLUMN_INFO AS TABLE_DDL
    FROM T NATURAL INNER JOIN C
    """
    # F-string interpolation later so remove unexpected curly brackets here in data such as in comments
    return session.sql(query).to_pandas()

def add_records_to_catalog(session,
                           catalog_database,
                           catalog_schema,
                           catalog_table,
                           new_df,
                           replace_catalog = False):
    
    if replace_catalog:
        current_df = session.table(f'{catalog_database}.{catalog_schema}.{catalog_table}')
        _ = current_df.merge(new_df, current_df['TABLENAME'] == new_df['TABLENAME'],
                 [F.when_matched().update({'DESCRIPTION': new_df['DESCRIPTION'],
                                           'CREATED_ON': new_df['CREATED_ON'],
                                           'EMBEDDINGS': F.call_udf('SNOWFLAKE.CORTEX.EMBED_TEXT_768',
                                                                   'e5-base-v2',
                                                                    new_df['DESCRIPTION'])}),
                  F.when_not_matched().insert({'TABLENAME': new_df['TABLENAME'],
                                               'DESCRIPTION': new_df['DESCRIPTION'],
                                               'CREATED_ON': new_df['CREATED_ON'],
                                               'EMBEDDINGS': F.call_udf('SNOWFLAKE.CORTEX.EMBED_TEXT_768',
                                                                   'e5-base-v2',
                                                                    new_df['DESCRIPTION'])})])
    else:
        new_df.write.save_as_table(table_name = [catalog_database, catalog_schema, catalog_table],
                                mode = "append",
                                column_order = "name")

def generate_description(session,
                         tablename,
                         prompt,
                         sampling_mode,
                         n,
                         model,
                         update_comment
                         ):
    
    from snowflake.snowpark.exceptions import SnowparkSQLException
    
    """
    Catalogs table objects in Snowflake.

    Args:
        session (Snowpark session) : ignore parameter
        tablename (string): Fully qualified Snowflake table name
        prompt (string): Prompt in format of f-string to pass to LLM
        sampling_mode (string): How to retrieve sample data records for table.
                                One of ['fast' (Default), 'nonnull']
                                - Pass 'fast' or omit to randomly sample records from each table.
                                - Pass 'nonnull' to prioritize least null records for table samples.
                                - Passing 'nonnull' will take considerably longer to run.
        n (int): Number of records to sample from table. Defaults to 5.
        model (string): Cortex model to generate table descriptions. Defaults to 'mistral-7b'.
        update_comment (bool): If True, update table's current comments. Defaults to False

    Returns:
        Dict
    """


    response = ''
    try:
        ctx_response, response = run_complete(session,
                                              tablename,
                                              model, 
                                              sampling_mode,
                                              n,
                                              prompt)
        if update_comment and ctx_response == 'success':
            try:
                session.sql(f"COMMENT IF EXISTS ON TABLE {tablename} IS '{response}'").collect()
            except SnowparkSQLException as e:
                try: # Table may actually be a view
                    session.sql(f"COMMENT IF EXISTS ON VIEW {tablename} IS '{response}'").collect()
                except Exception as e:
                    response = f'Error encountered: {str(e)}'
            except Exception as e:
                response = f'Error encountered: {str(e)}'
    except Exception as e:
        response = f'Error encountered: {str(e)}'
    return {
        'TABLENAME': tablename,
        'DESCRIPTION': response.replace("\\", "")
        }
