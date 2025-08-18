SET major = 2;
SET minor = 0;
SET COMMENT = concat('{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": ',$major,', "minor": ',$minor,'}}');

SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE DATABASE IF NOT EXISTS DATA_CATALOG
COMMENT = $COMMENT;
USE DATABASE DATA_CATALOG;

CREATE SCHEMA IF NOT EXISTS DATA_CATALOG.TABLE_CATALOG
COMMENT = $COMMENT;
USE SCHEMA DATA_CATALOG.TABLE_CATALOG;

-- Create API Integration for Git
CREATE OR REPLACE API INTEGRATION git_api_integration_snowflake_labs_emerging_solutions_toolbox
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs')
  ENABLED = TRUE;

-- Create Git Repository
CREATE OR REPLACE GIT REPOSITORY EMERGING_SOLUTION_TOOLBOX
  API_INTEGRATION = git_api_integration_snowflake_labs_emerging_solutions_toolbox
  ORIGIN = 'https://github.com/Snowflake-Labs/emerging-solutions-toolbox.git';

ALTER GIT REPOSITORY EMERGING_SOLUTION_TOOLBOX FETCH;

CREATE OR REPLACE STAGE DATA_CATALOG.TABLE_CATALOG.SRC_FILES
DIRECTORY = (ENABLE = true);

COPY FILES
  INTO @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/
  FROM @EMERGING_SOLUTION_TOOLBOX/branches/main/sfguide-data-crawler/src/
  PATTERN='.*[.]py';
-- PUT file://src/*.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

COPY FILES
  INTO @DATA_CATALOG.TABLE_CATALOG.SRC_FILES
  FROM @EMERGING_SOLUTION_TOOLBOX/branches/main/sfguide-data-crawler/streamlit/
  FILES=('manage.py', 'environment.yml');

COPY FILES
  INTO @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/pages/
  FROM @EMERGING_SOLUTION_TOOLBOX/branches/main/sfguide-data-crawler/streamlit/pages/
  FILES=('run.py');

-- PUT file://streamlit/manage.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
-- PUT file://streamlit/environment.yml @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
-- PUT file://streamlit/pages/run.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/pages/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

CREATE OR ALTER TABLE DATA_CATALOG.TABLE_CATALOG.TABLE_CATALOG (
  TABLENAME VARCHAR
  ,DESCRIPTION VARCHAR
  ,CREATED_ON TIMESTAMP
  ,EMBEDDINGS VECTOR(FLOAT, 768)
  ,COMMENT_UPDATED BOOLEAN
  )
COMMENT = $COMMENT;


CREATE OR REPLACE FUNCTION DATA_CATALOG.TABLE_CATALOG.PCTG_NONNULL(records VARIANT)
returns STRING
language python
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py')
COMMENT = $COMMENT
HANDLER = 'tables.pctg_nonnulls'
PACKAGES = ('pandas','snowflake-snowpark-python');

CREATE OR REPLACE PROCEDURE DATA_CATALOG.TABLE_CATALOG.CATALOG_TABLE(
                                                          tablename string,
                                                          prompt string,
                                                          sampling_mode string DEFAULT 'fast',
                                                          n integer DEFAULT 5,
                                                          model string DEFAULT 'mistral-7b',
                                                          update_comment boolean Default FALSE,
                                                          use_native_feature boolean DEFAULT FALSE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py', '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/prompts.py')
PACKAGES = ('snowflake-snowpark-python','joblib', 'pandas', 'snowflake-ml-python')
HANDLER = 'tables.generate_description'
COMMENT = $COMMENT
EXECUTE AS CALLER;

CREATE OR REPLACE PROCEDURE DATA_CATALOG.TABLE_CATALOG.DATA_CATALOG(target_database string,
                                                         catalog_database string,
                                                         catalog_schema string,
                                                         catalog_table string,
                                                         target_schema string DEFAULT '',
                                                         include_tables ARRAY DEFAULT null,
                                                         exclude_tables ARRAY DEFAULT null,
                                                         replace_catalog boolean DEFAULT FALSE,
                                                         sampling_mode string DEFAULT 'fast',
                                                         update_comment boolean Default FALSE,
                                                         n integer DEFAULT 5,
                                                         model string DEFAULT 'mistral-7b',
                                                         use_native_feature boolean DEFAULT FALSE
                                                         )
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas', 'snowflake-ml-python')
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py',
           '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/main.py',
           '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/prompts.py')
HANDLER = 'main.run_table_catalog'
COMMENT = $COMMENT
EXECUTE AS CALLER;

CREATE OR REPLACE STREAMLIT DATA_CATALOG.TABLE_CATALOG.DATA_CRAWLER
ROOT_LOCATION = '@data_catalog.table_catalog.src_files'
MAIN_FILE = '/manage.py'
QUERY_WAREHOUSE = $streamlit_warehouse
COMMENT = $COMMENT;
