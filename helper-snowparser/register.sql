SET db_name = 'GENAI_UTILITIES';
SET schema_name = 'UTILITIES';

SET major = 1;
SET minor = 2;
SET COMMENT = concat('{"origin": "sf_sit",
            "name": "snowparser",
            "version": {"major": ',$major,', "minor": ',$minor,'}}');

CREATE DATABASE IF NOT EXISTS IDENTIFIER($db_name)
COMMENT = $COMMENT;
USE DATABASE IDENTIFIER($db_name);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_name)
COMMENT = $COMMENT;

USE SCHEMA IDENTIFIER($schema_name);

-- Create stage for logic
CREATE STAGE IF NOT EXISTS DROPBOX
DIRECTORY = (ENABLE = true)
COMMENT = $COMMENT;

-- Create API Integration for Git
CREATE OR REPLACE API INTEGRATION git_api_integration_snowflake_labs_emerging_solutions_toolbox
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs')
  ENABLED = TRUE;

-- -- Create Git Repository
CREATE OR REPLACE GIT REPOSITORY EMERGING_SOLUTION_TOOLBOX
  API_INTEGRATION = git_api_integration_snowflake_labs_emerging_solutions_toolbox
  ORIGIN = 'https://github.com/Snowflake-Labs/emerging-solutions-toolbox.git';

ALTER GIT REPOSITORY EMERGING_SOLUTION_TOOLBOX FETCH;

COPY FILES
  INTO @DROPBOX
  FROM @EMERGING_SOLUTION_TOOLBOX/branches/snowparser/helper-snowparser/
  FILES=('prompts.py', 'tableau.py');

CREATE OR REPLACE PROCEDURE SNOWPARSER_SEMANTIC_DDL(
    tableau_data_source_file varchar,
    view_name varchar DEFAULT 'MY_SEMANTIC_VIEW',
    create_views boolean DEFAULT TRUE
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
IMPORTS = ('@DROPBOX/prompts.py',
        '@DROPBOX/tableau.py')
PACKAGES = (
'snowflake-snowpark-python==1.28.0',
'snowflake-ml-python==1.8.0',
'lxml==5.3.0')
HANDLER = 'get_ddl'
COMMENT = $COMMENT
EXECUTE AS CALLER
AS $$
from tableau import *

def get_ddl(session, tableau_data_source_file, view_name, create_views):
    tableau_model = TableauTDS(tableau_data_source_file, session, create_views)

    return tableau_model.get_view_ddl(view_name)
  $$;

CREATE OR REPLACE PROCEDURE SNOWPARSER_CREATE_SEMANTIC(
    tableau_data_source_file varchar,
    view_name varchar DEFAULT 'MY_SEMANTIC_VIEW',
    create_views boolean DEFAULT TRUE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
IMPORTS = ('@DROPBOX/prompts.py',
        '@DROPBOX/tableau.py')
PACKAGES = (
'snowflake-snowpark-python==1.28.0',
'snowflake-ml-python==1.8.0',
'lxml==5.3.0')
HANDLER = 'get_ddl'
COMMENT = $COMMENT
EXECUTE AS CALLER
AS $$
from tableau import *

def get_ddl(session, tableau_data_source_file, view_name, create_views):
    tableau_model = TableauTDS(tableau_data_source_file, session, create_views)

    return tableau_model.create_semantic_view(view_name)
  $$;

CREATE OR REPLACE PROCEDURE SNOWPARSER_GET_OBJECTS(
    tableau_data_source_file varchar,
    create_views boolean DEFAULT FALSE,
    defer_conversion boolean DEFAULT TRUE
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
IMPORTS = ('@DROPBOX/prompts.py',
        '@DROPBOX/tableau.py')
PACKAGES = (
'snowflake-snowpark-python==1.28.0',
'snowflake-ml-python==1.8.0',
'lxml==5.3.0')
HANDLER = 'get_objects'
COMMENT = $COMMENT
EXECUTE AS CALLER
AS $$
from tableau import *

def get_objects(session, tableau_data_source_file, create_views, defer_conversion):
    tableau_model = TableauTDS(tableau_data_source_file, session, create_views, defer_conversion)

    return tableau_model.get_object_names()
  $$;
