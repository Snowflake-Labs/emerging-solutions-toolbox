
SET db_name = 'GENAI_UTILITIES';
SET schema_name = 'UTILITIES';

USE DATABASE IDENTIFIER($db_name);
USE SCHEMA IDENTIFIER($schema_name);
SET major = 1;
SET minor = 1;
SET COMMENT = concat('{"origin": "sf_sit",
            "name": "snowparser_dbt",
            "version": {"major": ',$major,', "minor": ',$minor,'}}');

-- If you have ACCOUNTADMIN privileges, you can use the following GITHUB API INTEGRATION
-- to create upload the necessary files to Snowflake stage.
-- Create stage or use an existing one. Note that you will need to replace the stage name in later commands.
-- If you DO NOT have ACCOUNTADMIN privileges, upload the files manually to a Snowflake stage in a directory named dbt.
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
  FROM @EMERGING_SOLUTION_TOOLBOX/branches/snowparser-for-dbt/helper-snowparser-for-dbt/
  FILES=('dbt/semantic_manifest_parser.py', 'dbt/semantics_schema.py');

-- Create the stored procedure to generate the semantic view YAML using semantic models in the manifest.json
CREATE OR REPLACE PROCEDURE SNOWPARSER_DBT_SEMANTIC_YAML(
    manifest_file varchar,
    semantic_view_name varchar DEFAULT 'MY_SEMANTIC_VIEW',
    semantic_view_description varchar DEFAULT 'MY_SEMANTIC_VIEW_DESCRIPTION',
    semantic_models array DEFAULT []
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
IMPORTS = ('@DROPBOX/dbt/semantic_manifest_parser.py',
'@DROPBOX/dbt/semantics_schema.py')
PACKAGES = (
'requests',
'snowflake-snowpark-python==1.28.0')
HANDLER = 'get_yaml'
COMMENT = $COMMENT
EXECUTE AS CALLER
AS $$
from semantic_manifest_parser import Manifest

def get_yaml(session, manifest_file, semantic_view_name, semantic_view_description, semantic_models):
    manifest = Manifest.manifest_from_json_file(session.connection, manifest_file, selected_models=semantic_models)

    return manifest.generate_yaml(semantic_view_name, semantic_view_description)

  $$;

-- Create the stored procedure to get the objects from the manifest
-- This stored procedure should be used if the manifest.json does not contain semantic models.
CREATE OR REPLACE PROCEDURE SNOWPARSER_DBT_GET_OBJECTS(
    manifest_file varchar,
    dbt_models array DEFAULT [],
    parse_snowflake_columns boolean DEFAULT TRUE
)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
IMPORTS = ('@DROPBOX/dbt/semantic_manifest_parser.py',
'@DROPBOX/dbt/semantics_schema.py')
PACKAGES = (
'requests',
'snowflake-snowpark-python==1.28.0')
HANDLER = 'get_objects'
COMMENT = $COMMENT
EXECUTE AS CALLER
AS $$
from semantic_manifest_parser import Manifest

def get_objects(session, manifest_file, dbt_models, parse_snowflake_columns):
    manifest = Manifest.manifest_from_json_file(session.connection, manifest_file,selected_models=dbt_models)

    return manifest.convert(parse_snowflake_columns=parse_snowflake_columns)

  $$;

-- Example executions:
-- CALL SNOWPARSER_DBT_SEMANTIC_YAML(
--     manifest_file => '@DROPBOX/samples/manifest.json',
--     semantic_view_name => 'test_semantic_view',
--     semantic_view_description => 'test_semantic_view_description'
-- );

-- CALL SNOWPARSER_DBT_GET_OBJECTS(
--     manifest_file => '@DROPBOX/samples/manifest_wo_metricflow.json',
--     dbt_models =>  TO_ARRAY(['customers','order_items', 'orders', 'stg_locations', 'stg_products']),
--     parse_snowflake_columns => TRUE
-- );
