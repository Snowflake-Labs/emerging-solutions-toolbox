SET major = 1;
SET minor = 1;
SET COMMENT = concat('{"origin": "sf_sit",
            "name": "prompt_template_runner",
            "version": {"major": ',$major,', "minor": ',$minor,'}}');

CREATE DATABASE IF NOT EXISTS GENAI_UTILITIES
COMMENT = $COMMENT;

CREATE SCHEMA IF NOT EXISTS GENAI_UTILITIES.UTILITIES
COMMENT = $COMMENT;

-- Create stage for logic
CREATE STAGE IF NOT EXISTS GENAI_UTILITIES.UTILITIES.DROPBOX
DIRECTORY = (ENABLE = true)
COMMENT = $COMMENT;

-- Create API Integration for Git
CREATE OR REPLACE API INTEGRATION git_api_integration_snowflake_labs_emerging_solutions_toolbox
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs')
  ENABLED = TRUE;

-- Create Git Repository
CREATE OR REPLACE GIT REPOSITORY GENAI_UTILITIES.UTILITIES.EMERGING_SOLUTION_TOOLBOX
  API_INTEGRATION = git_api_integration_snowflake_labs_emerging_solutions_toolbox
  ORIGIN = 'https://github.com/Snowflake-Labs/emerging-solutions-toolbox.git';

ALTER GIT REPOSITORY GENAI_UTILITIES.UTILITIES.EMERGING_SOLUTION_TOOLBOX FETCH;

COPY FILES
  INTO @GENAI_UTILITIES.UTILITIES.DROPBOX
  FROM @GENAI_UTILITIES.UTILITIES.EMERGING_SOLUTION_TOOLBOX/branches/main/helper-prompt-template-runner/
  FILES=('prompt_parser.py');

CREATE OR REPLACE FUNCTION GENAI_UTILITIES.UTILITIES.PROMPT_TEMPLATE_PARSER(
  row_data OBJECT,
  prompt_template_file VARCHAR DEFAULT NULL,
  prompt_config VARIANT DEFAULT to_variant('{}'),
  include_metadata BOOLEAN DEFAULT FALSE
)
  RETURNS TABLE (PROMPT VARIANT)
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.9
  HANDLER = 'prompt_parser.PromptParser'
  IMPORTS = ('@GENAI_UTILITIES.UTILITIES.DROPBOX/prompt_parser.py')
  PACKAGES = ('pyyaml', 'snowflake-snowpark-python==1.24.0')
  COMMENT = $COMMENT;


CREATE OR REPLACE PROCEDURE GENAI_UTILITIES.UTILITIES.PROMPT_TEMPLATE_RUNNER(
    prompt_template_file varchar,
    origin_table varchar DEFAULT NULL,
    model varchar DEFAULT NULL,
    model_options variant DEFAULT to_variant('{}'),
    response_column varchar DEFAULT 'RESPONSE',
    output_table_name varchar DEFAULT 'TEMP',
    table_type varchar DEFAULT 'temporary',
    table_write_mode varchar DEFAULT 'overwrite',
    prompt_column varchar DEFAULT 'PROMPT'
)
  RETURNS TABLE()
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.10
  IMPORTS = ('@GENAI_UTILITIES.UTILITIES.DROPBOX/prompt_parser.py')
  HANDLER = 'prompt_parser.run_prompt_template'
  PACKAGES = ('snowflake-snowpark-python==1.24.0',
            'snowflake-ml-python==1.7.2')
  COMMENT = $COMMENT
  EXECUTE AS CALLER;

