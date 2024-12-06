SET major = 2;
SET minor = 2;
SET COMMENT = concat('{"origin": "sf_sit",
            "name": "evalanche",
            "version": {"major": ',$major,', "minor": ',$minor,'}}');

SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE DATABASE IF NOT EXISTS GENAI_UTILITIES
COMMENT = $COMMENT;

CREATE SCHEMA IF NOT EXISTS GENAI_UTILITIES.EVALUATION
COMMENT = $COMMENT;

CREATE OR ALTER TABLE GENAI_UTILITIES.EVALUATION.SAVED_EVALUATIONS
(EVAL_NAME VARCHAR,
DESCRIPTION VARCHAR,
METRIC_NAMES ARRAY,
SOURCE_SQL VARCHAR,
PARAM_ASSIGNMENTS VARIANT,
ASSOCIATED_OBJECTS VARIANT,
MODELS VARIANT)
COMMENT = $COMMENT;

CREATE OR ALTER TABLE GENAI_UTILITIES.EVALUATION.AUTO_EVALUATIONS
(EVAL_NAME VARCHAR,
DESCRIPTION VARCHAR,
METRIC_NAMES ARRAY,
SOURCE_SQL VARCHAR,
PARAM_ASSIGNMENTS VARIANT,
ASSOCIATED_OBJECTS VARIANT,
MODELS VARIANT)
COMMENT = $COMMENT;

CREATE OR ALTER TABLE GENAI_UTILITIES.EVALUATION.CUSTOM_METRICS
(METRIC_NAME VARCHAR,
STAGE_FILE_PATH VARCHAR,
CREATED_DATETIME TIMESTAMP,
SHOW_METRIC BOOLEAN,
CREATION_USER VARCHAR)
COMMENT = $COMMENT;

-- Create stage for App logic
CREATE STAGE IF NOT EXISTS GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE
DIRECTORY = (ENABLE = true)
COMMENT = $COMMENT;

PUT file://src.zip @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://src/*.py @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/src/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://home.py @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://environment.yml @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://pages/*.py @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/pages/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

-- Helper SPROC to remove custom_metrics
CREATE OR REPLACE PROCEDURE GENAI_UTILITIES.EVALUATION.DELETE_METRIC(metric_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = $COMMENT
EXECUTE AS CALLER
AS
$$
def run(session, metric_name):
    STAGE = "@GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE"
    TABLE = "GENAI_UTILITIES.EVALUATION.CUSTOM_METRICS"
    file_path = f"{STAGE}/{metric_name}.pkl"
    query = f"rm {file_path}"
    
    try:
        session.sql(query).collect()
        metrics_tbl = session.table(TABLE)
        metrics_tbl.delete(metrics_tbl["METRIC_NAME"] == metric_name)
        return f"{file_path} removed."
    except Exception as e:
        return f"An error occurred: {e}"
$$;

-- Cortex Analyst runner
CREATE OR REPLACE PROCEDURE GENAI_UTILITIES.EVALUATION.CORTEX_ANALYST_SQL(prompt STRING, semantic_file_path STRING)
RETURNS STRING
LANGUAGE PYTHON
PACKAGES = ('snowflake-snowpark-python')
RUNTIME_VERSION = '3.9'
HANDLER = 'process_message'
as
$$
import _snowflake
import json
def send_message(messages, semantic_file_path):
    """Calls the REST API and returns the response."""
    
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{semantic_file_path}",
    }
    resp = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/cortex/analyst/message",
            {},
            {},
            request_body,
            {},
            30000,
        )
    if resp["status"] < 400:
        response_content = json.loads(resp["content"])
        return response_content
    else:
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )

def process_message(session, prompt, semantic_file_path):
    """Processes a message and adds the response to the chat."""
    messages = []
    messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    response = send_message(messages, semantic_file_path)
    for item in response["message"]["content"]:
        if item["type"] == "sql":
            return item.get("statement", None)
    else:
        return None
$$;

-- Create Streamlit
CREATE OR REPLACE STREAMLIT GENAI_UTILITIES.EVALUATION.EVALUATION_APP
ROOT_LOCATION = '@GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE'
MAIN_FILE = 'home.py'
TITLE = "Evalanche: GenAI Evaluation Application"
QUERY_WAREHOUSE = $streamlit_warehouse
COMMENT = $COMMENT;
