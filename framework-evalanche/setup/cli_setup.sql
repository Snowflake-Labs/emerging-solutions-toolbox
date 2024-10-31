SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE DATABASE IF NOT EXISTS GENAI_UTILITIES
COMMENT = '{"origin": "sf_sit",
            "name": "evalanche",
            "version": {"major": 1, "minor": 0}}';

CREATE SCHEMA IF NOT EXISTS GENAI_UTILITIES.EVALUATION
COMMENT = '{"origin": "sf_sit",
            "name": "evalanche",
            "version": {"major": 1, "minor": 0}}';

CREATE OR ALTER TABLE GENAI_UTILITIES.EVALUATION.SAVED_EVALUATIONS
(EVAL_NAME VARCHAR,
DESCRIPTION VARCHAR,
METRIC_NAMES ARRAY,
SOURCE_SQL VARCHAR,
PARAM_ASSIGNMENTS VARIANT,
ASSOCIATED_OBJECTS VARIANT,
MODELS VARIANT)
COMMENT = '{"origin": "sf_sit",
            "name": "evalanche",
            "version": {"major": 1, "minor": 0}}';

CREATE OR ALTER TABLE GENAI_UTILITIES.EVALUATION.AUTO_EVALUATIONS
(EVAL_NAME VARCHAR,
DESCRIPTION VARCHAR,
METRIC_NAMES ARRAY,
SOURCE_SQL VARCHAR,
PARAM_ASSIGNMENTS VARIANT,
ASSOCIATED_OBJECTS VARIANT,
MODELS VARIANT)
COMMENT = '{"origin": "sf_sit",
            "name": "evalanche",
            "version": {"major": 1, "minor": 0}}';

-- Create stage for App logic
CREATE OR REPLACE STAGE GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE
DIRECTORY = (ENABLE = true)
COMMENT = '{"origin": "sf_sit",
            "name": "evalanche",
            "version": {"major": 1, "minor": 0}}';

PUT file://src.zip @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://src/*.py @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/src/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://home.py @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://environment.yml @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://pages/*.py @GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE/pages/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

-- Create Streamlit
CREATE OR REPLACE STREAMLIT GENAI_UTILITIES.EVALUATION.EVALUATION_APP
ROOT_LOCATION = '@GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE'
MAIN_FILE = 'home.py'
TITLE = "Evalanche: GenAI Evaluation Application"
QUERY_WAREHOUSE = $streamlit_warehouse
COMMENT = '{"origin": "sf_sit",
            "name": "evalanche",
            "version": {"major": 1, "minor": 0}}';
