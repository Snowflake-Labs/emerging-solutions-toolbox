/*************************************************************************************************************
Script:             Voice of the Customer for Snowflake App Setup
Create Date:        2024-07-29
Author:             Tyler White
Description:        Voice of the Customer for Snowflake
Copyright © 2025 Snowflake Inc. All rights reserved
**************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2025-04-28          Brandon Barker                           Initial Creation
*************************************************************************************************************/
/* create warehouse */
CREATE WAREHOUSE IF NOT EXISTS voc_wh
COMMENT = '{"origin":"sf_sit","name":"voice_of_the_customer","version":{"major":1, "minor":0}}';

/* create database */
CREATE OR REPLACE DATABASE voice_of_the_customer
COMMENT = '{"origin":"sf_sit","name":"voice_of_the_customer","version":{"major":1, "minor":0}}';

CREATE OR REPLACE SCHEMA voice_of_the_customer.previous_runs
COMMENT = '{"origin":"sf_sit","name":"voice_of_the_customer","version":{"major":1, "minor":0}}';

/* create schema */
CREATE OR REPLACE SCHEMA voice_of_the_customer.app
COMMENT = '{"origin":"sf_sit","name":"voice_of_the_customer","version":{"major":1, "minor":0}}';

/* create stage */
CREATE OR REPLACE STAGE voice_of_the_customer.app.stage_voice_of_the_customer
COMMENT = '{"origin":"sf_sit","name":"voice_of_the_customer","version":{"major":1, "minor":0}}';

/* create streamlit app */
CREATE OR REPLACE STREAMLIT voice_of_the_customer
ROOT_LOCATION = '@voice_of_the_customer.app.stage_voice_of_the_customer'
MAIN_FILE = '/app_main.py'
QUERY_WAREHOUSE = VOC_WH
COMMENT = '{"origin":"sf_sit","name":"voice_of_the_customer","version":{"major":1, "minor":0}}';

/* demo data */
CREATE OR REPLACE STAGE voice_of_the_customer.app.call_transcripts
URL = 's3://sfquickstarts/misc/call_transcripts/'
COMMENT = '{"origin":"sf_sit","name":"voice_of_the_customer","version":{"major":1, "minor":0},"attributes":{"component":"file_stage"}}';

CREATE OR REPLACE TRANSIENT TABLE voice_of_the_customer.app.call_transcripts_bronze (
    date_created date,
    language varchar,
    country varchar,
    product varchar,
    category varchar,
    damage_type varchar,
    transcript varchar
)
COMMENT = '{''origin'':''sf_sit'',''name'':''voice_of_the_customer'',''version'':{''major'':1, ''minor'':0},''attributes'':{''component'':''table''}}';

COPY INTO voice_of_the_customer.app.call_transcripts_bronze
FROM @voice_of_the_customer.app.call_transcripts
FILE_FORMAT =
(
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

CREATE OR REPLACE FUNCTION voice_of_the_customer.app.detect(arg1 STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
COMMENT = '{''origin'':''sf_sit'',''name'':''voice_of_the_customer'',''version'':{''major'':1, ''minor'':0},''attributes'':{''component'':''udf''}}'
PACKAGES=('langdetect','cloudpickle==3.0.0')
HANDLER='detect'
AS
$$
from langdetect import detect
$$;
