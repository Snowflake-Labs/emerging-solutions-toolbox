CREATE APPLICATION ROLE IF NOT EXISTS PUBLIC_DB_ROLE;
CREATE OR ALTER VERSIONED SCHEMA SRC;
CREATE  SCHEMA IF NOT EXISTS DATA;
GRANT ALL ON SCHEMA DATA TO APPLICATION ROLE PUBLIC_DB_ROLE;
GRANT USAGE ON SCHEMA SRC TO APPLICATION ROLE PUBLIC_DB_ROLE;


CREATE OR REPLACE procedure SRC.GET_CONFIGURATION_FOR_REFERENCE(ref_name STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS
  $$
  BEGIN
    CASE (ref_name)
      WHEN 'MY_EXTERNAL_ACCESS' THEN
        RETURN '{
          "type": "CONFIGURATION",
          "payload":{
            "host_ports":["api.myptv.com"],
            "allowed_secrets" : "ALL",
            "secret_references":["CONSUMER_SECRET"]
            }}';
      WHEN 'CONSUMER_SECRET' THEN
        RETURN '{
          "type": "CONFIGURATION",
          "payload":{
            "type" : "GENERIC_STRING",
            "security_integration" : {}
            }}';
END CASE;
RETURN '';
END;
$$;
GRANT USAGE ON PROCEDURE SRC.GET_CONFIGURATION_FOR_REFERENCE(string) TO APPLICATION ROLE PUBLIC_DB_ROLE;

CREATE OR REPLACE PROCEDURE src.init()
  RETURNS STRING
  LANGUAGE SQL
  AS
  $$
  BEGIN
  CREATE OR REPLACE FUNCTION src.get_match_id(payload OBJECT)
  RETURNS STRING
  LANGUAGE PYTHON
  EXTERNAL_ACCESS_INTEGRATIONS = ( reference('MY_EXTERNAL_ACCESS'))
  SECRETS = ('my_cred' = reference('CONSUMER_SECRET'))
  RUNTIME_VERSION = 3.11
  PACKAGES = ('pandas', 'requests')
  IMPORTS = ('/get_match_id.py')
  HANDLER = 'get_match_id.get_match_id';

  RETURN 'SUCCESS';
  END;
  $$;

GRANT USAGE ON PROCEDURE src.init() TO APPLICATION ROLE PUBLIC_DB_ROLE;


CREATE OR REPLACE PROCEDURE src.enrich_data(table_dict OBJECT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python','pandas','streamlit')
IMPORTS = ('/enrich_data.py')
HANDLER = 'enrich_data.enrich_data';

GRANT USAGE ON PROCEDURE SRC.enrich_data(OBJECT) TO APPLICATION ROLE PUBLIC_DB_ROLE;

CREATE OR REPLACE PROCEDURE src.enrich_data_wrapper(task_id NUMBER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python','pandas')
IMPORTS = ('/enrich_data_wrapper.py')
HANDLER = 'enrich_data_wrapper.enrich_data_wrapper';

GRANT USAGE ON PROCEDURE SRC.enrich_data_wrapper(NUMBER) TO APPLICATION ROLE PUBLIC_DB_ROLE;

CREATE SEQUENCE IF NOT EXISTS DATA.TASK_ID_SEQUENCE;

GRANT ALL ON SEQUENCE DATA.TASK_ID_SEQUENCE TO APPLICATION ROLE PUBLIC_DB_ROLE;

CREATE TABLE IF NOT EXISTS DATA.SCHEDULED_TASKS(
TASK_ID int,
JOB_SPECS VARIANT,
CREATED_DATETIME timestamp
);

GRANT SELECT ON DATA.SCHEDULED_TASKS TO APPLICATION ROLE PUBLIC_DB_ROLE;

CREATE VIEW IF NOT EXISTS DATA.METADATA AS SELECT * FROM SHARE.METADATA;


create or replace procedure SRC.update_reference(ref_name string, operation string, ref_or_alias string)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$ADD_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;
      RETURN NULL;
    END;
  $$;

grant usage on procedure SRC.update_reference(string,string,string) to application role PUBLIC_DB_ROLE;


CREATE OR REPLACE STREAMLIT src.streamlit
  FROM '/code_artifacts/streamlit'
  MAIN_FILE = '/streamlit.py'
  COMMENT = '{"origin": "sf_sit","name": "sit_api_enrichment_framework","version": "{major: 1, minor: 0}"}';

GRANT USAGE ON STREAMLIT src.streamlit TO APPLICATION ROLE PUBLIC_DB_ROLE;
