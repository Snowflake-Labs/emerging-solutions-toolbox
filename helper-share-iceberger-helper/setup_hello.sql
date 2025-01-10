CREATE APPLICATION ROLE IF NOT EXISTS PUBLIC_DB_ROLE;
CREATE OR ALTER VERSIONED SCHEMA SRC;
GRANT USAGE ON SCHEMA SRC TO APPLICATION ROLE PUBLIC_DB_ROLE;

CREATE SCHEMA IF NOT EXISTS TASKS;
GRANT USAGE ON SCHEMA TASKS TO APPLICATION ROLE PUBLIC_DB_ROLE;

CREATE SECURE VIEW IF NOT EXISTS SRC.DATA_TBL WITH CHANGE_TRACKING = TRUE AS SELECT * FROM TABLES.DATA_TBL;

GRANT SELECT ON VIEW SRC.DATA_TBL TO APPLICATION ROLE PUBLIC_DB_ROLE;


create or replace procedure SRC.update_reference(ref_name string, operation string, ref_or_alias string)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$ADD_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
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
  FROM '/code_artifacts'
  MAIN_FILE = '/streamlit.py'
  COMMENT = '{"origin": "sf_sit","name": "sit_share_iceberger_helper","version": "{major: 1, minor: 0}"}';

GRANT USAGE ON STREAMLIT src.streamlit TO APPLICATION ROLE PUBLIC_DB_ROLE;
