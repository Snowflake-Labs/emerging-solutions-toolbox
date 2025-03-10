--REPLACE MACROS IN LINES 3 and 4

SET C2I_ROLE = '<MY_ROLE>'; --owner of the streamlit app

SET SCM_DB = 'SIS_CMD_MGR_<MY_ROLE>';
SET SCM_WH = 'SIS_CMD_MGR_<MY_ROLE>_WH';
SET TASK_POLLING_INTERVAL_SECS = 1; --the number of seconds a task should wait to check for incoming commands to execute

USE ROLE IDENTIFIER($C2I_ROLE);
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($SCM_WH) WITH WAREHOUSE_SIZE = 'XSMALL';

--create SIS_CMD_MGR DB
CREATE OR REPLACE DATABASE IDENTIFIER($SCM_DB);

USE DATABASE IDENTIFIER($SCM_DB);

CREATE OR REPLACE SCHEMA CMD_MGR;
CREATE OR REPLACE SCHEMA LOGGING;
CREATE OR REPLACE SCHEMA RESULTS;
CREATE OR REPLACE SCHEMA UTIL;


USE SCHEMA LOGGING;

--CREATE LOGGING TABLE
CREATE OR REPLACE TABLE LOGS(timestamp TIMESTAMP_NTZ(9), status VARCHAR, msg VARIANT);


USE SCHEMA CMD_MGR;

--CREATE TABLE TO LOAD CMDS THAT CANNOT BE EXECUTED BY SiS
CREATE OR REPLACE TABLE COMMANDS(run_id NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1, app_name VARCHAR, cmd VARCHAR, cmd_hash VARCHAR, status VARCHAR, start_timestamp TIMESTAMP_NTZ(9), completed_timestamp TIMESTAMP_NTZ(9), generate_results_table BOOLEAN, results_table VARCHAR, notes VARCHAR)
COMMENT = '{"origin":"sf_sit","name":"sis_cmd_mgr","version":{"major":1, "minor":0},"attributes":{"env":"sis_cmd_mgr","component":"commands","type":"table"}}';

--CREATE STREAM ON COMMANDS TABLE
CREATE OR REPLACE STREAM COMMANDS_STREAM
    ON TABLE COMMANDS
    APPEND_ONLY = TRUE
    COMMENT = '{"origin":"sf_sit","name":"sis_cmd_mgr","version":{"major":1, "minor":0},"attributes":{"env":"sis_cmd_mgr","component":"commands_stream","type":"stream"}}';

--CREATE PROCEDURE TO EXECUTE COMMANDS
CREATE OR REPLACE PROCEDURE EXECUTE_CMD()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER='execute_cmd'
EXECUTE AS CALLER
AS
$$
import pandas as pd
from snowflake.snowpark.functions import col

def execute_cmd(session):
    try:
        #initialize
        run_id = None
        app_name = None
        cmd = None
        cmd_hash = None
        generate_results_table = None
        results_table = None
        cmd_log = None
            
        #create temp table from stream
        session.sql("CREATE OR REPLACE TEMPORARY TABLE CMD_MGR.COMMANDS_TEMP(run_id NUMBER(38,0), app_name VARCHAR, cmd VARCHAR, cmd_hash VARCHAR, generate_results_table BOOLEAN)").collect()
        
        #advance the stream by inserting into temp table from stream
        session.sql("INSERT INTO CMD_MGR.COMMANDS_TEMP SELECT run_id, app_name, cmd, cmd_hash, generate_results_table FROM CMD_MGR.COMMANDS_STREAM WHERE METADATA$ACTION = 'INSERT' ORDER BY start_timestamp ASC").collect()

        #select commands from temp table
        df_cmds = pd.DataFrame(session.sql(f"""SELECT run_id, app_name, cmd, cmd_hash, generate_results_table FROM CMD_MGR.COMMANDS_TEMP""").collect())

        for row in df_cmds.itertuples():
            run_id = row.RUN_ID
            app_name = row.APP_NAME
            cmd = row.CMD
            cmd_hash = row.CMD_HASH
            generate_results_table = row.GENERATE_RESULTS_TABLE
            results_table = ""
            cmd_log = cmd.replace('"', '')

            #execute cmd
            try:
                session.sql(f"""{cmd}""").collect()

                #materialize results if generate_results_table is True
                if generate_results_table:
                    df_results = pd.DataFrame(session.sql(f"""SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))""").collect())

                    session.write_pandas(
                        df_results
                        ,f"CMD_RESULTS_{cmd_hash}"
                        ,database="SIS_CMD_MGR_<MY_ROLE>"
                        ,schema="RESULTS"
                        ,auto_create_table=True
                        ,table_type="transient"
                        ,overwrite=True 
                    )

                    results_table = f"CMD_RESULTS_{cmd_hash}"

                #update COMMANDS table, set status to COMPLETE
                session.sql(f"""UPDATE CMD_MGR.COMMANDS SET 
                                    status = 'COMPLETE'
                                    ,completed_timestamp = SYSDATE()
                                    ,results_table = '{results_table}'
                                    ,notes = ''
                                WHERE 
                                    run_id = '{run_id}'
                                    AND app_name = '{app_name}'
                                    AND cmd_hash = '{cmd_hash}'""").collect()

                #add cmd to LOGS table
                session.sql(f"""INSERT INTO LOGGING.LOGS SELECT 
                                    SYSDATE()
                                    ,'SUCCESS'
                                    ,PARSE_JSON('{{"run_id":"{run_id}", "app_name":"{app_name}", "message":"Command executed successfully"}}')""").collect()

                return "Command(s) executed."
            except Exception as e:
                session.sql("rollback").collect()

                #remove unwanted characters from error msg
                error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

                msg_return = f"Failed: ${error_eraw}"

                #update COMMANDS table with ERROR
                session.sql(f"""UPDATE CMD_MGR.COMMANDS SET 
                                    status = 'ERROR'
                                    ,completed_timestamp = SYSDATE()
                                    ,notes = '{msg_return}'
                                WHERE 
                                    run_id = '{run_id}'
                                    AND app_name = '{app_name}'
                                    AND cmd_hash = '{cmd_hash}'""").collect()

                raise Exception(msg_return)
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = f"Failed: ${error_eraw}"

        #add ERROR to LOGS table
        session.sql(f"""INSERT INTO LOGGING.LOGS SELECT 
                            SYSDATE()
                            ,'ERROR'
                            ,PARSE_JSON('{{"run_id":"{run_id}", "app_name":"{app_name}", "message":"{msg_return}"}}')""").collect()

        raise Exception(msg_return)
$$
;

--CREATE PROCEDURE TO CREATE TASKS BASED ON THE POLLING INTERVAL SPECIFIED
CREATE OR REPLACE PROCEDURE GENERATE_TASKS(polling_interval_secs NUMBER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER='generate_tasks'
EXECUTE AS CALLER
AS
$$
import pandas as pd
import time
import math
from snowflake.snowpark.functions import col

def generate_tasks(session, polling_interval_secs):
    try:
        #set number of tasks based on polling_interval_secs
        task_num = math.floor(60/polling_interval_secs)

        #create tasks based on task_num
        for t in range(task_num):
            session.sql(f"""CREATE OR REPLACE TASK CMD_MGR.PROCESS_COMMANDS_{t+1} 
                            WAREHOUSE = 'SIS_CMD_MGR_<MY_ROLE>_WH'
                            SCHEDULE = '1 minute'
                            COMMENT = '{{"origin":"sf_sit","name":"sis_cmd_mgr","version":{{"major":1, "minor":0}},"attributes":{{"env":"sis_cmd_mgr","component":"process_commands_{t+1}","type":"task"}}}}'
                            WHEN 
                                SYSTEM$STREAM_HAS_DATA('CMD_MGR.COMMANDS_STREAM') 
                            AS 
                                CALL CMD_MGR.EXECUTE_CMD()
                        """).collect()

        #start each task
        for t in range(task_num):
            if t+1 > 1:
                time.sleep(polling_interval_secs)
            session.sql(f"""ALTER TASK CMD_MGR.PROCESS_COMMANDS_{t+1} RESUME""").collect()

        return 'Success'
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = f"Failed: ${error_eraw}"

        raise Exception(msg_return)
$$
;

--CREATE AND START TASK(S)
CALL GENERATE_TASKS($TASK_POLLING_INTERVAL_SECS);


USE SCHEMA UTIL;

--CREATE FUNCTION TO GENERATE A UUID THAT CAN BE USED IN A TABLE NAME, IF CMD RESULTS ARE TO BE MATERIALIZED
CREATE OR REPLACE FUNCTION TABLE_UUID(cmd VARCHAR)
  RETURNS NUMBER
  COMMENT = '{"origin":"sf_sit","name":"sis_cmd_mgr","version":{"major":1, "minor":0},"attributes":{"env":"sis_cmd_mgr","component":"table_uuid","type":"function"}}'
  AS
  $$
    HASH(cmd)
  $$
  ;

--UNSET VARS
UNSET(C2I_ROLE, SCM_DB, SCM_WH, TASK_POLLING_INTERVAL_SECS);

SELECT 'Done' AS STATUS;