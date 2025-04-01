CREATE DATABASE DATA_QUALITY;
CREATE SCHEMA DATA_QUALITY.CONFIG;
CREATE SCHEMA DATA_QUALITY.RESULTS;
CREATE SCHEMA DATA_QUALITY.TEMPORARY_DQ_OBJECTS;

create or replace TABLE DATA_QUALITY.CONFIG.DQ_CHECK_TYPES (
	CHECK_TYPE_ID NUMBER(38,0),
	CHECK_DESCRIPTION VARCHAR(200),
	CHECK_CATEGORY VARCHAR(200),
	COMPARES VARCHAR(200)
);

CREATE SEQUENCE DATA_QUALITY.CONFIG.DQ_JOB_ID_SEQUENCE;
create or replace TABLE DATA_QUALITY.CONFIG.DQ_JOBS (
	JOB_ID NUMBER(38,0) DEFAULT DATA_QUALITY.CONFIG.DQ_JOB_ID_SEQUENCE.NEXTVAL,
	JOB_NAME VARCHAR(500),
	CREATE_DTTM TIMESTAMP_NTZ(9),
	CREATE_BY VARCHAR(16777216),
	LAST_RUN TIMESTAMP_NTZ(9),
	SCHEDULE VARCHAR(50),
	JOB_SPECS VARIANT,
	LAST_UPDATED TIMESTAMP_NTZ(9),
	IS_ACTIVE BOOLEAN,
	SPROC_NAME VARCHAR(16777216),
	CHECK_CATEGORY VARCHAR(200),
	LABEL VARCHAR(16777216)
);

create or replace TABLE DATA_QUALITY.RESULTS.DQ_SNOWFLAKE_DMF_RESULTS (
	JOB_ID NUMBER(38,0),
	RUN_DATETIME TIMESTAMP_NTZ(9),
	RESULTS VARIANT,
	ALERT_FLAG BOOLEAN,
	ALERT_STATUS VARCHAR(16777216),
	COMMENTS VARCHAR(16777216)
);

create or replace TABLE DATA_QUALITY.RESULTS.DQ_ANOMALY_DETECT_RESULTS (
	JOB_ID NUMBER(38,0),
	RUN_DATETIME TIMESTAMP_NTZ(9),
	CHECK_TYPE_ID NUMBER(38,0),
	PARTITION_VALUES ARRAY,
	CHECK_TBL_NM VARCHAR(16777216),
	RECORD_IDS VARIANT,
	ANOMALY_SCORE FLOAT,
	ALERT_FLAG NUMBER(38,0),
	ALERT_STATUS VARCHAR(200),
	COMMENTS VARCHAR(16777216)
);

create or replace TABLE DATA_QUALITY.RESULTS.DQ_NON_STAT_CHECK_RESULTS (
	JOB_ID NUMBER(38,0),
	RUN_DATETIME TIMESTAMP_NTZ(9),
	CHECK_TYPE_ID NUMBER(38,0),
	PARTITION_VALUES ARRAY,
	CONTROL_TBL_NM VARCHAR(16777216),
	COMPARE_TBL_NM VARCHAR(16777216),
	RESULTS VARIANT,
	ALERT_FLAG NUMBER(38,0),
	ALERT_STATUS VARCHAR(200),
	COMMENTS VARCHAR(16777216)
);

create or replace TABLE DATA_QUALITY.RESULTS.DQ_SNOWFLAKE_DMF_RESULTS (
	JOB_ID NUMBER(38,0),
	RUN_DATETIME TIMESTAMP_NTZ(9),
	RESULTS VARIANT,
	ALERT_FLAG BOOLEAN,
	ALERT_STATUS VARCHAR(16777216),
	COMMENTS VARCHAR(16777216)
);

CREATE OR REPLACE TABLE DATA_QUALITY.CONFIG.control_report_run
(
 control_report_run_id     number NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER,
 start_timestamp           timestamp_ltz NOT NULL,
 end_timestamp             timestamp_ltz NULL
) CHANGE_TRACKING = TRUE;

-- ************************************** control_report
CREATE OR REPLACE TABLE DATA_QUALITY.CONFIG.control_report
(
 control_report_id      number NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER,
 object_var             variant,
 active_flg             boolean NOT NULL
) CHANGE_TRACKING = TRUE;

-- ************************************** analysis
CREATE OR REPLACE TABLE DATA_QUALITY.CONFIG.analysis
(
 analysis_id            number NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER,
 analysis_type_name     string NOT NULL,
 active_flg             boolean NOT NULL
) CHANGE_TRACKING = TRUE;

-- ************************************** control_report_result
CREATE OR REPLACE TABLE DATA_QUALITY.RESULTS.control_report_result
(
 control_report_result_id   number NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER,
 control_report_id          number NOT NULL,
 control_report_run_id      number NOT NULL,
 column_value               variant NOT NULL,
 column_cnt                 number NOT NULL
) CHANGE_TRACKING = TRUE;

-- ************************************** control_report_analysis
CREATE OR REPLACE TABLE DATA_QUALITY.CONFIG.control_report_analysis
(
 control_report_analysis_id     number NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER,
 analysis_id                    number NOT NULL,
 control_report_id              number NOT NULL,
 threshold                      number(38,6) NOT NULL,
 active_flg                     boolean NOT NULL
) CHANGE_TRACKING = TRUE;

create or replace view DATA_QUALITY.CONFIG.control_report_vw as
select cr.control_report_id,
       cr.object_var:database_name::string as database_name,
       cr.object_var:schema_name::string as schema_name,
       cr.object_var:table_name::string as table_name,
       cr.object_var:c_name::string as column_name,
       cr.object_var:c_datatype::string as datatype,
       cr.active_flg as active_flg
from DATA_QUALITY.CONFIG.control_report cr;

create or replace view DATA_QUALITY.CONFIG.sql_control_report_vw as
select database_name,
       schema_name,
       table_name,
       listagg(column_name,',') within group (order by control_report_id) as columns,
       listagg(control_report_id,',') within group (order by control_report_id) as control_report_id
from DATA_QUALITY.CONFIG.control_report_vw cr
where active_flg = True
group by 1,2,3;

CREATE OR REPLACE PROCEDURE DATA_QUALITY.CONFIG.DMF_WRAPPER(id int)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
HANDLER = 'dmf_wrapper'
PACKAGES = ('snowflake-snowpark-python')
AS $$
import json
import pandas as pd
def dmf_wrapper(session, id):
    sql_string = f"SELECT PARSE_JSON(JOB_SPECS) FROM DATA_QUALITY.CONFIG.DQ_JOBS WHERE JOB_ID = {id}"

    spec = session.sql(sql_string).collect()
    spec =  json.loads(spec[0][0])
    result_json={}
    a_flag = 0
    table = spec["TABLE"]
    if spec["COUNT_CHECK"]:
        row_count = session.sql(f"SELECT COUNT(*) as COUNT FROM {table}").to_pandas()
        result_json["row_count"] = row_count["COUNT"].values[0]
    for column_spec in spec["COLUMNS"]:
        column = str(column_spec["COLUMN"])
        result_json[column] = {}
        for index,check in enumerate(column_spec["CHECKS"]):
            check_query = f'SELECT SNOWFLAKE.CORE.{check}(SELECT {column} from {table}) as RESULT'
            result = session.sql(check_query).to_pandas()
            result_json[column][check]=result["RESULT"].values[0]
            if result_json[column][check] >= int(column_spec["THRESHOLDS"][index]):
            	a_flag = 1
    result_json = str(result_json).replace("'",'"')
    session.sql(f"""insert into DATA_QUALITY.RESULTS.DQ_SNOWFLAKE_DMF_RESULTS(JOB_ID,RUN_DATETIME,RESULTS,ALERT_FLAG,ALERT_STATUS,COMMENTS) 
    SELECT {id},CURRENT_TIMESTAMP(),PARSE_JSON('{result_json}'),{a_flag},'pending review',''""").collect()
    return result_json
$$;

CREATE OR REPLACE PROCEDURE DATA_QUALITY.CONFIG.METADATA_QUALITY(DATABASE_NAME VARCHAR(16777216), SCHEMA_NAME VARCHAR(16777216), TABLE_NAME VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

  let crrs RESULTSET := (EXECUTE IMMEDIATE ''insert into control_report_run (start_timestamp,end_timestamp) select current_timestamp(),null;'');
  
  let qrs RESULTSET := (EXECUTE IMMEDIATE ''select last_query_id() as qid;'');
  let c_qrs CURSOR FOR qrs;

  let query_id STRING := '''';
  FOR record IN c_qrs DO
    query_id := record.qid;
  END FOR;

  let crr_id_rs RESULTSET := (EXECUTE IMMEDIATE ''
                        select control_report_run_id 
                        from control_report_run 
                        CHANGES(INFORMATION => APPEND_ONLY) 
                        BEFORE(statement => '''''' || query_id || '''''') 
                        END(statement => '''''' || query_id || '''''');
                        '');
  let c_crr_id_rs CURSOR FOR crr_id_rs;

  let control_report_run_id NUMBER := null;
  FOR record IN c_crr_id_rs DO
    control_report_run_id := record.control_report_run_id;
  END FOR;

  let query VARCHAR DEFAULT ''SELECT * FROM DATA_QUALITY.CONFIG.control_report_vw WHERE database_name = ? AND schema_name = ? AND table_name = ? AND active_flg = True'';
  
  let rs RESULTSET := (EXECUTE IMMEDIATE :query USING (database_name, schema_name, table_name));
  let c1 CURSOR FOR rs;

  let insert_query := ''insert into DATA_QUALITY.RESULTS.control_report_result (control_report_id, control_report_run_id, column_value, column_cnt)
                      '';

  let cte_query := ''with x as (
                         select count(*) as value_cnt,'';
  
  let column_group STRING := '''';
  let grouping_group STRING := '''';
  let select_group STRING := '''';

  FOR record IN c1 DO
    column_group := column_group || record.column_name || '', '';
    grouping_group := grouping_group || ''grouping('' || record.column_name || '') as g'' || record.control_report_id || '', '';
    select_group := select_group || ''select '' || record.control_report_id || '', '' || control_report_run_id || '', '' || ''parse_json(''''{'' || record.column_name || '':''''||''''\"''''||''|| record.column_name ||''||''''\"''''||''''}'''')::variant, value_cnt from x where g'' || record.control_report_id || ''=0
    UNION ALL '';
  END FOR;

  let final_sql STRING := insert_query || cte_query || column_group || rtrim(grouping_group,'', '') || ''
                             from '' || database_name || ''.'' || schema_name || ''.'' || table_name || ''
                             group by grouping sets('' || rtrim(column_group,'', '') || '')) '' || rtrim(select_group,''UNION ALL '');
  let finals RESULTSET := (EXECUTE IMMEDIATE :final_sql);

  let crre RESULTSET := (EXECUTE IMMEDIATE ''update control_report_run set end_timestamp = current_timestamp() where control_report_run_id = '' || control_report_run_id);
  
RETURN final_sql;

END;
';

CREATE OR REPLACE STAGE DATA_QUALITY.CONFIG.CODE;
-- Load all files except install to this stage at this point










CREATE OR REPLACE PROCEDURE DATA_QUALITY.CONFIG.DQ_ANOMALY_DETECTION_SPROC("ARG1" VARCHAR(16777216), "ARG2" OBJECT)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','snowflake-ml-python','pandas','scikit-learn','cloudpickle==2.2.1')
HANDLER = 'udf_py_2034220827.compute'
IMPORTS = ('@DATA_QUALITY.CONFIG.CODE/utility_functions.py','@DATA_QUALITY.CONFIG.CODE/DATA_QUALITYCONFIGdq_anomaly_detection_sproc_1183842436314279374/udf_py_2034220827.zip')
EXECUTE AS OWNER
;

CREATE OR REPLACE PROCEDURE DATA_QUALITY.CONFIG.DQ_NON_STAT_SPROC("ARG1" VARCHAR(16777216), "ARG2" OBJECT)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','cloudpickle==2.2.1')
HANDLER = 'udf_py_994340372.compute'
IMPORTS = ('@DATA_QUALITY.CONFIG.CODE/utility_functions_non_stat.py','@DATA_QUALITY.CONFIG.CODE/DATA_QUALITYCONFIGdq_non_stat_sproc_8775928960113803498/udf_py_994340372.zip')
EXECUTE AS OWNER
;

-- stage for registering temp UDFs
CREATE OR REPLACE STAGE DATA_QUALITY.TEMPORARY_DQ_OBJECTS.CODE;

CREATE OR REPLACE STREAMLIT  DATA_QUALITY.CONFIG.DATA_QUALITY_MANAGER
ROOT_LOCATION = '@DATA_QUALITY.CONFIG.CODE'
MAIN_FILE = '/streamlit_app.py'
QUERY_WAREHOUSE = DEX_WH
COMMENT = '{"origin": "sf_sit","name": "sit_data_quality_framework","version": "{major: 1, minor: 0}"}';
