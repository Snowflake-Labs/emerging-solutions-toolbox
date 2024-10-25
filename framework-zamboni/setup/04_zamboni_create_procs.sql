USE ROLE ZAMBONI_ROLE;
USE WAREHOUSE XS_WH;
USE SCHEMA ZAMBONI_DB.ZAMBONI_UTIL;

--create MANAGE_LABEL
CREATE OR REPLACE PROCEDURE MANAGE_LABEL(label_name VARCHAR, description VARCHAR, attributes VARIANT)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'manage_label'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"manage_label"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def manage_label(session, label_name, description, attributes):
    try:
        #get max label_id
        label_id = pd.DataFrame(session.sql(f"""SELECT MAX(LABEL_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.LABELS""").collect()).iloc[0,0]

        if label_id:
            label_id += 1
        else:
            label_id = 1

        attributes_str = ''
        if type(attributes).__name__=='sqlNullWrapper':
            attributes_str = 'NULL'
        else:
            attributes_str = json.dumps(attributes)

        #insert label if it does not exist, update if it exists
        session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.LABELS l USING 
            (SELECT
                {label_id} LABEL_ID
                ,'{label_name}' LABEL_NAME
                ,{"$"}{"$"}{description}{"$"}{"$"} DESCRIPTION
                ,PARSE_JSON({"$"}{"$"}{attributes_str}{"$"}{"$"}) ATTRIBUTES
            ) AS nl
        ON 
            LOWER(l.LABEL_NAME) = LOWER(nl.LABEL_NAME)
        WHEN MATCHED THEN UPDATE SET 
            l.DESCRIPTION = nl.DESCRIPTION
            ,l.ATTRIBUTES = nl.ATTRIBUTES
            ,l.MODIFIED_TIMESTAMP = SYSDATE()
        WHEN NOT MATCHED THEN INSERT (LABEL_ID, LABEL_NAME, DESCRIPTION, ATTRIBUTES, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP) VALUES 
            (
                nl.LABEL_ID
                ,nl.LABEL_NAME
                ,nl.DESCRIPTION
                ,nl.ATTRIBUTES
                ,SYSDATE()
                ,NULL
            )""").collect()

        status = f"Label: {label_name} added/updated."
        
        return status
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;

--create MANAGE_OBJECT
CREATE OR REPLACE PROCEDURE MANAGE_OBJECT(object_type VARCHAR, database_name VARCHAR, schema_name VARCHAR, object_name VARCHAR, stage_location VARCHAR, labels ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'manage_object'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"manage_object"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def manage_object(session, object_type, database_name, schema_name, object_name, stage_location, labels):
    try:
        #get max object_id
        object_id = pd.DataFrame(session.sql(f"""SELECT MAX(OBJECT_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.OBJECTS""").collect()).iloc[0,0]

        if object_id:
            object_id += 1
        else:
            object_id = 1

        #object_types:
        #table, view, dynamic_table, materialzed_view, file

        if object_type.lower() in ['table', 'view', 'dynamic_table', 'materialized_view', '']:
            #get columns
            object_type_show = ''
            if object_type.lower() == 'dynamic_table':
                object_type_show = 'table'
            elif object_type.lower() == 'materialized_view':
                object_type_show = 'view'
            else:
                object_type_show = object_type
                
            session.sql(f"SHOW COLUMNS IN {object_type_show} {database_name}.{schema_name}.{object_name}").collect()

            #insert collection if it does not exist, update if the collection exists
            session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.OBJECTS obj USING 
                (SELECT
                    {object_id} OBJECT_ID
                    ,'{object_type}' OBJECT_TYPE
                    ,"database_name" DATABASE_NAME
                    ,"schema_name" SCHEMA_NAME
                    ,"table_name" OBJECT_NAME
                    ,ARRAY_AGG(object_construct(
                        'column_name',"column_name",
                        'data_type',parse_json("data_type"):type::varchar,
                        'is_nullable',parse_json("data_type"):nullable::boolean,
                        'precision',parse_json("data_type"):precision::number,
                        'scale',parse_json("data_type"):scale::number,
                        'length',parse_json("data_type"):length::number,
                        'byte_length',parse_json("data_type"):byteLength::number,
                        'description',null,
                        'null?',null
                        )) ATTRIBUTES
                    ,{labels} LABELS
                FROM table(RESULT_SCAN(LAST_QUERY_ID()))
                GROUP BY "database_name", "schema_name", "table_name"
                ) AS nobj 
            ON 
                LOWER(obj.DATABASE_NAME) = LOWER(nobj.DATABASE_NAME)
                AND LOWER(obj.SCHEMA_NAME) = LOWER(nobj.SCHEMA_NAME)
                AND LOWER(obj.OBJECT_NAME) = LOWER(nobj.OBJECT_NAME)
            WHEN MATCHED THEN UPDATE SET 
                obj.ATTRIBUTES = nobj.ATTRIBUTES
                ,obj.LABELS = ARRAY_CAT(obj.LABELS, nobj.LABELS)
                ,obj.MODIFIED_TIMESTAMP = SYSDATE()
            WHEN NOT MATCHED THEN INSERT (OBJECT_ID, OBJECT_TYPE, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, ATTRIBUTES, LABELS, ADDED_TIMESTAMP, MODIFIED_TIMESTAMP) VALUES 
                (
                    nobj.OBJECT_ID
                    ,nobj.OBJECT_TYPE
                    ,nobj.DATABASE_NAME
                    ,nobj.SCHEMA_NAME
                    ,nobj.OBJECT_NAME
                    ,nobj.ATTRIBUTES
                    ,nobj.LABELS
                    ,SYSDATE()
                    ,NULL
                )""").collect()
            status = f"{object_type.upper()}: {database_name}.{schema_name}.{object_name} attributes added/updated."
        #if object_type.lower() == 'file':
            #do something
        else:
            status = f"Error: Object type: {object_type} is invalid.  Please submit with a valid object type."
        
        return status
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;



--create MANAGE_COLLECTION
CREATE OR REPLACE PROCEDURE MANAGE_COLLECTION(objects VARIANT, collection_name VARCHAR, prev_collection_name VARCHAR, collection_type VARCHAR, labels ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'manage_collection'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"manage_collection"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def manage_collection(session, objects, collection_name, prev_collection_name, collection_type, labels):
    try:
        collection_id = 0
        
        if not prev_collection_name or prev_collection_name.lower() == collection_name.lower():
            prev_collection_name = collection_name

        #get collection_id, if collection_name exists - allows the user to rename collection, if desired
        df_existing_collection_id = pd.DataFrame(session.sql(f"""SELECT COLLECTION_ID FROM ZAMBONI_DB.ZAMBONI_METADATA.COLLECTIONS WHERE LOWER(COLLECTION_NAME) = '{prev_collection_name.lower()}'""").collect())

        if not df_existing_collection_id.empty:
            collection_id = df_existing_collection_id.iloc[0,0]
        else:
            #get max collection_id
            collection_id = pd.DataFrame(session.sql(f"""SELECT MAX(COLLECTION_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.COLLECTIONS""").collect()).iloc[0,0]

            if collection_id:
                collection_id += 1
            else:
                collection_id = 1
            
        status = f'{collection_type.upper()} is an invalid collection type.  Please use either STANDARD or CUSTOM.'
        
        if collection_type.lower() == 'standard':
            objects_str = json.dumps(objects)
        
            #insert collection if it does not exist, update if the collection exists
            session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.COLLECTIONS c USING 
                (SELECT 
                    {collection_id} COLLECTION_ID
                    ,'{collection_name}' COLLECTION_NAME
                    ,'{collection_type}' COLLECTION_TYPE
                    ,PARSE_JSON({"$"}{"$"}{objects_str}{"$"}{"$"}) OBJECTS
                    ,{labels} LABELS
                ) AS nc 
            --ON LOWER(c.COLLECTION_NAME) = LOWER('{collection_name}')
            ON c.COLLECTION_ID = {collection_id}
            WHEN MATCHED THEN UPDATE SET 
                c.COLLECTION_NAME = nc.COLLECTION_NAME
                ,c.COLLECTION_TYPE = nc.COLLECTION_TYPE
                ,c.OBJECTS = nc.OBJECTS
                ,c.LABELS = ARRAY_CAT(c.LABELS, nc.LABELS)
                ,c.MODIFIED_TIMESTAMP = SYSDATE()
            WHEN NOT MATCHED THEN INSERT (COLLECTION_ID, COLLECTION_NAME, COLLECTION_TYPE, OBJECTS, LABELS, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP) VALUES 
                (
                    nc.COLLECTION_ID
                    ,nc.COLLECTION_NAME
                    ,nc.COLLECTION_TYPE
                    ,nc.OBJECTS
                    ,nc.LABELS
                    ,SYSDATE()
                    ,NULL
                )""").collect()
    
            status = f"Collection: {collection_name} created/updated."

        #if collection_type.lower() == 'custom':
        
        return status
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;


--create MANAGE_PROCESS
CREATE OR REPLACE PROCEDURE MANAGE_PROCESS(processes VARIANT)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'manage_process'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"manage_process"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def manage_process(session, processes):
    try:
        #insert each process (target) into the PROCESSES table
        for process in processes["targets"]:
            #get max process_id
            process_id = pd.DataFrame(session.sql(f"""SELECT MAX(PROCESS_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES""").collect()).iloc[0,0]
    
            if process_id:
                process_id += 1
            else:
                process_id = 1
            
            process_name = process["process_name"]
            process_type_id = process["process_type_id"]
            labels = process["labels"]
            attributes_str = json.dumps(process)
            
            #insert process if it does not exist, update if it exists
            session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES p USING 
                (SELECT
                    {process_id} PROCESS_ID
                    ,'{process_name}' PROCESS_NAME
                    ,{process_type_id} PROCESS_TYPE_ID
                    ,PARSE_JSON({"$"}{"$"}{attributes_str}{"$"}{"$"}) ATTRIBUTES
                    ,{labels} LABELS
                ) AS np 
            ON LOWER(p.PROCESS_NAME) = LOWER('{process_name}')
            WHEN MATCHED THEN UPDATE SET 
                p.ATTRIBUTES = np.ATTRIBUTES
                ,p.LABELS = ARRAY_CAT(p.LABELS, np.LABELS)
                ,p.MODIFIED_TIMESTAMP = SYSDATE()
            WHEN NOT MATCHED THEN INSERT (PROCESS_ID, PROCESS_NAME, PROCESS_TYPE_ID, ATTRIBUTES, LABELS, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP) VALUES 
                (
                    np.PROCESS_ID
                    ,np.PROCESS_NAME
                    ,np.PROCESS_TYPE_ID
                    ,np.ATTRIBUTES
                    ,np.LABELS
                    ,SYSDATE()
                    ,NULL
                )""").collect()
        
        return 'Process(es) created/updated'
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;


--create MANAGE_DAG
CREATE OR REPLACE PROCEDURE MANAGE_DAG(parent_process_name VARCHAR, child_processes VARIANT, labels ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'manage_dag'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"manage_dag"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def manage_dag(session, parent_process_name, child_processes, labels):
    try:
        #get max parent_process_id
        parent_process_id = pd.DataFrame(session.sql(f"""SELECT MAX(PARENT_PROCESS_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG""").collect()).iloc[0,0]

        if parent_process_id:
            parent_process_id += 1
        else:
            parent_process_id = 1
        
        #add child_processes into the PROCESS_DAG table
        child_processes_str = json.dumps(child_processes)
        
        session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG d USING 
                (SELECT 
                    {parent_process_id} PARENT_PROCESS_ID
                    ,'{parent_process_name}' PARENT_PROCESS_NAME
                    ,PARSE_JSON({"$"}{"$"}{child_processes_str}{"$"}{"$"}) CHILD_PROCESSES
                    ,{labels} LABELS
                ) AS nd 
            ON LOWER(d.PARENT_PROCESS_NAME) = LOWER(nd.PARENT_PROCESS_NAME)
            WHEN MATCHED THEN UPDATE SET 
                d.CHILD_PROCESSES = nd.CHILD_PROCESSES
                ,d.LABELS = ARRAY_CAT(d.LABELS, nd.LABELS)
                ,d.MODIFIED_TIMESTAMP = SYSDATE()
            WHEN NOT MATCHED THEN INSERT (PARENT_PROCESS_ID, PARENT_PROCESS_NAME, CHILD_PROCESSES, LABELS, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP) VALUES 
                (
                    nd.PARENT_PROCESS_ID
                    ,nd.PARENT_PROCESS_NAME
                    ,nd.CHILD_PROCESSES
                    ,nd.LABELS
                    ,SYSDATE()
                    ,NULL
                )""").collect()
        
        return f"""Process DAG: {parent_process_name} created/updated"""
            
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;


--create CREATE_PROCESS
CREATE OR REPLACE PROCEDURE CREATE_PROCESS(parent_process_id NUMBER(38,0), process_id NUMBER(38,0), run_id NUMBER(38,0), prev_process_name VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_process'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"create_process"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import re
import time
import json
from snowflake.snowpark.functions import col

def create_process(session, parent_process_id, process_id, run_id, prev_process_name):
    try:
        #get max log_id
        log_id = pd.DataFrame(session.sql(f"""SELECT MAX(PROCESS_LOG_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG""").collect()).iloc[0,0]

        if log_id:
            log_id += 1
        else:
            log_id = 1
        
        #get parent_process_name
        parent_process_name = pd.DataFrame(session.sql(f"""SELECT PARENT_PROCESS_NAME FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG WHERE PARENT_PROCESS_ID = {parent_process_id}""").collect()).iloc[0,0]
        
        #get attributes
        process_df = pd.DataFrame(session.sql(f"""SELECT ATTRIBUTES FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES WHERE PROCESS_ID = {process_id}""").collect())
        attributes = json.loads(process_df.iloc[0,0])
        attributes_str = json.dumps(attributes)

        #get process details
        process_type_id = attributes["process_type_id"]
        process_name = attributes["process_name"]
        
        #get process_type details
        process_type_df = pd.DataFrame(session.sql(f"""SELECT TEMPLATE, OBJECT_TYPE, OBJECT_ACTION FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE PROCESS_TYPE_ID = {process_type_id}""").collect())
        template = process_type_df.iloc[0,0]
        object_type = process_type_df.iloc[0,1]
        object_action = process_type_df.iloc[0,2]

        #generate sql_command, based on template
        sql_command = pd.DataFrame(session.sql(f"""SELECT ZAMBONI_DB.ZAMBONI_UTIL.GET_SQL_JINJA(
                                                    {"$"}{"$"}{template}{"$"}{"$"}
                                                    ,PARSE_JSON({"$"}{"$"}{attributes_str}{"$"}{"$"}))""").collect()).iloc[0,0]

        #insert process start into PROCESS_LOG
        session.sql(f"""INSERT INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG (PROCESS_LOG_ID, PROCESS_RUN_ID, PARENT_PROCESS_ID, PROCESS_ID, PROCESS_START_TIMESTAMP, PROCESS_END_TIMESTAMP, PROCESS_OUTPUT) SELECT {log_id}, {run_id}, {parent_process_id}, {process_id}, SYSDATE(), NULL, OBJECT_CONSTRUCT('msg', OBJECT_CONSTRUCT('parent_process_name', '{parent_process_name}', 'process_name','{process_name}', 'status', 'started'))""").collect()


        #get target wh
        target_wh = attributes["settings"]["warehouse"]

        #if creating a dynamic table, set schedule lag interval or downstream
        if object_type.lower() == "dynamic table":
            downstream = attributes["settings"]["downstream"]
            if downstream:
                sql_command = re.sub(r"(?<=TARGET_LAG\s=\s)(\'.*\')", "DOWNSTREAM", sql_command)

        #execute command
        results_df = pd.DataFrame(session.sql(f"""{sql_command}""").collect())

        #details var
        details = ''
        
        if object_action.lower() == "create" and object_type.lower() in ["dynamic table", "table", "view", "materialized view"]:
            #get target object
            target_obj_str = attributes["target"]["object"]
            target_obj_db = target_obj_str.split(".")[0]
            target_obj_sch = target_obj_str.split(".")[1]
            target_obj_name = target_obj_str.split(".")[2]

            #get target collection ID
            target_col_id = attributes["target"]["collection_id"]    

            #get target alias
            target_alias = attributes["target"]["alias"]  

            #get labels
            labels = attributes["labels"]
            labels_str = json.dumps(labels)
        
            #call manage_object to add new object to OBJECTS table,
            session.sql(f"""CALL MANAGE_OBJECT('dynamic_table', '{target_obj_db}', '{target_obj_sch}', '{target_obj_name}', NULL, PARSE_JSON({"$"}{"$"}{labels_str}{"$"}{"$"}))""").collect()

            #get new object_id
            object_id = pd.DataFrame(session.sql(f"""SELECT OBJECT_ID FROM ZAMBONI_DB.ZAMBONI_METADATA.OBJECTS
                                                        WHERE LOWER(DATABASE_NAME) = LOWER('{target_obj_db}')
                                                        AND LOWER(SCHEMA_NAME) = LOWER('{target_obj_sch}')
                                                        AND LOWER(OBJECT_NAME) = LOWER('{target_obj_name}')""").collect()).iloc[0,0]
            
            #call manage_collection along w/all fields to the target collection  

            #get all fields from new target table
            add_obj_str = pd.DataFrame(session.sql(f"""SELECT OBJECT_CONSTRUCT(
                                                    'object_id',{object_id}
                                                    ,'attributes',ARRAY_AGG(f.value:column_name))::varchar FROM
                                                    ZAMBONI_DB.ZAMBONI_METADATA.OBJECTS, TABLE(FLATTEN(ATTRIBUTES)) f 
                                                    WHERE LOWER(DATABASE_NAME) = LOWER('{target_obj_db}')
                                                    AND LOWER(SCHEMA_NAME) = LOWER('{target_obj_sch}')
                                                    AND LOWER(OBJECT_NAME) = LOWER('{target_obj_name}')""").collect()).iloc[0,0]
                                                
            add_obj = json.loads(add_obj_str)
            
            #get existing collection objects
            col_df = pd.DataFrame(session.sql(f"""SELECT COLLECTION_NAME, OBJECTS::varchar FROM ZAMBONI_DB.ZAMBONI_METADATA.COLLECTIONS WHERE COLLECTION_ID = {target_col_id}""").collect())

            collection_name = col_df.iloc[0,0]
            objects_str = col_df.iloc[0,1]

            objects = json.loads(objects_str)

            #add new obj to existing collection
            if not any(obj["object_id"] == object_id for obj in objects["objects"]):
                objects["objects"].append(add_obj)

            updated_obj_str = json.dumps(objects)

            #update collection
            session.sql(f"""CALL ZAMBONI_DB.ZAMBONI_UTIL.MANAGE_COLLECTION(PARSE_JSON({"$"}{"$"}{updated_obj_str}{"$"}{"$"}), '{collection_name}', NULL, 'standard', PARSE_JSON({"$"}{"$"}{labels_str}{"$"}{"$"}))""").collect()            
        
            #set PROCESS_LOG details
            results = results_df.iloc[0,0]
            details = f"""'details',{"$"}{"$"}{results}{"$"}{"$"}"""
        elif (object_action.lower() in ['merge_insert', 'merge_delete']) or (object_action.lower() == 'create' and object_type.lower() in ['table']):            
            #create task to incrementally merge or fully refresh table
            target_lag = attributes["settings"]["target_lag"]
            target_interval = attributes["settings"]["target_interval"]
            
            schedule = f"SCHEDULE = '{target_lag} {target_interval}'"
            after = ""

            if prev_process_name != "":
                schedule = ""
                after = f"AFTER ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_{prev_process_name}_TASK"
            
            session.sql(f"""CREATE OR REPLACE TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_{process_name}_TASK
                                WAREHOUSE = '{target_wh}'
                                {schedule}
                                COMMENT = '{{"origin" :"sf_sit","name" :"zamboni","version" :{{"major" :1, "minor" :0}},"attributes" :"UPDATE_{process_name}_TASK"}}'
                                {after}
                                AS
                                    CALL ZAMBONI_DB.ZAMBONI_UTIL.UPDATE_TARGET({parent_process_id}, {process_id}, {"$"}{"$"}{sql_command}{"$"}{"$"})""").collect()

        #set PROCESS_LOG details
        if object_action.lower() == 'merge_insert':
            rows_inserted = results_df.iloc[0,0]
            rows_updated = results_df.iloc[0,1]
            details = f"""'rows_inserted',{rows_inserted}, 'rows_updated',{rows_updated}"""
        elif object_action.lower() == 'merge_delete':
            rows_inserted = results_df.iloc[0,0]
            rows_deleted = results_df.iloc[0,1]
            details = f"""'rows_inserted',{rows_inserted}, 'rows_deleted',{rows_deleted}"""
        else:
            results = results_df.iloc[0,0]
            details = f"""'details',{"$"}{"$"}{results}{"$"}{"$"}"""

        #update PROCESS_LOG
        session.sql(f"""UPDATE ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG SET PROCESS_END_TIMESTAMP = SYSDATE(), PROCESS_OUTPUT = OBJECT_CONSTRUCT('msg', OBJECT_CONSTRUCT('parent_process_name', '{parent_process_name}', 'process_name','{process_name}', 'status','created and started.', {details})) WHERE PROCESS_RUN_ID = {run_id} AND PARENT_PROCESS_ID = {parent_process_id} AND PROCESS_ID = {process_id}""").collect()

        return f'Process: {process_name} created and started'
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG l USING 
                        (SELECT
                            {log_id} PROCESS_LOG_ID
                            ,{run_id} PROCESS_RUN_ID
                            ,{parent_process_id} PARENT_PROCESS_ID
                            ,{process_id} PROCESS_ID
                            ,OBJECT_CONSTRUCT('error',{"$"}{"$"}{error_eraw}{"$"}{"$"}) PROCESS_OUTPUT
                        ) AS el 
                    ON 
                        LOWER(l.PROCESS_RUN_ID) = LOWER(el.PROCESS_RUN_ID)
                        AND LOWER(l.PARENT_PROCESS_ID) = LOWER(el.PARENT_PROCESS_ID)
                        AND LOWER(l.PROCESS_ID) = LOWER(el.PROCESS_ID)
                    WHEN MATCHED THEN UPDATE SET 
                        l.PROCESS_OUTPUT = el.PROCESS_OUTPUT
                        ,l.PROCESS_END_TIMESTAMP = SYSDATE()
                    WHEN NOT MATCHED THEN INSERT (PROCESS_LOG_ID, PROCESS_RUN_ID, PARENT_PROCESS_ID, PROCESS_ID, PROCESS_START_TIMESTAMP, PROCESS_END_TIMESTAMP, PROCESS_OUTPUT) VALUES 
                        (
                            el.PROCESS_LOG_ID
                            ,el.PROCESS_RUN_ID
                            ,el.PARENT_PROCESS_ID
                            ,el.PROCESS_ID
                            ,SYSDATE()
                            ,NULL
                            ,el.PROCESS_OUTPUT
                        )""").collect()

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;



--create UPDATE_TARGET
CREATE OR REPLACE PROCEDURE UPDATE_TARGET(parent_process_id NUMBER(38,0), process_id NUMBER(38,0), sql_command VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'update_target'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"update_target"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def update_target(session, parent_process_id, process_id, sql_command):
    try:
        #get max log_id
        log_id = pd.DataFrame(session.sql(f"""SELECT MAX(PROCESS_LOG_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG""").collect()).iloc[0,0]

        if log_id:
            log_id += 1
        else:
            log_id = 1
                
        #get max run_id
        run_id = pd.DataFrame(session.sql(f"""SELECT MAX(PROCESS_RUN_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG""").collect()).iloc[0,0]

        if run_id:
            run_id += 1
        else:
            run_id = 1

        #get process details
        process_df = pd.DataFrame(session.sql(f"""SELECT PROCESS_NAME, PROCESS_TYPE_ID, FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES WHERE PROCESS_ID = {process_id}""").collect())
        process_name = process_df.iloc[0,0]
        process_type_id = process_df.iloc[0,1]

        #get process_type details
        process_type_df = pd.DataFrame(session.sql(f"""SELECT OBJECT_TYPE, OBJECT_ACTION FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE PROCESS_TYPE_ID = {process_type_id}""").collect())
        object_type = process_type_df.iloc[0,0]
        object_action = process_type_df.iloc[0,1]

        #get parent_process_name
        parent_process_name = pd.DataFrame(session.sql(f"""SELECT PARENT_PROCESS_NAME FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG WHERE PARENT_PROCESS_ID = {parent_process_id}""").collect()).iloc[0,0]
        
        #insert process start into PROCESS_LOG
        session.sql(f"""INSERT INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG (PROCESS_LOG_ID, PROCESS_RUN_ID, PARENT_PROCESS_ID, PROCESS_ID, PROCESS_START_TIMESTAMP, PROCESS_END_TIMESTAMP, PROCESS_OUTPUT) SELECT {log_id}, {run_id}, {parent_process_id}, {process_id}, SYSDATE(), NULL, OBJECT_CONSTRUCT('msg', OBJECT_CONSTRUCT('parent_process_name', '{parent_process_name}', 'process_name','{process_name}', 'status', 'started'))""").collect()

        #execute command
        results_df = pd.DataFrame(session.sql(f"""{sql_command}""").collect())

        #details var
        details = ''

        if object_action.lower() == 'create' and object_type.lower() in ['table']:
            #set PROCESS_LOG details
            results = results_df.iloc[0,0]
            details = f"""'details',{"$"}{"$"}{results}{"$"}{"$"}"""
        elif object_action.lower() in ['merge_insert', 'merge_delete']:
            #set PROCESS_LOG details
            if object_action.lower() == 'merge_insert':
                rows_inserted = results_df.iloc[0,0]
                rows_updated = results_df.iloc[0,1]
                details = f"""'rows_inserted',{rows_inserted}, 'rows_updated',{rows_updated}"""
            elif object_action.lower() == 'merge_delete':
                rows_inserted = results_df.iloc[0,0]
                rows_deleted = results_df.iloc[0,1]
                details = f"""'rows_inserted',{rows_inserted}, 'rows_deleted',{rows_deleted}"""

        #update PROCESS_LOG
        session.sql(f"""UPDATE ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG SET PROCESS_END_TIMESTAMP = SYSDATE(), PROCESS_OUTPUT = OBJECT_CONSTRUCT('msg', OBJECT_CONSTRUCT('parent_process_name', '{parent_process_name}', 'process_name','{process_name}', 'status','completed', {details})) WHERE PROCESS_RUN_ID = {run_id} AND PROCESS_ID = {process_id}""").collect()
        
        return f'Target updated for parent process: {parent_process_name}, process: {process_name}.'
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG l USING 
                        (SELECT
                            {log_id} PROCESS_LOG_ID
                            ,{run_id} PROCESS_RUN_ID
                            ,{parent_process_id} PARENT_PROCESS_ID
                            ,{process_id} PROCESS_ID
                            ,OBJECT_CONSTRUCT('error',{"$"}{"$"}{error_eraw}{"$"}{"$"}) PROCESS_OUTPUT
                        ) AS el 
                    ON 
                        LOWER(l.PROCESS_RUN_ID) = LOWER(el.PROCESS_RUN_ID)
                        AND LOWER(l.PARENT_PROCESS_ID) = LOWER(el.PARENT_PROCESS_ID)
                        AND LOWER(l.PROCESS_ID) = LOWER(el.PROCESS_ID)
                    WHEN MATCHED THEN UPDATE SET 
                        l.PROCESS_OUTPUT = el.PROCESS_OUTPUT
                        ,l.PROCESS_END_TIMESTAMP = SYSDATE()
                    WHEN NOT MATCHED THEN INSERT (PROCESS_LOG_ID, PROCESS_RUN_ID, PARENT_PROCESS_ID, PROCESS_ID, PROCESS_START_TIMESTAMP, PROCESS_END_TIMESTAMP, PROCESS_OUTPUT) VALUES 
                        (
                            el.PROCESS_LOG_ID
                            ,el.PROCESS_RUN_ID
                            ,el.PARENT_PROCESS_ID
                            ,el.PROCESS_ID
                            ,SYSDATE()
                            ,NULL
                            ,el.PROCESS_OUTPUT
                        )""").collect()

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;



--create CREATE_PARENT_PROCESS
CREATE OR REPLACE PROCEDURE CREATE_PARENT_PROCESS(parent_process_id NUMBER(38,0))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_parent_process'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"create_parent_process"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col
from operator import itemgetter

def create_parent_process(session, parent_process_id):
    try:
        #get max run_id
        run_id = pd.DataFrame(session.sql(f"""SELECT MAX(PROCESS_RUN_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG""").collect()).iloc[0,0]

        if run_id:
            run_id += 1
        else:
            run_id = 1
                
        #get child processes
        child_processes_df = pd.DataFrame(session.sql(f"""SELECT PARENT_PROCESS_NAME, CHILD_PROCESSES FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG WHERE PARENT_PROCESS_ID = {parent_process_id}""").collect())
        parent_process_name = child_processes_df.iloc[0,0]
        child_processes = json.loads(child_processes_df.iloc[0,1])
        child_processes_str = json.dumps(child_processes)

        #sort child processes by process order number
        child_processes_sorted = json.dumps(sorted(child_processes["child_processes"], key=itemgetter('process_order')))
        child_processes_sorted_json = json.loads(child_processes_sorted)

        #create list to store tasks
        resume_task_list = []

        #get length of child_processes_sorted list
        prev_process_name = ""

        #run each process, in order
        for index, cp in enumerate(child_processes_sorted_json):
            process_id = cp["process_id"]

            #get attributes
            process_df = pd.DataFrame(session.sql(f"""SELECT ATTRIBUTES FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES WHERE PROCESS_ID = {process_id}""").collect())
            attributes = json.loads(process_df.iloc[0,0])

            #get process details
            process_type_id = attributes["process_type_id"]
            process_name = attributes["process_name"]
            
            #get process_type details
            process_type_df = pd.DataFrame(session.sql(f"""SELECT OBJECT_TYPE, OBJECT_ACTION FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE PROCESS_TYPE_ID = {process_type_id}""").collect())
            object_type = process_type_df.iloc[0,0]
            object_action = process_type_df.iloc[0,1]

            #if a merge or create table statement, set prev_process_name if there is more than one process of this type
            if (object_action.lower() in ['merge_insert', 'merge_delete']) or (object_action.lower() == 'create' and object_type.lower() in ['table']):
                if index > 0:
                    prev_process_name = child_processes_sorted_json[index - 1]["process_name"]

            #create process
            session.sql(f"""CALL ZAMBONI_DB.ZAMBONI_UTIL.CREATE_PROCESS({parent_process_id},{process_id},{run_id},'{prev_process_name}')""").collect()

            if (object_action.lower() in ['merge_insert', 'merge_delete']) or (object_action.lower() == 'create' and object_type.lower() in ['table']):
                #insert task into list
                resume_task_list.insert(0,f"ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_{process_name}_TASK")

        #start each child task
        for task in resume_task_list:
            session.sql(f"""ALTER TASK {task} RESUME""").collect()

        return f'Parent process: {parent_process_name} successfully created and started.' 
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;

--create UPDATE_PROCESS
CREATE OR REPLACE PROCEDURE UPDATE_PROCESS(parent_process_id NUMBER(38,0), process_id NUMBER(38,0), run_id NUMBER(38,0), action VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'update_process'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"update_process"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def update_process(session, parent_process_id, process_id, run_id, action):
    try:
        #get max log_id
        log_id = pd.DataFrame(session.sql(f"""SELECT MAX(PROCESS_LOG_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG""").collect()).iloc[0,0]

        if log_id:
            log_id += 1
        else:
            log_id = 1
        
        #get parent_process_name
        parent_process_name = pd.DataFrame(session.sql(f"""SELECT PARENT_PROCESS_NAME FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG WHERE PARENT_PROCESS_ID = {parent_process_id}""").collect()).iloc[0,0]
        
        #get attributes
        process_df = pd.DataFrame(session.sql(f"""SELECT ATTRIBUTES FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES WHERE PROCESS_ID = {process_id}""").collect())
        attributes = json.loads(process_df.iloc[0,0])

        #get process details
        process_type_id = attributes["process_type_id"]
        process_name = attributes["process_name"]
        
        #get process_type details
        process_type_df = pd.DataFrame(session.sql(f"""SELECT OBJECT_TYPE, OBJECT_ACTION FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE PROCESS_TYPE_ID = {process_type_id}""").collect())
        object_type = process_type_df.iloc[0,0]
        object_action = process_type_df.iloc[0,1]

        if (object_action.lower() in ['merge_insert', 'merge_delete']) or (object_action.lower() == 'create' and object_type.lower() in ['table']):            
            #alter task
            session.sql(f"""ALTER TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_{process_name}_TASK {action.upper()}""").collect()

            #insert process action into PROCESS_LOG
            session.sql(f"""INSERT INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG (PROCESS_LOG_ID, PROCESS_RUN_ID, PARENT_PROCESS_ID, PROCESS_ID, PROCESS_START_TIMESTAMP, PROCESS_END_TIMESTAMP, PROCESS_OUTPUT) SELECT {log_id}, {run_id}, {parent_process_id}, {process_id}, SYSDATE(), NULL, OBJECT_CONSTRUCT('msg', OBJECT_CONSTRUCT('parent_process_name', '{parent_process_name}', 'process_name','{process_name}', 'status', '{action.lower()}'))""").collect()

            return f'Process {process_name} status: {action.lower()}'
        else:
            return f'Process {process_name} is not a task'
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        session.sql(f"""MERGE INTO ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG l USING 
                        (SELECT
                            {log_id} PROCESS_LOG_ID
                            ,{run_id} PROCESS_RUN_ID
                            ,{parent_process_id} PARENT_PROCESS_ID
                            ,{process_id} PROCESS_ID
                            ,OBJECT_CONSTRUCT('error',{"$"}{"$"}{error_eraw}{"$"}{"$"}) PROCESS_OUTPUT
                        ) AS el 
                    ON 
                        LOWER(l.PROCESS_RUN_ID) = LOWER(el.PROCESS_RUN_ID)
                        AND LOWER(l.PARENT_PROCESS_ID) = LOWER(el.PARENT_PROCESS_ID)
                        AND LOWER(l.PROCESS_ID) = LOWER(el.PROCESS_ID)
                    WHEN MATCHED THEN UPDATE SET 
                        l.PROCESS_OUTPUT = el.PROCESS_OUTPUT
                        ,l.PROCESS_END_TIMESTAMP = SYSDATE()
                    WHEN NOT MATCHED THEN INSERT (PROCESS_LOG_ID, PROCESS_RUN_ID, PARENT_PROCESS_ID, PROCESS_ID, PROCESS_START_TIMESTAMP, PROCESS_END_TIMESTAMP, PROCESS_OUTPUT) VALUES 
                        (
                            el.PROCESS_LOG_ID
                            ,el.PROCESS_RUN_ID
                            ,el.PARENT_PROCESS_ID
                            ,el.PROCESS_ID
                            ,SYSDATE()
                            ,NULL
                            ,el.PROCESS_OUTPUT
                        )""").collect()

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;


--create UPDATE_PARENT_PROCESS
CREATE OR REPLACE PROCEDURE UPDATE_PARENT_PROCESS(parent_process_id NUMBER(38,0), action VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'update_parent_process'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"update_parent_process"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col
from operator import itemgetter

def update_parent_process(session, parent_process_id, action):
    try:
        #get max run_id
        run_id = pd.DataFrame(session.sql(f"""SELECT MAX(PROCESS_RUN_ID) FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG""").collect()).iloc[0,0]

        if run_id:
            run_id += 1
        else:
            run_id = 1
                
        #get child processes
        child_processes_df = pd.DataFrame(session.sql(f"""SELECT PARENT_PROCESS_NAME, CHILD_PROCESSES FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG WHERE PARENT_PROCESS_ID = {parent_process_id}""").collect())
        parent_process_name = child_processes_df.iloc[0,0]
        child_processes = json.loads(child_processes_df.iloc[0,1])

        #sort child processes in based on action
        reverse_flag = False
        if action.lower() == "resume":
            reverse_flag = True

        child_processes_sorted = json.dumps(sorted(child_processes["child_processes"], key=itemgetter('process_order'), reverse=reverse_flag))
        child_processes_sorted_json = json.loads(child_processes_sorted)

        #run each process, in order
        for index, cp in enumerate(child_processes_sorted_json):
            process_id = cp["process_id"]

            #get attributes
            process_df = pd.DataFrame(session.sql(f"""SELECT ATTRIBUTES FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES WHERE PROCESS_ID = {process_id}""").collect())
            attributes = json.loads(process_df.iloc[0,0])

            #get process details
            process_type_id = attributes["process_type_id"]
            process_name = attributes["process_name"]
            
            #get process_type details
            process_type_df = pd.DataFrame(session.sql(f"""SELECT OBJECT_TYPE, OBJECT_ACTION FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE PROCESS_TYPE_ID = {process_type_id}""").collect())
            object_type = process_type_df.iloc[0,0]
            object_action = process_type_df.iloc[0,1]

            #if a merge or create table statement, call UPDATE_PROCESS to update task
            if (object_action.lower() in ['merge_insert', 'merge_delete']) or (object_action.lower() == 'create' and object_type.lower() in ['table']):
                session.sql(f"""CALL ZAMBONI_DB.ZAMBONI_UTIL.UPDATE_PROCESS({parent_process_id},{process_id},{run_id}, '{action}')""").collect()

        return f'Parent process {parent_process_name} status: {action.lower()}.' 
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;

--create GET_JINJA_SQL
create or replace function GET_SQL_JINJA(template string, parameters variant)
  returns string
  language python
  runtime_version = 3.8
  handler='apply_sql_template'
  packages = ('six','jinja2==3.0.3','markupsafe')
  comment='{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"manage_process"}'
as
$$
# Most of the following code is copied from the jinjasql package, which is not included in Snowflake's python packages
from __future__ import unicode_literals
import jinja2
from six import string_types
from copy import deepcopy
import os
import re
from jinja2 import Environment
from jinja2 import Template
from jinja2.ext import Extension
from jinja2.lexer import Token
from markupsafe import Markup

try:
    from collections import OrderedDict
except ImportError:
    # For Python 2.6 and less
    from ordereddict import OrderedDict

from threading import local
from random import Random

_thread_local = local()

# This is mocked in unit tests for deterministic behaviour
random = Random()


class JinjaSqlException(Exception):
    pass

class MissingInClauseException(JinjaSqlException):
    pass

class InvalidBindParameterException(JinjaSqlException):
    pass

class SqlExtension(Extension):

    def extract_param_name(self, tokens):
        name = ""
        for token in tokens:
            if token.test("variable_begin"):
                continue
            elif token.test("name"):
                name += token.value
            elif token.test("dot"):
                name += token.value
            else:
                break
        if not name:
            name = "bind#0"
        return name

    def filter_stream(self, stream):
        """
        We convert
        {{ some.variable | filter1 | filter 2}}
            to
        {{ ( some.variable | filter1 | filter 2 ) | bind}}
        ... for all variable declarations in the template
        Note the extra ( and ). We want the | bind to apply to the entire value, not just the last value.
        The parentheses are mostly redundant, except in expressions like {{ '%' ~ myval ~ '%' }}
        This function is called by jinja2 immediately
        after the lexing stage, but before the parser is called.
        """
        while not stream.eos:
            token = next(stream)
            if token.test("variable_begin"):
                var_expr = []
                while not token.test("variable_end"):
                    var_expr.append(token)
                    token = next(stream)
                variable_end = token

                last_token = var_expr[-1]
                lineno = last_token.lineno
                # don't bind twice
                if (not last_token.test("name")
                    or not last_token.value in ('bind', 'inclause', 'sqlsafe')):
                    param_name = self.extract_param_name(var_expr)

                    var_expr.insert(1, Token(lineno, 'lparen', u'('))
                    var_expr.append(Token(lineno, 'rparen', u')'))
                    var_expr.append(Token(lineno, 'pipe', u'|'))
                    var_expr.append(Token(lineno, 'name', u'bind'))
                    var_expr.append(Token(lineno, 'lparen', u'('))
                    var_expr.append(Token(lineno, 'string', param_name))
                    var_expr.append(Token(lineno, 'rparen', u')'))

                var_expr.append(variable_end)
                for token in var_expr:
                    yield token
            else:
                yield token

def sqlsafe(value):
    """Filter to mark the value of an expression as safe for inserting
    in a SQL statement"""
    return Markup(value)

def bind(value, name):
    """A filter that prints %s, and stores the value
    in an array, so that it can be bound using a prepared statement
    This filter is automatically applied to every {{variable}}
    during the lexing stage, so developers can't forget to bind
    """
    if isinstance(value, Markup):
        return value
    elif requires_in_clause(value):
        raise MissingInClauseException("""Got a list or tuple.
            Did you forget to apply '|inclause' to your query?""")
    else:
        return _bind_param(_thread_local.bind_params, name, value)

def bind_in_clause(value):
    values = list(value)
    results = []
    for v in values:
        results.append(_bind_param(_thread_local.bind_params, "inclause", v))

    clause = ",".join(results)
    clause = "(" + clause + ")"
    return clause

def _bind_param(already_bound, key, value):
    _thread_local.param_index += 1
    new_key = "%s_%s" % (key, _thread_local.param_index)
    already_bound[new_key] = value

    param_style = _thread_local.param_style
    if param_style == 'qmark':
        return "?"
    elif param_style == 'format':
        return "%s"
    elif param_style == 'numeric':
        return ":%s" % _thread_local.param_index
    elif param_style == 'named':
        return ":%s" % new_key
    elif param_style == 'pyformat':
        return "%%(%s)s" % new_key
    elif param_style == 'asyncpg':
        return "$%s" % _thread_local.param_index
    else:
        raise AssertionError("Invalid param_style - %s" % param_style)

def requires_in_clause(obj):
    return isinstance(obj, (list, tuple))

def is_dictionary(obj):
    return isinstance(obj, dict)

class JinjaSql(object):
    # See PEP-249 for definition
    # qmark "where name = ?"
    # numeric "where name = :1"
    # named "where name = :name"
    # format "where name = %s"
    # pyformat "where name = %(name)s"
    VALID_PARAM_STYLES = ('qmark', 'numeric', 'named', 'format', 'pyformat', 'asyncpg')
    def __init__(self, env=None, param_style='format'):
        self.env = env or Environment()
        self._prepare_environment()
        self.param_style = param_style

    def _prepare_environment(self):
        self.env.autoescape=True
        self.env.add_extension(SqlExtension)
        self.env.filters["bind"] = bind
        self.env.filters["sqlsafe"] = sqlsafe
        self.env.filters["inclause"] = bind_in_clause

    def prepare_query(self, source, data):
        if isinstance(source, Template):
            template = source
        else:
            template = self.env.from_string(source)

        return self._prepare_query(template, data)

    def _prepare_query(self, template, data):
        try:
            _thread_local.bind_params = OrderedDict()
            _thread_local.param_style = self.param_style
            _thread_local.param_index = 0
            query = template.render(data)
            bind_params = _thread_local.bind_params
            if self.param_style in ('named', 'pyformat'):
                bind_params = dict(bind_params)
            elif self.param_style in ('qmark', 'numeric', 'format', 'asyncpg'):
                bind_params = list(bind_params.values())
            return query, bind_params
        finally:
            del _thread_local.bind_params
            del _thread_local.param_style
            del _thread_local.param_index

# Non-JinjaSql package code starts here
def quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        #baseline sql injection deterrance
        new_value2 = re.sub(r"[^a-zA-Z0-9_.-]","",new_value)
        return "'{}'".format(new_value2)
    return value

def get_sql_from_template(query, bind_params):
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = quote_sql_string(val)
    return query % params

def strip_blank_lines(text):
    '''
    Removes blank lines from the text, including those containing only spaces.
    https://stackoverflow.com/questions/1140958/whats-a-quick-one-liner-to-remove-empty-lines-from-a-python-string
    '''
    return os.linesep.join([s for s in text.splitlines() if s.strip()])

def apply_sql_template(template, parameters):
    '''
    Apply a JinjaSql template (string) substituting parameters (dict) and return
    the final SQL.
    '''
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(template, parameters)
    return strip_blank_lines(get_sql_from_template(query, bind_params))

$$;

--create TEST_QUERY
CREATE OR REPLACE PROCEDURE TEST_QUERY(sql_command VARCHAR)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'test_query'
COMMENT = '{"origin" :"sf_sit","name" :"zamboni","version" :{"major" :1, "minor" :0},"attributes" :"test_query"}'
EXECUTE AS CALLER
AS 
$$
import pandas as pd
import time
import json
from snowflake.snowpark.functions import col

def test_query(session, sql_command):
    try:
        #create dataframe
        query_df = pd.DataFrame(session.sql(f"""{sql_command} LIMIT 100""").collect())

        data_df = session.create_dataframe(query_df)
        return data_df
        
    except Exception as e:
        session.sql("rollback").collect()

        #remove unwanted characters from error msg
        error_eraw = str(e).replace("'","").replace("\r"," ").replace("\n"," ").replace("\r\n"," ").replace("\n\r"," ")

        msg_return = "Failed: " + error_eraw

        raise Exception(msg_return)

$$
;