
/* ==============================================
  File: step_8_glue_sync_task_manager.sql
  Description:  This script leverages a driving table to create streams and tasks for each iceberg table in Snowflake that needs to be synced over to Athena.
                It also resumes the tasks upon creation.  Commented this out if not desired.
                To process the tables in the list, their include_flags need to be set to 'Y'. 
                The include_flag is set to 'N' for each table after its stream and task are created 
                Streams and tasks are created in the same database and schema as the snowflake iceberg tables. 
                To create the procedure, do the following: 
                - If the driving table is named different, update the table name in the script accordingly.
                - It assumes the update proc, update_glue_metadata_location, is in the same db and schema as this proc. Adjust if necessary.
                - Replace update_glue_metadata_location with the your procedure name if it is named differently.
                - It is assumed that update_glue_metadata_location is in the same database and schema as the procedure as this proc.  Adjust if necessary.
                - Modify the script if to create stream and tasks in different database or schema than those for the iceberg tables.
                - streams are created with the name of the table with '_glue_sync_str' suffix, and tasks are created with the name of the table with '_glue_sync_task' suffix. 
                  Update the sufffix if needed.
                
Sample Call:
 -----------------------------------------------
call GLUE_SYNC_TASK_MANAGER('XSMALL_WH');
----------------------------------------------- 
 ===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-07-10   | J. Ma         | Created
2025-07-25   | J. Ma         | Updated the call in task to update_glue_metadata_location to pass on get_ddl
2025-07-25   | J. Ma         | Updated the call in task to fully qualify all identifiers: streams, snowflake tables, tasks.  
2025-07-28   | J. Ma         | Updated the procedure to take warehouse name as a parameter for task creation. Added error handling and logging.
             |               | Update the procedure to generate tasks with fually qualified objects, and update include_flag to 'N'.
2025-09-17   | M.Henderson   | Renamed proc to GLUE_SYNC_TASK_MANAGER.
             |               | Updated the procedure to check the ICEBERG_METADATA_SYNC table (formerly iceberg_table_list).
             |               | Removed 'wh_name' parameter since it's included in the ICEBERG_METADATA_SYNC table.
             |               | Updated stream and tasks to be glue-specific (in case this is replicated for other catalogs).
===============================================
*/

SET T2I_ROLE = CURRENT_ROLE();
SET T2I_SCH = 'ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR';
SET T2I_WH = 'ICEBERG_MIGRATOR_WH';

USE ROLE IDENTIFIER($T2I_ROLE);
USE WAREHOUSE IDENTIFIER($T2I_WH);

USE SCHEMA IDENTIFIER($T2I_SCH);

CREATE OR REPLACE PROCEDURE GLUE_SYNC_TASK_MANAGER()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
 declare
    rs RESULTSET default (SELECT TABLE_INSTANCE_ID, TABLE_RUN_ID, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, DESTINATION, TARGET_DATABASE, TARGET_TABLE, UPDATE_FREQUENCY_MINS, WAREHOUSE, EXTERNAL_ACCESS_INTEGRATION 
                          from ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.ICEBERG_METADATA_SYNC where LOWER(DESTINATION) = 'aws_glue' and LOWER(SYNC_STARTED) = 'n');


    vw1_cur CURSOR for rs;
    my_sql varchar;
    source_table_fqn varchar;
    stream_name varchar;
    task_name varchar ;
    lcnt number default 0;
    V_PROC_NAME VARCHAR(255) DEFAULT 'GLUE_SYNC_TASK_MANAGER';
begin
    for vw1 in vw1_cur do
       source_table_fqn := vw1.source_database||'.'||vw1.source_schema||'.'||vw1.source_table;

       lcnt := lcnt + 1;

       stream_name := 'ICEBERG_MIGRATOR_DB.AWS_GLUE_SYNC.' || concat(vw1.source_table, '_glue_sync_str_') || vw1.table_instance_id || '_' || vw1.table_run_id;
       my_sql := 'CREATE OR REPLACE STREAM ' || :stream_name || ' ON TABLE ' || :source_table_fqn || ' SHOW_INITIAL_ROWS = TRUE;';
       execute immediate :my_sql;
       
       task_name := 'ICEBERG_MIGRATOR_DB.AWS_GLUE_SYNC.' || concat(vw1.source_table, '_glue_sync_task_') || vw1.table_instance_id || '_' || vw1.table_run_id;
       my_sql :=  'CREATE OR REPLACE TASK ' || :task_name ||
            '\nWAREHOUSE = '|| vw1.warehouse || 
            '\nSCHEDULE = '''|| vw1.update_frequency_mins ||' MINUTE''' ||
            '\nWHEN SYSTEM$STREAM_HAS_DATA('''||:stream_name||''')' ||
            '\nAS' ||
            '\nBEGIN' ||
                '\ncall ICEBERG_MIGRATOR_DB.AWS_GLUE_SYNC.update_glue_metadata_location_' || vw1.external_access_integration || '('||vw1.table_instance_id||', '||vw1.table_run_id||', '''||vw1.target_database||'''' ||
                ','''||vw1.target_table||'''' ||  
                ',get_ddl(''table'', '''||:source_table_fqn||''')' ||
                ',CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('''||:source_table_fqn||''')), ''metadataLocation'') AS VARCHAR)' ||
                ','''||:stream_name||''');' ||
            '\nEND;';   
       execute immediate :my_sql;

       my_sql :=  ' alter task ' || :task_name || ' resume ';
       execute immediate :my_sql;

       my_sql := 'UPDATE ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.ICEBERG_METADATA_SYNC SET SYNC_STARTED = ''Y'' WHERE TABLE_INSTANCE_ID = '||vw1.table_instance_id||' AND TABLE_RUN_ID = '||vw1.table_run_id|| ';';
       execute immediate :my_sql;
    end for;
    
    
    SYSTEM$LOG_INFO('PROCEDURE ' || :V_PROC_NAME || ' completed successfully. Total tables synced: ' || :lcnt);
    return 'Success. Number of tables synced: '||:lcnt;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(
            'PROCEDURE ' || :V_PROC_NAME || ' failed. ' ||
            'Error Code: ' || SQLCODE || '. ' ||
            'Error Message: ' || SQLERRM 
        );

    RETURN 'Error recording log event: ' || SQLERRM;

end;
$$; 