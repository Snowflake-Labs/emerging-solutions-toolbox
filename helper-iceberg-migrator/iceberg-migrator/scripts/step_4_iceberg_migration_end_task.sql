SET T2I_ROLE = CURRENT_ROLE();
SET T2I_SCH = 'ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR';
SET T2I_WH = 'ICEBERG_MIGRATOR_WH';

USE ROLE IDENTIFIER($T2I_ROLE);
USE WAREHOUSE IDENTIFIER($T2I_WH);

USE SCHEMA IDENTIFIER($T2I_SCH);

CREATE or REPLACE PROCEDURE iceberg_migration_end_task(RUN_ID FLOAT)
RETURNS VARIANT 
LANGUAGE JAVASCRIPT 
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 3}}' 
EXECUTE AS CALLER 
AS
$$
    // ---------------------------------------------------------------------------------------------
    // Name: iceberg_migration_end_task
    // --------------------------------------------------------------------------------------------- 
    // End task in the DAG that sets final state and drops the dynamically created tasks 
    // ---------------------------------------------------------------------------------------------
    //      run_id (integer)            The identifier for the run 
    // ---------------------------------------------------------------------------------------------
    // Returns: JSON (Variant)
    //      result:    Succeeded = True/Failed = False 
    //      message:   Error message 
    // ---------------------------------------------------------------------------------------------
    // Date         Who         Description 
    // 07/17/2024   S Ramsey    Initial version  
    // 05/16/2025   M Henderson Add support to remove delta table-related objects created during run
    // ---------------------------------------------------------------------------------------------
    // ----- Standard tag 
    var std_tag = {origin: 'sf_sit', name: 'table_to_iceberg', version:{major: 1, minor: 3}}

    // -- Define return value
    var ret = {}
    ret['result'] = true
    ret['message'] = null
    
    var tags = {}

    // ---- Do stuff 
    is_err = false

    try 
    {
        // ---- Set query tag 
        tags['procName'] = arguments.callee.name
        tags['runId'] = RUN_ID
        std_tag['attributes'] = tags
        //snowflake.execute({sqlText: `alter session set query_tag='`+JSON.stringify(std_tag)+`'`})

        // ---- Clean up tasks 
        sql_txt = `SHOW TASKS STARTS WITH 'IM_${RUN_ID}_'`;
        //ret['debugSql'] = sql_txt ;
        var resultSet = snowflake.execute({sqlText: sql_txt});

        while (resultSet.next()) {
            sql_txt = `DROP TASK ${resultSet.getColumnValue('name')}`;
            //ret['debugSql'] = sql_txt ;
            snowflake.execute({sqlText: sql_txt});
        }

        //get external volume for this run
        let ext_vol = "";
        var rset = snowflake.execute({sqlText: `SELECT tool_value 
                                                FROM ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG 
                                                WHERE LOWER(tool_name) = 'iceberg_migrator' 
                                                AND LOWER(tool_parameter) = 'external_volume';`});
        
        while(rset.next()) {
            ext_vol = rset.getColumnValue(1);
        }

        // ---- Remove delta table related objects created during run, if created
        snowflake.execute({sqlText: `DROP FILE FORMAT IF EXISTS ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.FF_PARQUET;`});
        snowflake.execute({sqlText: `DROP STAGE IF EXISTS ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.STAGE_${ext_vol};`});
        snowflake.execute({sqlText: `DROP STORAGE INTEGRATION IF EXISTS SI_${ext_vol}`});

        // ---- Set end state 
        sql_txt = `update ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_run_log set end_time = current_timestamp() where run_id=${RUN_ID}`
        snowflake.execute({sqlText: sql_txt})
    }
    catch(err)
    {
        is_err = true
		ret['result'] = false
        ret['message'] = err.message

        if (err.code === undefined) {
            ret['errorCode']  = err.code 
            ret['errorState'] = err.state
            ret['errorStack'] = err.stackTraceTxt 
        }
    }
    // ---- Unset query tag
    //snowflake.execute({sqlText: "alter session unset query_tag"})

    return ret
$$;

--unset vars
UNSET (T2I_ROLE, T2I_SCH, T2I_WH);