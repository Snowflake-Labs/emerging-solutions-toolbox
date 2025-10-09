SET T2I_ROLE = CURRENT_ROLE();
SET T2I_SCH = 'ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR';
SET T2I_WH = 'ICEBERG_MIGRATOR_WH';

USE ROLE IDENTIFIER($T2I_ROLE);
USE WAREHOUSE IDENTIFIER($T2I_WH);

USE SCHEMA IDENTIFIER($T2I_SCH);

CREATE or REPLACE PROCEDURE iceberg_migration_dispatcher()
RETURNS VARIANT 
LANGUAGE JAVASCRIPT
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 3}}' 
EXECUTE AS CALLER 
AS
$$
    // ---------------------------------------------------------------------------------------------
    // Name: iceberg_migration_dispatcher
    // --------------------------------------------------------------------------------------------- 
    // Spools up dynamic tasks to migrate tables to iceberg.   The tables are defined in the 
    // MIGRATION_TABLE and have not already been processed.  Configuration is pulled from the 
    // SNOWFLAKE_TOOL_CONFIG table. 
    // ---------------------------------------------------------------------------------------------
    // Returns: JSON (Variant)
    //      result:    Succeeded = True/Failed = False 
    //      message:   Error message 
    // ---------------------------------------------------------------------------------------------
    // Date         Who         Description 
    // 07/15/2024   S Ramsey    Initial version based on data generator dispatcher    
    // 07/23/2024   S Ramsey    Add support for handling the truncations of timestamps to milliseconds
    // 09/16/2024   S Ramsey	Work through issues with quoted object names  
    // 09/19/2024   S Ramsey	Add support to transforming timezone 
    // 05/05/2025   S Ramsey    Added support to create the iceberg table in a new database/schema
    // 05/13/2025   S Ramsey    Fix some issues where code got corrupted 
    // 05/14/2025   S Ramsey    Add support for clustering target table
    // 05/15/2025   M Henderson Updated MIGRATE_TABLE_TO_ICEBERG call to include TABLE_TYPE and
    //                          TABLE_LOCATION, and DELTA_CATALOG_INTEGRATION
    // 09/17/2025   M Henderson Updated MIGRATE_TABLE_TO_ICEBERG call to include AWS Glue-related
    //                          fields.
    // ---------------------------------------------------------------------------------------------

    // ----- Standard tag 
    var std_tag = {origin: 'sf_sit', name: 'table_to_iceberg', version:{major: 1, minor: 3}}

    // ----- Define return value
    var ret = {}
    ret['result'] = true
    ret['message'] = null

    // ---- Do stuff 
    is_err = false

    try 
    {
        // ---- Pull configuration from config table
        sql_txt = `SELECT OBJECT_AGG(TOOL_PARAMETER, TOOL_VALUE::variant) AS SETTINGS FROM ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG WHERE TOOL_NAME IN ('ICEBERG_MIGRATOR', 'ALL')`
        // ret['debugSql'] = sql_txt 

        rs = snowflake.execute({sqlText: sql_txt})
        rs.next()
        settings = rs.getColumnValue('SETTINGS')   
        settings['procName'] = arguments.callee.name
        if (! (settings['timezone_conversion'] == 'CHAR' || settings['timezone_conversion'] == 'NTZ' )) {
            settings['timezone_conversion'] = 'NONE'
        }
        ret['settings'] = settings

        // ---- Check version 
        if (settings['version'] != '1.2') { 
            ret['result'] = false
            ret['message']  =  'Incompatible iceberg migration version, please upgrade to version 1.2'
        }
        else {
            // ---- Set query tag 
            std_tag['attributes'] = settings
            //snowflake.execute({sqlText: `alter session set query_tag='`+JSON.stringify(std_tag)+`'`})

            // ---- Get a count of the number of tables to migrate (cap based on config)
            sql_txt = `select seq8()+1 as tbl_seq, * from ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table where table_instance_id not in (select table_instance_id from ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table_log) and tbl_seq <= ${settings.max_tables_run}`
            //ret['debugSql'] = sql_txt 
            let tbl_rs = snowflake.execute({sqlText: sql_txt})
    
            // -- Error out if no tables to process 
            if ( tbl_rs.getRowCount() ==0 ) {
			    throw new Error('No tables to process')
		    }

            // ---- Create entry in run log table 
            // -- Get sequence value 
            var seq_rs = snowflake.execute({sqlText: 'SELECT ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.run_id_seq.nextval'})
            seq_rs.next()
            run_id = seq_rs.getColumnValue('NEXTVAL')
            ret['runID'] = run_id
            sql_txt = `insert into ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_run_log (run_id, start_time) values (${run_id}, current_timestamp())`
            snowflake.execute({sqlText: sql_txt})

            // ---- Create start task 
            start_task_name = `ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.IM_${run_id}_START`
            snowflake.execute({sqlText:  `CREATE OR REPLACE TASK ${start_task_name} WAREHOUSE = ${settings.warehouse_name} COMMENT = '${JSON.stringify(std_tag)}' AS SELECT NULL`})

            // ---- Loop through tables and create array of task names and stored procedure calls 
            task_cnt = 0
            var task_arr = []
            var tbl_id_arr = []
            while (tbl_rs.next())
			{
                
                var task_itm = {}
                task_itm['name'] = `ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.IM_${run_id}_${tbl_rs.TABLE_INSTANCE_ID}_${tbl_rs.TABLE_CATALOG}_${tbl_rs.TABLE_SCHEMA}_${tbl_rs.TABLE_NAME}`
                task_itm['name'] = task_itm['name'].replaceAll(" ", "_").toUpperCase()

                if (tbl_rs.TARGET_TABLE_CATALOG===tbl_rs.TABLE_CATALOG && tbl_rs.TARGET_TABLE_SCHEMA===tbl_rs.TABLE_SCHEMA ) {
                    tbl_rs.TARGET_TABLE_CATALOG = null; 
                    tbl_rs.TARGET_TABLE_SCHEMA = null; 
                } 

                //conditionally set applicable parameters to NULL                
                let tbl_location = (tbl_rs.TABLE_LOCATION == null) ? null : `'${tbl_rs.TABLE_LOCATION}'`;
                let tbl_catalog = (tbl_rs.TABLE_CATALOG == null) ? null : `'${tbl_rs.TABLE_CATALOG}'`;
                let tbl_schema = (tbl_rs.TABLE_SCHEMA == null) ? null : `'${tbl_rs.TABLE_SCHEMA}'`;
                let target_tbl_catalog = (tbl_rs.TARGET_TABLE_CATALOG == null) ? null : `'${tbl_rs.TARGET_TABLE_CATALOG}'`;
                let target_tbl_schema = (tbl_rs.TARGET_TABLE_SCHEMA == null) ? null : `'${tbl_rs.TARGET_TABLE_SCHEMA}'`;
                let update_frequency_mins = (tbl_rs.UPDATE_FREQUENCY_MINS == null) ? null : `${tbl_rs.UPDATE_FREQUENCY_MINS}`;
                let warehouse = (tbl_rs.WAREHOUSE == null) ? null : `'${tbl_rs.WAREHOUSE}'`;

                task_itm['proc'] = `ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.MIGRATE_TABLE_TO_ICEBERG(
                                        ${tbl_rs.TABLE_INSTANCE_ID}
                                        ,${run_id}
                                        ,'${tbl_rs.TABLE_TYPE}'
                                        ,${tbl_location}
                                        ,${tbl_catalog}
                                        ,${tbl_schema}
                                        ,'${tbl_rs.TABLE_NAME}'
                                        ,'${tbl_rs.TARGET_TYPE}'
                                        ,${target_tbl_catalog}
                                        ,${target_tbl_schema}
                                        ,'${tbl_rs.TARGET_TABLE_NAME}'
                                        ,${update_frequency_mins}
                                        ,${warehouse}
                                        ,'${settings.external_volume}'
                                        ,'${settings.delta_catalog_integration}'
                                        ,'${settings.location_pattern}'
                                        ,'${settings.aws_glue_external_access_integration}'
                                        ,parse_json('${JSON.stringify(tbl_rs.TABLE_CONFIGURATION)}')
                                        ,${settings.truncate_time}
                                        ,'${settings.timezone_conversion}'
                                        ,${settings.count_only_validation})`
                task_arr.push(task_itm)
                tbl_id_arr.push(tbl_rs.TABLE_INSTANCE_ID)
                task_cnt++

			}
            //ret['taskArray'] = task_arr
            //ret['tableIdArray'] = tbl_id_arr
            //ret['taskCnt'] = task_cnt

            // ---- Loop through array from about to create tasks 
            for (let i = 0; i < task_cnt; i++) {
                if ( i < settings.max_parallel_tasks ) {
                    after_task = start_task_name
                }
                else {
                    after_mod = i % settings.max_parallel_tasks
                    after_div = Math.floor(i/settings.max_parallel_tasks) - 1 
                    after_task = task_arr[(after_div * settings.max_parallel_tasks ) + after_mod].name                    
                } 

                snowflake.execute({sqlText: `CREATE OR REPLACE TASK ${task_arr[i].name} WAREHOUSE = ${settings.warehouse_name} COMMENT = '${JSON.stringify(std_tag)}' AFTER ${after_task} AS CALL ${task_arr[i].proc}`})
                snowflake.execute({sqlText: `ALTER TASK ${task_arr[i].name} RESUME`})
            }

            // ---- Create and link end task
            end_task_name = `ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.IM_${run_id}_END`           
            snowflake.execute({sqlText: `CREATE OR REPLACE TASK ${end_task_name} WAREHOUSE = ${settings.warehouse_name} COMMENT = '${JSON.stringify(std_tag)}' FINALIZE = ${start_task_name} AS CALL ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.iceberg_migration_end_task(${run_id})`})
            snowflake.execute({sqlText: `ALTER TASK ${end_task_name} RESUME`})

            // ---- Set all tables in the list to queued state 
            sql_txt = `insert into ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table_log(run_id, table_instance_id, state_code, log_time)
                select :1, table_instance_id, 'QUEUED', current_timestamp()
                from ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table where array_contains(table_instance_id, PARSE_JSON(:2))`
    	    snowflake.execute({sqlText: sql_txt, binds:[run_id, JSON.stringify(tbl_id_arr)]})

            // ---- Kick off start task 
            snowflake.execute({sqlText: `EXECUTE TASK  ${start_task_name}`})
        
            // ---- Cleanup
        }
    }
    catch(err)
    {
        is_err = true
		ret['finishTimeStamp'] = getCurrentTimestamp()
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

	// =============================================================================================
  	// modular functions for simplification and reuse
	// =============================================================================================

    // ---------------------------------------------------------------------------------------------
    // Name: getCurrentTimestamp
    // --------------------------------------------------------------------------------------------- 
    // Returns current time 
    // ---------------------------------------------------------------------------------------------
	function getCurrentTimestamp()
	{
		let currTS = new Date()
		return currTS.getFullYear() + "-" + (currTS.getMonth() + 1).toString().padStart(2, '0') + "-" + currTS.getDate().toString().padStart(2, '0') + " " + currTS.getHours().toString().padStart(2, '0') + ":" + currTS.getMinutes().toString().padStart(2, '0') + ":" + currTS.getSeconds().toString().padStart(2, '0')
	}
	
$$;

--unset vars
UNSET (T2I_ROLE, T2I_SCH, T2I_WH);