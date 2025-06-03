SET T2I_ROLE = CURRENT_ROLE();
SET T2I_SCH = 'ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR';
SET T2I_WH = 'ICEBERG_MIGRATOR_WH';

USE ROLE IDENTIFIER($T2I_ROLE);
USE WAREHOUSE IDENTIFIER($T2I_WH);

USE SCHEMA IDENTIFIER($T2I_SCH);

CREATE OR REPLACE PROCEDURE migrate_table_to_iceberg 
(
	TABLE_INSTANCE_ID FLOAT, 
	RUN_ID FLOAT, 
	TABLE_TYPE TEXT,
	TABLE_LOCATION TEXT,
	TABLE_CATALOG TEXT, 
	TABLE_SCHEMA TEXT, 
	TABLE_NAME TEXT,
	TARGET_TABLE_CATALOG TEXT, 
	TARGET_TABLE_SCHEMA TEXT, 
	EXTERNAL_VOLUME TEXT,
	CATALOG_INTEGRATION TEXT, 
	LOCATION_PATTERN TEXT,
	TABLE_CONFIGURATION VARIANT, 
	TRUNCATE_TIME BOOLEAN DEFAULT FALSE, 
	TIMEZONE_CONVERSION TEXT DEFAULT 'NONE', 
	COUNT_ONLY BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT 
LANGUAGE JAVASCRIPT
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 2}}' 
EXECUTE AS CALLER
AS
$$
    // ---------------------------------------------------------------------------------------------
    // Name: migrate_table_to_iceberg
    // --------------------------------------------------------------------------------------------- 
    // The procedure will migrate a single table to iceberg bases on the parameters passed in. 
    // ---------------------------------------------------------------------------------------------
    // Parameters: 
   	//      table_instance_id (integer) 	The table instance id
	//      run_id (integer)            	The identifier for the run
	//		table_type (varchar)			The source table type
	//		table_location (varchar)		The base url of the location of the table (non-FDN tables)
    //      table_catalog (varchar)     	Database where table to load is located  
    //      table_schema (varchar)      	Schema where table to load is located 
    //      table_name (varchar)        	Name of table to migrate 
	//      target_table_catalog (varchar)  Target database where table to load is located  
	//      target_table_schema (varchar)   Target schema where table to load is located 
    //      external_volume (varchar)  	 	Name of the external volume where the iceberg table will be created  
	//		catalog (varchar)				Catalog Integration (for Delta Tables)
    //      location_pattern (varchar)  	The naming pattern for the iceberg table where the following 
	//										are substituted ${TABLE_CATALOG}, ${TABLE_SCHEMA} and 
	// 										${TABLE_NAME} with the values from the parameters above
	//      table_configuration (variant)	Additional table configuration information 
	//											cluster_key (text) -  String with clustering column list 
	//      truncate_time (boolean)			Truncate the time to milliseconds
	//		timezone_conversion (varchar)	How are timezone conversion handled 
	//										    NONE - Fail if table has a timstamp_tz column 
	//											CHAR - Convert timstamp_tz to character
	//											NTZ  - Change to timstamp_tz to timstamp_ntz as UTC
	//		count_only (boolean)			Do a count only check
    // ---------------------------------------------------------------------------------------------
    // Returns: JSON (Variant)
    //      result:  	Succeeded = True/Failed = False
    //      message:   	Message returned by failed outside of the load
    // ---------------------------------------------------------------------------------------------
    // Date         Who         Description 
	// 07/15/2024   S Ramsey 	Initial version based on data generator roots 
	// 07/24/2024   S Ramsey    Add support for handling the truncations of timestamps to milliseconds
	// 09/16/2024   S Ramsey	Work through issues with quoted object names  
	// 09/19/2024   S Ramsey	Add support to transforming timezone 
	// 05/05/2025   S Ramsey    Added support to create the iceberg table in a new database/schema
    // 05/13/2025   S Ramsey    Fix some issues where code got corrupted 
	// 05/14/2025   S Ramsey    Add support for clustering target table
	// 05/15/2025	M Henderson	Add support to migrate other table types to Iceberg. Currently, only
	//							Snowflake (FDN) and Delta tables are supported.
    // ---------------------------------------------------------------------------------------------
    // ----- Standard tag 
    var std_tag = {origin: 'sf_sit', name: 'table_to_iceberg', version:{major: 1, minor: 2}}

    // ----- Define return value
    var ret = {}
    ret['result'] = true
    ret['message'] = null
    ret['debugSql'] = null
	ret['startTimeStamp'] = getCurrentTimestamp()

    var fk_tbl_cnt = 0
	var dbgstep = 0
    var sql_pk_drp_tbl = ""
	var errorModule = "Main"
	var sql_txt = ""
	
	var v_replace =  (typeof TARGET_TABLE_CATALOG === 'undefined') 
	if ( v_replace ) {
		var v_icename = `"${TABLE_CATALOG}"."${TABLE_SCHEMA}"."${TABLE_NAME}_${RUN_ID}_tmp"`
	}
	else {
		var v_icename = `"${TARGET_TABLE_CATALOG}"."${TARGET_TABLE_SCHEMA}"."${TABLE_NAME}"`
	}

	if (TABLE_TYPE.toLocaleLowerCase() == 'delta') {
		v_icename = v_icename.replace(/"/g, '');
	}
	
	var v_location = ""
	
	// ---- Set the query tag so we can identify all the SQL executing in the procedure
    var tags = {}
	tags['tableInstId'] = TABLE_INSTANCE_ID
	tags['runId'] = RUN_ID
	tags['table_type'] = TABLE_TYPE
	tags['table_location'] = TABLE_LOCATION
    tags['db'] = TABLE_CATALOG
    tags['schema'] = TABLE_SCHEMA
	tags['targetDb'] = TABLE_CATALOG
    tags['targetSchema'] = TABLE_SCHEMA
    tags['table'] = TABLE_NAME
    tags['extVolume'] = EXTERNAL_VOLUME
	tags['catalogIntegration'] = CATALOG_INTEGRATION
    tags['locPattern'] = LOCATION_PATTERN
	tags['tableConfig'] = TABLE_CONFIGURATION
	tags['truncTime'] = TRUNCATE_TIME
	tags['timeZoneConvert'] = TIMEZONE_CONVERSION
	tags['countOnlyValidation'] = COUNT_ONLY
    tags['procName'] = arguments.callee.name
	ret['parameters'] = tags

    // ---- Set the query tag so we can identify all the SQL executing in the procedure (Cannot alter session when running as owner)
	std_tag['attributes'] = tags
    //snowflake.execute({sqlText: `alter session set query_tag='`+JSON.stringify(std_tag)+`';`})

    // ---- Do stuff 
    is_err = false;

    try 
    {
		// ---- Create row in table log
    	snowflake.execute({sqlText: `insert into ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table_log (table_instance_id, run_id, state_code, log_time, log_message) values (:1, :2, 'RUNNING', current_timestamp(), null)`, binds:[TABLE_INSTANCE_ID, RUN_ID]})

		let table_catalog = TABLE_CATALOG;
		let table_schema = TABLE_SCHEMA;

        //TODO: If TABLE_TYPE is DELTA, create a "temp" external table to validate
		if (TABLE_TYPE.toLocaleLowerCase() == 'delta') {
			//get current cloud
			let cloud = "";
			let storage_base_url = "";
			let credential_str = "";

			//describe external volume to get necessary details
			snowflake.execute({sqlText: `DESCRIBE EXTERNAL VOLUME ${EXTERNAL_VOLUME};`});

			let rset = snowflake.execute({sqlText: `SELECT "property_value" 
													FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) 
													WHERE LOWER("property") = 'storage_location_1';`});

			while (rset.next()) {
				property_value = JSON.parse(rset.getColumnValue(1));
				cloud = property_value['STORAGE_PROVIDER'];
				storage_base_url = property_value['STORAGE_BASE_URL'];
				
				if (cloud == "S3") {
					let storage_aws_role_arn = property_value['STORAGE_AWS_ROLE_ARN'];
					credential_str = `STORAGE_AWS_ROLE_ARN = '${storage_aws_role_arn}'`;
				}

				if (cloud == "AZURE") {
					let azure_tenant_id = property_value['AZURE_TENANT_ID'];
					credential_str = `AZURE_TENANT_ID = '${azure_tenant_id}'`;
				}
			}
			
			//create storage location, if it doesn't exist
			snowflake.execute({sqlText: `CREATE STORAGE INTEGRATION IF NOT EXISTS SI_${EXTERNAL_VOLUME}
											TYPE = EXTERNAL_STAGE
											STORAGE_PROVIDER = '${cloud}'
											ENABLED = TRUE
											${credential_str}
											STORAGE_ALLOWED_LOCATIONS = ('${storage_base_url}')
											COMMENT = '${JSON.stringify(std_tag)}';`});

			//create stage
			snowflake.execute({sqlText: `CREATE STAGE IF NOT EXISTS ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.STAGE_${EXTERNAL_VOLUME}
											URL = '${storage_base_url}'
											STORAGE_INTEGRATION = SI_${EXTERNAL_VOLUME}
											DIRECTORY = (
												ENABLE = true
											)
											COMMENT = '${JSON.stringify(std_tag)}';`});

			//create file format, if it doesn't exist
			snowflake.execute({sqlText: `CREATE FILE FORMAT IF NOT EXISTS ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.FF_PARQUET TYPE = parquet COMMENT = '${JSON.stringify(std_tag)}';`});

			//create "temp" external table - this is used to validate the delta table before creating the iceberg table
			snowflake.execute({sqlText: `CREATE OR REPLACE EXTERNAL TABLE ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.${TABLE_NAME}
											USING TEMPLATE(
												SELECT ARRAY_AGG(OBJECT_CONSTRUCT('COLUMN_NAME',COLUMN_NAME, 'TYPE',TYPE, 'NULLABLE', NULLABLE, 'EXPRESSION',EXPRESSION))
												FROM TABLE(
													INFER_SCHEMA(
														LOCATION=>'@ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.STAGE_${EXTERNAL_VOLUME}/${TABLE_NAME}/',
														FILE_FORMAT=>'ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.FF_PARQUET'
													)
												)
											)
											WITH LOCATION = @ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.STAGE_${EXTERNAL_VOLUME}/${TABLE_NAME}/
											FILE_FORMAT = ( TYPE = PARQUET )
											AUTO_REFRESH = FALSE
											REFRESH_ON_CREATE = FALSE
											COMMENT = '${JSON.stringify(std_tag)}'
										;`});

			snowflake.execute({sqlText: `ALTER EXTERNAL TABLE ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.${TABLE_NAME} REFRESH;`});

			table_catalog = 'ICEBERG_MIGRATOR_DB';
			table_schema = 'ICEBERG_STAGING';

		}

		
		// -- Validates the table is good to go and gets other table properties
		chk = checkValid(table_catalog, table_schema)
	
		if ( !chk.result ) {
			throw new Error(chk.message);
		}

		//set table name
		var v_tablename = `"${table_catalog}"."${table_schema}"."${TABLE_NAME}"`
		var v_oldname = `"${table_catalog}"."${table_schema}"."${TABLE_NAME}_${RUN_ID}_old"`

		if (TABLE_TYPE.toLocaleLowerCase() == 'delta') {
			v_tablename = v_tablename.replace(/"/g, '');
			v_oldname = v_oldname.replace(/"/g, '');
		}

		// -- Build location 
		v_location = LOCATION_PATTERN
		if (v_replace) {
			v_location = v_location.replaceAll("${TABLE_CATALOG}",TABLE_CATALOG.replaceAll(" ", "-"))
			v_location = v_location.replaceAll("${TABLE_SCHEMA}",TABLE_SCHEMA.replaceAll(" ", "-"))
		}
		else {
			v_location = v_location.replaceAll("${TABLE_CATALOG}",TARGET_TABLE_CATALOG.replaceAll(" ", "-"))
			v_location = v_location.replaceAll("${TABLE_SCHEMA}",TARGET_TABLE_SCHEMA.replaceAll(" ", "-"))
		}
		v_location = v_location.replaceAll("${TABLE_NAME}",TABLE_NAME.replaceAll(" ", "-"))

		// -- Create iceberg table
		sql_txt = createIcebergDDL(TABLE_TYPE, TABLE_LOCATION, v_tablename, v_icename, EXTERNAL_VOLUME, CATALOG_INTEGRATION, v_location, TABLE_CONFIGURATION, TRUNCATE_TIME, TIMEZONE_CONVERSION)
		ret['debugSql'] = sql_txt
		snowflake.execute({sqlText: sql_txt});

        // -- Change the grants on external table
		if ( !cloneTablePermissions(v_tablename, v_icename) ){
			throw(new Error('Failed to set permissions'))
		}

		// -- Get list of columns in table 
		tblCols = getColumnList(v_tablename, TRUNCATE_TIME, TIMEZONE_CONVERSION)
		//ret['tblCols'] = tblCols;

		// ---- If provided cluster then add order by 
		order_by = '' 
		if ( TABLE_CONFIGURATION !== null) {
			if ( TABLE_CONFIGURATION.cluster_key !== null) {
				order_by = `ORDER BY ${TABLE_CONFIGURATION.cluster_key}`	
			} 
		}

		if (TABLE_TYPE.toLocaleLowerCase() == 'snowflake') {
			// -- Insert data into iceberg table  
			sql_txt = `INSERT INTO ${v_icename} (${tblCols.insertList}) SELECT ${tblCols.selectList} FROM ${v_tablename} ${order_by}`
			ret['debugSql'] = sql_txt
			snowflake.execute({sqlText: sql_txt});

			// -- Validate the data in the table(s) match
			if ( COUNT_ONLY ){
				sql_txt = `
				WITH a AS 
				(
					SELECT COUNT(*) AS hag FROM ${v_icename}
					MINUS 
					SELECT COUNT(*) AS hag FROM ${v_tablename}
				)
				SELECT COUNT(*) AS cnt 
				FROM a`
			}
			else {
			sql_txt = `
				WITH a AS 
				(
					SELECT HASH_AGG(${tblCols.iceSelectList}) AS hag FROM ${v_icename}
					MINUS 
					SELECT HASH_AGG(${tblCols.selectList}) AS hag FROM ${v_tablename}
				)
				SELECT COUNT(*) AS cnt 
				FROM a`
			}
			ret['debugSql'] = sql_txt
			let v_cnt_rs = snowflake.execute({sqlText: sql_txt})
			v_cnt_rs.next()

			// -- If validation OK do the swap.  
			if (v_cnt_rs.CNT == 0) {
				if ( v_replace ) {
					// -- Rename normal table to some backup name 
					sql_txt = `ALTER TABLE ${v_tablename} RENAME TO ${v_oldname}`
					snowflake.execute({sqlText: sql_txt});

					// -- Rename new iceberg table to old table name 
					sql_txt = `ALTER ICEBERG TABLE ${v_icename} RENAME TO ${v_tablename}`
					snowflake.execute({sqlText: sql_txt});

					// -- Drop normal table
					sql_txt = `DROP TABLE ${v_oldname}`
					snowflake.execute({sqlText: sql_txt});
				}
			}
			else {
				throw(new Error(`Migration validation failed.  Total mismatched rows (${v_cnt_rs.CNT})`))
			}
		}
        
		//if TABLE_TYPE is 'delta', drop external table
		if (TABLE_TYPE.toLocaleLowerCase() == 'delta') {
			snowflake.execute({sqlText: `DROP EXTERNAL TABLE ${v_tablename};`});
		}
    }
    catch(err)
    {
        is_err = true;
		ret['finishTimeStamp'] = getCurrentTimestamp()
		ret['result'] = false;
        ret['message'] = err.message;
    }

	// -- Cleanup if failed
	if (is_err) {
		snowflake.execute({sqlText: `DROP ICEBERG TABLE IF EXISTS ${v_icename}`});
	}

	ret['finishTimeStamp'] = getCurrentTimestamp()

 	// ---- Update metadata tables as complete 
	if (ret['result']) {
		ret['debugSql'] = null;
		snowflake.execute({sqlText: `insert into ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table_log (table_instance_id, run_id, state_code, log_time, log_message) values (:1, :2, 'COMPLETE', current_timestamp(), null)`, binds:[TABLE_INSTANCE_ID, RUN_ID]})
	}
	else {
    	snowflake.execute({sqlText: `insert into ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table_log (table_instance_id, run_id, state_code, log_time, log_message) values (:1, :2, 'FAILED', current_timestamp(), :3)`, binds:[TABLE_INSTANCE_ID, RUN_ID, ret['message']]})
	}

	 // ---- Reset query tag (Cannot alter session when running as owner)
	 //snowflake.execute({sqlText: "alter session unset query_tag;"});

	 return ret;


	// =============================================================================================
  	// Modular functions for simplification and reuse
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
	
    // ---------------------------------------------------------------------------------------------
    // Name: cloneTablePermissions
    // --------------------------------------------------------------------------------------------- 
	// Parameters: 
   	//      source_table (text) Source table to pull permissions from
	//      target_table (text) Target table to apply permissions on
    // ---------------------------------------------------------------------------------------------
    // Returns success/failure
    // ---------------------------------------------------------------------------------------------
	function cloneTablePermissions(source_table, target_table)
	{
    	var valRet = true 
		try 
		{
			// --- Get permissions from source table 
			let table_rs = snowflake.execute({sqlText: `SHOW GRANTS ON TABLE ${source_table}`})

			// --- Loop through results and grant to new table 
			while (table_rs.next())
			{
				sql_txt = `GRANT ${table_rs.privilege} ON TABLE ${target_table} TO ROLE ${table_rs.grantee_name}`
				snowflake.execute({sqlText: sql_txt});			
			}
		}
		catch(err) {
			valRet = false 
		}

		return valRet
	}

    // ---------------------------------------------------------------------------------------------
    // Name: getColumnList
    // --------------------------------------------------------------------------------------------- 
	// Parameters: 
   	//      source_table (text) Source table to pull list from
	//		truncate_time (boolean)	Truncate the time to milliseconds 
	//		timezone_conversion (text) How to handle timestamps with timezones 
    // ---------------------------------------------------------------------------------------------
    // Returns: Collection with select list and insert list 
    // ---------------------------------------------------------------------------------------------
	function getColumnList(source_table, truncate_time, timezone_conversion)
	{
		// --- Get permissions from source table 
    	var retList = {}
		retList['insertList'] = ''
		retList['selectList'] = ''
		retList['iceSelectList'] = ''

		// -- Validates the table exists and is not an iceberg table 
		let column_rs = snowflake.execute({sqlText: `DESCRIBE TABLE ${source_table}`})

		// --- Loop through results and grant to new table 
		i = 0
		while (column_rs.next())
		{
			// -- If not first row add comma 
			if ( i >= 1 ){
				retList['insertList'] += ','
				retList['selectList'] += ','
				retList['iceSelectList'] += ','
			}

			// -- Add name to insert list 
			retList['insertList'] += `"${column_rs.name}"`

			// -- depending on data type transform data 
			regex = new RegExp('TIMESTAMP_TZ.*')
			if (regex.test(column_rs.type))  {
				if (timezone_conversion=='NTZ') {
					retList['selectList'] += `CAST(CONVERT_TIMEZONE('UTC', CAST("${column_rs.name}" AS TIMESTAMP_TZ(6))) AS TIMESTAMP_NTZ(6))`
					retList['iceSelectList'] += `CAST(CONVERT_TIMEZONE('UTC', CAST("${column_rs.name}" AS TIMESTAMP_TZ(6))) AS TIMESTAMP_NTZ(6))`
				}
				else if (timezone_conversion=='CHAR') {
					regex = /TIMESTAMP_TZ\(([0-9])\)/gi
					prec = column_rs.type.replaceAll(regex, '$1')
					
					if (prec != '0') { 
						retList['selectList'] += `to_char("${column_rs.name}", 'YYYY-MM-DD HH24:MI:SS.FF${prec} TZHTZM')`
					}
					else {
						retList['selectList'] += `to_char("${column_rs.name}", 'YYYY-MM-DD HH24:MI:SS TZHTZM')`
					}
					retList['iceSelectList'] += `"${column_rs.name}"`
				} 
			}
			else if ( truncate_time ){
				// -- If Time or Timestap then round down to microseconds 
				regex = new RegExp('TIME.*')
				if (regex.test(column_rs.type)) {
					retList['selectList'] += `date_trunc('microseconds', "${column_rs.name}")`
					retList['iceSelectList'] += `date_trunc('microseconds', "${column_rs.name}")`
				}
				else {
					retList['selectList'] += `"${column_rs.name}"`
					retList['iceSelectList'] += `"${column_rs.name}"`
				}		
			} 
			else {
				retList['selectList'] += `"${column_rs.name}"`
				retList['iceSelectList'] += `"${column_rs.name}"`
				
			}
			i++
		}
		return retList
	}


    // ---------------------------------------------------------------------------------------------
    // Name: createIcebergDDL
    // ---------------------------------------------------------------------------------------------
	// Parameters: 
	//		table_type (text)				The source table type
	//		table_location (text)			The base url of the location of the table (non-FDN tables)
	// 		source_table (text) 			Source table to pull permissions from
	//     	target_table (text) 			Target iceberg table table to create 
	//	   	volume (text)					Volume name to create the iceberg table on 
	//		catalog_int (text)				Catalog Integration (for Delta Tables)
	//     	location (text)					Location of the delta tables on the external volume
	//      table_configuration (variant)	Additional table configuration information 
	//		truncate_time (boolean)			Truncate the time to milliseconds 
	//		timezone_conversion (text) 		How to handle timestamps with timezones 
    // ---------------------------------------------------------------------------------------------
    // Returns DDL to create iceberg table 
    // ---------------------------------------------------------------------------------------------
	function createIcebergDDL(table_type, table_location, source_table, target_table, volume, catalog_int, location, table_configuration, truncate_time, timezone_conversion)
	{
		// --- Get permissions from source table 
    	var valRet = '' 
		
		if (table_type.toLocaleLowerCase() == 'snowflake') {
			// ---- Get DDL for source table 
			let table_rs = snowflake.execute({sqlText: `SELECT GET_DDL('TABLE',  '${source_table}', true) AS ddl`})
			table_rs.next()
			valRet = table_rs.DDL 

			// ---- Replace VARCHAR
			regex = /VARCHAR\(\d*\)/gi
			valRet = valRet.replaceAll(regex, 'STRING')

			// ---- Replace float 
			regex = /( )FLOAT([ ,])/gi
			valRet = valRet.replaceAll(regex, '$1double$2')	

			// ---- Replace timestamp_tz based on definitions 
			regex = /(TIMESTAMP_TZ.*)\(([0-9])\)/gi
			if (timezone_conversion=='NTZ') {
				valRet = valRet.replaceAll(regex, 'TIMESTAMP_NTZ(6)')
			}
			else if (timezone_conversion=='CHAR') {
				valRet = valRet.replaceAll(regex, 'STRING')
			} 
			
			// ---- If time is truncated then replace time precision 
			if ( truncate_time ){
				regex = /(TIME.*)\([0-9]\)/gi
				valRet = valRet.replaceAll(regex, '$1(6)')
			}

			// ---- Replace create statement and target table 
			regex = /create or replace TABLE.*\(/i
			valRet = valRet.replace(regex, `create or replace iceberg table ${target_table} (`)		
		
			// ---- Replace target table
			//valRet = valRet.replace(source_table, target_table)

			// ---- If provided replace cluster 
			if ( table_configuration !== null) {
				if ( table_configuration.cluster_key !== null) {
					valRet = valRet.replace(';', ` CLUSTER BY (${table_configuration.cluster_key});`)	
				} 
			}

			// ---- Add additional iceberg config
			valRet = valRet.replace(';', ` CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = '${volume}' BASE_LOCATION = '${location}' COMMENT = '${JSON.stringify(std_tag)}'`)

		} else if (table_type.toLocaleLowerCase() == 'delta') {			
			//create Iceberg from Delta table DDL
			base = target_table.split(".")[2];

			valRet = `CREATE OR REPLACE ICEBERG TABLE ${target_table}
						CATALOG=${catalog_int}
						EXTERNAL_VOLUME = ${volume}
						BASE_LOCATION = '${base}' 
						AUTO_REFRESH = TRUE
						COMMENT = '${JSON.stringify(std_tag)}';`;
		}
	
		return valRet
	}


    // ---------------------------------------------------------------------------------------------
    // Name: checkValid
    // --------------------------------------------------------------------------------------------- 
    // Returns a structure with a result of true if valid and a message for failed validation 
    // ---------------------------------------------------------------------------------------------
	function checkValid(tbl_catalog, tbl_schema)
	{
    	var valRet = {}
    	valRet['result'] = true

		// -- Validates the table exists and is not an iceberg table 
		v_csql = `select table_owner, is_iceberg from ${tbl_catalog}.information_schema.tables where table_schema = '${tbl_schema}' and LOWER(table_name) = LOWER('${TABLE_NAME}')`
		let v_chk_rs = snowflake.execute({sqlText: v_csql})

		if (v_chk_rs.getRowCount() == 0) {
			valRet['result'] = false	
			valRet['message'] = `${v_tablename} table does not exist`
		}
		else {
			v_chk_rs.next()

			// -- Grab table owner while here 
			valRet['owner'] = v_chk_rs.TABLE_OWNER

			// -- Check if iceberg 
			if (v_chk_rs.IS_ICEBERG == 'YES') {
				valRet['result'] = false	
				valRet['message'] = `${v_tablename} is an iceberg table`
			}
			else {
				// -- Determine if timezone conversion 
				if (TIMEZONE_CONVERSION=='NONE'){
					v_timestamp_tz = 'TIMESTAMP_TZ'
				}
				else {
					v_timestamp_tz = '--'
				}
				// -- Validates the table does not contain data types that are not supported by iceberg 
				// if TABLE_TYPE is delta, omit the VALUE column from the source external table
				let value_col_check = TABLE_TYPE.toLocaleLowerCase() == "delta" ? "and column_name != 'VALUE'" : "";

				v_csql = `select count(*) as cnt, array_to_string(array_agg(column_name || ' (' || data_type || ')'), ', ') as cols
							from ${tbl_catalog}.information_schema.columns
							where table_schema = '${tbl_schema}' 
							and table_name = '${TABLE_NAME}'
							and data_type in ('${v_timestamp_tz}','VARIANT','OBJECT','ARRAY','GEOGRAPHY','GEOMETRY','VECTOR')
							${value_col_check}`
				
				let v_chk_rs = snowflake.execute({sqlText: v_csql})
				v_chk_rs.next() 
				if (v_chk_rs.CNT != 0) {
					valRet['result'] = false	
					valRet['message'] = `${v_tablename} contains the following columns that are not supported in iceberg: ${v_chk_rs.COLS}`
				}		
			}
		}
		return (valRet)
	}

  $$
  ;

--unset vars
UNSET (T2I_ROLE, T2I_SCH, T2I_WH);