SET T2I_ROLE = CURRENT_ROLE();
SET T2I_SCH = 'ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR';
SET T2I_WH = 'ICEBERG_MIGRATOR_WH';

USE ROLE IDENTIFIER($T2I_ROLE);
USE WAREHOUSE IDENTIFIER($T2I_WH);

USE SCHEMA IDENTIFIER($T2I_SCH);

CREATE or REPLACE PROCEDURE iceberg_insert_delta_tables(external_volume VARCHAR, target_catalog VARCHAR, target_schema VARCHAR, table_list ARRAY)
RETURNS VARIANT 
LANGUAGE JAVASCRIPT
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 2}}' 
EXECUTE AS CALLER 
AS
$$
    // ---------------------------------------------------------------------------------------------
    // Name: iceberg_insert_delta_tables
    // --------------------------------------------------------------------------------------------- 
    // Inserts all or specified delta tables in the specified External Volume into the MIGRATION
    // TABLE to beconverted to Iceberg.
    // ---------------------------------------------------------------------------------------------
    //      external_volume (text)  The External Volume pointing to the delta tables
    //      target_catalog (text)   The target database to create the iceberg table
    //      target_schema (text)    The target schema to create the iceberg table
    //      table_list (array)      List of tables to migrate. The user can also pass a one element
    //                              array containing '*', to migrate all tables from External 
    //                              Volume's base URL.
    // ---------------------------------------------------------------------------------------------
    // Returns: JSON (Variant)
    //      result:    Succeeded = True/Failed = False 
    //      message:   Error message 
    // ---------------------------------------------------------------------------------------------
    // Date         Who         Description 
    // 05/15/2025   M Henderson Initial version
    // ---------------------------------------------------------------------------------------------

    // ----- Standard tag 
    var std_tag = {origin: 'sf_sit', name: 'table_to_iceberg', version:{major: 1, minor: 2}}

    // ----- Define return value
    var ret = {}
    ret['result'] = true
    ret['message'] = null

    // ---- Do stuff 
    is_err = false

    try 
    {
        //check table_list
        if ((TABLE_LIST.length == 0) || (TABLE_LIST.length > 1 && TABLE_LIST.includes("*"))) {
            ret['result'] = false
            ret['message']  =  "Invalid table_list array. Please resubmit with either a list of valid table names or a list only containing '*'."
        }

        //get cloud
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


        //add forward slash if the location does not have it:
        let location = storage_base_url;

        if (!location.endsWith("/")) {
            location = `${storage_base_url}/`;
        }

        //build insert statement
        let insert_stmt = `INSERT INTO ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.MIGRATION_TABLE(table_type, table_location, table_name, target_type, target_table_catalog, target_table_schema, target_table_name) VALUES\n`;

        //if table_list only contains '*', get list of all tables
        if (TABLE_LIST.length == 1 && TABLE_LIST[0] == '*') {
            snowflake.execute({sqlText: `LIST @ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.STAGE_${EXTERNAL_VOLUME};`});
            snowflake.execute({sqlText: `SELECT DISTINCT REGEXP_SUBSTR("name", '${location}([A-Za-z0-9_-]*)/', 1,1,'e') AS tbls
                                            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE tbls IS NOT NULL;`});
            let rset = snowflake.execute({sqlText: `SELECT ARRAY_AGG(DISTINCT tbls) WITHIN GROUP (ORDER BY tbls) 
                                            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`});
            //add insert values
            while (rset.next()) {
                TABLE_LIST = rset.getColumnValue(1);
            }
        }

        //add insert values
        for (let i = 0; i < TABLE_LIST.length; i++) {
            let tbl = TABLE_LIST[i];
            if (i==0) {
                insert_stmt += `('delta', '${location}', '${tbl}', 'snowflake', '${TARGET_CATALOG}', '${TARGET_SCHEMA}', '${tbl}')\n`
            } else {
                insert_stmt += `,('delta', '${location}', '${tbl}', 'snowflake', '${TARGET_CATALOG}', '${TARGET_SCHEMA}', '${tbl}')\n`
            }
        }

        //insert the tables into MIGRATION_TABLE
        snowflake.execute({sqlText: insert_stmt});
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
	
$$;

--unset vars
UNSET (T2I_ROLE, T2I_SCH, T2I_WH);