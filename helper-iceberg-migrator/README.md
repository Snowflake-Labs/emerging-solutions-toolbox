# Iceberg Migrator

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="200" align="right";">
</a>Iceberg Migrator is a solution created by Snowflake’s Solution Innovation Team (SIT). The solution allows a customer to perform bulk migrations of native Snowflake and Delta tables to Iceberg tables. 

This tool is ideal for the following use cases:
    - Migrating existing Snowflake tables to Iceberg files at a designated cloud storage location. This may be beneficial for customers who want to make their  Snowflake data available outside of Snowflake, but have Snowflake manage the Iceberg metadata and catalog.
    - Delta Lake Integration. This may be beneficial for customers who want their Delta Lake tables directly integrated with Snowflake, for querying and managing Delta Lake data within the Snowflake ecosystem.

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is provided `as is` and without warranty. Snowflake will not offer any support for the use of the sample code. The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features. We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Copyright (c) [Current Year] Snowflake Inc. All Rights Reserved.

## Pre-requisites

### Migrating Snowflake Tables:

- External:
    - a storage bucket/blob in the same region as the Snowflake account this solution will be installed.
- Snowflake:
    - Granted either the `ACCOUNTADMIN` role or a role with the `CREATE EXTERNAL VOLUME` privilege.
    - An existing Snowflake EXTERNAL VOLUME registered to a storage bucket/blob in the same region as the Snowflake account this solution will be installed.
    - This tool can be used to create the EXTERNAL VOLUME.
        -   Visit https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume for instructions.
    - Schema to install the metadata tables, views and procedures
    - Warehouse that will be used for migration to iceberg
- Role Permissions:
    - Ability to query and update all the object in the tool schema
    - Ability to query all the views in the tool schema
    - Ability to create and execute procedures in the tool schema
    - Ability to use the warehouse for processing
    - Ability to create, execute, monitor and drop tasks in the tool schema
    - Usage of the external volume
    - Ability to create and drop objects in the databases and schemas that contain table to be modified
    - Ability to create objects in the target databases and schemas (if not replacing existing Snowflake tables)
    - Ability to set permissions and change ownership of the newly created iceberg tables

### Delta Lake Integration:

- External:
    - Table Delta files stored in cloud storage (S3, AZURE, or GCS)
- Snowflake:
    - Granted either the `ACCOUNTADMIN` role or a role with the `CREATE EXTERNAL VOLUME` privilege..
    - An existing Snowflake EXTERNAL VOLUME
        - This tool can be used to create an EXTERNAL VOLUME. 
        - Visit https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume for instructions for the cloud storage where the Delta files reside. 
        -  **NOTE:** the External Volume's `STORAGE_BASE_URL` must contain directories for each table to migrate. Each table directory must contain that table's Delta files
    - Granted either the `ACCOUNTADMIN` role or a role with the `CREATE INTEGRATION` privilege.
    - Granted either the `ACCOUNTADMIN` role or a role with the `CREATE STORAGE INTEGRATION` privilege.
- Role Permissions:
    - Ability to query and update all the object in the tool schema 
    - Ability to query all the views in the tool schema 
    - Ability to create and execute procedures in the tool schema
    - Ability to use the warehouse for processing
    - Ability to create, execute, monitor and drop tasks in the tool schema 
    - Ability to create and drop stages in the staging schema 
    - Ability to create and drop the Storage Integration
    - Usage of the External Volume
    - Usage of Storage Integration
    - Ability to create and drop objects in the databases and schemas that contain table to be modified 
    - Ability to create Iceberg Tables in the target databases and schemas 

## Installation

This repo includes the `im_install` notebook, located in the `installer/` directory, that installs the Iceberg Migrator solution and SiS app. The notebook should be imported in the appicable Snowflake account and executed, using a role with the specified permissions. Refer to the notebook for more details.

**NOTE:** the `im_install` notebook is standalone and always references the latest version of Iceberg Migrator. This allows the notebook to be distributed separately from the full repo.

## Running and monitoring (Streamlit)

- Follow the instructions in the included Streamlit app to migrate Snowflake or Delta tables, monitor the migration logs, create External Volumes and Catalog Integrations, or update settings.

## Running and monitoring (non-Streamlit)

- Insert list of tables into the **_migration_table_** table.
    ```
    insert into iceberg_migrator.migration_table
    (
        table_catalog, 
        table_schema, 
        table_name 
    )
    values 
        ('TEST_DB','TEST_SCHEMA','TEST_TABLE_1'), 
        ('TEST_DB','TEST_SCHEMA','TEST_TABLE_2'), 
        ('TEST_DB','TEST_SCHEMA','TEST_TABLE_2'); 
    ```

- Once the table is populated then execute **_iceberg_migration_dispatcher_** procedure.

    ```
    call ICEBERG_MIGRATION_DISPATCHER();
    ```
    This will return a variant/JSON 
    ```
        {
            "message": null,
            "result": true,
            "runID": 502,
            "settings": {
                "external_volume": "iceberg_external_volume",
                "location_pattern": "${TABLE_CATALOG}/${TABLE_SCHEMA}/${TABLE_NAME}",
                "max_parallel_tasks": "3",
                "max_tables_run": "50",
                "procName": "ICEBERG_MIGRATION_DISPATCHER",
                "truncate_time": "TRUE",
                "version": "1.0",
                "warehouse_name": "sramsey_wh"
            }
        }
    ```
    Key elements from the JSON are the **_result_** and **_message_**, which indicates if the code was successfully executed **_result = true_** and the associated error **_message_**.  The **_runID_** is the number that is associated with the log entries in the run and table conversion log.  The **_settings_** are the details pulled in from the configuration table.

- Monitor process

    The first thing to check would be the tasks.  You will either see tasks, meaning that tasks are running, or nothing will be returned meaning all the migrations have completed. 
    ```
    show tasks like 'IM%';
    ```

    You can also query log tables to see the status of the current/last executed iceberg migration.
    ```
    select mt.*, mtl.* exclude (table_instance_id) 
    from migration_table_log mtl
    inner join migration_table mt
       on mt.table_instance_id = mtl.table_instance_id
    where log_time >= (select max(start_time) from migration_run_log)
    order by log_time desc;
    ```
- Table migration error message types 
    - Table does not exist 
    - Table is already converted to iceberg 
    - Source table contains data types that are not supported by iceberg
    - Other Snowflake errors that were not handled by the migrator code.  These will be returned with all the details from the Snowflake error message.  