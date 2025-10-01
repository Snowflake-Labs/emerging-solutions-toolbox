SET T2I_ROLE = CURRENT_ROLE();
SET T2I_SCH = 'ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR';
SET T2I_WH = 'ICEBERG_MIGRATOR_WH';
SET T2I_MAX_PAR_TASKS = '3';
SET T2I_MAX_TBLS = '50';
SET T2I_EV = '';
SET T2I_CI = '';
SET T2I_EAI = '';

USE ROLE IDENTIFIER($T2I_ROLE);
USE WAREHOUSE IDENTIFIER($T2I_WH);

USE SCHEMA IDENTIFIER($T2I_SCH);

-- RUN_ID_SEQ
CREATE OR REPLACE SEQUENCE RUN_ID_SEQ
START WITH 1
INCREMENT BY 1;
--- TABLE_ID_SEQ
CREATE OR REPLACE SEQUENCE TABLE_ID_SEQ
START WITH 1
INCREMENT BY 1;

--  RUN_MIGRATION_LOG
CREATE OR REPLACE TABLE MIGRATION_RUN_LOG
(
 RUN_ID          integer    NOT NULL    DEFAULT  RUN_ID_SEQ.NEXTVAL COMMENT 'Identifier for the run of the iceberg migration tool',
 START_TIME      timestamp              COMMENT 'Start time of the run of the iceberg migration',
 END_TIME        timestamp              COMMENT 'End time of the iceberg migration',
 CONSTRAINT PK_1 PRIMARY KEY ( RUN_ID )
)
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 3}, "description":"Runtime information for the complete execution of a group of tables"}';

--  MIGRATION_TABLE
CREATE OR REPLACE TABLE MIGRATION_TABLE
(
 TABLE_INSTANCE_ID     integer      NOT NULL DEFAULT TABLE_ID_SEQ.NEXTVAL COMMENT 'Identifier of table to be migrated',
 TABLE_TYPE            varchar      NOT NULL COMMENT 'The source table type',
 TABLE_LOCATION        varchar      NULL COMMENT 'The base url of the location of the table (non-FDN tables)',
 TABLE_CATALOG         varchar      NULL COMMENT 'Database that the source table belongs to',
 TABLE_SCHEMA          varchar      NULL COMMENT 'Schema that the source table belongs to.',
 TABLE_NAME            varchar      NOT NULL COMMENT 'Name of source table',
 TARGET_TYPE           varchar      NOT NULL COMMENT 'Where the iceberg table will be queried from',
 TARGET_TABLE_CATALOG  varchar      NULL COMMENT 'Database that the target table belongs to, null if conversion of source table in place',
 TARGET_TABLE_SCHEMA   varchar      NULL COMMENT 'Schema that the target table belongs to, null if conversion of source table in place',
 TARGET_TABLE_NAME     varchar      NOT NULL COMMENT 'The name of the target table',
 UPDATE_FREQUENCY_MINS integer      NULL COMMENT 'The frequency at which to sync Snowflake-managed metadata to another catalog',
 WAREHOUSE             varchar      NULL COMMENT 'The warehouse used to sync Snowflake-managed metadata to another catalog',
 TABLE_CONFIGURATION   variant      NULL COMMENT 'Additional table configuration parameters',
 INSERT_DATE           timestamp    NOT NULL DEFAULT current_timestamp(),
 CONSTRAINT PK_1 PRIMARY KEY ( TABLE_INSTANCE_ID )
)
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 3}, "description":"Queue of tables to migrate to iceberg"}';

--  MIGRATION_LOG
CREATE OR REPLACE TABLE MIGRATION_TABLE_LOG
(
 TABLE_INSTANCE_ID      integer     NOT NULL    COMMENT'Identifier of table to be migrated',
 RUN_ID                 integer     NOT NULL    COMMENT 'Identifier for the run of the iceberg migration tool',
 STATE_CODE             varchar(10) NOT NULL    COMMENT 'State code: Queued, Running, Failed, Complete',
 LOG_TIME               timestamp   NOT NULL DEFAULT current_timestamp()   COMMENT 'Time of the log event',
 LOG_MESSAGE            varchar                 COMMENT 'Additional message',

 CONSTRAINT PK_1 PRIMARY KEY ( TABLE_INSTANCE_ID, RUN_ID, STATE_CODE, LOG_TIME ),
 CONSTRAINT FK_1 FOREIGN KEY ( RUN_ID )             REFERENCES MIGRATION_RUN_LOG    ( RUN_ID ),
 CONSTRAINT FK_2 FOREIGN KEY ( TABLE_INSTANCE_ID )  REFERENCES MIGRATION_TABLE      ( TABLE_INSTANCE_ID )
)
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 3}, "description":"Runtime information for a table load"}';

--  SNOWFLAKE_TOOL_CONFIG
CREATE OR REPLACE TABLE SNOWFLAKE_TOOL_CONFIG
(
 TOOL_NAME      varchar(30) NOT NULL COMMENT 'Name of the tool that the settings are for "ALL" designates that all tools pull in this setting.',
 TOOL_PARAMETER varchar(64) NOT NULL COMMENT 'Name of the parameter',
 TOOL_VALUE     varchar(128) NOT NULL COMMENT 'Value associated with the parameter',

 CONSTRAINT PK_1 PRIMARY KEY ( TOOL_NAME, TOOL_PARAMETER )
)
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 3}, "description":"The configuration table contains customer specific configuration information used by various PS developed tools"}';


-- Populate the SNOWFLAKE_TOOL_CONFIG table
TRUNCATE TABLE SNOWFLAKE_TOOL_CONFIG;
INSERT INTO SNOWFLAKE_TOOL_CONFIG (TOOL_NAME, TOOL_PARAMETER, TOOL_VALUE)
VALUES 
    ('ICEBERG_MIGRATOR', 'warehouse_name',$T2I_WH),
    ('ICEBERG_MIGRATOR', 'max_parallel_tasks',$T2I_MAX_PAR_TASKS),
    ('ICEBERG_MIGRATOR', 'max_tables_run',$T2I_MAX_TBLS),
    ('ICEBERG_MIGRATOR', 'external_volume',$T2I_EV),
    ('ICEBERG_MIGRATOR', 'delta_catalog_integration',$T2I_CI),
    ('ICEBERG_MIGRATOR', 'aws_glue_external_access_integration',$T2I_EAI),
    ('ICEBERG_MIGRATOR', 'location_pattern','${TABLE_CATALOG}/${TABLE_SCHEMA}/${TABLE_NAME}'),
    ('ICEBERG_MIGRATOR', 'truncate_time','TRUE'),
    ('ICEBERG_MIGRATOR', 'timezone_conversion','NONE'),
    ('ICEBERG_MIGRATOR', 'count_only_validation', 'FALSE'),
    ('ICEBERG_MIGRATOR', 'version','1.2');


--  ICEBERG_METADATA_SYNC
CREATE OR REPLACE TABLE ICEBERG_METADATA_SYNC
(
 TABLE_INSTANCE_ID              integer      NOT NULL COMMENT 'Table Identifier of iceberg table to sync - from the migration_table_log',
 TABLE_RUN_ID                   integer      NOT NULL COMMENT 'Run Identifier of iceberg table to sync - from the migration_table_log',
 SOURCE_DATABASE                varchar      NOT NULL COMMENT 'The source database',
 SOURCE_SCHEMA                  varchar      NOT NULL COMMENT 'The source schema',
 SOURCE_TABLE                   varchar      NOT NULL COMMENT 'The iceberg table name',
 DESTINATION                    varchar      NOT NULL COMMENT 'Where the iceberg metadata will be synced to',
 TARGET_DATABASE                varchar      NOT NULL COMMENT 'Target database at destination',
 TARGET_SCHEMA                  varchar      NULL COMMENT 'Target schema at destination',
 TARGET_TABLE                   varchar      NOT NULL COMMENT 'Target table at destination',
 UPDATE_FREQUENCY_MINS          integer      NOT NULL COMMENT 'The frequency at which to sync Snowflake-managed metadata to another catalog',
 WAREHOUSE                      varchar      NOT NULL COMMENT 'The warehouse used to sync Snowflake-managed metadata to another catalog',
 EXTERNAL_ACCESS_INTEGRATION    varchar      NOT NULL COMMENT 'The EAI to use to access the relevant external catalog',
 SYNC_STARTED                   varchar      NOT NULL COMMENT 'Y/N flag indicating whether the sync has started',
 UPDATED_TIMESTAMP              datetime     NULL COMMENT 'The datetime of the last sync',
 INSERT_DATE                    timestamp    NOT NULL DEFAULT current_timestamp(),
 CONSTRAINT PK_1 PRIMARY KEY ( TABLE_INSTANCE_ID,  TABLE_RUN_ID)
)
COMMENT = '{"origin": "sf_sit", "name": "table_to_iceberg", "version":{"major": 1, "minor": 3}, "description":"Queue of iceberg tables to sync to external catalog"}';


--unset vars
UNSET (T2I_ROLE, T2I_SCH, T2I_WH, T2I_MAX_PAR_TASKS, T2I_MAX_TBLS, T2I_EV, T2I_CI, T2I_EAI);