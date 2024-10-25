--ZAMBONI DEMO
--2024.10.16

USE ROLE ZAMBONI_ROLE;
USE WAREHOUSE XS_WH;


------------CLEANUP: BEGIN------------
--Delete any registered labels
DELETE FROM ZAMBONI_DB.ZAMBONI_METADATA.LABELS;

--Delete any registered objects
DELETE FROM ZAMBONI_DB.ZAMBONI_METADATA.OBJECTS;

--Delete any registered collections
DELETE FROM ZAMBONI_DB.ZAMBONI_METADATA.COLLECTIONS;

--Delete any registered processes
DELETE FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES;

--Delete any registered processes
DELETE FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES;

--Delete any registered process DAGs
DELETE FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG;

--Delete any process logs
DELETE FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG;

--remove any update target tasks
CREATE OR REPLACE SCHEMA ZAMBONI_DB.ZAMBONI_TASKS;

--drop dynamic table, if exists
DROP DYNAMIC TABLE IF EXISTS ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_DYNAMIC;

--remove any records from existing target table
DELETE FROM ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_INCREMENTAL;
-----------CLEANUP: END------------




USE SCHEMA ZAMBONI_DB.ZAMBONI_UTIL;

------------------------------STEP 1: ADD/UPDATE LABELS------------------------------
CALL MANAGE_LABEL('label1', 'A label to organize related objects, collections, and processes/dags for: label1', NULL);
CALL MANAGE_LABEL('label2', 'A label to organize related objects, collections, and processes/dags for: label2', NULL);

--verify labels added
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.LABELS;

------------------STEP 2: ADD/UPDATE OBJECTS TO USE FOR COLLECTION------------------
CALL MANAGE_OBJECT('table', 'ZAMBONI_DB', 'ZAMBONI_SRC', 'INVENTORY_ON_HANDS', NULL, ARRAY_CONSTRUCT('label1','label2'));
CALL MANAGE_OBJECT('table', 'ZAMBONI_DB', 'ZAMBONI_SRC', 'INVENTORY_TRANSACTIONS', NULL, ARRAY_CONSTRUCT('label1','label2'));
CALL MANAGE_OBJECT('table', 'ZAMBONI_DB', 'ZAMBONI_TGT', 'INVENTORY_BY_TRANSACTION_INCREMENTAL', NULL, ARRAY_CONSTRUCT('label1','label2'));

--verify objects added
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.OBJECTS;



------------STEP 3: ADD/UPDATE COLLECTIONS, CONTAINING FIELDS FROM TABLES------------
CALL MANAGE_COLLECTION(PARSE_JSON($$
{
    "objects": [
      {     
        "object_id" : 1,
        "attributes" : [
          "ITEMID",
          "LOCATIONID",
          "PROJECT",
          "TYPE",
          "AVAILABLEFORSUPPLYDATE",
          "BATCH",
          "UOM",
          "QUANTITY",
          "PROCESSTYPE",
          "EXPIRATIONDATE",
          "SITEOWNER",
          "ITEMOWNER",
          "ONHANDPOSTDATETIME",
          "MEASURE",
          "NODETYPE",
          "LOB",
          "LOTNUMBER",
          "STORE",
          "CTOITEMID",
          "CTOBOMID"
        ]
      },
      {
        "object_id" : 2,
        "attributes" : [
          "STARTDATE",
          "TRANSACTIONCODE"
        ]
      }
    ]
}$$), 'COL_INVENTORY_TRANSACTION_1', NULL, 'standard', ARRAY_CONSTRUCT('label1','label2'));


CALL MANAGE_COLLECTION(PARSE_JSON($$
{
    "objects": [
      {     
        "object_id" : 3,
        "attributes" : [
            "RECORD_ID",
            "ITEM_ID",
            "LOCATION_ID",
            "PROJECT_NAME",
            "TYPE",
            "SUPPLY_DATE",
            "BATCH_ID",
            "QUANTITY_SUM",
            "STORE_NAME", 
            "START_DATE", 
            "TRANSACTION_CODE"
        ]
      }
    ]
}$$), 'COL_INVENTORY_TRANSACTION_2', NULL, 'standard', ARRAY_CONSTRUCT('label1','label2'));


--check collections
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.COLLECTIONS;



----------------------------STEP 4: INSPECT PROCESS_TYPES----------------------------
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES;
    
    

---------------------------STEP 5: CREATE/UPDATE PROCESSES---------------------------
-----NOTE: this proc creates a process for each target, based on process_type_id-----
CALL MANAGE_PROCESS(PARSE_JSON($$
{
  "targets" : [
    {
      "process_name" : "target_dt_inventory_by_transaction",
      "process_type_id" : 1,
      "distinct": true,
      "top": null,
      "columns" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "MAX(OBJ_1.PROJECT) PROJECT",
        "MAX(OBJ_1.TYPE) TYPE",
        "MAX(OBJ_1.AVAILABLEFORSUPPLYDATE) AVAILABLEFORSUPPLYDATE",
        "RIGHT(OBJ_1.BATCH, 4) BATCH",
        "SUM(OBJ_1.QUANTITY) QUANTITY_SUM",
        "MAX(OBJ_1.STORE) STORE",
        "MAX(OBJ_2.STARTDATE) STARTDATE",
        "MAX(OBJ_2.TRANSACTIONCODE) TRANSACTIONCODE"
      ],
      "group_by" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "RIGHT(OBJ_1.BATCH, 4)"
      ],
      "join" : [
        {
          "collection_id" : 1,
          "alias" : "OBJ_2",
          "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_TRANSACTIONS",
          "keys" : [
            {
              "attr_1" : "OBJ_1.ITEMID",
              "operator" : "=",
              "attr_2" : "OBJ_2.ITEMID",
              "condition" : ""
            }
          ]
        }
      ],
      "order_by_cols" : [
        "ITEMID",
        "LOCATIONID",
        "BATCH",
        "AVAILABLEFORSUPPLYDATE"
      ],
      "settings" : {
        "downstream" : true,
        "target_interval" : "hours",
        "target_lag" : 24,
        "warehouse" : "xs_wh"
      },
      "source" : {
        "collection_id" : 1,
        "alias" : "OBJ_1",
        "key" : "ITEMID",
        "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_ON_HANDS"
      },
      "target" : {
        "collection_id" : 2,
        "alias" : "OBJ3",
        "key" : null,
        "object" : "ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_DYNAMIC"
      },
      "where" : [
        {
          "attr_1" : "OBJ_1.LOCATIONID",
          "operator" : "IN",
          "attr_2" : "('LDC2', 'LDC3')",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.ITEMID",
          "operator" : "!=",
          "attr_2" : "'CFGB09'",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.AVAILABLEFORSUPPLYDATE ",
          "operator" : "!=",
          "attr_2" : "'2023-10-30'",
          "condition" : ""
        }
      ],
      "mapping" : [],
      "labels" : ["label1", "label2"]
    },
    {
      "process_name" : "target_incremental_merge_inventory_by_transaction_1",
      "process_type_id" : 2,
      "distinct": false,
      "top": 1000,
      "columns" : [
        "HASH(OBJ_1.ITEMID,OBJ_1.LOCATIONID,MAX(OBJ_1.PROJECT),MAX(OBJ_1.AVAILABLEFORSUPPLYDATE),RIGHT(OBJ_1.BATCH, 4),SUM(OBJ_1.QUANTITY),MAX(OBJ_1.STORE),MAX(OBJ_2.STARTDATE),MAX(OBJ_2.TRANSACTIONCODE)) RECORD_ID" ,
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "MAX(OBJ_1.PROJECT) PROJECT",
        "MAX(OBJ_1.TYPE) TYPE",
        "MAX(OBJ_1.AVAILABLEFORSUPPLYDATE) AVAILABLEFORSUPPLYDATE",
        "RIGHT(OBJ_1.BATCH, 4) BATCH",
        "SUM(OBJ_1.QUANTITY) QUANTITY_SUM",
        "MAX(OBJ_1.STORE) STORE",
        "MAX(OBJ_2.STARTDATE) STARTDATE",
        "MAX(OBJ_2.TRANSACTIONCODE) TRANSACTIONCODE"
      ],
      "group_by" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "RIGHT(OBJ_1.BATCH, 4)"
      ],
      "join" : [
        {
          "collection_id" : 1,
          "alias" : "OBJ_2",
          "key" : "ITEMID",
          "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_TRANSACTIONS"
        }
      ],
      "order_by_pos" : [1,2,3,7,6],
      "settings" : {
        "target_interval" : "minute",
        "target_lag" : 1440,
        "warehouse" : "xs_wh",
        "when_matched" : [],
        "when_not_matched" : []
      },
      "source" : {
        "collection_id" : 1,
        "alias" : "OBJ_1",
        "key" : "ITEMID",
        "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_ON_HANDS"
      },
      "target" : {
        "collection_id" : 2,
        "alias" : "TGT_1",
        "key" : null,
        "object" : "ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_INCREMENTAL"
      },
      "where" : [
        {
          "attr_1" : "OBJ_1.LOCATIONID",
          "operator" : "IN",
          "attr_2" : "('LDC2', 'LDC3')",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.ITEMID",
          "operator" : "!=",
          "attr_2" : "'CFGB09'",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.AVAILABLEFORSUPPLYDATE ",
          "operator" : "!=",
          "attr_2" : "'2023-10-30'",
          "condition" : ""
        }
      ],
      "mapping" : [
        {
            "source_attr" : "RECORD_ID",
            "target_attr" : "RECORD_ID",
            "merge_on" : "Y",
            "update" : "N",
            "insert" : "Y"
        },
        {
            "source_attr" : "ITEMID",
            "target_attr" : "ITEM_ID",
            "merge_on" : "N",
            "update" : "Y",
            "insert" : "Y"
        },
        {
            "source_attr" : "LOCATIONID",
            "target_attr" : "LOCATION_ID",
            "merge_on" : "N",
            "update" : "Y",
            "insert" : "Y"
        },
        {
            "source_attr" : "PROJECT",
            "target_attr" : "PROJECT_NAME",
            "merge_on" : "N",
            "update" : "Y",
            "insert" : "Y"
        },
        {
          "source_attr" : "TYPE",
          "target_attr" : "TYPE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "AVAILABLEFORSUPPLYDATE",
          "target_attr" : "SUPPLY_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "BATCH",
          "target_attr" : "BATCH_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "QUANTITY_SUM",
          "target_attr" : "QUANTITY_SUM",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STORE",
          "target_attr" : "STORE_NAME",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STARTDATE",
          "target_attr" : "START_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "TRANSACTIONCODE",
          "target_attr" : "TRANSACTION_CODE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        }
      ],
      "labels" : ["label1", "label2"]
    },
    {
      "process_name" : "target_incremental_merge_inventory_by_transaction_2",
      "process_type_id" : 2,
      "distinct": false,
      "top": 1000,
      "columns" : [
        "HASH(OBJ_1.ITEMID,OBJ_1.LOCATIONID,MAX(OBJ_1.PROJECT),MAX(OBJ_1.AVAILABLEFORSUPPLYDATE),RIGHT(OBJ_1.BATCH, 4),SUM(OBJ_1.QUANTITY),MAX(OBJ_1.STORE),MAX(OBJ_2.STARTDATE),MAX(OBJ_2.TRANSACTIONCODE)) RECORD_ID" ,
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "MAX(OBJ_1.PROJECT) PROJECT",
        "MAX(OBJ_1.TYPE) TYPE",
        "MAX(OBJ_1.AVAILABLEFORSUPPLYDATE) AVAILABLEFORSUPPLYDATE",
        "RIGHT(OBJ_1.BATCH, 4) BATCH",
        "SUM(OBJ_1.QUANTITY) QUANTITY_SUM",
        "MAX(OBJ_1.STORE) STORE",
        "MAX(OBJ_2.STARTDATE) STARTDATE",
        "MAX(OBJ_2.TRANSACTIONCODE) TRANSACTIONCODE"
      ],
      "group_by" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "RIGHT(OBJ_1.BATCH, 4)"
      ],
      "join" : [
        {
          "collection_id" : 1,
          "alias" : "OBJ_2",
          "key" : "ITEMID",
          "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_TRANSACTIONS"
        }
      ],
      "order_by_pos" : [1,2,3,7,6],
      "settings" : {
        "target_interval" : "minute",
        "target_lag" : 1440,
        "warehouse" : "xs_wh",
        "when_matched" : [],
        "when_not_matched" : []
      },
      "source" : {
        "collection_id" : 1,
        "alias" : "OBJ_1",
        "key" : "ITEMID",
        "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_ON_HANDS"
      },
      "target" : {
        "collection_id" : 2,
        "alias" : "TGT_1",
        "key" : null,
        "object" : "ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_INCREMENTAL"
      },
      "where" : [
        {
          "attr_1" : "OBJ_1.LOCATIONID",
          "operator" : "=",
          "attr_2" : "'LDC1'",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.QUANTITY",
          "operator" : ">",
          "attr_2" : "500",
          "condition" : ""
        }
      ],
      "mapping" : [
        {
          "source_attr" : "RECORD_ID",
          "target_attr" : "RECORD_ID",
          "merge_on" : "Y",
          "update" : "N",
          "insert" : "Y"
        },
        {
          "source_attr" : "ITEMID",
          "target_attr" : "ITEM_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "LOCATIONID",
          "target_attr" : "LOCATION_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "PROJECT",
          "target_attr" : "PROJECT_NAME",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "TYPE",
          "target_attr" : "TYPE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "AVAILABLEFORSUPPLYDATE",
          "target_attr" : "SUPPLY_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "BATCH",
          "target_attr" : "BATCH_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "QUANTITY_SUM",
          "target_attr" : "QUANTITY_SUM",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STORE",
          "target_attr" : "STORE_NAME",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STARTDATE",
          "target_attr" : "START_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "TRANSACTIONCODE",
          "target_attr" : "TRANSACTION_CODE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        }
      ],
      "labels" : ["label1", "label2"]
    }
  ]
}
$$)
);

--check PROCESSES
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESSES;




-------------------STEP 6: CHECK THE SQL GENERATED FOR EACH PROCESS-------------------
--test target_dynamic_table template
SELECT ZAMBONI_DB.ZAMBONI_UTIL.GET_SQL_JINJA(
    (SELECT TEMPLATE FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE LOWER(PROCESS_TYPE) = 'target_dynamic_table'), 
    PARSE_JSON($$
    {
      "process_name" : "target_dt_inventory_by_transaction",
      "process_type_id" : 1,
      "distinct": true,
      "top": null,
      "columns" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "MAX(OBJ_1.PROJECT) PROJECT",
        "MAX(OBJ_1.TYPE) TYPE",
        "MAX(OBJ_1.AVAILABLEFORSUPPLYDATE) AVAILABLEFORSUPPLYDATE",
        "RIGHT(OBJ_1.BATCH, 4) BATCH",
        "SUM(OBJ_1.QUANTITY) QUANTITY_SUM",
        "MAX(OBJ_1.STORE) STORE",
        "MAX(OBJ_2.STARTDATE) STARTDATE",
        "MAX(OBJ_2.TRANSACTIONCODE) TRANSACTIONCODE"
      ],
      "group_by" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "RIGHT(OBJ_1.BATCH, 4)"
      ],
      "join" : [
        {
          "collection_id" : 1,
          "alias" : "OBJ_2",
          "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_TRANSACTIONS",
          "keys" : [
            {
              "attr_1" : "OBJ_1.ITEMID",
              "operator" : "=",
              "attr_2" : "OBJ_2.ITEMID",
              "condition" : ""
            }
          ]
        }
      ],
      "order_by_cols" : [
        "ITEMID",
        "LOCATIONID",
        "BATCH",
        "AVAILABLEFORSUPPLYDATE"
      ],
      "settings" : {
        "downstream" : true,
        "target_interval" : "hours",
        "target_lag" : 24,
        "warehouse" : "xs_wh"
      },
      "source" : {
        "collection_id" : 1,
        "alias" : "OBJ_1",
        "key" : "ITEMID",
        "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_ON_HANDS"
      },
      "target" : {
        "collection_id" : 2,
        "alias" : "OBJ3",
        "key" : null,
        "object" : "ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_DYNAMIC"
      },
      "where" : [
        {
          "attr_1" : "OBJ_1.LOCATIONID",
          "operator" : "IN",
          "attr_2" : "('LDC2', 'LDC3')",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.ITEMID",
          "operator" : "!=",
          "attr_2" : "'CFGB09'",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.AVAILABLEFORSUPPLYDATE ",
          "operator" : "!=",
          "attr_2" : "'2023-10-30'",
          "condition" : ""
        }
      ],
      "mapping" : [],
      "labels" : ["label1", "label2"]
    }$$)
);


--generate target_incremental_merge_inventory_by_transaction_1 SQL from template
SELECT ZAMBONI_DB.ZAMBONI_UTIL.GET_SQL_JINJA(
    (SELECT TEMPLATE FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE LOWER(PROCESS_TYPE) = 'target_incremental_merge_insert'), 
    PARSE_JSON($$
    {
      "process_name" : "target_incremental_merge_inventory_by_transaction_1",
      "process_type_id" : 2,
      "distinct": false,
      "top": 1000,
      "columns" : [
        "HASH(OBJ_1.ITEMID,OBJ_1.LOCATIONID,MAX(OBJ_1.PROJECT),MAX(OBJ_1.AVAILABLEFORSUPPLYDATE),RIGHT(OBJ_1.BATCH, 4),SUM(OBJ_1.QUANTITY),MAX(OBJ_1.STORE),MAX(OBJ_2.STARTDATE),MAX(OBJ_2.TRANSACTIONCODE)) RECORD_ID" ,
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "MAX(OBJ_1.PROJECT) PROJECT",
        "MAX(OBJ_1.TYPE) TYPE",
        "MAX(OBJ_1.AVAILABLEFORSUPPLYDATE) AVAILABLEFORSUPPLYDATE",
        "RIGHT(OBJ_1.BATCH, 4) BATCH",
        "SUM(OBJ_1.QUANTITY) QUANTITY_SUM",
        "MAX(OBJ_1.STORE) STORE",
        "MAX(OBJ_2.STARTDATE) STARTDATE",
        "MAX(OBJ_2.TRANSACTIONCODE) TRANSACTIONCODE"
      ],
      "group_by" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "RIGHT(OBJ_1.BATCH, 4)"
      ],
      "join" : [
        {
          "collection_id" : 1,
          "alias" : "OBJ_2",
          "key" : "ITEMID",
          "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_TRANSACTIONS"
        }
      ],
      "order_by_pos" : [1,2,3,7,6],
      "settings" : {
        "target_interval" : "minute",
        "target_lag" : 1440,
        "warehouse" : "xs_wh",
        "when_matched" : [],
        "when_not_matched" : []
      },
      "source" : {
        "collection_id" : 1,
        "alias" : "OBJ_1",
        "key" : "ITEMID",
        "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_ON_HANDS"
      },
      "target" : {
        "collection_id" : 2,
        "alias" : "TGT_1",
        "key" : null,
        "object" : "ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_INCREMENTAL"
      },
      "where" : [
        {
          "attr_1" : "OBJ_1.LOCATIONID",
          "operator" : "IN",
          "attr_2" : "('LDC2', 'LDC3')",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.ITEMID",
          "operator" : "!=",
          "attr_2" : "'CFGB09'",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.AVAILABLEFORSUPPLYDATE ",
          "operator" : "!=",
          "attr_2" : "'2023-10-30'",
          "condition" : ""
        }
      ],
      "mapping" : [
        {
          "source_attr" : "RECORD_ID",
          "target_attr" : "RECORD_ID",
          "merge_on" : "Y",
          "update" : "N",
          "insert" : "Y"
        },
        {
          "source_attr" : "ITEMID",
          "target_attr" : "ITEM_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "LOCATIONID",
          "target_attr" : "LOCATION_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "PROJECT",
          "target_attr" : "PROJECT_NAME",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "TYPE",
          "target_attr" : "TYPE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "AVAILABLEFORSUPPLYDATE",
          "target_attr" : "SUPPLY_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "BATCH",
          "target_attr" : "BATCH_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "QUANTITY_SUM",
          "target_attr" : "QUANTITY_SUM",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STORE",
          "target_attr" : "STORE_NAME",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STARTDATE",
          "target_attr" : "START_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "TRANSACTIONCODE",
          "target_attr" : "TRANSACTION_CODE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        }
      ],
      "labels" : ["label1", "label2"]
    }$$)
);

--generate target_incremental_merge_inventory_by_transaction_2 SQL from template
SELECT ZAMBONI_DB.ZAMBONI_UTIL.GET_SQL_JINJA(
    (SELECT TEMPLATE FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES WHERE LOWER(PROCESS_TYPE) = 'target_incremental_merge_insert'), 
    PARSE_JSON($$
    {
      "process_name" : "target_incremental_merge_inventory_by_transaction_2",
      "process_type_id" : 2,
      "distinct": false,
      "top": 1000,
      "columns" : [
        "HASH(OBJ_1.ITEMID,OBJ_1.LOCATIONID,MAX(OBJ_1.PROJECT),MAX(OBJ_1.AVAILABLEFORSUPPLYDATE),RIGHT(OBJ_1.BATCH, 4),SUM(OBJ_1.QUANTITY),MAX(OBJ_1.STORE),MAX(OBJ_2.STARTDATE),MAX(OBJ_2.TRANSACTIONCODE)) RECORD_ID" ,
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "MAX(OBJ_1.PROJECT) PROJECT",
        "MAX(OBJ_1.TYPE) TYPE",
        "MAX(OBJ_1.AVAILABLEFORSUPPLYDATE) AVAILABLEFORSUPPLYDATE",
        "RIGHT(OBJ_1.BATCH, 4) BATCH",
        "SUM(OBJ_1.QUANTITY) QUANTITY_SUM",
        "MAX(OBJ_1.STORE) STORE",
        "MAX(OBJ_2.STARTDATE) STARTDATE",
        "MAX(OBJ_2.TRANSACTIONCODE) TRANSACTIONCODE"
      ],
      "group_by" : [
        "OBJ_1.ITEMID",
        "OBJ_1.LOCATIONID",
        "RIGHT(OBJ_1.BATCH, 4)"
      ],
      "join" : [
        {
          "collection_id" : 1,
          "alias" : "OBJ_2",
          "key" : "ITEMID",
          "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_TRANSACTIONS"
        }
      ],
      "order_by_pos" : [1,2,3,7,6],
      "settings" : {
        "target_interval" : "minute",
        "target_lag" : 1440,
        "warehouse" : "xs_wh",
        "when_matched" : [],
        "when_not_matched" : []
      },
      "source" : {
        "collection_id" : 1,
        "alias" : "OBJ_1",
        "key" : "ITEMID",
        "object" : "ZAMBONI_DB.ZAMBONI_SRC.INVENTORY_ON_HANDS"
      },
      "target" : {
        "collection_id" : 2,
        "alias" : "TGT_1",
        "key" : null,
        "object" : "ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_INCREMENTAL"
      },
      "where" : [
        {
          "attr_1" : "OBJ_1.LOCATIONID",
          "operator" : "=",
          "attr_2" : "'LDC1'",
          "condition" : "AND"
        },
        {
          "attr_1" : "OBJ_1.QUANTITY",
          "operator" : ">",
          "attr_2" : "500",
          "condition" : ""
        }
      ],
      "mapping" : [
        {
          "source_attr" : "RECORD_ID",
          "target_attr" : "RECORD_ID",
          "merge_on" : "Y",
          "update" : "N",
          "insert" : "Y"
        },
        {
          "source_attr" : "ITEMID",
          "target_attr" : "ITEM_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "LOCATIONID",
          "target_attr" : "LOCATION_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "PROJECT",
          "target_attr" : "PROJECT_NAME",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "TYPE",
          "target_attr" : "TYPE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "AVAILABLEFORSUPPLYDATE",
          "target_attr" : "SUPPLY_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "BATCH",
          "target_attr" : "BATCH_ID",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "QUANTITY_SUM",
          "target_attr" : "QUANTITY_SUM",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STORE",
          "target_attr" : "STORE_NAME",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "STARTDATE",
          "target_attr" : "START_DATE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        },
        {
          "source_attr" : "TRANSACTIONCODE",
          "target_attr" : "TRANSACTION_CODE",
          "merge_on" : "N",
          "update" : "Y",
          "insert" : "Y"
        }
      ],
      "labels" : ["label1", "label2"]
    }$$)
);



------------------------------STEP 7: CREATE PROCESS DAG:  dag_dt_inventory_by_transaction------------------------------
CALL MANAGE_DAG('dag_dt_inventory_by_transaction', PARSE_JSON('
{
  "child_processes" : [
    {
      "process_id" : 1,
      "process_name" : "target_dt_inventory_by_transaction",
      "process_order" : 1
    }
  ]
}'), ARRAY_CONSTRUCT('label1','label2'));

--check process_dag
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG ORDER BY PARENT_PROCESS_ID DESC;



--------------------------------STEP 8: CREATE PARENT PROCESS: dag_dt_inventory_by_transaction-------------------------------
--get parent_process_id value for dag_dt_inventory_by_transaction
CALL CREATE_PARENT_PROCESS(1);


--check process_log
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG;



---------------------STEP 9: CHECK THAT DYNAMIC TABLE WAS CREATED --------------------
SELECT * FROM ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_DYNAMIC;



--------STEP 10: CHECK THAT TARGET COLLECTION WAS UPDATED WITH NEW DYNAMIC TABLE------
SELECT OBJECTS FROM ZAMBONI_DB.ZAMBONI_METADATA.COLLECTIONS WHERE COLLECTION_ID = 2;



------------------------------STEP 11: CREATE PROCESS DAG:  dag_merge_inventory_by_transaction------------------------------
CALL MANAGE_DAG('dag_merge_inventory_by_transaction', PARSE_JSON('
{
  "child_processes" : [
    {
      "process_id" : 3,
      "process_name" : "target_incremental_merge_inventory_by_transaction_2",
      "process_order" : 2
    },
    {
      "process_id" : 2,
      "process_name" : "target_incremental_merge_inventory_by_transaction_1",
      "process_order" : 1
    }
  ]
}'), ARRAY_CONSTRUCT('label1','label2'));

--check process_dag
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_DAG ORDER BY PARENT_PROCESS_ID DESC;



--------------------------------STEP 12: CREATE PARENT PROCESS: dag_merge_inventory_by_transaction-------------------------------
--get parent_process_id value for dag_merge_inventory_by_transaction

CALL CREATE_PARENT_PROCESS(2);


--check process_log
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG;


-----------------STEP 13: DESCRIBE THE TWO TASKS THAT UPDATE INCREMENTAL TABLE ---------------
DESCRIBE TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_TARGET_INCREMENTAL_MERGE_INVENTORY_BY_TRANSACTION_1_TASK;

DESCRIBE TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_TARGET_INCREMENTAL_MERGE_INVENTORY_BY_TRANSACTION_2_TASK;
--NOTICE in the PREDECESSOR column that the previous task is listed, meaning that this task won't execute until the previous one is complete
--NOTE:  the Task DAG can also be viewed in Snowsight by navigating to the task from the Databases tab.



-------------------STEP 14: CHECK THAT INCREMENTAL TABLE WAS UPDATED -----------------
SELECT * FROM ZAMBONI_DB.ZAMBONI_TGT.INVENTORY_BY_TRANSACTION_INCREMENTAL;



--------------------------------STEP 15: SUSPEND PROCESS DAG: dag_merge_inventory_by_transaction-------------------------------
CALL UPDATE_PARENT_PROCESS(2, 'suspend');


--check process_log
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG;

-----------------STEP 16: DESCRIBE THE DAG'S TWO TASKS TO VERIFY THAT THE TASKS ARE NOW SUSPENDED---------------
DESCRIBE TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_TARGET_INCREMENTAL_MERGE_INVENTORY_BY_TRANSACTION_1_TASK;
--check the STATE column
DESCRIBE TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_TARGET_INCREMENTAL_MERGE_INVENTORY_BY_TRANSACTION_2_TASK;
--check the STATE column




--------------------------------STEP 17: RESUME PROCESS DAG: dag_merge_inventory_by_transaction-------------------------------
CALL UPDATE_PARENT_PROCESS(2, 'resume');


--check process_log
SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_LOG;

-----------------STEP 16: DESCRIBE THE DAG'S TWO TASKS TO VERIFY THAT THE TASKS ARE NOW RESUMED---------------
DESCRIBE TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_TARGET_INCREMENTAL_INSERT_INVENTORY_BY_TRANSACTION_INCREMENTAL_1_TASK;
--check the STATE column

DESCRIBE TASK ZAMBONI_DB.ZAMBONI_TASKS.UPDATE_TARGET_INCREMENTAL_INSERT_INVENTORY_BY_TRANSACTION_INCREMENTAL_2_TASK;
--check the STATE column