# Enable Query Acceleration Service for Warehouses with Eligible Queries

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

The query acceleration service (QAS) can accelerate parts of the query workload in a warehouse. When it is enabled for a warehouse, it can improve overall warehouse performance by reducing the impact of outlier queries, which are queries that use more resources than the typical query. The query acceleration service does this by offloading portions of the query processing work to shared compute resources that are provided by the service.

For more information, visit:  https://docs.snowflake.com/en/user-guide/query-acceleration-service#label-query-acceleration-eligible-queries.

This app identifies warehouses that execute queries that are eligible for QAS, along with the option to enable QAS for each warehouse.

This app will:
- check the `QUERY_ACCELERATION_ELIGIBLE` account usage view for warehouses that execute queries that are eligible for QAS.
    - The user can toggle the minimum number of eligible queries to check for, along with the threshold of average execution time is eligible for the service
- enable QAS for each selected warehouse (optional)

## Setup

Adjust the database and schema in the following snippets to match your environment.

1. Upload the Streamlit application to an internal stage.
```sql
CREATE DATABASE IF NOT EXISTS SIT_SOLUTIONS;
CREATE OR REPLACE SCHEMA SIT_SOLUTIONS.QAS;
CREATE OR REPLACE STAGE SIT_SOLUTIONS.QAS.CODE_STAGE;
PUT query_acc_warehouses.py @SIT_SOLUTIONS.QAS.CODE_STAGE AUTO_COMPRESS=FALSE FORCE=TRUE;
```
2. Create the Streamlit application from the uploaded file.
```sql
CREATE OR REPLACE STREAMLIT SIT_SOLUTIONS.QAS.QAS_STREAMLIT
ROOT_LOCATION = '@sit_solutions.qas.code_stage'
MAIN_FILE = 'query_acc_warehouses.py'
QUERY_WAREHOUSE = SLINGSHOT_WH
COMMENT='{"origin":"sf_sit","name":"qas_eligible_warehouses","version":{"major":1, "minor":0},"attributes":"session_tag"}';
```

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.


Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

## Tagging

Please see `TAGGING.md` for details on object comments.