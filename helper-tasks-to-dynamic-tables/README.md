# Replace Tasks with Dynamic Tables

This notebook identifies eligible tasks that can be converted to Dynamic tables.  Dynamic tables simplify data engineering in Snowflake by providing a reliable, cost-effective, and automated way to transform data. Not every stream/task can or should be replaced.  

This notebook will:
- check actively running tasks in a Snowflake account to find tasks that `INSERT` or `MERGE` into an existing table (from a base table) or create a table using `CTAS`
- identify whether the target table is currently in a share
    - **NOTE:** Data Providers should take additional steps to ensure any affected shared tables don't impact Consumers before switching to Dynamic tables
- generate the DDL to create the Dynamic table that will replace the task
    - **NOTE:** this will be done for each task in the task graph
- execute the Dynamic table DDL and remove the existing stream/task (optional)
- remove the existing target table from the share, if applicable (optional)
- drop existing stream/task (optional)
- drop the existing target table (optional)

## Prerequisites:

- The user executing this notebook, must have access to the `SNOWFLAKE` database.
- The user must have the `CREATE DYNAMIC TABLE` privilge on the schema where the new Dynamic Table will be created.
- The user must own the tasks in the database(s) set in **STEP 3**.

## Support Notice
All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2024 Snowflake Inc. All Rights Reserved.

The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features.  We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Please see TAGGING.md for details on object comments.