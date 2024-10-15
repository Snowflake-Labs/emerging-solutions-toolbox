# Replace Non-Task commands with Dynamic Tables

This notebook identifies `CTAS` and `INSERT OVERWRITE` commands executed multiple times over a given time frame (not via a task), that can be converted to Dynamic tables.  Dynamic tables simplify data engineering in Snowflake by providing a reliable, cost-effective, and automated way to transform data. Not every command can or should be replaced.  

This notebook will:
- check the `QUERY_HISTORY` account usage view for the commands that have successfully completed, more than once, over the last 24 hours.
- identify whether the current target table is in a share.
    - **NOTE:** Data Providers should take additional steps to ensure any affected shared tables don't impact Consumers before switching to Dynamic tables.
- generate the DDL to create the Dynamic table that will replace the commands
- execute the Dynamic table DDL (optional)
- remove the existing target table from the share, if applicable (optional)
- drop the existing target table (optional)

## Prerequisites:

- The user executing this notebook, must have access to the `SNOWFLAKE` database.
- The user must have the `CREATE DYNAMIC TABLE` privilge on the schema where the new Dynamic Table will be created.

## Support Notice
All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2024 Snowflake Inc. All Rights Reserved.

The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features.  We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Please see TAGGING.md for details on object comments.