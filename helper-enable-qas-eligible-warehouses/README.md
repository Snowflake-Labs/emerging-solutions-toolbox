# Enable Query Acceleration Service for Warehouses with Eligible Queries

The query acceleration service (QAS) can accelerate parts of the query workload in a warehouse. When it is enabled for a warehouse, it can improve overall warehouse performance by reducing the impact of outlier queries, which are queries that use more resources than the typical query. The query acceleration service does this by offloading portions of the query processing work to shared compute resources that are provided by the service.

For more information, visit:  https://docs.snowflake.com/en/user-guide/query-acceleration-service#label-query-acceleration-eligible-queries.

This app identifies warehouses that execute queries that are eligible for QAS, along with the option to enable QAS for each warehouse.

This app will:
- check the `QUERY_ACCELERATION_ELIGIBLE` account usage view for warehouses that execute queries that are eligible for QAS.
    - The user can toggle the minimum number of eligible queries to check for, along with the threshold of average execution time is eligible for the service
- enable QAS for each selected warehouse (optional)

## Support Notice
All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2024 Snowflake Inc. All Rights Reserved.

The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features.  We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Please see TAGGING.md for details on object comments.
