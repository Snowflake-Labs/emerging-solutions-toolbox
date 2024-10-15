# Secured Views to Dynamic Tables Conversion

## Overview

This project provides a process for converting secured views in Snowflake shares to dynamic tables. Secured views are essential for data governance, enabling organizations to share data while maintaining strict control over sensitive information. However, as data volumes grow, secured views can become challenging to manage and can impact performance. By converting these views to dynamic tables, we enhance data freshness, reduce maintenance overhead, and optimize performance.

## Why Convert Secured Views to Dynamic Tables?

As data-sharing requirements grow, managing secured views in Snowflake can become increasingly complex. Converting these views to dynamic tables offers several key benefits:

- **Performance Optimization**  
  Dynamic tables improve query performance by precomputing results and leveraging Snowflake’s storage optimizations, reducing response times and resource usage compared to repeatedly processing complex secured views.

- **Data Freshness**  
  Dynamic tables ensure data is up-to-date with automatic refresh mechanisms, making them ideal for time-sensitive, frequently accessed data shared with external partners.

- **Reduced Maintenance**  
  With automatic clustering and data organization, dynamic tables minimize the need for manual maintenance, freeing up resources for analysis rather than upkeep.

- **Scalability and Access Control**  
  Dynamic tables scale effortlessly with Snowflake’s architecture, making them better suited for high-volume data sharing while simplifying access control and governance.

Converting secured views to dynamic tables enhances performance, data freshness, and scalability, supporting efficient, secure, and maintainable data-sharing practices.

## Conversion Process

![flowchart](/helper-secure-views-to-dynamic-tables/secure_views_dt_conversion.png)

The process involves the following steps:

1. **Identify Secured Views**  
   - Scan the Snowflake database to identify secured views within the database share.

2. **Extract Object Dependencies**  
   - Query dependencies of each secured view to understand the underlying tables and objects.

3. **Qualify Views for Conversion**  
   - Apply criteria to determine which secured views are best suited for conversion, such as dependency complexity and potential performance gains.

4. **Generate Conversion Code**  
   - For views that qualify, generate SQL to convert these views into dynamic tables, leveraging Snowflake's dynamic table features for improved performance.

This process streamlines secured view management, helping organizations optimize data-sharing performance and scalability in Snowflake.

## Support Notice
All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2024 Snowflake Inc. All Rights Reserved.

The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features.  We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Please see TAGGING.md for details on object comments.
