# CONVERT2ICEBERG

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

Convert2Iceberg is a Streamlit in Snowflake (SiS) app that converts a Snowflake table (FDN-based) to a Snowflake-managed Iceberg table, either by selecting the table by name or choosing a query that contains table(s) to convert. In addition, this app also allows the user compare query execution statistics between a query before and after the conversion to Iceberg.

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is
provided `as is` and without warranty. Snowflake will not offer any support for the use
of the sample code. The purpose of the code is to provide customers with easy access to
innovative ideas that have been built to accelerate customers' adoption of key
Snowflake features. We certainly look for customers' feedback on these solutions and
will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

## Prerequisites:
- The user deploying this app must have a role with the following privileges:
    - Either the `ACCOUNTADMIN` role or a role with the `CREATE EXTERNAL VOLUME` privilege.
    - At a minimum, granted the `CREATE TABLE` privilege on the schema that contains the desired table to convert.
    - `EXECUTE TASK ON ACCOUNT`
    - `CREATE WAREHOUSE ON ACCOUNT`
    - Access to the `ACCOUNT_USAGE` schema in the SNOWFLAKE native app.
- The user either:
    - has an existing Snowflake `EXTERNAL VOLUME` registered to a storage location in the same cloud/region as the Snowflake account.
    - has a storage location in the same cloud/region as the Snowflake account (if an existing `EXTERNAL VOLUME` does not exist).
        - Convert2Iceberg can be used to create an `EXTERNAL VOLUME`.

## Installation
This repo includes the `c2i_install` notebook, located in the `installer/` directory, that installs the Convert2Iceberg SiS app. The notebook should be imported in the appicable Snowflake account and executed, using the desired role. Refer to the notebook for more details.

**NOTE:** the `c2i_install` notebook is standalone and always references the latest version of Convert2Iceberg. This allows the notebook to be distributed separately from the full repo.

## IMPORTANT
This app is scoped only to the tables and queries that the installing role has access to. Due to this, the app can only be used by that role and it <ins>**should not**</ins> be shared to other roles.

This is due to Caller's Rights being blocked in SiS. This app will be updated to support multiple roles, once the limitation is removed.

## Tagging

Please see `TAGGING.md` for details on object comments.
