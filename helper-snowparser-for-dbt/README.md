## Snowparser for dbt
Snowparser is a Helper created by Snowflake’s Solution Innovation Team (SIT).

> [!IMPORTANT]
> Snowparser for dbt is under active development. Please reach out to your account rep to learn more and get connected with the development team for further assistance.

## Support Notice
All sample code is provided for reference purposes only. Please note that this code is provided `as is` and without warranty. Snowflake will not offer any support for the use of the sample code.

## Copyright Notice
Copyright (c) [Current Year] Snowflake Inc. All Rights Reserved.

## Purpose
The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features. We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

## Tagging
Please see `TAGGING.md` for details on object comments.

## Overview
Snowparser for dbt enables Snowflake users to generate Snowflake Semantic [Models](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/semantic-model-spec)/[Views](https://docs.snowflake.com/LIMITEDACCESS/semantic-views/overview) from existing dbt projects using dbt [manifest JSON](https://docs.getdbt.com/reference/artifacts/manifest-json) artifact files.  The Snowparser utility is operationalized via a Snowflake Stored Procedure and callable via SQL or Python API.

The Snowparser for dbt utility is most useful if your dbt project incorporates [MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow), e.g. it has [semantic models](https://docs.getdbt.com/docs/build/semantic-models) usually found in yaml files. It is these rich elements that the parsing logic incorporates into a Snowflake semantic model. MetricFlow is supported in both dbt Cloud and dbt Core.

## Prerequisites
To utilize Snowparser, please take note of the following requirements.

- Your Snowflake account must be enabled for [Semantic Views](https://docs.snowflake.com/user-guide/views-semantic/overview).
- A [manifest JSON](https://docs.getdbt.com/reference/artifacts/manifest-json) must be obtained from a dbt project run and uploaded to Snowflake stage. They can be found in the Artifacts section of a dbt Cloud job.
- The dbt project must use Snowflake exclusively as a data source.
- It is highly recommended that your dbt project use MetricFlow semantic models.

## Setup
Snowparser for dbt can be registered as a Stored Procedure. Below are a couple options to do so.

### Option 1 (Requires ACCOUNTADMIN)
Copy and paste the contents of `setup.sql` into a Snowsight SQL Worksheet (or [VSCode with Snowflake Extension]((https://docs.snowflake.com/en/user-guide/vscode-ext))).
Ensure your context role is appropriate as this will be the owning role of the corresponding objects.
Execute the entire SQL Worksheet.

### Option 2 (Does Not Require ACCOUNTADMIN)
Download the contents of `dbt` and upload to a Snowflake stage in Snowsight.
Files should be uploaded to a directory named `dbt/` within the Snowflake stage.
For example, the path for semantic_manifest_parser.py may be `@DROPBOX/dbt/semantic_manifest_parser.py`.

Once the 2 files have been uploaded to a Snowflake stage, you may copy and paste the 2 `CREATE OR REPLACE PROCEDURE` found at the bottom of `setup.sql` ensuring you revise the IMPORTS path to the approriate stage. Please maintain the COMMENT portion for continuity with future versions. Please see `TAGGING.md` for details on object comments.

## Manifest File

Artifact files are produced when dbt projects are run as a standalone job or pipeline. Nearly all dbt run types/commands produce a `manifest.json` for all dbt projects with an additional `semantic_manifest.json` for dbt projects that include MetricFlow. `dbt parse —-no-partial-parse` is a simple command to create these artifact files without actually initiating any write commands to a data source. Artifact files can be downloaded directly from dbtCloud job artifacts. dbtCore users can also generate these files.

## Executing
Below is an example of executing the stored procedure to parse dbt MetricFlow semantic models to create a Snowflake Semantic YAML.

```sql
-- Returns semantic YAML as string
CALL SNOWPARSER_DBT_SEMANTIC_YAML(
    manifest_file => '@DROPBOX/samples/manifest.json', -- Can be fully qualified stage
    semantic_view_name => 'my_semantic_view',
    semantic_view_description => 'Semantic view about customers and orders'
);
```

Snowflake Semantic YAMLs can be converted to native Semantic Views with the [CREATE_SEMANTIC_VIEW_FROM_YAML function](https://docs.snowflake.com/en/sql-reference/stored-procedures/system_create_semantic_view_from_yaml).

If your dbt project does not make use of MetricFlow semantic models, you can use Snowparser for dbt to extract tables and columns captured in your dbt models.
It is recommended to pass a list of dbt models to limit the number of dbt models considered semantically relevant. Otherwise, all dbt models will be used.

```sql
-- Returns array of tables and column objects
CALL SNOWPARSER_DBT_GET_OBJECTS(
    manifest_file => '@DROPBOX/samples/manifest_wo_metricflow.json',
    dbt_models =>  TO_ARRAY(['customers','order_items', 'orders', 'stg_locations', 'stg_products'])
);
```

## Supported Translations
The below chart highlights what elements of dbt semantic models and metrics are translated into Snowflake semantic elements.

| dbt Semantic Model Element | Definition | Translation Supported | Snowflake Semantic Element |
|---------------------------|------------|---------------------|--------------------------|
| Node Relation | Materialized table-like entity that captures semantic model | yes | Base table |
| Entity | Real-world concepts in a business such as customers, transactions, and ad campaigns | yes | Dimension |
| Measure | Aggregations performed on columns in your model; can be used as final metrics or as building blocks for more complex metrics | yes | Fact |
| Auto-Create Metric | Measure labeled with create_metric = True and default aggregation | yes | Global metric |
| Dimension | Represent the non-aggregatable columns in your data set, which are the attributes, features, or characteristics that describe or categorize data | yes | Time Dimension if type = time, otherwise Dimension |
| Simple Metric | Point directly to a measure; may think of it as a function that takes only one measure as the input | yes | Global metric |
| Derived Metric | Defined as an expression of other metrics; allow you to do calculations on top of metrics | yes | Global metric |
| Cumulative Metric | aggregate a measure over a given window; requires time spine model | no | - |
| Ratio Metric | involve a numerator metric and a denominator metric | yes | Global metric |
| Conversion Metric | track when a base event and a subsequent conversion event occur for an entity within a set time period | no | - |
| Saved Queries | Way to save commonly used queries in MetricFlow | no | - |
| Filters | Saved Queries and Metrics can include filters | no | - |


## Feedback
Please add issues to GitHub or email Jason Summer (jason.summer@snowflake.com).
