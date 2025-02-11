![Emerging Solutions Toolbox Banner](banner_emerging_solutions_toolbox.png)

# Emerging Solutions Toolbox
#### Brought to you by Snowflake's Solution Innovation Team (SIT)
The Emerging Solutions Toolbox is a collection of solutions created by Snowflake's Solution Innovation Team (SIT).  Each folder represents a separate solution, and solutions can range anywhere between helpful administrative notebooks to fully-fledged frameworks and native applications.  Each solution contains its own licensing agreement, telemetry tagging information, and details on the solution.  Check back frequently, as we'll continually be adding new solutions.

## Support Notice
All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2024 Snowflake Inc. All Rights Reserved.

The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features.  We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Please see TAGGING.md for details on object comments.


## Solution Types
A solution can represent one of three types:

* FRAMEWORK - A configurable solution meant for continuous use, augmenting the Snowflake experience
* HELPER - A typically lightweight utility meant to be used as-needed for specific tasks
* DEMO - An example meant to be used to understand a topic using synthetic data


## Solution List

| Solution | Type | Description |
| --- | --- | --- |
| [Supply Chain Network Optimization](https://github.com/Snowflake-Labs/sfguide-supply-chain-network-optimization/) | DEMO | A solution using optimization programming to make least cost fulfillment decisions. |
| [Solution Installation Wizard](https://github.com/Snowflake-Labs/sfguide-solution-installation-wizard/) | FRAMEWORK | A Native App that helps facilitate consumers deploy code from the provider. |
| [Alert Hub](https://github.com/Snowflake-Labs/sfguide-alert-hub/) | FRAMEWORK | A Streamlit for managing and templatizing Alerts. |
| [Application Control Framework](https://github.com/Snowflake-Labs/sfguide-application-control-framework/) | FRAMEWORK | A framework that allows for rule-based access and consumer experiences within Native Apps. |
| [Data Crawler](https://github.com/Snowflake-Labs/sfguide-data-crawler/) | FRAMEWORK | A solution prompting a Cortex Large Language Model (LLM) to generate a natural language description of each table within a database and/or schema. |
| [Cohort Builder](https://github.com/Snowflake-Labs/sfguide-cohort-builder/) | FRAMEWORK | The Cohort Builder Model leverages Snowflake and Snowpark to create, manage, and schedule cohorts effectively. |
| [API Enrichment Framework](https://github.com/Snowflake-Labs/sfguide-getting-started-with-api-enrichment-framework/) | FRAMEWORK | A Native App designed to quick start API-driven data enrichment. |
| [Data Quality Manager](https://github.com/Snowflake-Labs/sfguide-getting-started-with-data-quality-manager/) | FRAMEWORK | An application designed to implement data quality checks, using both Snowflake data metric functions (DMFs) and custom checks. |
| [ML (Machine Learning) Sidekick](https://github.com/Snowflake-Labs/sfguide-build-and-deploy-snowpark-ml-models-using-streamlit-snowflake-notebooks/) | FRAMEWORK | A no-code app built using Streamlit in Snowflake, designed for building and deploying machine learning models in Snowflake. |
| [DSPy](https://github.com/stanfordnlp/dspy/) | FRAMEWORK | Added Snowflake Cortex support for DSPy, and framework for optimizing lanugage model prompts and weights. |
| [Semantic Model Generator](https://github.com/Snowflake-Labs/semantic-model-generator/) | FRAMEWORK | A semantic model generator for use with Snowflake's Cortex Analyst, including support for migrating other semantic models including dbt and Looker. |
| [Data Model Mapper](https://github.com/Snowflake-Labs/sfguide-data-model-mapper/) | FRAMEWORK | A native app solution with no-code UI that helps consumers model, map, and share provider-standardized data to providers. |
| [Migrate Tasks to Dynamic Tables Notebook](/helper-tasks-to-dynamic-tables) | HELPER | A notebook that helps users identify eligible Tasks that can be migrated to Dynamic Tables (DTs), with the option to examine the DT DDL and/or migrate. |
| [Migrate Non-Tasks to Dynamic Tables Notebook](/helper-non_tasks-to-dynamic-tables) | HELPER | A notebook that helps users identify eligible repetitive commands, that are executed not using tasks, that can be migrated to Dynamic Tables (DTs), with the option to examine the DT DDL and/or migrate. |
| [Migrate Secure Views to Dynamic Tables Notebook](/helper-secure-views-to-dynamic-tables) | HELPER | A notebook that helps users identify eligible secure views to migrate to Dynamic Tables (DT). |
| [Share (Ice)Berger Helper](/helper-share-iceberger-helper) | HELPER | A Native app that helps Consumers turn Provider data into iceberg tables for external use
| [Zamboni](/framework-zamboni) | FRAMEWORK | Zamboni allows a customer to create a pipeline of Source-to-Target transformations with little to no ELT engineering required.|
| [Enable Query Acceleration Service (QAS) for Eligible Warehouses](/helper-enable-qas-eligible-warehouses) | HELPER | A notebook that helps users identify warehouses eligible to enable the query acceleration service (QAS). |
| [Evalanche](/framework-evalanche) | FRAMEWORK | A Streamlit in Snowflake app to orchestrate any Generative AI use case evaluations. |
| [Serverless Task Migration](/helper-serverless-task-migration) | HELPER | An app that helps identify any tasks on an account that would benefit from being serverless. |
| [Prompt Template Runner](/helper-prompt-template-runner) | HELPER | A utility to execute Snowflake Cortex prompts against Snowflake tables/views via configuration file. |