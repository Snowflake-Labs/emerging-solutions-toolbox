# EVALANCHE: The GenAI Evaluation Application
Created by Jason Summer, *Solution Innovation Architect - AI/ML*

All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2024 Snowflake Inc. All Rights Reserved.

Please see TAGGING.md for details on object comments.

## Table of Contents

* [Table of Contents](#table-of-contents)
* [Overview](#overview)
* [How it  Works](#how-it-works)
    + [Metrics](#metrics)
    + [Data Sources](#data-sources)
    + [Evaluation](#evaluation)
* [Setup](#setup)
    + [Snowflake CLI](#snowflake-cli)
    + [VSCode with Snowflake Extension](#vscode-with-snowflake-extension)
* [Running](#release)
* [Advanced](#advanced)
    + [Custom Metrics](#custom-metrics)
    + [Crafting a LLM Pipeline Stored Procedure](#crafting-a-llm-pipeline-stored-procedure)

# Overview
Evalanche is a Streamlit in Snowflake (SiS) application that provides a single location to evaluate and compare generative AI use case outputs in a streamlined, on demand, and automated fashion. Regardless if your goal is to measure the quality of RAG-based LLM solutions or accuracy of SQL generation, Evalanche provides a scalable, customizable, and trackable way to do it.

# How it Works
Evalanche's primary structure is based on 2 components: 1) Metrics and 2) Data Sources. Together, Metrics and Data Sources can be combined to make an Evaluation.

## Metrics
Metrics are primarily built using LLM-as-a-Judge mechanisms. This means that we rely on one LLM (referred to here as a "Scorer") to use its judgement in scoring the output of another LLM. In this context, we frame specific prompts to assess our generative AI responses based on specific criteria, such as conciseness or relevancy.

In addition to a prompt, a given Metric will have some required inputs. For example, let's say we are building a Fact Checker of sorts. In assessing the factual accuracy of an LLM output for a particular use case we probably have some reference content against which we should assess the generated response. Only then can we know what's factually accurate for a user's given question, right? Those are required inputs - the reference content and the user's question.

We continue to add metrics and welcome new ones! Please also see [Custom Metrics](#custom-metrics) module below if you'd like to add your own.

## Data Sources
Metrics are run against Data Sources. More specifically, we need 1+ Snowflake tables that have the required inputs for the desired Metric(s). Continuing with our Fact Checker example from the above, a congruent Data Source would be a Snowflake table that has columns for the user question, reference content, generated response. If you are comparing multiple LLM pipelines, which is oftentimes the case, you may store your questions and reference content in one table and your corresponding generated responses in another table. In other words, your expected results (or "ground truth" or "reference set") and actual results are separated. The app allows you to seamlessly bring these together.

In some instances, you may not even have your generated responses yet but you do have reference set. You can run the reference set inputs through your LLM pipeline(s) from the app. Before doing so, encapsulate each LLM pipeline in a stored procedure.

## Evaluation
Once you've selected Metric(s) and configured your Data Source for the Metric(s), you're ready to run your evaluation. A basic evaluation will show the Metric result(s). You may also save the evaluation, which will record the Metric(s) and Data Source and be operationalized as a Stored Procedure. A saved evaluation is ready as an on demand evaluation with a button click from the app homepage. Alternatively, if may also automate the evaluation. Automated evaluation will monitor your Data Source and run the Metric(s) against any new rows that arise and record the results in a Snowflake table for monitoring.

# Setup
Evalanche is deployed to Streamlit in Snowflake and can be done so using multiple methods. First obtain the source code for Evalanche by either downloading this repo or cloning the repository locally. We list a few options below for deployment. Use the one based on your desired tool.

## Snowflake CLI
See [Snowflake CLI installation documentation](https://docs.snowflake.com/developer-guide/snowflake-cli/index) for instructions. Once installed, configure your connection parameters OR pass them via command flags. Run the below command in terminal from the project root to deploy the application.
```bash
snow sql -f setup.sql
```

## VSCode with Snowflake Extension
See [Snowflake Extension for Visual Studio Code installation documentation](https://docs.snowflake.com/en/user-guide/vscode-ext) for instructions. Once installed, sign into Snowflake in the extension. Execute all of `setup.sql` from VSCode.

# Running
Once Evalanche is deployed to Streamlit in Snowflake, the app is ready for use. Login into Snowsight and open the app named Evalanche: GenAI Evaluation Application. If desired, this can be done directly from terminal with Snowflake CLI command

```bash
snow streamlit get-url EVALUATION_APP --open --database CORTEX_ANALYST_UTILITIES --schema EVALUATION
```

# Advanced
## Custom Metrics
While we continue to add Metrics, you may have a use case that warrants a unique one. Module `src/custom_metrics.py` has been added for this purpose. The module has an example custom Metric with additional instructions.

Every Metric must be a child class of class `Metric` with `name`, `description`, `required` attributes. It must also have an `evaluate` method that calculates the actual metric result per record. Please feel free to use the sample one as a template and create your own with new prompts added to `src/prompts.py`.

Please note that you must take two actions before custom metrics are available:
1) Add the class name to custom_metrics at the bottom of `src/custom_metrics.py`.
2) Re-compress `src/` as `src.zip` and upload to Snowflake stage STREAMLIT_STAGE as noted in `setup.sql`.

> **Note:** The terminal zip command does not create the required file for step #2 above. Instead, please compress via Mac Finder or Windows Explore user interface.
Alternatively, for Mac only, you may use the following command in the project root: ```ditto -c -k --sequesterRsrc --keepParent src src.zip```. For Windows machine, you may use the following command in the project root: ```Compress-Archive -Path .\src\* -DestinationPath .\src.zip -Update```.

## Crafting a LLM Pipeline Stored Procedure
To run a reference dataset through your desired LLM pipelines on the data page, we must first encapsulated the pipeline logic in a Stored Procedure. To take advantage of this feature, the stored procedure must have a single VARIANT input type and return a single value. When we execute the stored procedure, a single row from the reference dataset will be passed in the form of a Python dictionary. In other words, a row in the reference dataset that looks like:
```markdown
| TASK        | PERSONA |
|-----------------------|
| Tell a joke | Pirate  |
```
will be passed to the stored procedure as:
```python
{
    "QUESTION": "What is the capital of France?",
    "QUESTION_TYPE": "Geography"
}
```
A appropriately crafted stored procedure could look like the below.
```sql
CREATE OR REPLACE PROCEDURE MY_PIPELINE(INPUT VARIANT)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python', 'snowflake-ml-python')
  HANDLER = 'run'
AS
$$
def run(session, INPUT):
    from snowflake.cortex import Complete

    prompt = f"""You will receive a task and a persona.
    Respond to the task speaking like the persona.
    Task: {INPUT['TASK']}
    Persona: {INPUT['PERSONA']}"""
    return Complete(session = session,
                    model = 'llama3.1-8b',
                    prompt = prompt)
$$;
```
