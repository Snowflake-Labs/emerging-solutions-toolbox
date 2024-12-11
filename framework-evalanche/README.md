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
* [Extras](#extras)
    + [Custom Metrics](#custom-metrics)
    + [Generating Results to Evaluate](#generating-results-to-evaluate)

# Overview
Evalanche is a Streamlit in Snowflake (SiS) application that provides a single location to evaluate and compare generative AI use case outputs in a streamlined, on demand, and automated fashion. Regardless if your goal is to measure the quality of RAG-based LLM solutions or accuracy of SQL generation, Evalanche provides a scalable, customizable, and trackable way to do it.

> **Note:** Snowflake provides a few tools/frameworks for conducting LLM evaluations. 
This solution, Evalanche, serves as a generalized application to make it easy to create and automate LLM use case evaluations. 
Most LLM use cases can be evaluated with Evalanche through out of the box metrics or custom metrics. 
Alternatively, Snowflake's AI Observability (Private Preview) is powered by open source [TruLens](https://www.trulens.org/), which provides extensible evaluations and tracing for LLM apps including RAGs and LLM agents. 
Lastly, [Cortex Search Evaluation and Tuning Studio](https://github.com/Snowflake-Labs/cortex-search/tree/main/examples/streamlit-evaluation) (Private Preview) offers systematic evaluation and search quality improvements for specific search-based use cases.
Please contact your account representative to learn more about any of these other offerings. 

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

## Snowsight Worksheet
Copy and paste the contents of `setup/git_setup.sql` into a Snowsight SQL Worksheet. Ensure your context role is appropriate as this will be the owning role of the Streamlit app. Execute the entire SQL Worksheet.

## Snowflake CLI
See [Snowflake CLI installation documentation](https://docs.snowflake.com/developer-guide/snowflake-cli/index) for instructions. Once installed, configure your connection parameters OR pass them via command flags. Run the below command in terminal from the project root to deploy the application.
```bash
snow sql -f setup/cli_setup.sql
```

## VSCode with Snowflake Extension
See [Snowflake Extension for Visual Studio Code installation documentation](https://docs.snowflake.com/en/user-guide/vscode-ext) for instructions. Once installed, sign into Snowflake in the extension. Execute all of `setup/git_setup.sql` from VSCode.

# Running
Once Evalanche is deployed to Streamlit in Snowflake, the app is ready for use. Login into Snowsight and open the app named Evalanche: GenAI Evaluation Application. If desired, this can be done directly from terminal with Snowflake CLI command

```bash
snow streamlit get-url EVALUATION_APP --open --database GENAI_UTILITIES --schema EVALUATION
```

# Extras
## Custom Metrics
While we continue to add Metrics, you may very well have a use case that warrants a unique one. This is expected. New simple prompt-based metrics can be added from the homepage's Add Metric button. 

To craft a new metric, give it a name, description and prompt. Examples are shown in the app. When crafting a prompt, variables should be enclosed with curly brackets (`{}`). Ideal metrics prompt the LLM to return a result of an integer-based scale, such as 1-5 corresponding to a [Likert scale](https://en.wikipedia.org/wiki/Likert_scale). True/False responses should be crafted as 1/0 responses, respectively. These variables will be populated by the values in the user-selected data sources. Here's an example of a basic prompt:

```text
Please act as an impartial judge and evaluate the quality of the response provided by the AI Assistant to the user question displayed below.
Your evaluation should consider RELEVANCY. You will be given a user question and the AI Assistant's answer.
Your job is to rate the assistant's response from 1 to 5, where 5 indicates you strongly agree that the response is
RELEVANT
and 1 indicates you strongly disagree.
Avoid any position biases and ensure that the order in which the content presented does not affect your evaluation.
Be as objective as possible. Output your rating with just the number rating value.
[User Question]
{question}

[The Start of the AI Assistant's Answer]
{ai_response}
[The End of the AI Assistant's Answer]
```

To remove a metric as selectable in the app, deselect it in the Manage Metrics menu. To completely delete a metric, we've added a helper store procedure. As an example, the below can be run from any Snowflake SQL interface to delete a custom metric named, `Rudeness`.
```
CALL GENAI_UTILITIES.EVALUATION.DELETE_METRIC('Rudeness');
```

Lastly, please be aware that Streamlit in Snowflake now supports multiple python versions. Custom metrics may only be available with consistent Python versions. For example, if you create a custom metric while running the app with Python version 3.11, the custom metric will only be available in subsequent sessions when running Python 3.11. 

## Generating Results to Evaluate
Evalanche primarily assumes you've saved LLM outputs to table(s) in Snowflake for us to evaluate. That may not be the case. Evalanche supports two ways to generate outputs using either a custom LLM pipeline or a Cortex Analyst runner. Both options are available from the data page (under "Need to Generate Results?") once you've selected your desired Metric(s).  

### Crafting a Stored Procedure for your Custom LLM Pipeline
To run a reference dataset through your desired LLM pipelines on the data page, we must first encapsulated the pipeline logic in a Stored Procedure. To take advantage of this feature, the stored procedure must have a single VARIANT input type and return a single value. When we execute the stored procedure, a single row from the reference dataset will be passed in the form of a Python dictionary. In other words, a row in the reference dataset that looks like:
```markdown
| TASK        | PERSONA |
|-----------------------|
| Tell a joke | Pirate  |
```
will be passed to the stored procedure as:
```python
{
    "TASK": "Tell a joke",
    "PERSONA": "Pirate"
}
```
An appropriately crafted stored procedure could look like the below.
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

### Using the Cortex Analyst Runner
To run a gold or reference set of questions through Cortex Analyst, select the target semantic model and the table containing the reference questions. The SQL results will be written to a table for further evaluation with the Cortex Analyst-suggested metric. 

## Model Options and Selection
Out of the box Metrics have defaulted LLMs. These defaults are selected based on balancing availability, performance, and cost. However, depending on your region, the defaulted LLM may not be available. If that is the case, please select an alternative LLM. See LLM availability [here](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions?utm_cta=website-homepage-live-demo#availability).

# Feedback
Please add issues to GitHub or email Jason Summer (jason.summer@snowflake.com).