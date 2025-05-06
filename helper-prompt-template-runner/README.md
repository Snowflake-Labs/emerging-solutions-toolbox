# Cortex Prompt Template Runner

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

The Prompt Template Runner enables Snowflake users to create and manage Cortex Complete calls against tables/views using a simple configuration file.
The Prompt Template Runner takes inspiration from LangChain [prompt templates](https://python.langchain.com/docs/concepts/prompt_templates/) and [YAML prompts](https://www.restack.io/docs/langchain-knowledge-langchain-yaml-prompt-guide), but is purpose-built to be operationalized against Snowflake table records.
Here, the key difference is that prompt variables may be imputed by literal constants (similar to Langchain) OR column values from the underlying table.

The helper provides 2 utilities to operationalize templated prompts against Snowflake tables:

1) **PROMPT_TEMPLATE_PARSER**: User Defined Table Function (UDTF) to create a multi-message prompt from every record in a table, replacing variables with literal and/or table values.
2) **PROMPT_TEMPLATE_RUNNER**: Stored Procedure to run a prompt template through the PROMPT_TEMPLATE_PARSER and prompt an LLM. This stored procedure is the primary utility.

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is
provided `as is` and without warranty. Snowflake will not offer any support for the use
of the sample code. The purpose of the code is to provide customers with easy access to
innovative ideas that have been built to accelerate customers' adoption of key
Snowflake features. We certainly look for customers' feedback on these solutions and
will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

# Specifications
The configuration file currently supports the below elements in the top-level `prompt` key.

| Element | Description |
|---------|-------------|
| `name` | Name of the prompt template. |
| `version` | Version of the prompt template. |
| `messages` | List of objects containing role and content keys to serve as multi-message prompt to Cortex. |
| `literal_variables` | Objects mapping variable and **constant** replacement value in prompt. |
| `column_variables` | Objects mapping variable and **column** replacement value in prompt. |
| `origin_table` | Table against which Cortex Complete will be called. |
| `model` | Cortex Complete [model string](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex#arguments). |
| `model_options` | Object specifiying zero or more [model hyperparameters](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex#arguments). |

Here is an example of a YAML prompt configuration file:
```yaml
prompt:
  name: "Review Sentiment Checker"
  version: "1.0"
  messages:
    - role : "system"
      content: |
        You are a helpful marketing assistant.
        You will be given a movie review and sentiment
        and must determine if the sentiment is accurate.
        Your responses should be {format}.
    - role : "user"
      content: |
        Here is the review: {review}.
        Here is the sentiment: {sentiment}.
  literal_variables:
    format: "concise"
  column_variables:
    review: "REVIEW"
    sentiment: "SENTIMENT"
  origin_table: 'JSUMMER.SAMPLE_DATA.MOVIES_LIMITED'
  model: 'llama3.2-3b'
  model_options:
    max_tokens: 100
    temperature: 0.5
```

## Messages
Messages contains the prompt or history of messages. The argument must be an array of objects representing a conversation in chronological order. Each object must contain a `role` key and a `content` key. The `content` value is a prompt or a response, depending on the role. The role must be one of the following: `system`, `user`, `assistant`. See the [Cortex Complete documentation](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex) for more information.

If passing only a single message, set `role` as 'user'.

## Variables
Variables are any parts of the prompts/messages that should be replaced for every record in the table.
These variables can be replaced (e.g. imputed) by a constant, meaning every table record will feature the same value in the prompt. Alternatively, a variable can be replaced (e.g. imputed) by a column value in the corresponding record.

> **Important:** Variables must be enclosed in brackets in the prompts/messages.

For example, you may have a table with columns `PRODUCT_NAME` and `PRODUCT_DESCRIPTION` and want to create a slogan for each one based on a consistent theme. Your prompt could be something like the below.

```yaml
...
  messages:
    - role : "user"
      content: |
        Create a slogan for this product: {product_name}
        Here is the description:
        {description}
        The slogan should include themes from {theme}.
```

And your variables section could like the below.
```yaml
...
  literal_variables:
    theme: "baseball"
  column_variables:
    product_name: "PRODUCT_NAME"
    description: "PRODUCT_DESCRIPTION"
```

# Setup as Stored Procedure
The utilities (e.g. the UDTF and Stored Procedures) can be registered directly by using Snowflake's git integration. Below are a couple options to do so.

## Snowsight Worksheet
Copy and paste the contents of `register.sql` into a Snowsight SQL Worksheet. Ensure your context role is appropriate as this will be the owning role of the corresponding objects. Execute the entire SQL Worksheet.

## VSCode with Snowflake Extension
See [Snowflake Extension for Visual Studio Code installation documentation](https://docs.snowflake.com/en/user-guide/vscode-ext) for instructions. Once installed, sign into Snowflake in the extension. Execute all of `register.sql` from VSCode. Ensure your context role is appropriate as this will be the owning role of the corresponding objects.

# Running
The configuration file is passed as a parameter when invoking the utilities.
The file should be in Snowflake Stage.

## PROMPT_TEMPLATE_RUNNER

The PROMPT_TEMPLATE_RUNNER is a Stored Procedure that can be executed standalone with all necessary components driven from a configuration file.
The below arguments can be passed explicitly to the utility in addition to the configuration file.
Arguments passed explicitly will be prioritized over those in the configuration file.
- `name`
- `version`
- `messages`
- `literal_variables`
- `column_variables`
- `origin_table`
- `model`
- `model_options`

### Examples

Calling the stored procedure relying on the configuration file for all arguments.
```sql
CALL GENAI_UTILITIES.UTILITIES.PROMPT_TEMPLATE_RUNNER('@JSUMMER.PUBLIC.DROPBOX/prompt_template.yaml');
```

Calling the stored procedure and passing explicit arguments to override those in the configuration file.
```sql
CALL GENAI_UTILITIES.UTILITIES.PROMPT_TEMPLATE_RUNNER(
    prompt_template_file => '@JSUMMER.PUBLIC.DROPBOX/prompt_template.yaml',
    origin_table => 'JSUMMER.SAMPLE_DATA.MOVIES_LIMITED',
    model => 'llama3.2-1b',
    model_options => {'temperature': 0.1,
                      'max_tokens': 90}
    );
```

## PROMPT_TEMPLATE_PARSER

The PROMPT_TEMPLATE_PARSER is a UDTF and should be called against a Snowflake table. Its intended to create a prompt for each table record and is used as part of the PROMPT_TEMPLATE_RUNNER stored prodedure. Here, we expose it as its own utility as well.
A configuration file in Snowflake stage can be passed to the UDTF OR an explicit object containing the same arguments can be passed.

### Examples

Calling PROMPT_TEMPLATE_PARSER using a configuration file in stage.
```sql
WITH CTE
AS (
    SELECT
        *,
        OBJECT_CONSTRUCT(*) AS ROW_DICT, -- Necessary to pass row values to prompt
        '@JSUMMER.PUBLIC.DROPBOX/prompt_template.yaml' AS CONFIG_FILE
    FROM JSUMMER.SAMPLE_DATA.MOVIES_LIMITED -- Sample table
)
SELECT
    * EXCLUDE (ROW_DICT, CONFIG_FILE) -- Don't need to include these extra columns
FROM CTE
, TABLE(GENAI_UTILITIES.UTILITIES.PROMPT_TEMPLATE_PARSER(
        row_data => ROW_DICT,
        prompt_template_file => CONFIG_FILE,
        include_metadata => TRUE -- Set to FALSE to just get messages array back without metadata
        ))
```

```python
import snowflake.snowpark.functions as F

origin_table = 'JSUMMER.SAMPLE_DATA.MOVIES_LIMITED'
prompt_column = 'PROMPT'

# Must first create object construct column as separate request to avoid selection error
df = session.table(origin_table).with_column('ROW_DATA', F.object_construct('*'))

# Calling the UDTF using call_table_function from snowpark.functions
df = df.with_column(prompt_column, F.call_table_function(
        'GENAI_UTILITIES.UTILITIES.PROMPT_TEMPLATE_PARSER',
        F.col('ROW_DATA'),
        '@JSUMMER.PUBLIC.DROPBOX/prompt_template.yaml',
        F.parse_json(F.lit(None)), # Can omit if using a configuration file
        F.lit(True) # Can omit and will default to False
        )).drop('ROW_DATA')
df.show()
```

Calling PROMPT_TEMPLATE_PARSER without a configuration file and instead passing the parameters in-line.
```python
import snowflake.snowpark.functions as F

config = {
    "name": "Review Sentiment Checker",
    "version": "1.0",
    "messages": [
        {
            "role": "system",
            "content": "You are a helpful marketing assistant. "
                       "You will be given a movie review and sentiment "
                       "and must determine if the sentiment is accurate. "
                       "Your responses should be {format}."
        },
        {
            "role": "user",
            "content": "Here is the review: {review}. "
                       "Here is the sentiment: {sentiment}."
        }
    ],
    "literal_variables": {
        "format": "concise"
    },
    "column_variables": {
        "review": "REVIEW",
        "sentiment": "SENTIMENT"
    },
}

origin_table = 'JSUMMER.SAMPLE_DATA.MOVIES_LIMITED'
prompt_column = 'PROMPT'

# Must first create object construct column as separate request to avoid selection error
df = session.table(origin_table).with_column('ROW_DATA', F.object_construct('*'))

# Calling the UDTF using call_table_function from snowpark.functions
df = df.with_column(prompt_column, F.call_table_function(
        'GENAI_UTILITIES.UTILITIES.PROMPT_TEMPLATE_PARSER',
        F.col('ROW_DATA'),
        F.lit(''),
        F.to_variant(F.lit(config)),
        F.lit(False))).drop('ROW_DATA')
df.show()
```

# Metadata Tracking
The below metadata elements from the prompt template (or explicitly passed) are added to the Cortex Complete response object:
- name
- version
- model_options

# Feedback
Please add issues to GitHub or email Jason Summer (jason.summer@snowflake.com).
