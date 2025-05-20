## Voice of the Customer

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

Voice of the Customer is a Framework created by Snowflake’s Solution Innovation Team
(SIT). This solution leverages Snowflake’s Cortex Functions, including LLMs and AI SQL,
to handle the summarization, categorization, translation, and sentiment analysis of large
text objects such as call transcripts, chat histories, and feedback data. The process
provides a comprehensive and detailed view of customer interactions, enabling better
insights and decision-making.

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is
provided `as is` and without warranty. Snowflake will not offer any support for the use
of the sample code. The purpose of the code is to provide customers with easy access to
innovative ideas that have been built to accelerate customers' adoption of key
Snowflake features. We certainly look for customers' feedback on these solutions and
will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

## Setup 

### Streamlit in Snowflake

Using [Snowflake CLI](https://docs.snowflake.com/developer-guide/snowflake-cli/index),
you can run the following command to install the Streamlit app in your Snowflake account.

```sh
cd framework-voice-of-the-customer
cat install.sql put.sql | snow sql -i
```

### OSS Streamlit

If you would like to run the Streamlit app locally, you can use the following command
to install the required packages.

```sh
cd framework-voice-of-the-customer
pip install -r requirements.txt
```

Then, you can run the Streamlit app using the following command with a default
connection available in a [connections.toml](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file)
file.

```sh
streamlit run app_main.py
```

## Tagging

Please see `TAGGING.md` for details on object comments.
