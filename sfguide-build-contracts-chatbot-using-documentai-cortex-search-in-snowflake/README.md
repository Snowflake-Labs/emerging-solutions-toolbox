# Chat with Contract PDFs using Snowflake

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

This solution analyzes contracts using a combination of Document AI, Cortex Complete, and Cortex Search. It allows the user to extract specific information in a tabular format from a document and then ask natural language questions about the set of documents, facilitating a better understanding of each document. The tabular data is leveraged as filters on the Cortex Search service to reduce the number of rows the service needs to look across to return the most relevant information. The Snowflake Cortex Complete function contextualizes the responses to the questions asked against the search service.

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is provided `as is` and without warranty. Snowflake will not offer any support for the use of the sample code. The purpose of the code is to provide customers with easy access to innovative ideas that have been built to accelerate customers' adoption of key Snowflake features. We certainly look for customers' feedback on these solutions and will be updating features, fixing bugs, and releasing new solutions on a regular basis.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

## Pre-requisites

To run the tutorial, you will first need to download necessary documents for testing. You will use a sample dataset of the Federal Open Market Committee (FOMC) meeting minutes for this tutorial. This is a sample of twelve 10-page documents with meeting notes from FOMC meetings from 2023 and 2024. Download the files directly from your browser by following this [link](https://drive.google.com/file/d/1C6TdVjy6d-GnasGO6ZrIEVJQRcedDQxG/view).  You will need to put these files into a folder called data inside your folder where you cloned the repo.

## Setup

To create the Streamlit in Snowflake application, execute the following commands
with Snowflake CLI:

```sh
python3 ./sfguide_build_contracts_chatbot_using_documentai_cortex_search_in_snowflake/pdf_chatbot_demo.py
cd app
snow streamlit deploy --replace --database "CORTEX_SEARCH_TUTORIAL_DB" --schema "PUBLIC"
```

#### Publicly available sample data

We found [the Atticus project](https://www.atticusprojectai.org/cuad) to be a great source of free data and Atticus project already has example questions you can use for the Document AI model.

#### Build the Document AI Model (If needed)

If you want to leverage specific extractions to serve as filters in the document chatbot you can do so with a document AI model.  Below are the steps to do so.
1. Create the Document AI Project
![image](https://github.com/user-attachments/assets/0fffdfef-96c2-49c5-a0f7-4f1368239bce)

2. Add Documents
![image](https://github.com/user-attachments/assets/55b75fc2-08d0-4c2d-b247-192f422eaae0)

3. Add values and verify answers
![image](https://github.com/user-attachments/assets/8b6afe58-8235-46fe-8192-12434d19d847)

4. If you like the model accuracy you can publish the model otherwise you can fine tune the model 
![image](https://github.com/user-attachments/assets/06ce5bee-eb3f-4933-932e-581e80ffde04)

## Tagging

Please see `TAGGING.md` for details on object comments.
