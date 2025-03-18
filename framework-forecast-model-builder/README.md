# Forecast Model Builder

The Forecast Model Builder helps with the process of building an end-to-end Snowflake
Notebook-based forecasting solution for your time series data including:

- Performing Exploratory Data Analysis on your time series data
- Executing feature engineering, advanced modeling, and model registration for your
single or multi-series partitioned data
- Running inferencing against the models stored in the registry

For examples, please review the [Quickstart](https://quickstarts.snowflake.com/guide/building_scalable_time_series_forecasting_models_on_snowflake/index.html#0)
and the [Medium article](https://medium.com/@rachel.blum_83237/c389e108f0be). 

## Architecture

![image](https://github.com/user-attachments/assets/fbcb05dc-d307-4e23-8cd4-d1f8c99dd6c3)

## Installation/Setup

1. Download from GitHub and then import the Forecast_Model_Builder_Deployment.ipynb
notebook to Snowsight.

2. Follow the documented instructions in the Deployment notebook. Here are instructions
for using the zipped file to stage method:
  - Go to the Emerging Solutions Toolbox Github Repository and download a zipped file
  of the repository.Image
  - Go to the Forecast Model Builder Deployment Notebook and run the DEPLOYMENT cell in
  the notebook. This cell will create a stage if it doesn't already exist named
  FORECAST_MODEL_BUILDER.BASE.NOTEBOOK_TEMPLATES.
  - Upload a zipped copy of the Forecast Model Builder Github Repository to that stage. 
  - Re-run the DEPLOYMENT cell
  - You should see the success message of "FORECAST_MODEL_BUILDER fully deployed!"
  - Run the cell PROJECT_DEPLOY.
  - Set your project name. Each project gets own schema and set of notebooks and each
  notebook will be prefixed with the Project Name.
  - Click the Create button and go back to the main Notebooks page.
3. The project name you provide will be the prefix for the notebooks and the schema
name that are created in this deployment
4. The solution including three notebooks (eda, modeling and inference) will be created
within your new named schema (<YOUR_PROJECT_NAME>).

## Support Notice

All sample code is provided for reference purposes only. Please note that this code is
provided “AS IS” and without warranty.  Snowflake will not offer any support for use of
the sample code.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

Please see TAGGING.md for details on object comments.
