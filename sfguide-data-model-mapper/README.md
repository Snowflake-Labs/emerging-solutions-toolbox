# Data Model Mapper
<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

The Data Model Mapper native app helps providers get conformed data from partners at scale, from any cloud/region.  It provides a Streamlit UI that enables business users to model, map, and share their data to a provider's data specification, all without SQL and without needing to be a data engineer.

## Overview
There is now an included administration streamlit application that faciliates defining target entities and attributes the consumer application would map to/towards. You may choose to install the admin portion, or you may skip and just install the consumer application with already populated demo data. For example new users, it may be easier to install the admin application once you are familiar with the application, or are closer to implementing data model mapper into production.

In the provided data_model_mapper notebook, we have constructed a single-account, end-to-end example using a supply chain use case.  Imagine a product distributor needing to manage inventory at a number of stores.  If every store has their own data model, that can present a significant data engineering challenge to create and manage all of the pipelines.  With the Data Model Mapper solves this by helping each store's business users model, map, and share their data with the provider, with the data matching the provider's specification.


The consumer gets an easy tool to help get their data shared, and the provider gets data in the same format from all of their consumers.

1. The Provider defines Target Collections/Entities/Attributes (logical sets, tables, and columns) to act as definitions for the consumer to use to model/map their data.
2. The Provider lists the app and shares with consumers
3. The Consumer uses the app's Streamlit UI to model and map their data to the provider's target specification, all without SQL
4. The Consumer shares the data back to the provider in the prescribed shape
5. The Provider adds the share and incorporates into their pipelines

## More information
There is a blog posting  [here](https://medium.com/snowflake/data-model-mapper-a-snowflake-native-app-for-data-collaboration-at-scale-️-a641f14f5699) that provides even more explanation and detail about the app!

## Deployment Directions

#### Consumer application with populated demo data
1. Import the data_model_mapper.ipynb notebook into your Snowflake environment
2. Hit Run All
3. Open the DATA_MODEL_MAPPER_APP from the Apps window in Snowsight and follow directions

#### Target Administration Streamlit application
1. Import data_model_mapper_admin_installer into your Snowflake environment
2. Hit Run All
3. Open the DATA_MODEL_MAPPER_ADMIN streamlit application from streamlit window in Snowsight and follow directions

## Reference Architecture
![Reference Architecture](reference-architecture.png)

## Support Notice
All sample code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

Please see TAGGING.md for details on object comments.