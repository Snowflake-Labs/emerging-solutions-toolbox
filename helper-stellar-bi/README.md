# Stellar BI

Stellar BI parses Power BI files (`.pbit` and `.bim`), creates Snowflake semantic views from the parsed model, enabling AI applications and cross-platform analytics. It translates DAX and Power Query M expressions to SQL and helps identify optimal DirectQuery strategies.

**Status**: Covers common DAX and M functions used in real Power BI models. Stellar BI supports 46 DAX functions and 33 Power Query M functions. Shows unsupported patterns after parsing your files. See [limitations.md](limitations.md) for details.

**Important**: Stellar BI creates Snowflake semantic views for AI applications and other BI tools. Power BI itself cannot currently consume Snowflake semantic views - that requires Microsoft to add support.

All sample code is provided for reference purposes only. Please note that this code is provided "AS IS" and without warranty. Snowflake will not offer any support for use of the sample code.

Copyright (c) 2025 Snowflake Inc. All Rights Reserved.

## ✨ Why Use Stellar BI?

* **Enable AI**: Generate [semantic views](https://docs.snowflake.com/en/user-guide/views-semantic/overview) for Cortex Analyst and other AI applications
* **Cost Savings**: Replace fixed Power BI Premium costs with pay-per-use Snowflake compute
* **DirectQuery Decisions**: See table sizes to help decide DirectQuery vs. import per partition
* **Centralized Definitions**: Store business logic in Snowflake instead of individual BI files.

## To Use Stellar BI - End Users

### 1. Preparing Power BI Files
Stellar BI supports:
- **`.pbit`** - Power BI Template files
- **`.bim`** - Tabular model metadata files (Analysis Services)

If you have a `.pbix` file, convert it to `.pbit`:
1. Open your `.pbix` file in Power BI Desktop
2. Go to **File** → **Export** → **Power BI template**
3. Save as `.pbit` file (you can leave the description blank).
4. Use the `.pbit` file with Stellar BI.

Alternatively, you can use the test pbit file [test1.pbit](tests/data/test1.pbit).

### 2. Run Stellar BI Streamlit App
There are two methods to run Stellar BI:
#### Method A: Run Stellar BI in Snowhouse
You can run Stellar BI in Snowhouse [here](https://app.snowflake.com/sfcogsops/snowhouse_aws_us_west_2/#/streamlit-apps/APPLIED_FIELD_ENGINEERING.UI.STELLAR_BI).

#### Method B: Install Stellar BI in your Snowflake Account
The following notebook installers are available to end users:

- **`bi.ipynb`**: For users with SYSADMIN role (creates new database and objects)
- **`bi_no_sysadmin.ipynb`**: For users without SYSADMIN role (uses existing database, user must have CREATE SCHEMA / STAGE / TABLE / STREAMLIT privilege)

##### Steps
1. Download the notebook installers from the repo.

2. Import the notebook into your Snowflake account:
   **- Snowsight > Projects > Notebooks > Import `.ipynb` file**.

3. Follow the configuration instructions in the notebook header.

4. Run the notebook to install Stellar BI.
This will create the Stellar BI SiS App (STELLAR_BI is the default name) in your Snowflake account.

The installer will also create database, schemas, stages, roles, and warehouses to be used by Stellar BI.

5. Run STELLAR_BI Streamlit App.


## Supported Features and Limitations
See [limitations.md](limitations.md) for detailed information on supported functions and current limitations.

## Walkthrough Video
[Stellar BI Walkthrough Video](https://drive.google.com/file/d/14JZU3TSpNSpJH4KaSLqFDfmgThJIkEBW/view)

## For Developers
**[README_dev.md](README_dev.md)** - Please refer to this Readme for development and debugging purposes.

## Roadmap
Stellar BI is graduating to SE Colleges, PS Guilds and Partners.

## License
This project is licensed under the Apache 2.0 License - see [LICENSE](LICENSE) file for details.

